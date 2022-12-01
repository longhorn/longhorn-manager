package controller

import (
	"fmt"
	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"
	"reflect"
	"time"
)

type VolumeCloneController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds         *datastore.DataStore
	cacheSyncs []cache.InformerSynced
}

func NewVolumeCloneController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string,
) *VolumeCloneController {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)

	vcc := &VolumeCloneController{
		baseController: newBaseController("longhorn-volume-clone", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-volume-clone-controller"}),
	}

	ds.VolumeInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    vcc.enqueueVolume,
		UpdateFunc: func(old, cur interface{}) { vcc.enqueueVolume(cur) },
		DeleteFunc: vcc.enqueueVolume,
	}, 0)
	vcc.cacheSyncs = append(vcc.cacheSyncs, ds.VolumeInformer.HasSynced)

	return vcc
}

func (vcc *VolumeCloneController) enqueueVolume(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	vcc.queue.Add(key)

	vol, ok := obj.(*longhorn.Volume)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}
		vol, ok = deletedState.Obj.(*longhorn.Volume)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	if types.IsDataFromVolume(vol.Spec.DataSource) {
		if srcVolName := types.GetVolumeName(vol.Spec.DataSource); srcVolName != "" {
			// trigger sync for the source volume
			vcc.queue.Add(vcc.namespace + "/" + srcVolName)
		}
	}

}

func (vcc *VolumeCloneController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer vcc.queue.ShutDown()

	vcc.logger.Infof("Start Longhorn volume clone controller")
	defer vcc.logger.Infof("Shutting down Longhorn volume clone controller")

	if !cache.WaitForNamedCacheSync(vcc.name, stopCh, vcc.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(vcc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (vcc *VolumeCloneController) worker() {
	for vcc.processNextWorkItem() {
	}
}

func (vcc *VolumeCloneController) processNextWorkItem() bool {
	key, quit := vcc.queue.Get()
	if quit {
		return false
	}
	defer vcc.queue.Done(key)
	err := vcc.syncHandler(key.(string))
	vcc.handleErr(err, key)
	return true
}

func (vcc *VolumeCloneController) handleErr(err error, key interface{}) {
	if err == nil {
		vcc.queue.Forget(key)
		return
	}

	vcc.logger.WithError(err).Warnf("Error syncing Longhorn volume %v", key)
	vcc.queue.AddRateLimited(key)
	return
}

func (vcc *VolumeCloneController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: failed to sync volume %v", vcc.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != vcc.namespace {
		return nil
	}
	return vcc.reconcile(name)
}

func (vcc *VolumeCloneController) reconcile(volName string) (err error) {
	vol, err := vcc.ds.GetVolume(volName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	if !vcc.isResponsibleFor(vol) {
		return nil
	}

	va, err := vcc.ds.GetLHVolumeAttachment(types.GetLHVolumeAttachmentNameFromVolumeName(volName))
	if err != nil {
		return err
	}
	existingVA := va.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if reflect.DeepEqual(existingVA.Spec, va.Spec) {
			return
		}

		if _, err = vcc.ds.UpdateLHVolumeAttachmet(va); err != nil {
			return
		}
	}()

	expectedAttachments := make(map[string]bool)

	// case 1: this volume is target of a clone
	if isTargetVolumeOfCloning(vol) {
		cloningAttachmentID := longhorn.GetAttachmentID(longhorn.AttacherTypeVolumeCloneController, volName)
		vcc.createOrUpdateAttachment(cloningAttachmentID, vol.Status.OwnerID, va, longhorn.TrueValue)
		expectedAttachments[cloningAttachmentID] = true
	}

	// case 2: this volume is source of a clone
	vols, err := vcc.ds.ListVolumes()
	if err != nil {
		return err
	}
	for _, v := range vols {
		attachmentID := longhorn.GetAttachmentID(longhorn.AttacherTypeVolumeCloneController, v.Name)
		if isTargetVolumeOfCloning(v) && types.GetVolumeName(v.Spec.DataSource) == vol.Name {
			vcc.createOrUpdateAttachment(attachmentID, vol.Status.OwnerID, va, longhorn.AnyValue)
			expectedAttachments[attachmentID] = true
		}
	}

	// Delete unexpected attachments
	for attachmentID, attachment := range va.Spec.Attachments {
		if attachment.Type == longhorn.AttacherTypeVolumeCloneController {
			if _, ok := expectedAttachments[attachmentID]; !ok {
				delete(va.Spec.Attachments, attachmentID)
			}
		}
	}

	return nil
}

func (vcc *VolumeCloneController) createOrUpdateAttachment(attachmentID string, nodeID string, va *longhorn.VolumeAttachment, disableFrontend string) {
	if va.Spec.Attachments == nil {
		va.Spec.Attachments = make(map[string]*longhorn.Attachment)
	}

	attachment, ok := va.Spec.Attachments[attachmentID]
	if !ok {
		// Create new one
		attachment = &longhorn.Attachment{
			ID:     attachmentID,
			Type:   longhorn.AttacherTypeVolumeCloneController,
			NodeID: nodeID,
			Parameters: map[string]string{
				"disableFrontend": disableFrontend,
			},
		}
	}
	if attachment.NodeID != nodeID {
		attachment.NodeID = nodeID
	}
	va.Spec.Attachments[attachment.ID] = attachment
}

func (vcc *VolumeCloneController) isResponsibleFor(vol *longhorn.Volume) bool {
	return vcc.controllerID == vol.Status.OwnerID
}
