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

type VolumeRestoreController struct {
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

func NewVolumeRestoreController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string,
) *VolumeRestoreController {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)

	vrsc := &VolumeRestoreController{
		baseController: newBaseController("longhorn-volume-restore", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-volume-restore-controller"}),
	}

	ds.VolumeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    vrsc.enqueueVolume,
		UpdateFunc: func(old, cur interface{}) { vrsc.enqueueVolume(cur) },
		DeleteFunc: vrsc.enqueueVolume,
	})
	vrsc.cacheSyncs = append(vrsc.cacheSyncs, ds.VolumeInformer.HasSynced)

	return vrsc
}

func (vrsc *VolumeRestoreController) enqueueVolume(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	vrsc.queue.Add(key)
}

func (vrsc *VolumeRestoreController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer vrsc.queue.ShutDown()

	vrsc.logger.Infof("Start Longhorn restore controller")
	defer vrsc.logger.Infof("Shutting down Longhorn restore controller")

	if !cache.WaitForNamedCacheSync(vrsc.name, stopCh, vrsc.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(vrsc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (vrsc *VolumeRestoreController) worker() {
	for vrsc.processNextWorkItem() {
	}
}

func (vrsc *VolumeRestoreController) processNextWorkItem() bool {
	key, quit := vrsc.queue.Get()
	if quit {
		return false
	}
	defer vrsc.queue.Done(key)
	err := vrsc.syncHandler(key.(string))
	vrsc.handleErr(err, key)
	return true
}

func (vrsc *VolumeRestoreController) handleErr(err error, key interface{}) {
	if err == nil {
		vrsc.queue.Forget(key)
		return
	}

	vrsc.logger.WithError(err).Warnf("Error syncing Longhorn volume %v", key)
	vrsc.queue.AddRateLimited(key)
	return
}

func (vrsc *VolumeRestoreController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: failed to sync volume %v", vrsc.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != vrsc.namespace {
		return nil
	}
	return vrsc.reconcile(name)
}

func (vrsc *VolumeRestoreController) reconcile(volName string) (err error) {
	vol, err := vrsc.ds.GetVolume(volName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	if !vrsc.isResponsibleFor(vol) {
		return nil
	}

	vaName := types.GetLHVolumeAttachmentNameFromVolumeName(volName)
	va, err := vrsc.ds.GetLHVolumeAttachment(vaName)
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

		if _, err = vrsc.ds.UpdateLHVolumeAttachmet(va); err != nil {
			return
		}
	}()

	restoringAttachmentID := longhorn.GetAttachmentID(longhorn.AttacherTypeVolumeRestoreController, volName)

	if vol.Status.RestoreRequired {
		if va.Spec.Attachments == nil {
			va.Spec.Attachments = make(map[string]*longhorn.Attachment)
		}
		restoringAttachment, ok := va.Spec.Attachments[restoringAttachmentID]
		if !ok {
			//create new one
			restoringAttachment = &longhorn.Attachment{
				ID:     restoringAttachmentID,
				Type:   longhorn.AttacherTypeVolumeRestoreController,
				NodeID: vol.Status.OwnerID,
				Parameters: map[string]string{
					"disableFrontend": "true",
				},
			}
		}
		if restoringAttachment.NodeID != vol.Status.OwnerID {
			restoringAttachment.NodeID = vol.Status.OwnerID
		}
		va.Spec.Attachments[restoringAttachment.ID] = restoringAttachment
	} else {
		delete(va.Spec.Attachments, restoringAttachmentID)
	}

	return nil
}

func (vrsc *VolumeRestoreController) isResponsibleFor(vol *longhorn.Volume) bool {
	return vrsc.controllerID == vol.Status.OwnerID
}
