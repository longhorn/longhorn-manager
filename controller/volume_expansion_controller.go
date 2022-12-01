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

type VolumeExpansionController struct {
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

func NewVolumeExpansionController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string,
) *VolumeExpansionController {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)

	vec := &VolumeExpansionController{
		baseController: newBaseController("longhorn-volume-expansion", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-volume-expansion-controller"}),
	}

	ds.VolumeInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    vec.enqueueVolume,
		UpdateFunc: func(old, cur interface{}) { vec.enqueueVolume(cur) },
		DeleteFunc: vec.enqueueVolume,
	}, 0)
	vec.cacheSyncs = append(vec.cacheSyncs, ds.VolumeInformer.HasSynced)

	return vec
}

func (vec *VolumeExpansionController) enqueueVolume(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	vec.queue.Add(key)
}

func (vec *VolumeExpansionController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer vec.queue.ShutDown()

	vec.logger.Infof("Start Longhorn expansion controller")
	defer vec.logger.Infof("Shutting down Longhorn expansion controller")

	if !cache.WaitForNamedCacheSync(vec.name, stopCh, vec.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(vec.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (vec *VolumeExpansionController) worker() {
	for vec.processNextWorkItem() {
	}
}

func (vec *VolumeExpansionController) processNextWorkItem() bool {
	key, quit := vec.queue.Get()
	if quit {
		return false
	}
	defer vec.queue.Done(key)
	err := vec.syncHandler(key.(string))
	vec.handleErr(err, key)
	return true
}

func (vec *VolumeExpansionController) handleErr(err error, key interface{}) {
	if err == nil {
		vec.queue.Forget(key)
		return
	}

	vec.logger.WithError(err).Warnf("Error syncing Longhorn volume %v", key)
	vec.queue.AddRateLimited(key)
	return
}

func (vec *VolumeExpansionController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: failed to sync volume %v", vec.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != vec.namespace {
		return nil
	}
	return vec.reconcile(name)
}

func (vec *VolumeExpansionController) reconcile(volName string) (err error) {
	vol, err := vec.ds.GetVolume(volName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	if !vec.isResponsibleFor(vol) {
		return nil
	}

	vaName := types.GetLHVolumeAttachmentNameFromVolumeName(volName)
	va, err := vec.ds.GetLHVolumeAttachment(vaName)
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

		if _, err = vec.ds.UpdateLHVolumeAttachmet(va); err != nil {
			return
		}
	}()

	expandingAttachmentID := longhorn.GetAttachmentID(longhorn.AttacherTypeVolumeExpansionController, volName)

	if vol.Status.ExpansionRequired {
		if va.Spec.Attachments == nil {
			va.Spec.Attachments = make(map[string]*longhorn.Attachment)
		}
		expandingAttachment, ok := va.Spec.Attachments[expandingAttachmentID]
		if !ok {
			//create new one
			expandingAttachment = &longhorn.Attachment{
				ID:     expandingAttachmentID,
				Type:   longhorn.AttacherTypeVolumeExpansionController,
				NodeID: vol.Status.OwnerID,
				Parameters: map[string]string{
					"disableFrontend": "true",
				},
			}
		}
		if expandingAttachment.NodeID != vol.Status.OwnerID {
			expandingAttachment.NodeID = vol.Status.OwnerID
		}
		va.Spec.Attachments[expandingAttachment.ID] = expandingAttachment
	} else {
		delete(va.Spec.Attachments, expandingAttachmentID)
	}

	return nil
}

func (vec *VolumeExpansionController) isResponsibleFor(vol *longhorn.Volume) bool {
	return vec.controllerID == vol.Status.OwnerID
}
