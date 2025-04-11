package controller

import (
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type VolumeRebuildingController struct {
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

func NewVolumeRebuildingController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string,
) (*VolumeRebuildingController, error) {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)

	vbc := &VolumeRebuildingController{
		baseController: newBaseController("longhorn-volume-rebuilding", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-volume-rebuilding-controller"}),
	}

	var err error
	if _, err = ds.VolumeInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    vbc.enqueueVolume,
		UpdateFunc: func(old, cur interface{}) { vbc.enqueueVolume(cur) },
		DeleteFunc: vbc.enqueueVolume,
	}, 0); err != nil {
		return nil, err
	}
	vbc.cacheSyncs = append(vbc.cacheSyncs, ds.VolumeInformer.HasSynced)

	if _, err = ds.SettingInformer.AddEventHandlerWithResyncPeriod(cache.FilteringResourceEventHandler{
		FilterFunc: isSettingOfflineReplicaRebuilding,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    vbc.enqueueSetting,
			UpdateFunc: func(old, cur interface{}) { vbc.enqueueSetting(cur) },
		},
	}, 0); err != nil {
		return nil, err
	}
	vbc.cacheSyncs = append(vbc.cacheSyncs, ds.SettingInformer.HasSynced)

	return vbc, nil
}

func (vbc *VolumeRebuildingController) enqueueVolume(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object %#v: %v", obj, err))
		return
	}

	vbc.queue.Add(key)
}

func (vbc *VolumeRebuildingController) enqueueVolumeAfter(obj interface{}, duration time.Duration) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("enqueueVolumeAfter: failed to get key for object %#v: %v", obj, err))
		return
	}

	vbc.queue.AddAfter(key, duration)
}

func isSettingOfflineReplicaRebuilding(obj interface{}) bool {
	setting, ok := obj.(*longhorn.Setting)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return false
		}

		// use the last known state, to enqueue, dependent objects
		setting, ok = deletedState.Obj.(*longhorn.Setting)
		if !ok {
			return false
		}
	}

	return setting.Name == string(types.SettingNameOfflineReplicaRebuilding)
}

func (vbc *VolumeRebuildingController) enqueueSetting(obj interface{}) {
	_, ok := obj.(*longhorn.Setting)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to requeue the claimed volumes
		_, ok = deletedState.Obj.(*longhorn.Setting)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained non Setting object: %#v", deletedState.Obj))
			return
		}
	}

	vs, err := vbc.ds.ListVolumesRO()
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to list volumes: %v", err))
		return
	}

	for _, v := range vs {
		if v.Spec.OfflineRebuild != longhorn.VolumeOfflineRebuildIgnored {
			continue
		}
		vbc.enqueueVolume(v)
	}
}

func (vbc *VolumeRebuildingController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer vbc.queue.ShutDown()

	vbc.logger.Infof("Start Longhorn rebuilding controller")
	defer vbc.logger.Infof("Shutting down Longhorn rebuilding controller")

	if !cache.WaitForNamedCacheSync(vbc.name, stopCh, vbc.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(vbc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (vbc *VolumeRebuildingController) worker() {
	for vbc.processNextWorkItem() {
	}
}

func (vbc *VolumeRebuildingController) processNextWorkItem() bool {
	key, quit := vbc.queue.Get()
	if quit {
		return false
	}
	defer vbc.queue.Done(key)
	err := vbc.syncHandler(key.(string))
	vbc.handleErr(err, key)
	return true
}

func (vbc *VolumeRebuildingController) handleErr(err error, key interface{}) {
	if err == nil {
		vbc.queue.Forget(key)
		return
	}

	log := vbc.logger.WithField("Volume", key)
	handleReconcileErrorLogging(log, err, "Failed to sync Longhorn volume")
	vbc.queue.AddRateLimited(key)
}

func (vbc *VolumeRebuildingController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: failed to sync volume %v", vbc.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != vbc.namespace {
		return nil
	}
	return vbc.reconcile(name)
}

func (vbc *VolumeRebuildingController) reconcile(volName string) (err error) {
	vol, err := vbc.ds.GetVolume(volName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	if !vbc.isResponsibleFor(vol) {
		return nil
	}

	va, err := vbc.ds.GetLHVolumeAttachmentByVolumeName(volName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		vbc.enqueueVolumeAfter(vol, constant.LonghornVolumeAttachmentNotFoundRetryPeriod)
		return nil
	}
	existingVA := va.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if reflect.DeepEqual(existingVA.Spec, va.Spec) {
			return
		}

		if _, err = vbc.ds.UpdateLHVolumeAttachment(va); err != nil {
			return
		}
	}()
	existingVol := vol.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if reflect.DeepEqual(existingVol.Spec, vol.Spec) {
			return
		}

		if _, err = vbc.ds.UpdateVolume(vol); err != nil {
			return
		}
	}()

	rebuildingAttachmentTicketID := longhorn.GetAttachmentTicketID(longhorn.AttacherTypeVolumeRebuildingController, volName)
	csiTicketExists := isVATicketExists(va, longhorn.AttacherTypeCSIAttacher)
	isOfflineRebuildEnabled, err := vbc.isVolumeOfflineRebuildEnabled(vol.Spec.OfflineRebuild)
	if err != nil {
		return err
	}

	if isOfflineRebuildEnabled && vol.Status.Robustness != longhorn.VolumeRobustnessFaulted && !csiTicketExists {
		if vol.Status.State == longhorn.VolumeStateDetached {
			va, err = vbc.syncLHVolumeAttachementForOfflineRebuild(vol, va, rebuildingAttachmentTicketID)
			if err != nil {
				return err
			}
			return nil
		}
		if vol.Status.State == longhorn.VolumeStateAttaching {
			return nil
		}

		isVolumeInRebuilding, err := vbc.isVolumeReplicasRebuilding(vol)
		if err != nil {
			return err
		}
		if isVolumeInRebuilding {
			return nil
		}
	}

	if isOfflineRebuildEnabled && vol.Status.Robustness == longhorn.VolumeRobustnessFaulted {
		warnMsg := fmt.Sprintf("Volume %v is faulted, skip rebuilding", volName)
		vbc.logger.Info(warnMsg)
		vbc.eventRecorder.Event(vol, corev1.EventTypeWarning, constant.EventReasonCanceledOfflineRebuild, warnMsg)
	}

	if !isOfflineRebuildEnabled && isVATicketExists(va, longhorn.AttacherTypeVolumeRebuildingController) {
		warnMsg := fmt.Sprintf("Canceling volume %v offline rebuilding", volName)
		vbc.logger.Info(warnMsg)
		vbc.eventRecorder.Event(vol, corev1.EventTypeWarning, constant.EventReasonCanceledOfflineRebuild, warnMsg)
	}

	delete(va.Spec.AttachmentTickets, rebuildingAttachmentTicketID)
	vol.Spec.DisableFrontend = false
	return nil
}

func (vbc *VolumeRebuildingController) isVolumeOfflineRebuildEnabled(offlineRebuild longhorn.VolumeOfflineRebuild) (bool, error) {
	offlineReplicaRebuildingValue, err := vbc.ds.GetSettingAsBool(types.SettingNameOfflineReplicaRebuilding)
	if err != nil {
		return false, err
	}
	return (offlineRebuild == longhorn.VolumeOfflineRebuildEnabled) || (offlineRebuild == longhorn.VolumeOfflineRebuildIgnored && offlineReplicaRebuildingValue), nil
}

func (vbc *VolumeRebuildingController) syncLHVolumeAttachementForOfflineRebuild(vol *longhorn.Volume, va *longhorn.VolumeAttachment, attachmentID string) (*longhorn.VolumeAttachment, error) {
	replicas, err := vbc.ds.ListVolumeReplicasRO(vol.Name)
	if err != nil {
		return va, err
	}
	if !util.IsVolumeReplicasHealthy(vol.Spec.NumberOfReplicas, replicas) {
		createOrUpdateAttachmentTicket(va, attachmentID, vol.Status.OwnerID, longhorn.TrueValue, longhorn.AttacherTypeVolumeRebuildingController)
		vol.Spec.DisableFrontend = true
	}
	return va, nil
}

func (vbc *VolumeRebuildingController) isVolumeReplicasRebuilding(vol *longhorn.Volume) (bool, error) {
	engines, err := vbc.ds.ListVolumeEngines(vol.Name)
	if err != nil {
		return false, err
	}
	engine, err := vbc.ds.PickVolumeCurrentEngine(vol, engines)
	if err != nil {
		return false, err
	}
	if engine == nil || engine.Status.ReplicaModeMap == nil {
		return true, nil
	}

	healthyCount := 0
	for _, mode := range engine.Status.ReplicaModeMap {
		if mode == longhorn.ReplicaModeRW {
			healthyCount++
		}
	}

	if healthyCount < vol.Spec.NumberOfReplicas {
		return true, nil
	}

	return false, nil
}

func isVATicketExists(va *longhorn.VolumeAttachment, ticketType longhorn.AttacherType) bool {
	if va != nil && len(va.Spec.AttachmentTickets) > 0 {
		for _, ticket := range va.Spec.AttachmentTickets {
			if ticket.Type == ticketType {
				return true
			}
		}
	}

	return false
}

func (vbc *VolumeRebuildingController) isResponsibleFor(vol *longhorn.Volume) bool {
	return vbc.controllerID == vol.Status.OwnerID
}
