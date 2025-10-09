package controller

import (
	"fmt"
	"reflect"
	"time"

	"github.com/cockroachdb/errors"
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
	"github.com/longhorn/longhorn-manager/scheduler"
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

	// replica scheduler to check if there is a schedulable disk
	scheduler *scheduler.ReplicaScheduler

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
	vbc.scheduler = scheduler.NewReplicaScheduler(ds)

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

	if _, err = ds.ReplicaInformer.AddEventHandlerWithResyncPeriod(cache.FilteringResourceEventHandler{
		FilterFunc: isReplicaOwnerChanged,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    vbc.enqueueVolumeForReplica,
			UpdateFunc: func(old, cur interface{}) { vbc.enqueueVolumeForReplica(cur) },
		},
	}, 0); err != nil {
		return nil, err
	}
	vbc.cacheSyncs = append(vbc.cacheSyncs, ds.ReplicaInformer.HasSynced)

	if _, err = ds.NodeInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		UpdateFunc: vbc.enqueueVolumeNodeReady,
	}, 0); err != nil {
		return nil, err
	}
	vbc.cacheSyncs = append(vbc.cacheSyncs, ds.NodeInformer.HasSynced)

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
		return false
	}
	return setting.Name == string(types.SettingNameOfflineReplicaRebuilding)
}

func (vbc *VolumeRebuildingController) enqueueSetting(obj interface{}) {
	_, ok := obj.(*longhorn.Setting)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
		return
	}

	vs, err := vbc.ds.ListVolumesRO()
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to list volumes: %v", err))
		return
	}

	for _, v := range vs {
		if v.Spec.OfflineRebuilding == longhorn.VolumeOfflineRebuildingDisabled {
			continue
		}
		vbc.enqueueVolume(v)
	}
}

func isReplicaOwnerChanged(obj interface{}) bool {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		return false
	}
	if !r.DeletionTimestamp.IsZero() {
		return true
	}
	if r.Spec.NodeID == "" || r.Status.OwnerID == "" {
		return false
	}

	return r.Spec.NodeID != r.Status.OwnerID
}

func (vbc *VolumeRebuildingController) enqueueVolumeForReplica(obj interface{}) {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
		return
	}

	v, err := vbc.ds.GetVolumeRO(r.Spec.VolumeName)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get volume: %v", err))
		return
	}
	if v.Status.State != longhorn.VolumeStateDetached {
		return
	}

	vbc.enqueueVolume(v)
}

func (vbc *VolumeRebuildingController) enqueueVolumeNodeReady(old, cur interface{}) {
	oldNode, ok := old.(*longhorn.Node)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("received unexpected old object: %#v", old))
		return
	}
	curNode, ok := cur.(*longhorn.Node)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("received unexpected current object: %#v", cur))
		return
	}

	if curNode == nil || curNode.Status.Conditions == nil || curNode.Status.DiskStatus == nil {
		return
	}

	if !hasNodeBecomeSchedulable(oldNode, curNode) && !hasDiskBecomeSchedulable(oldNode, curNode) && !hasDiskStatusBecomeSchedulable(oldNode, curNode) {
		return
	}

	// When a node gets ready, enqueue degraded volumes
	vs, err := vbc.ds.ListVolumesRO()
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to list volumes: %w", err))
		return
	}
	for _, v := range vs {
		if v.Spec.OfflineRebuilding == longhorn.VolumeOfflineRebuildingDisabled {
			continue
		}
		if v.Status.State != longhorn.VolumeStateDetached {
			continue
		}

		vbc.enqueueVolume(v)
	}
}

func hasNodeBecomeSchedulable(oldNode, curNode *longhorn.Node) bool {
	oldReadyStatus := types.GetCondition(oldNode.Status.Conditions, longhorn.NodeConditionTypeReady).Status
	curReadyStatus := types.GetCondition(curNode.Status.Conditions, longhorn.NodeConditionTypeReady).Status
	oldSchedulableStatus := types.GetCondition(oldNode.Status.Conditions, longhorn.NodeConditionTypeSchedulable).Status
	curSchedulableStatus := types.GetCondition(curNode.Status.Conditions, longhorn.NodeConditionTypeSchedulable).Status

	readyChanged := oldReadyStatus != curReadyStatus && curReadyStatus == longhorn.ConditionStatusTrue
	schedulableChanged := oldSchedulableStatus != curSchedulableStatus && curSchedulableStatus == longhorn.ConditionStatusTrue
	schedulingChanged := oldNode.Spec.AllowScheduling != curNode.Spec.AllowScheduling && curNode.Spec.AllowScheduling
	evictedChanged := oldNode.Spec.EvictionRequested != curNode.Spec.EvictionRequested && curNode.Spec.EvictionRequested

	return readyChanged || schedulableChanged || schedulingChanged || evictedChanged
}

func hasDiskBecomeSchedulable(oldNode, curNode *longhorn.Node) bool {
	for diskName, diskSpec := range curNode.Spec.Disks {
		if !diskSpec.AllowScheduling || diskSpec.EvictionRequested {
			continue
		}
		oldDiskSpec, exists := oldNode.Spec.Disks[diskName]
		if !exists || !oldDiskSpec.AllowScheduling || oldDiskSpec.EvictionRequested {
			return true
		}
	}

	return false
}

func hasDiskStatusBecomeSchedulable(oldNode, curNode *longhorn.Node) bool {
	for diskName, diskStatus := range curNode.Status.DiskStatus {
		if !isDiskSchedulable(diskStatus) {
			continue
		}
		if _, exists := oldNode.Status.DiskStatus[diskName]; !exists {
			return true
		}
		if !isDiskSchedulable(oldNode.Status.DiskStatus[diskName]) {
			return true
		}
	}

	return false
}

func isDiskSchedulable(diskStatus *longhorn.DiskStatus) bool {
	ready := types.GetCondition(diskStatus.Conditions, longhorn.DiskConditionTypeReady).Status == longhorn.ConditionStatusTrue
	schedulable := types.GetCondition(diskStatus.Conditions, longhorn.DiskConditionTypeSchedulable).Status == longhorn.ConditionStatusTrue

	return ready && schedulable
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
	vol, err := vbc.ds.GetVolumeRO(volName)
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

	rebuildingAttachmentTicketID := longhorn.GetAttachmentTicketID(longhorn.AttacherTypeVolumeRebuildingController, volName)
	deleteVATicketRequired := true
	defer func() {
		if deleteVATicketRequired {
			delete(va.Spec.AttachmentTickets, rebuildingAttachmentTicketID)
		}
	}()

	isOfflineRebuildEnabled, err := vbc.isVolumeOfflineRebuildEnabled(vol)
	if err != nil {
		return err
	}
	if !isOfflineRebuildEnabled {
		return nil
	}

	if !vol.DeletionTimestamp.IsZero() {
		vbc.logger.Infof("Volume %v is deleting, skip offline rebuilding", volName)
		return nil
	}

	if vol.Status.Robustness == longhorn.VolumeRobustnessFaulted {
		vbc.logger.Warnf("Volume %v is faulted, skip offline rebuilding", volName)
		return nil
	}

	if util.IsHigherPriorityVATicketExisting(va, longhorn.AttacherTypeVolumeRebuildingController) {
		return nil
	}

	if vol.Status.State == longhorn.VolumeStateDetached {
		_, err = vbc.syncLHVolumeAttachmentForOfflineRebuild(vol, va, rebuildingAttachmentTicketID)
		if err != nil {
			return err
		}
		deleteVATicketRequired = false
		return nil
	}
	if vol.Status.State == longhorn.VolumeStateAttaching {
		deleteVATicketRequired = false
		return nil
	}

	engine, err := vbc.getVolumeEngine(vol)
	if err != nil {
		return err
	}
	if engine == nil {
		vbc.logger.Warnf("Volume %v engine not found, skip offline rebuilding", volName)
		return nil
	}

	if engine.Status.ReplicaModeMap == nil {
		// wait for engine status synced
		deleteVATicketRequired = false
		return nil
	}
	if vbc.isVolumeReplicasRebuilding(vol, engine) {
		deleteVATicketRequired = types.GetCondition(vol.Status.Conditions, longhorn.VolumeConditionTypeScheduled).Status == longhorn.ConditionStatusFalse
		return nil
	}

	return nil
}

func (vbc *VolumeRebuildingController) isVolumeOfflineRebuildEnabled(vol *longhorn.Volume) (bool, error) {
	if vol.Spec.OfflineRebuilding == longhorn.VolumeOfflineRebuildingEnabled {
		return true, nil
	}

	globalOfflineRebuildingEnabled, err := vbc.ds.GetSettingAsBoolByDataEngine(types.SettingNameOfflineReplicaRebuilding, vol.Spec.DataEngine)
	if err != nil {
		return false, err
	}
	return globalOfflineRebuildingEnabled && vol.Spec.OfflineRebuilding != longhorn.VolumeOfflineRebuildingDisabled, nil
}

func (vbc *VolumeRebuildingController) syncLHVolumeAttachmentForOfflineRebuild(vol *longhorn.Volume, va *longhorn.VolumeAttachment, attachmentID string) (*longhorn.VolumeAttachment, error) {
	replicas, err := vbc.ds.ListVolumeReplicasRO(vol.Name)
	if err != nil {
		return va, err
	}

	if !vbc.isVolumeReplicasHealthy(vol.Spec.NumberOfReplicas, replicas) {
		if va.Spec.AttachmentTickets == nil {
			va.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{}
		}
		replicaReusableOrSchedulable, err := vbc.canReuseOrScheduleReplicas(vol, replicas)
		if err != nil {
			return va, err
		}
		if !replicaReusableOrSchedulable {
			return va, nil
		}
		createOrUpdateAttachmentTicket(va, attachmentID, vol.Status.OwnerID, longhorn.AnyValue, longhorn.AttacherTypeVolumeRebuildingController)
	}
	return va, nil
}

func (vbc *VolumeRebuildingController) isVolumeReplicasHealthy(numberOfReplicas int, replicas map[string]*longhorn.Replica) bool {
	if len(replicas) < numberOfReplicas {
		return false
	}

	healthyCount := 0
	for _, replica := range replicas {
		if replica.Spec.NodeID != replica.Status.OwnerID {
			continue
		}
		if replica.Spec.FailedAt != "" {
			continue
		}
		// the volume might be newly created and not attached yet.
		if replica.Spec.HealthyAt != "" || (replica.Spec.LastFailedAt == "" && replica.Spec.LastHealthyAt == "") {
			healthyCount++
		}
	}
	return healthyCount >= numberOfReplicas
}

func (vbc *VolumeRebuildingController) canReuseOrScheduleReplicas(vol *longhorn.Volume, rs map[string]*longhorn.Replica) (bool, error) {
	reusableFailedReplica, err := vbc.scheduler.CheckAndReuseFailedReplica(rs, vol, "")
	if err != nil {
		return false, err
	}
	if reusableFailedReplica != nil {
		vbc.logger.Infof("Reusing failed replica %v for volume %v", reusableFailedReplica.Name, vol.Name)
		return true, nil
	}

	nodes, err := vbc.scheduler.ListSchedulableNodes(vol.Spec.DataEngine)
	if err != nil {
		return false, err
	}
	// After a node reboot or uncordoned, it might be listed in the nodeInfo but its instance manager pod is not ready.
	// Return an error if nodes are scheduled but their pods are not ready to requeue the volume.
	for nodeName := range nodes {
		if isReady, err := vbc.ds.CheckInstanceManagersReadiness(vol.Spec.DataEngine, nodeName); !isReady {
			if err != nil {
				return false, err
			}
			return false, fmt.Errorf("instance manager on node %s is not ready", nodeName)
		}
	}

	return vbc.hasReplicaDiskCandidates(vol, rs)
}

func (vbc *VolumeRebuildingController) hasReplicaDiskCandidates(vol *longhorn.Volume, rs map[string]*longhorn.Replica) (bool, error) {
	engine, err := vbc.getVolumeEngine(vol)
	if err != nil {
		return false, err
	}
	if engine == nil {
		vbc.logger.Warnf("Volume %v engine not found, skip offline rebuilding", vol.Name)
		return false, nil
	}
	newReplica := newReplicaCR(vol, engine, "")
	replicaDiskCandidates, multiError := vbc.scheduler.FindDiskCandidates(newReplica, rs, vol)
	if reason := multiError.JoinReasons(); reason != "" {
		return false, fmt.Errorf("failed to get replica disk candidates for volume %v: %v", vol.Name, reason)
	}

	return len(replicaDiskCandidates) > 0, nil
}

func (vbc *VolumeRebuildingController) getVolumeEngine(vol *longhorn.Volume) (*longhorn.Engine, error) {
	engines, err := vbc.ds.ListVolumeEngines(vol.Name)
	if err != nil {
		return nil, err
	}

	return vbc.ds.PickVolumeCurrentEngine(vol, engines)
}

func (vbc *VolumeRebuildingController) isVolumeReplicasRebuilding(vol *longhorn.Volume, engine *longhorn.Engine) bool {
	healthyCount := 0
	for _, mode := range engine.Status.ReplicaModeMap {
		if mode == longhorn.ReplicaModeWO {
			return true
		}
		if mode == longhorn.ReplicaModeRW {
			healthyCount++
		}
	}

	return healthyCount < vol.Spec.NumberOfReplicas
}

func (vbc *VolumeRebuildingController) isResponsibleFor(vol *longhorn.Volume) bool {
	return vbc.controllerID == vol.Status.OwnerID
}
