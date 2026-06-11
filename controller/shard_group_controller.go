package controller

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	spdkrpc "github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/scheduler"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// spdkRPCTimeout bounds each SPDK service call issued during ShardGroup reconciliation.
const spdkRPCTimeout = 30 * time.Second

type ShardGroupController struct {
	*baseController

	namespace    string
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds        *datastore.DataStore
	scheduler *scheduler.ShardScheduler

	// rebuildingLock guards inProgressRebuildingMap so the per-node rebuild limit is
	// checked and reserved atomically across the controller's parallel workers.
	rebuildingLock sync.Mutex
	// inProgressRebuildingMap maps a ShardGroup name to the time a rebuild was reserved
	// for it. A reservation bridges the lag between issuing the rebuild RPC and the
	// durable RebuildInProgress flag becoming visible; it is dropped once the flag
	// appears or after shardRebuildReservationTTL.
	inProgressRebuildingMap map[string]time.Time

	cacheSyncs []cache.InformerSynced
}

// sgReconcileCtx carries pre-fetched state for one ShardGroup reconcile cycle,
// eliminating redundant datastore lookups and SPDK connections across sub-functions.
type sgReconcileCtx struct {
	shardGroup *longhorn.ShardGroup
	shards     map[string]*longhorn.Shard // re-fetched after scheduling so the loop sees fresh placements
	engine     *longhorn.Engine           // nil when detached or engine not yet started
	spdkClient spdkrpc.SPDKServiceClient  // nil when engine is nil or SPDK unreachable
	log        *logrus.Entry
}

func NewShardGroupController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string,
) (*ShardGroupController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	c := &ShardGroupController{
		baseController: newBaseController("longhorn-shard-group", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds:        ds,
		scheduler: scheduler.NewShardScheduler(ds),

		inProgressRebuildingMap: map[string]time.Time{},

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-shard-group-controller"}),
	}

	var err error
	if _, err = ds.ShardGroupInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueShardGroup,
		UpdateFunc: c.enqueueShardGroupOnUpdate,
		DeleteFunc: c.enqueueShardGroup,
	}); err != nil {
		return nil, err
	}
	c.cacheSyncs = append(c.cacheSyncs, ds.ShardGroupInformer.HasSynced)

	// Re-enqueue owning ShardGroup when a Shard changes.
	if _, err = ds.ShardInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(cur interface{}) { c.enqueueShardGroupForShard(cur) },
		UpdateFunc: func(old, cur interface{}) { c.enqueueShardGroupForShard(cur) },
		DeleteFunc: func(cur interface{}) { c.enqueueShardGroupForShard(cur) },
	}); err != nil {
		return nil, err
	}
	c.cacheSyncs = append(c.cacheSyncs, ds.ShardInformer.HasSynced)

	// Re-enqueue the ShardGroup when its engine changes (state, health).
	if _, err = ds.EngineInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(cur interface{}) { c.enqueueShardGroupForEngine(cur) },
		UpdateFunc: func(old, cur interface{}) { c.enqueueShardGroupForEngine(cur) },
		DeleteFunc: func(cur interface{}) { c.enqueueShardGroupForEngine(cur) },
	}); err != nil {
		return nil, err
	}
	c.cacheSyncs = append(c.cacheSyncs, ds.EngineInformer.HasSynced)

	// Re-enqueue the ShardGroup when its volume changes (resize, attach/detach).
	if _, err = ds.VolumeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(cur interface{}) { c.enqueueShardGroupForVolume(cur) },
		UpdateFunc: func(old, cur interface{}) { c.enqueueShardGroupForVolume(cur) },
		DeleteFunc: func(cur interface{}) { c.enqueueShardGroupForVolume(cur) },
	}); err != nil {
		return nil, err
	}
	c.cacheSyncs = append(c.cacheSyncs, ds.VolumeInformer.HasSynced)

	return c, nil
}

func (c *ShardGroupController) enqueueShardGroup(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object %#v: %v", obj, err))
		return
	}
	c.queue.Add(key)
}

// enqueueShardGroupOnUpdate re-enqueues the changed ShardGroup, and when a rebuild
// just finished on it (RebuildInProgress went true -> false), also wakes the other
// groups on the same engine node so a rebuild the per-node limit deferred can take
// the freed slot without waiting for the next resync.
func (c *ShardGroupController) enqueueShardGroupOnUpdate(old, cur interface{}) {
	c.enqueueShardGroup(cur)

	oldShardGroup, ok := old.(*longhorn.ShardGroup)
	if !ok {
		return
	}
	curShardGroup, ok := cur.(*longhorn.ShardGroup)
	if !ok {
		return
	}
	if oldShardGroup.Status.RebuildInProgress && !curShardGroup.Status.RebuildInProgress {
		c.enqueueRebuildCandidatesOnNode(curShardGroup.Spec.NodeID)
	}
}

// enqueueRebuildCandidatesOnNode wakes every ShardGroup on the given engine node that
// still needs a rebuild (degraded or with a failed shard), so a rebuild deferred by
// the concurrent-rebuild limit starts as soon as a running one frees a slot.
func (c *ShardGroupController) enqueueRebuildCandidatesOnNode(nodeID string) {
	if nodeID == "" {
		return
	}
	shardGroups, err := c.ds.ListShardGroupsRO()
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to list shard groups to wake rebuild candidates on node %v: %v", nodeID, err))
		return
	}
	for _, shardGroup := range shardGroups {
		if shardGroup.Spec.NodeID != nodeID {
			continue
		}
		if shardGroup.Status.State == longhorn.ShardGroupStateDegraded || shardGroup.Status.FailedCount > 0 {
			c.enqueueShardGroup(shardGroup)
		}
	}
}

func (c *ShardGroupController) enqueueShardGroupForShard(obj interface{}) {
	shard, ok := obj.(*longhorn.Shard)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}
		shard, ok = deletedState.Obj.(*longhorn.Shard)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	shardGroupName := shard.Spec.ShardGroupName
	if shardGroupName == "" {
		return
	}

	shardGroup, err := c.ds.GetShardGroupRO(shardGroupName)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("failed to get shard group %v for shard %v: %v", shardGroupName, shard.Name, err))
		}
		return
	}
	c.enqueueShardGroup(shardGroup)
}

func (c *ShardGroupController) enqueueShardGroupForEngine(obj interface{}) {
	engine, ok := obj.(*longhorn.Engine)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}
		engine, ok = deletedState.Obj.(*longhorn.Engine)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	shardGroup, err := c.ds.GetShardGroupRO(engine.Spec.VolumeName)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("failed to get shard group for engine %v: %v", engine.Name, err))
		}
		return
	}
	c.enqueueShardGroup(shardGroup)
}

func (c *ShardGroupController) enqueueShardGroupForVolume(obj interface{}) {
	volume, ok := obj.(*longhorn.Volume)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}
		volume, ok = deletedState.Obj.(*longhorn.Volume)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	shardGroup, err := c.ds.GetShardGroupRO(volume.Name)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("failed to get shard group for volume %v: %v", volume.Name, err))
		}
		return
	}
	c.enqueueShardGroup(shardGroup)
}

func (c *ShardGroupController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info("Starting Longhorn ShardGroup controller")
	defer c.logger.Info("Shut down Longhorn ShardGroup controller")

	if !cache.WaitForNamedCacheSync(c.name, stopCh, c.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (c *ShardGroupController) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *ShardGroupController) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)
	err := c.syncShardGroup(key.(string))
	c.handleErr(err, key)
	return true
}

func (c *ShardGroupController) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	log := c.logger.WithField("shardGroup", key)
	if c.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn ShardGroup")
		c.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn ShardGroup out of the queue")
	c.queue.Forget(key)
}

func (c *ShardGroupController) syncShardGroup(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync shard group %v", key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != c.namespace {
		return nil
	}
	return c.reconcile(name)
}

func (c *ShardGroupController) reconcile(shardGroupName string) (err error) {
	shardGroup, err := c.ds.GetShardGroup(shardGroupName)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return err
		}
		return nil
	}

	if !c.isResponsibleFor(shardGroup) {
		return nil
	}

	if shardGroup.Status.OwnerID != c.controllerID {
		shardGroup.Status.OwnerID = c.controllerID
		shardGroup, err = c.ds.UpdateShardGroupStatus(shardGroup)
		if err != nil {
			if datastore.ErrorIsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		getLoggerForShardGroup(c.logger, shardGroup).Infof("ShardGroup got new owner %v", c.controllerID)
	}

	existingShardGroup := shardGroup.DeepCopy()

	// Resolve the engine CR and open the engine-node SPDK connection; sgReconcileCtx
	// carries them for the rest of this cycle.
	rctx := &sgReconcileCtx{
		shardGroup: shardGroup,
		log:        getLoggerForShardGroup(c.logger, shardGroup),
	}

	var spdkConn *grpc.ClientConn
	if shardGroup.Spec.NodeID != "" {
		engine, engineErr := c.resolveEngine(shardGroup)
		if engineErr != nil {
			return engineErr
		}
		rctx.engine = engine

		if engine != nil {
			client, conn, dialErr := c.dialSPDK(shardGroup.Spec.NodeID)
			if dialErr != nil {
				rctx.log.WithError(dialErr).Warn("Cannot connect to SPDK service; SPDK-dependent operations will be skipped this cycle")
			} else {
				rctx.spdkClient = client
				spdkConn = conn
			}
		}
	}
	defer func() {
		if spdkConn != nil {
			spdkConn.Close() //nolint:errcheck
		}
	}()

	if !shardGroup.DeletionTimestamp.IsZero() {
		defer func() {
			if reflect.DeepEqual(existingShardGroup.Status, shardGroup.Status) {
				return
			}
			if _, updateErr := c.ds.UpdateShardGroupStatus(shardGroup); updateErr != nil {
				rctx.log.WithError(updateErr).Error("Failed to update ShardGroup status during deletion")
				if err == nil {
					err = updateErr
				}
			}
		}()
		shards, listErr := c.ds.ListShardsByShardGroup(shardGroup.Name)
		if listErr != nil {
			return listErr
		}
		for _, shard := range shards {
			if shard.DeletionTimestamp != nil {
				if cleanupErr := c.cleanupShard(rctx, shard); cleanupErr != nil {
					return cleanupErr
				}
			}
		}
		return c.cleanupShardGroup(rctx)
	}

	defer func() {
		if err != nil {
			return
		}
		if reflect.DeepEqual(existingShardGroup.Status, shardGroup.Status) {
			return
		}
		if _, updateErr := c.ds.UpdateShardGroupStatus(shardGroup); updateErr != nil {
			if datastore.ErrorIsConflict(errors.Cause(updateErr)) {
				rctx.log.WithError(updateErr).Debugf("Requeue %v due to conflict", shardGroupName)
				c.enqueueShardGroup(shardGroup)
				return
			}
			err = updateErr
		}
	}()

	// Step 1: ensure Shard CRs match spec.
	if err := c.syncShards(rctx); err != nil {
		return err
	}

	// Step 2: pull the latest EC health state from SPDK into Shard CR statuses
	// before the per-shard loop dispatches on them.
	if err := c.syncECHealth(rctx); err != nil {
		return err
	}

	// Step 3: fetch the shard list (after syncShards + syncECHealth), schedule any
	// unplaced shards, then run the single per-shard reconcile loop.
	shards, err := c.ds.ListShardsByShardGroup(shardGroup.Name)
	if err != nil {
		return err
	}
	rctx.shards = shards

	if err := c.scheduleShards(rctx); err != nil {
		return err
	}

	// Re-fetch after scheduleShards so reconcileShard sees the up-to-date NodeID/DiskUUID/Size.
	shards, err = c.ds.ListShardsByShardGroup(shardGroup.Name)
	if err != nil {
		return err
	}
	rctx.shards = shards

	for _, shard := range shards {
		if err := c.reconcileShard(rctx, shard); err != nil {
			return err
		}
	}

	// Step 4: group-level operations that need a consistent view of all shards.
	if err := c.syncShardRebuildQoS(rctx); err != nil {
		return err
	}

	// Step 5: derive ShardGroup status from the current shard snapshot. Must
	// run before syncProcess: the readiness gate reads Status.ECShardAddressMap.
	if err := c.syncStatus(rctx); err != nil {
		return err
	}

	// Step 6: provision and monitor the long-lived ShardGroup process. Must run
	// after syncStatus, which fills Status.ECShardAddressMap that the readiness
	// gate checks; until the shards are ready this is a no-op.
	return c.syncProcess(rctx)
}

// reconcileShard is the single dispatch table for per-shard work. Cases are evaluated
// in priority order; the first matching case wins.
func (c *ShardGroupController) reconcileShard(rctx *sgReconcileCtx, shard *longhorn.Shard) error {
	if shard.Status.Role == "" {
		fresh, err := c.ds.GetShard(shard.Name)
		if err != nil {
			return err
		}
		if fresh.Status.Role == "" {
			if fresh.Spec.SlotIndex < rctx.shardGroup.Spec.DataChunks {
				fresh.Status.Role = longhorn.ShardRoleData
			} else {
				fresh.Status.Role = longhorn.ShardRoleParity
			}
			if _, err := c.ds.UpdateShardStatus(fresh); err != nil {
				return errors.Wrapf(err, "failed to set role for shard %v", shard.Name)
			}
		}
	}

	switch {
	case shard.DeletionTimestamp != nil:
		return c.cleanupShard(rctx, shard)
	case shard.Spec.NodeID == "":
		// Unscheduled; scheduleShards will assign placement next cycle.
		return nil
	case shard.Status.State == longhorn.ShardStateFailed:
		return c.reconcileFailedShard(rctx, shard)
	case shard.Status.State == longhorn.ShardStateReplacing:
		// ShardGroupShardReplace in flight; syncECHealth advances state.
		return nil
	default:
		// State == "" (fresh CR) or Normal: ensure the shard instance is running.
		return c.syncShardInstance(rctx, shard)
	}
}

// cleanupShard tears down the SPDK instance for a deleted Shard CR and removes its finalizer.
func (c *ShardGroupController) cleanupShard(rctx *sgReconcileCtx, shard *longhorn.Shard) error {
	log := rctx.log.WithField("shard", shard.Name)

	if shard.Spec.NodeID != "" {
		instanceManager, err := c.ds.GetInstanceManagerByInstance(shard)
		if err != nil && !datastore.ErrorIsNotFound(err) {
			return errors.Wrapf(err, "failed to get instance manager for shard %v", shard.Name)
		}
		if instanceManager != nil {
			instanceManagerClient, err := engineapi.NewInstanceManagerClient(instanceManager, false)
			if err != nil {
				return errors.Wrapf(err, "failed to create instance manager client for shard %v", shard.Name)
			}
			deleteErr := instanceManagerClient.InstanceDelete(longhorn.DataEngineTypeV2, shard.Name, "", engineapi.InstanceTypeShard, shard.Spec.DiskUUID, true)
			if closeErr := instanceManagerClient.Close(); closeErr != nil {
				log.WithError(closeErr).Warn("Failed to close instance manager client")
			}
			if deleteErr != nil && !types.ErrorIsNotFound(deleteErr) {
				return errors.Wrapf(deleteErr, "failed to delete instance for shard %v", shard.Name)
			}
			log.Infof("Deleted shard instance on node %v", shard.Spec.NodeID)
		}
	}

	return c.ds.RemoveFinalizerForShard(shard)
}

// scheduleShards assigns NodeID/DiskUUID/Size to any Shard CRs that have not yet
// been placed. Hard anti-affinity is enforced: each shard lands on a distinct node.
func (c *ShardGroupController) scheduleShards(rctx *sgReconcileCtx) error {
	shardGroup := rctx.shardGroup
	log := rctx.log

	volume, err := c.ds.GetVolumeRO(shardGroup.Spec.VolumeName)
	if err != nil {
		return err
	}

	usedNodes := map[string]bool{}
	for _, shard := range rctx.shards {
		if shard.Spec.NodeID != "" {
			usedNodes[shard.Spec.NodeID] = true
		}
	}

	// List schedulable nodes at most once per reconcile: only if a shard needs placing,
	// then reuse that snapshot instead of deep-copying the node set for every shard.
	var nodes map[string]*longhorn.Node
	for _, shard := range rctx.shards {
		if shard.DeletionTimestamp != nil || shard.Spec.NodeID != "" {
			continue
		}

		if nodes == nil {
			nodes, err = c.scheduler.ListSchedulableNodes()
			if err != nil {
				return err
			}
		}

		placement, skipReasons, err := c.scheduler.ScheduleShard(shardGroup, volume, usedNodes, nodes)
		if err != nil {
			return errors.Wrapf(err, "failed to schedule shard %v", shard.Name)
		}
		if placement == nil {
			// ScheduleShard adds a reason for each node/disk it rejects, so an empty
			// set means there were no nodes to consider at all - log that explicitly
			// instead of a blank JoinReasons().
			if len(skipReasons) == 0 {
				log.Warnf("No schedulable node available for shard %v; will retry", shard.Name)
			} else {
				log.Warnf("No schedulable node or disk for shard %v: %v; will retry", shard.Name, skipReasons.JoinReasons())
				log.Debugf("Shard %v could not be placed on any disk: %v", shard.Name, skipReasons.Error())
			}
			continue
		}

		fresh, err := c.ds.GetShard(shard.Name)
		if err != nil {
			return err
		}
		fresh.Spec.NodeID = placement.NodeID
		fresh.Spec.DiskUUID = placement.DiskUUID
		fresh.Spec.DiskPath = placement.DiskPath
		fresh.Spec.Size = placement.Size
		if _, err := c.ds.UpdateShard(fresh); err != nil {
			return errors.Wrapf(err, "failed to update shard %v after scheduling", shard.Name)
		}
		usedNodes[placement.NodeID] = true
	}

	return nil
}

// syncShardInstance creates or refreshes the SPDK shard instance for a scheduled
// Shard CR. Each cycle issues a live InstanceGet so a Stopped or Error instance
// transitions the Shard CR to ShardStateFailed (driving reconcileFailedShard via
// the dispatch switch); a Running instance refreshes Status.StorageIP/Port from
// the authoritative source so any IM-side port re-allocation is picked up.
func (c *ShardGroupController) syncShardInstance(rctx *sgReconcileCtx, shard *longhorn.Shard) error {
	log := rctx.log

	instanceManager, err := c.ds.GetInstanceManagerByInstance(shard)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			log.Debugf("Instance manager for shard %v not found yet; will retry", shard.Name)
			return nil
		}
		return err
	}

	instanceManagerClient, err := engineapi.NewInstanceManagerClient(instanceManager, false)
	if err != nil {
		return errors.Wrapf(err, "failed to create instance manager client for shard %v", shard.Name)
	}
	defer func() {
		if closeErr := instanceManagerClient.Close(); closeErr != nil {
			log.WithError(closeErr).Warn("Failed to close instance manager client")
		}
	}()

	lvsName, lvsUUID, err := c.getDiskLvsInfo(shard)
	if err != nil {
		return errors.Wrapf(err, "failed to get lvstore info for shard %v", shard.Name)
	}

	instance, err := instanceManagerClient.InstanceGet(longhorn.DataEngineTypeV2, shard.Name, engineapi.InstanceTypeShard)
	if err != nil {
		instance, err = instanceManagerClient.ShardInstanceCreate(&engineapi.ShardInstanceCreateRequest{
			Shard:   shard,
			LvsName: lvsName,
			LvsUUID: lvsUUID,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to create instance for shard %v", shard.Name)
		}
	}

	if instance == nil || instance.Status.State != longhorn.InstanceStateRunning {
		return c.markShardInstanceNotRunning(rctx, shard, instance)
	}

	instanceManagerPod, err := c.ds.GetPodRO(instanceManager.Namespace, instanceManager.Name)
	if err != nil {
		return errors.Wrapf(err, "failed to get pod for instance manager %v", instanceManager.Name)
	}
	storageIP := c.ds.GetIPFromPodByCNISetting(instanceManagerPod, types.SettingNameStorageNetwork)
	livePort := int32(instance.Status.PortStart)

	fresh, err := c.ds.GetShard(shard.Name)
	if err != nil {
		return err
	}

	needsUpdate := false
	if fresh.Status.StorageIP != storageIP {
		fresh.Status.StorageIP = storageIP
		needsUpdate = true
	}
	if fresh.Status.Port != livePort {
		fresh.Status.Port = livePort
		needsUpdate = true
	}
	if fresh.Status.State != longhorn.ShardStateNormal && fresh.Status.State != longhorn.ShardStateReplacing {
		// First Running observation, or recovery from a prior Failed state.
		fresh.Status.State = longhorn.ShardStateNormal
		needsUpdate = true
	}
	if !needsUpdate {
		return nil
	}
	updated, err := c.ds.UpdateShardStatus(fresh)
	if err != nil {
		return errors.Wrapf(err, "failed to update shard %v status after instance start", shard.Name)
	}
	// shard is this reconcile's own copy in rctx.shards; write the saved status back
	// so syncStatus builds ECShardAddressMap from the refreshed StorageIP/Port/State
	// this pass instead of a tick later.
	*shard = *updated
	return nil
}

// markShardInstanceNotRunning transitions a Shard CR to ShardStateFailed when its
// SPDK instance is no longer Running. LastFailureTimestamp is set on the first
// transition so the replace delay starts from the actual failure, not
// from a later cycle that re-observes the same dead instance.
func (c *ShardGroupController) markShardInstanceNotRunning(rctx *sgReconcileCtx, shard *longhorn.Shard, instance *longhorn.InstanceProcess) error {
	instanceState := longhorn.InstanceState("")
	if instance != nil {
		instanceState = instance.Status.State
	}

	fresh, err := c.ds.GetShard(shard.Name)
	if err != nil {
		return err
	}

	needsUpdate := false
	if fresh.Status.State != longhorn.ShardStateFailed {
		rctx.log.Warnf("Shard %v instance is not Running (state=%v); marking shard as Failed", shard.Name, instanceState)
		fresh.Status.State = longhorn.ShardStateFailed
		needsUpdate = true
	}
	if fresh.Status.LastFailureTimestamp == "" {
		fresh.Status.LastFailureTimestamp = util.Now()
		needsUpdate = true
	}
	if !needsUpdate {
		return nil
	}
	updated, err := c.ds.UpdateShardStatus(fresh)
	if err != nil {
		return errors.Wrapf(err, "failed to mark shard %v as failed (instance state=%v)", shard.Name, instanceState)
	}
	// shard is this reconcile's own copy in rctx.shards; write the saved status back
	// so syncStatus counts this failure in the derived ShardGroup state this pass
	// instead of a tick later.
	*shard = *updated
	return nil
}

// reconcileFailedShard runs the two-step recovery (replace, then rebuild) for a
// Shard CR whose SPDK slot syncECHealth marked FAILED. The steps are separate so
// each SPDK RPC is issued at most once per failure:
//
//  1. Replace: call ShardGroupShardReplace and set ReplaceTriggered. Skipped on
//     later cycles until syncECHealth clears the flag when the slot leaves Failed.
//  2. Rebuild: call ShardGroupShardRebuildStart once the replace is in flight. If
//     the controller crashes between the two, syncECHealth restarts the rebuild.
func (c *ShardGroupController) reconcileFailedShard(rctx *sgReconcileCtx, shard *longhorn.Shard) error {
	if rctx.engine == nil || rctx.spdkClient == nil {
		return nil
	}
	if shard.Spec.NodeID == "" {
		// No node assigned yet; scheduleShards will place it next cycle.
		return nil
	}
	if shard.Status.StorageIP == "" {
		// No instance yet; provision it first so ShardGroupShardReplace gets a valid address.
		return c.syncShardInstance(rctx, shard)
	}

	if !shard.Status.ReplaceTriggered {
		if c.shouldDelayReplace(rctx, shard) {
			return nil
		}
		if err := c.triggerShardReplace(rctx, shard); err != nil {
			return err
		}
		// ReplaceTriggered is now set on the shard; fall through to attempt rebuild start
		// in the same cycle so a healthy controller path has minimum latency.
	}

	return c.triggerShardRebuild(rctx, shard)
}

// shouldDelayReplace returns true while the replenishment-wait interval has not yet
// elapsed since the shard's last failure, so a transient outage has time to
// self-heal before a replace is triggered.
func (c *ShardGroupController) shouldDelayReplace(rctx *sgReconcileCtx, shard *longhorn.Shard) bool {
	if shard.Status.LastFailureTimestamp == "" {
		return false
	}
	failedAt, err := util.ParseTime(shard.Status.LastFailureTimestamp)
	if err != nil {
		return false
	}
	waitInterval, err := c.ds.GetSettingAsInt(types.SettingNameReplicaReplenishmentWaitInterval)
	if err != nil {
		return false
	}
	if time.Since(failedAt) < time.Duration(waitInterval)*time.Second {
		rctx.log.Debugf("Shard %v failed at %v; deferring replacement until %vs replenishment interval elapses",
			shard.Name, shard.Status.LastFailureTimestamp, waitInterval)
		return true
	}
	return false
}

// triggerShardReplace issues ShardGroupShardReplace for a failed shard and sets
// ReplaceTriggered on success. If the RPC fails, the Shard CR is deleted so
// syncShards re-provisions a fresh replacement next cycle.
//
// The live port is re-read from the IM first, because the IM may have re-allocated
// PortStart since Shard.Status.Port was cached. If the instance is no longer
// Running, the Shard CR is deleted (same re-provision path) rather than replacing
// with a stale address.
func (c *ShardGroupController) triggerShardReplace(rctx *sgReconcileCtx, shard *longhorn.Shard) error {
	log := rctx.log

	livePort, isRunning, err := c.getLiveShardPort(shard)
	if err != nil {
		return errors.Wrapf(err, "failed to verify live shard %v address before replace", shard.Name)
	}
	if !isRunning {
		log.Warnf("Shard %v instance is not Running before replace; deleting CR for fresh re-provisioning", shard.Name)
		if delErr := c.ds.DeleteShard(shard.Name); delErr != nil && !datastore.ErrorIsNotFound(delErr) {
			return errors.Wrapf(delErr, "failed to delete shard %v for re-provisioning", shard.Name)
		}
		return nil
	}

	shardAddress := fmt.Sprintf("%s:%d", shard.Status.StorageIP, livePort)
	log.Infof("Replacing failed shard slot %v with instance at %v", shard.Spec.SlotIndex, shardAddress)

	ctx, cancel := context.WithTimeout(context.Background(), spdkRPCTimeout)
	_, err = rctx.spdkClient.ShardGroupShardReplace(ctx, &spdkrpc.ShardGroupShardReplaceRequest{
		ShardGroupName: rctx.shardGroup.Name,
		ShardName:      shard.Name,
		ShardAddress:   shardAddress,
	})
	cancel()

	if err != nil {
		log.WithError(err).Warnf("ShardGroupShardReplace failed for shard %v; deleting CR for re-provisioning", shard.Name)
		if delErr := c.ds.DeleteShard(shard.Name); delErr != nil && !datastore.ErrorIsNotFound(delErr) {
			return errors.Wrapf(delErr, "failed to delete shard %v for re-provisioning", shard.Name)
		}
		return nil
	}

	fresh, err := c.ds.GetShard(shard.Name)
	if err != nil {
		return err
	}
	fresh.Status.ReplaceTriggered = true
	updated, err := c.ds.UpdateShardStatus(fresh)
	if err != nil {
		return errors.Wrapf(err, "failed to mark shard %v as replace-triggered", shard.Name)
	}
	// shard is this reconcile's own copy in rctx.shards, so it is safe to copy the
	// saved shard into it. Later steps in this pass read rctx.shards and will see
	// ReplaceTriggered set. That makes shard newer than the other shards in the map.
	*shard = *updated
	return nil
}

// getLiveShardPort fetches the shard's live PortStart from its InstanceManager.
// Returns (port, true, nil) when the instance is Running; (0, false, nil) when
// the instance is missing or in any non-Running state; (_, _, err) on transport
// errors that the caller should treat as transient.
func (c *ShardGroupController) getLiveShardPort(shard *longhorn.Shard) (int32, bool, error) {
	instanceManager, err := c.ds.GetInstanceManagerByInstance(shard)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	instanceManagerClient, err := engineapi.NewInstanceManagerClient(instanceManager, false)
	if err != nil {
		return 0, false, err
	}
	defer func() {
		_ = instanceManagerClient.Close()
	}()
	instance, err := instanceManagerClient.InstanceGet(longhorn.DataEngineTypeV2, shard.Name, engineapi.InstanceTypeShard)
	if err != nil || instance == nil || instance.Status.State != longhorn.InstanceStateRunning {
		return 0, false, nil
	}
	return int32(instance.Status.PortStart), true, nil
}

// triggerShardRebuild issues ShardGroupShardRebuildStart once a shard replace is in flight.
// It respects the concurrent-rebuild-per-node limit; if the controller crashes between the
// replace and this call, syncECHealth restarts the rebuild.
func (c *ShardGroupController) triggerShardRebuild(rctx *sgReconcileCtx, shard *longhorn.Shard) error {
	shardGroup := rctx.shardGroup
	log := rctx.log

	// A rebuild is group-level. Once one failed shard this cycle has started it, the
	// remaining failed shards do not need to re-issue the same RPC.
	if shardGroup.Status.RebuildInProgress {
		return nil
	}

	canRebuild, err := c.canStartShardRebuild(shardGroup)
	if err != nil {
		return err
	}
	if !canRebuild {
		log.Debugf("Concurrent rebuild limit reached on engine node %v; deferring rebuild for shard slot %v",
			shardGroup.Spec.NodeID, shard.Spec.SlotIndex)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), spdkRPCTimeout)
	_, err = rctx.spdkClient.ShardGroupShardRebuildStart(ctx, &spdkrpc.ShardGroupShardRebuildStartRequest{
		ShardGroupName: rctx.shardGroup.Name,
	})
	cancel()

	if err != nil {
		log.WithError(err).Warnf("ShardGroupShardRebuildStart failed after replacing shard %v; syncECHealth will restart it", shard.Name)
	} else {
		shardGroup.Status.RebuildInProgress = true
	}
	return nil
}

// cleanupShardGroup deletes all owned Shard CRs and removes the finalizer once done.
func (c *ShardGroupController) cleanupShardGroup(rctx *sgReconcileCtx) error {
	shardGroup := rctx.shardGroup
	log := rctx.log

	// Tear down the long-lived ShardGroup process first with cleanupRequired=true
	// so the SPDK service authorizes bdev_lvol_delete + bdev_lvol_delete_lvstore
	// + bdev_ec_delete (full destruction). Must complete before shard teardown
	// so the ShardGroup process disconnects from the shard NVMe-oF endpoints
	// before the shards' SPDK instances stop.
	if err := c.teardownShardGroupProcess(rctx, true); err != nil {
		return err
	}

	shards, err := c.ds.ListShardsByShardGroup(shardGroup.Name)
	if err != nil {
		return err
	}

	if len(shards) > 0 {
		for _, shard := range shards {
			if shard.DeletionTimestamp.IsZero() {
				if err := c.ds.DeleteShard(shard.Name); err != nil && !datastore.ErrorIsNotFound(err) {
					return err
				}
			}
		}
		return nil
	}

	log.Info("All shards cleaned up, removing ShardGroup finalizer")
	return c.ds.RemoveFinalizerForShardGroup(shardGroup)
}

// syncShards ensures the correct number of Shard CRs exist for this ShardGroup.
func (c *ShardGroupController) syncShards(rctx *sgReconcileCtx) error {
	shardGroup := rctx.shardGroup
	totalSlots := shardGroup.Spec.DataChunks + shardGroup.Spec.ParityChunks
	if totalSlots == 0 {
		return nil
	}

	existing, err := c.ds.ListShardsByShardGroup(shardGroup.Name)
	if err != nil {
		return err
	}

	bySlot := make(map[int]*longhorn.Shard, len(existing))
	for _, shard := range existing {
		bySlot[shard.Spec.SlotIndex] = shard
	}

	for slot := 0; slot < totalSlots; slot++ {
		if _, ok := bySlot[slot]; ok {
			continue
		}

		labels := types.GetShardGroupLabels(shardGroup.Name)
		if shardGroup.Spec.VolumeName != "" {
			for k, v := range types.GetVolumeLabels(shardGroup.Spec.VolumeName) {
				labels[k] = v
			}
		}

		// Status.Role is set later by reconcileShard, not at creation: the Shard
		// status subresource strips Status on Create.
		shard := &longhorn.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-%d", shardGroup.Name, slot),
				Namespace: shardGroup.Namespace,
				Labels:    labels,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: longhorn.SchemeGroupVersion.String(),
						Kind:       types.LonghornKindShardGroup,
						Name:       shardGroup.Name,
						UID:        shardGroup.UID,
					},
				},
			},
			Spec: longhorn.ShardSpec{
				ShardGroupName: shardGroup.Name,
				SlotIndex:      slot,
				Size:           0,
			},
		}

		if _, err := c.ds.CreateShard(shard); err != nil && !apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(err, "failed to create shard for slot %v of shard group %v", slot, shardGroup.Name)
		}
	}

	return nil
}

// syncStatus derives ShardGroup status from the shard snapshot in rctx.
func (c *ShardGroupController) syncStatus(rctx *sgReconcileCtx) error {
	shardGroup := rctx.shardGroup
	totalSlots := shardGroup.Spec.DataChunks + shardGroup.Spec.ParityChunks
	shardRefs := make([]string, totalSlots)
	failedCount := 0
	addrMap := make(map[string]string, totalSlots)

	for _, shard := range rctx.shards {
		slotIndex := shard.Spec.SlotIndex
		if slotIndex >= 0 && slotIndex < totalSlots {
			shardRefs[slotIndex] = shard.Name
		}
		if shard.Status.State == longhorn.ShardStateFailed {
			failedCount++
		}
		// Build the canonical slot-to-address map. Only include shards in ShardStateNormal
		// with non-empty StorageIP/Port - a stale-but-non-empty address from a stopped
		// SPDK process must not pass downstream readiness checks.
		if shard.Status.State != longhorn.ShardStateNormal {
			continue
		}
		if shard.Status.StorageIP == "" || shard.Status.Port == 0 {
			continue
		}
		addrMap[strconv.Itoa(slotIndex)] = fmt.Sprintf("%s:%d", shard.Status.StorageIP, shard.Status.Port)
	}

	shardGroup.Status.ShardRefs = shardRefs
	shardGroup.Status.FailedCount = failedCount
	shardGroup.Status.ECShardAddressMap = addrMap
	shardGroup.Status.State = c.deriveState(shardGroup.Spec.ParityChunks, failedCount)

	// While a rebuild is in progress, report Rebuilding: fault tolerance is reduced
	// until it completes.
	if shardGroup.Status.RebuildInProgress && shardGroup.Status.State != longhorn.ShardGroupStateOffline {
		shardGroup.Status.State = longhorn.ShardGroupStateRebuilding
	}

	// GrowInProgress is only meaningful when the array is fully healthy.
	if shardGroup.Status.GrowInProgress && shardGroup.Status.State == longhorn.ShardGroupStateHealthy {
		shardGroup.Status.State = longhorn.ShardGroupStateGrowing
	}

	return nil
}

func (c *ShardGroupController) deriveState(parityChunks, failedCount int) longhorn.ShardGroupState {
	switch {
	case failedCount == 0:
		return longhorn.ShardGroupStateHealthy
	case failedCount <= parityChunks:
		return longhorn.ShardGroupStateDegraded
	default:
		return longhorn.ShardGroupStateOffline
	}
}

// getDiskLvsInfo returns the SPDK lvstore name and UUID for the disk hosting the shard.
func (c *ShardGroupController) getDiskLvsInfo(shard *longhorn.Shard) (lvsName, lvsUUID string, err error) {
	node, err := c.ds.GetNodeRO(shard.Spec.NodeID)
	if err != nil {
		return "", "", err
	}
	for _, diskStatus := range node.Status.DiskStatus {
		if diskStatus.DiskUUID == shard.Spec.DiskUUID {
			return diskStatus.DiskName, diskStatus.DiskUUID, nil
		}
	}
	return "", "", fmt.Errorf("cannot find disk for shard %v (diskUUID %v)", shard.Name, shard.Spec.DiskUUID)
}

// resolveEngine returns the active Engine CR for the ShardGroup's volume on the engine
// node, or nil if none is found.
func (c *ShardGroupController) resolveEngine(shardGroup *longhorn.ShardGroup) (*longhorn.Engine, error) {
	engines, err := c.ds.ListEnginesByNodeRO(shardGroup.Spec.NodeID)
	if err != nil {
		return nil, err
	}
	for _, engine := range engines {
		if engine.Spec.VolumeName == shardGroup.Spec.VolumeName && engine.DeletionTimestamp == nil {
			return engine, nil
		}
	}
	return nil, nil
}

// dialSPDK opens a gRPC connection to the SPDK service on the given node.
// The caller is responsible for closing the returned *grpc.ClientConn.
func (c *ShardGroupController) dialSPDK(nodeID string) (spdkrpc.SPDKServiceClient, *grpc.ClientConn, error) {
	instanceManager, err := c.ds.GetRunningInstanceManagerByNodeRO(nodeID, longhorn.DataEngineTypeV2)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to get instance manager for node %v", nodeID)
	}

	instanceManagerPod, err := c.ds.GetPodRO(instanceManager.Namespace, instanceManager.Name)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to get pod for instance manager %v", instanceManager.Name)
	}

	storageIP := c.ds.GetIPFromPodByCNISetting(instanceManagerPod, types.SettingNameStorageNetwork)
	serviceURL := fmt.Sprintf("%s:%d", storageIP, engineapi.InstanceManagerSpdkServiceDefaultPort)

	conn, err := grpc.NewClient(serviceURL,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithNoProxy(),
		grpc.WithDisableServiceConfig(),
	)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to connect to SPDK service at %v", serviceURL)
	}

	return spdkrpc.NewSPDKServiceClient(conn), conn, nil
}

// syncProcess provisions and monitors the long-lived ShardGroup process that
// owns the EC volume's bdev_ec, lvstore, head lvol, and NVMe-oF export. The
// lvstore and head lvol live on the encoded shard blocks, so they survive
// detach, IM restart, and engine-node failover, which makes fast re-attach
// possible.
func (c *ShardGroupController) syncProcess(rctx *sgReconcileCtx) error {
	shardGroup := rctx.shardGroup
	log := rctx.log

	// Deletion is handled by cleanupShardGroup off the deletion path, not here.
	if shardGroup.DeletionTimestamp != nil {
		return nil
	}

	// Volume controller has not bound the ShardGroup to a node yet (initial
	// detached state of a freshly-created CR). Idle until first attach.
	if shardGroup.Spec.NodeID == "" {
		return nil
	}

	// Salvage discriminator: a ShardGroup that was already created
	// (HeadLvolUUID populated) must be re-provisioned with salvage=true.
	//
	// salvage=false against an existing lvstore fails: bdev_ec_create
	// auto-imports it, then bdev_lvol_create_lvstore returns -EPERM because the
	// bdev is already claimed by the lvol module.
	//
	// HeadLvolUUID is the signal rather than LvstoreUUID only because it is
	// already returned in InstanceResponse.Status.Uuid; LvstoreUUID would need
	// a proto extension. Both mean the lvstore exists on the shards.
	salvage := shardGroup.Status.HeadLvolUUID != ""

	// Re-bind: the process is currently bound to an InstanceManager whose
	// NodeID no longer matches Spec.NodeID. Tear down on the old IM with
	// cleanup=false (preserves lvstore on encoded blocks); the salvage flag
	// above already handles re-discovery on the new node.
	if shardGroup.Status.InstanceManagerName != "" {
		oldIM, err := c.ds.GetInstanceManagerRO(shardGroup.Status.InstanceManagerName)
		if err != nil && !datastore.ErrorIsNotFound(err) {
			return err
		}
		switch {
		case oldIM == nil:
			log.Warnf("ShardGroup process IM %v not found; clearing stale binding (salvage=%v)",
				shardGroup.Status.InstanceManagerName, salvage)
			c.clearShardGroupProcessStatus(rctx)
		case oldIM.Spec.NodeID != shardGroup.Spec.NodeID:
			log.Infof("ShardGroup process bound to node %v but Spec.NodeID is %v; re-binding (preserve lvstore)",
				oldIM.Spec.NodeID, shardGroup.Spec.NodeID)
			if err := c.teardownShardGroupProcessOnIM(rctx, oldIM, false); err != nil {
				return err
			}
			c.clearShardGroupProcessStatus(rctx)
		}
	}

	// Readiness gate (a): all k+m slots have an address.
	expected := shardGroup.Spec.DataChunks + shardGroup.Spec.ParityChunks
	if len(shardGroup.Status.ECShardAddressMap) != expected {
		log.Debugf("ShardGroup process readiness gate: %v/%v shard addresses ready; waiting",
			len(shardGroup.Status.ECShardAddressMap), expected)
		return nil
	}

	// Readiness gate (b): every Shard CR is observed Normal. An address can
	// outlive the instance that served it, so a full address map alone is not
	// liveness.
	for _, shard := range rctx.shards {
		if shard.Status.State != longhorn.ShardStateNormal {
			log.Debugf("ShardGroup process readiness gate: shard %v state=%v; waiting",
				shard.Name, shard.Status.State)
			return nil
		}
	}

	// Find the IM on Spec.NodeID. GetRunningInstanceManagerByNodeRO returns
	// only IMs in the Running state, so a transient IM-pod restart appears as
	// NotFound and we silently retry.
	im, err := c.ds.GetRunningInstanceManagerByNodeRO(shardGroup.Spec.NodeID, longhorn.DataEngineTypeV2)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			log.Debugf("InstanceManager on node %v not running yet; will retry", shardGroup.Spec.NodeID)
			return nil
		}
		return err
	}

	imClient, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return errors.Wrapf(err, "failed to create instance manager client for ShardGroup %v", shardGroup.Name)
	}
	defer func() {
		if closeErr := imClient.Close(); closeErr != nil {
			log.WithError(closeErr).Warn("Failed to close instance manager client")
		}
	}()

	instance, err := imClient.InstanceGet(longhorn.DataEngineTypeV2, shardGroup.Name, engineapi.InstanceTypeShardGroup)
	// Provision when there is no running instance on this IM:
	//   1. err != nil (gRPC NotFound): no record at all.
	//   2. State == Stopped: a prior teardown with cleanupRequired=false left a
	//      Stopped record; re-creating drives it back to Running.
	needCreate := err != nil || instance.Status.State == longhorn.InstanceStateStopped
	if needCreate {
		size, sizeErr := c.getVolumeSize(shardGroup)
		if sizeErr != nil {
			return errors.Wrapf(sizeErr, "failed to look up volume size for ShardGroup %v", shardGroup.Name)
		}
		instance, err = imClient.ShardGroupInstanceCreate(&engineapi.ShardGroupInstanceCreateRequest{
			ShardGroup:       shardGroup,
			Size:             size,
			ShardAddressMap:  shardGroup.Status.ECShardAddressMap,
			SalvageRequested: salvage,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to create ShardGroup process for %v on %v", shardGroup.Name, shardGroup.Spec.NodeID)
		}
		log.Infof("Created ShardGroup process for %v on node %v (salvage=%v, size=%v)",
			shardGroup.Name, shardGroup.Spec.NodeID, salvage, size)
	}

	return c.refreshShardGroupProcessStatus(rctx, im, instance)
}

// refreshShardGroupProcessStatus copies live process state into ShardGroup.Status
// fields. Called every reconcile tick (whether the process was just provisioned
// or has been Running for many cycles) so transient IM port re-allocations and
// state transitions are picked up promptly.
func (c *ShardGroupController) refreshShardGroupProcessStatus(rctx *sgReconcileCtx, im *longhorn.InstanceManager, instance *longhorn.InstanceProcess) error {
	shardGroup := rctx.shardGroup

	imPod, err := c.ds.GetPodRO(im.Namespace, im.Name)
	if err != nil {
		return errors.Wrapf(err, "failed to get pod for instance manager %v", im.Name)
	}
	storageIP := c.ds.GetIPFromPodByCNISetting(imPod, types.SettingNameStorageNetwork)

	shardGroup.Status.InstanceManagerName = im.Name
	shardGroup.Status.StorageIP = storageIP
	shardGroup.Status.Port = int32(instance.Status.PortStart)
	shardGroup.Status.NQN = instance.Status.Endpoint
	shardGroup.Status.ProcessState = instance.Status.State
	// The IM sets instance.Status.UUID = shardGroup.HeadLvolUUID for shardgroup-type
	// instances, so this records the head lvol identity, not a generic process UUID.
	// syncProcess reads it to decide salvage.
	shardGroup.Status.HeadLvolUUID = instance.Status.UUID
	return nil
}

// teardownShardGroupProcessOnIM tears down the ShardGroup process on the given
// InstanceManager. Used both for re-bind (cleanupRequired=false) and for delete
// (cleanupRequired=true, called by cleanupShardGroup).
//
// cleanupRequired=false preserves lvstore + head lvol on the encoded shard
// blocks: the SPDK service unexposes the NVMe-oF target and disconnects from
// shards but does NOT call bdev_lvol_delete or bdev_lvol_delete_lvstore. The
// next ShardGroupInstanceCreate with SalvageRequested=true will re-attach.
//
// cleanupRequired=true authorises full destruction. Used only on volume delete.
func (c *ShardGroupController) teardownShardGroupProcessOnIM(rctx *sgReconcileCtx, im *longhorn.InstanceManager, cleanupRequired bool) error {
	shardGroup := rctx.shardGroup
	imClient, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return errors.Wrapf(err, "failed to create instance manager client for ShardGroup %v teardown", shardGroup.Name)
	}
	defer func() {
		if closeErr := imClient.Close(); closeErr != nil {
			rctx.log.WithError(closeErr).Warn("Failed to close instance manager client during teardown")
		}
	}()

	if err := imClient.InstanceDelete(longhorn.DataEngineTypeV2, shardGroup.Name, "", engineapi.InstanceTypeShardGroup, "", cleanupRequired); err != nil {
		if !types.ErrorIsNotFound(err) {
			return errors.Wrapf(err, "failed to delete ShardGroup process %v on %v (cleanupRequired=%v)",
				shardGroup.Name, im.Spec.NodeID, cleanupRequired)
		}
	}
	rctx.log.Infof("Tore down ShardGroup process %v on %v (cleanupRequired=%v)", shardGroup.Name, im.Spec.NodeID, cleanupRequired)
	return nil
}

// teardownShardGroupProcess tears down the process on the IM currently named in
// Status.InstanceManagerName. Called from cleanupShardGroup with
// cleanupRequired=true. No-op if no IM is named.
func (c *ShardGroupController) teardownShardGroupProcess(rctx *sgReconcileCtx, cleanupRequired bool) error {
	shardGroup := rctx.shardGroup
	if shardGroup.Status.InstanceManagerName == "" {
		return nil
	}
	im, err := c.ds.GetInstanceManagerRO(shardGroup.Status.InstanceManagerName)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			c.clearShardGroupProcessStatus(rctx)
			return nil
		}
		return err
	}
	if err := c.teardownShardGroupProcessOnIM(rctx, im, cleanupRequired); err != nil {
		return err
	}
	c.clearShardGroupProcessStatus(rctx)
	return nil
}

// clearShardGroupProcessStatus zeroes the in-memory runtime fields after a
// process teardown. HeadLvolUUID is intentionally NOT zeroed here: it identifies
// the persistent on-disk head lvol encoded across the shards (see syncProcess
// salvage handling). It is cleared only when the ShardGroup CR is garbage-collected
// on volume delete.
func (c *ShardGroupController) clearShardGroupProcessStatus(rctx *sgReconcileCtx) {
	shardGroup := rctx.shardGroup
	shardGroup.Status.InstanceManagerName = ""
	shardGroup.Status.StorageIP = ""
	shardGroup.Status.Port = 0
	shardGroup.Status.NQN = ""
	shardGroup.Status.ProcessState = ""
}

// getVolumeSize returns the user-visible size for the ShardGroup's volume.
// The ShardGroup process uses this to size the head lvol exposed to the engine.
//
// Returns an error when Spec.Size is not yet populated, so syncProcess defers
// provisioning by one reconcile tick rather than silently creating a zero-sized
// head lvol - a race that can occur if syncProcess wins against the Volume
// CSI ControllerCreateVolume path setting Spec.Size.
func (c *ShardGroupController) getVolumeSize(shardGroup *longhorn.ShardGroup) (uint64, error) {
	vol, err := c.ds.GetVolumeRO(shardGroup.Spec.VolumeName)
	if err != nil {
		return 0, err
	}
	if vol.Spec.Size <= 0 {
		return 0, fmt.Errorf("volume %v has no size set yet", shardGroup.Spec.VolumeName)
	}
	return uint64(vol.Spec.Size), nil
}

// syncECHealth queries the SPDK service for the current EC slot states and updates
// the Shard CR statuses and the ShardGroup rebuild flag. It also restarts an
// interrupted rebuild if needed.
func (c *ShardGroupController) syncECHealth(rctx *sgReconcileCtx) error {
	if rctx.engine == nil || rctx.spdkClient == nil {
		return nil
	}

	shardGroup := rctx.shardGroup
	log := rctx.log

	ctx, cancel := context.WithTimeout(context.Background(), spdkRPCTimeout)
	defer cancel()

	protoShardGroup, err := rctx.spdkClient.ShardGroupGet(ctx, &spdkrpc.ShardGroupGetRequest{Name: shardGroup.Name})
	if err != nil {
		log.WithError(err).Warn("Failed to get ShardGroup EC state from SPDK; skipping EC health sync")
		return nil
	}

	ecStatus := protoShardGroup.GetEcStatus()

	slotMap := make(map[uint32]*spdkrpc.EcSlot, len(ecStatus.GetSlots()))
	for _, slot := range ecStatus.GetSlots() {
		slotMap[slot.GetSlotIndex()] = slot
	}

	shards, err := c.ds.ListShardsByShardGroup(shardGroup.Name)
	if err != nil {
		return err
	}

	anyReplacing := false

	for _, shard := range shards {
		slot, ok := slotMap[uint32(shard.Spec.SlotIndex)]
		if !ok {
			continue
		}

		newState := ecSlotStateToShardState(slot.GetState())
		if newState == longhorn.ShardStateReplacing {
			anyReplacing = true
		}

		rebuildProgress := 0
		if ecStatus.GetRebuildInProgress() {
			progress := ecStatus.GetRebuildProgress()
			if progress != nil && progress.GetCurrentSlot() == uint32(shard.Spec.SlotIndex) {
				rebuildProgress = int(progress.GetPercentComplete())
			}
		}

		needsTimestamp := newState == longhorn.ShardStateFailed && shard.Status.LastFailureTimestamp == ""
		if shard.Status.State == newState && shard.Status.RebuildProgress == rebuildProgress && !needsTimestamp {
			continue
		}

		fresh, err := c.ds.GetShard(shard.Name)
		if err != nil {
			return err
		}
		fresh.Status.State = newState
		fresh.Status.RebuildProgress = rebuildProgress
		if newState == longhorn.ShardStateFailed {
			// Start the replace delay (LastFailureTimestamp) only for a shard with
			// a running instance, so a brief failure does not trigger a replace. A
			// failed shard with no StorageIP is the not-yet-provisioned replacement
			// for an already-failed slot, which reconcileFailedShard provisions
			// before replacing - so do not start the delay here.
			if fresh.Status.LastFailureTimestamp == "" && fresh.Status.StorageIP != "" {
				fresh.Status.LastFailureTimestamp = util.Now()
				c.eventRecorder.Eventf(shardGroup, corev1.EventTypeWarning, constant.EventReasonShardFailed,
					"shard %v (slot %v) of volume %v failed", fresh.Name, fresh.Spec.SlotIndex, shardGroup.Spec.VolumeName)
			}
		} else {
			fresh.Status.LastFailureTimestamp = ""
			// Clear the in-flight replace marker now that SPDK has advanced the slot out
			// of Failed; reconcileFailedShard may re-set it if the slot regresses.
			fresh.Status.ReplaceTriggered = false
		}
		if _, err := c.ds.UpdateShardStatus(fresh); err != nil {
			return errors.Wrapf(err, "failed to update shard %v status", fresh.Name)
		}
	}

	shardGroup.Status.RebuildInProgress = ecStatus.GetRebuildInProgress()
	shardGroup.Status.ScrubInProgress = ecStatus.GetScrubProgress() != nil
	shardGroup.Status.WIBDirtyRegion = int(ecStatus.GetWibStatus().GetDirtyRegions())

	// A replace and its rebuild are two separate RPCs, so a crash between them
	// can leave a slot marked replacing with no rebuild running. Restart the
	// rebuild when that happens (subject to the concurrent-rebuild limit).
	if anyReplacing && !ecStatus.GetRebuildInProgress() {
		canRebuild, err := c.canStartShardRebuild(shardGroup)
		if err != nil {
			log.WithError(err).Warn("Failed to check rebuild limit before restarting an interrupted rebuild; will retry")
		} else if !canRebuild {
			log.Debugf("Concurrent rebuild limit reached on engine node %v; deferring the interrupted-rebuild restart", shardGroup.Spec.NodeID)
		} else {
			log.Info("Restarting interrupted shard rebuild")
			rebuildCtx, rebuildCancel := context.WithTimeout(context.Background(), spdkRPCTimeout)
			defer rebuildCancel()
			if _, err := rctx.spdkClient.ShardGroupShardRebuildStart(rebuildCtx, &spdkrpc.ShardGroupShardRebuildStartRequest{
				ShardGroupName: rctx.shardGroup.Name,
			}); err != nil {
				log.WithError(err).Warn("Failed to restart shard rebuild; will retry")
			} else {
				shardGroup.Status.RebuildInProgress = true
			}
		}
	}

	return nil
}

// syncShardRebuildQoS applies the volume's rebuild bandwidth limit to the EC engine
// while a shard rebuild is active.
func (c *ShardGroupController) syncShardRebuildQoS(rctx *sgReconcileCtx) error {
	shardGroup := rctx.shardGroup
	if !shardGroup.Status.RebuildInProgress || rctx.engine == nil || rctx.spdkClient == nil {
		return nil
	}

	log := rctx.log

	globalQoS, err := c.ds.GetSettingAsIntByDataEngine(
		types.SettingNameReplicaRebuildingBandwidthLimit,
		longhorn.DataEngineTypeV2,
	)
	if err != nil {
		return err
	}

	volume, err := c.ds.GetVolumeRO(shardGroup.Spec.VolumeName)
	if err != nil {
		return err
	}

	effectiveMBps := globalQoS
	if volume.Spec.ReplicaRebuildingBandwidthLimit > 0 {
		effectiveMBps = volume.Spec.ReplicaRebuildingBandwidthLimit
	}

	// Convert MiB/s -> stripes/sec. One stripe spans all k+m shards:
	// stripe_bytes = strip_size_kb * 1024 * (k+m).
	var maxStripesPerSec uint32
	totalChunks := shardGroup.Spec.DataChunks + shardGroup.Spec.ParityChunks
	if effectiveMBps > 0 && shardGroup.Spec.StripSizeKB > 0 && totalChunks > 0 {
		maxStripesPerSec = uint32((effectiveMBps * 1024 * 1024) / int64(shardGroup.Spec.StripSizeKB*1024*totalChunks))
	}

	ctx, cancel := context.WithTimeout(context.Background(), spdkRPCTimeout)
	defer cancel()

	if _, err := rctx.spdkClient.ShardGroupShardRebuildQosSet(ctx, &spdkrpc.ShardGroupShardRebuildQosSetRequest{
		ShardGroupName:   rctx.shardGroup.Name,
		MaxStripesPerSec: maxStripesPerSec,
	}); err != nil {
		log.WithError(err).Warn("Failed to set rebuild QoS; will retry")
		return nil
	}

	log.Debugf("Set rebuild QoS to %v MBps (%v stripes/sec)", effectiveMBps, maxStripesPerSec)
	return nil
}

// shardRebuildReservationTTL bounds how long an in-memory rebuild reservation is held
// before the durable RebuildInProgress flag confirms it. It only needs to exceed the lag
// between issuing the rebuild RPC and that flag becoming visible in the informer cache
// (normally seconds); the generous value just bounds how long a reservation lingers if a
// rebuild is issued but SPDK never reports it running.
const shardRebuildReservationTTL = 2 * time.Minute

// canStartShardRebuild reports whether the concurrent-replica-rebuild-per-node limit
// allows starting a new EC shard rebuild on the shardGroup's engine node, and reserves a
// slot when it does. The counted unit is one ShardGroup rebuild (which may move up to
// parity-m slots at once): ShardGroup.Status.RebuildInProgress is true iff a rebuild is
// really running, and it is attributed to the engine node (Spec.NodeID).
//
// A ShardGroup is owned by exactly one controller (the manager on its engine node), so a
// per-controller lock plus an in-memory reservation makes the limit strict across the
// controller's parallel workers. The reservation bridges the lag between issuing the
// rebuild RPC and RebuildInProgress becoming visible in the informer cache: it counts a
// just-allowed rebuild for a sibling worker that a bare cache count would miss. Once the
// durable flag appears the reservation is dropped and the flag carries the count; a
// reservation that never confirms is expired after shardRebuildReservationTTL. On a
// controller restart the map is empty and the count falls back to the durable flag.
//
// Residual (parity with the replica rebuild limiter): the reservation is in-memory, so a
// crash in the reserve-to-durable window, or an engine-node failover with two owners, can
// transiently allow one extra rebuild. syncECHealth's idempotent restart heals it. Both
// bound quickly and match how RAID1 replica rebuilds behave.
func (c *ShardGroupController) canStartShardRebuild(shardGroup *longhorn.ShardGroup) (bool, error) {
	limit, err := c.ds.GetSettingAsInt(types.SettingNameConcurrentReplicaRebuildPerNodeLimit)
	if err != nil {
		return false, err
	}
	if limit < 1 {
		return true, nil
	}

	c.rebuildingLock.Lock()
	defer c.rebuildingLock.Unlock()

	shardGroups, err := c.ds.ListShardGroupsRO()
	if err != nil {
		return false, err
	}

	onNode := map[string]*longhorn.ShardGroup{}
	for _, other := range shardGroups {
		if other.Spec.NodeID == shardGroup.Spec.NodeID {
			onNode[other.Name] = other
		}
	}

	// Count rebuilds actually running on the node (durable RebuildInProgress), and drop any
	// reservation now covered by that flag - a reservation only bridges the pre-flag lag.
	count := 0
	for name, other := range onNode {
		if other.Status.RebuildInProgress {
			count++
			delete(c.inProgressRebuildingMap, name)
		}
	}

	// Count reservations still bridging the lag; expire ones whose group is gone or that
	// never became a running rebuild within the TTL.
	for name, reservedAt := range c.inProgressRebuildingMap {
		if _, ok := onNode[name]; !ok || time.Since(reservedAt) > shardRebuildReservationTTL {
			delete(c.inProgressRebuildingMap, name)
			continue
		}
		count++
	}

	// This group already holds a slot (running or reserved): allow it idempotently, without
	// consuming a second slot.
	if shardGroup.Status.RebuildInProgress {
		return true, nil
	}
	if _, reserved := c.inProgressRebuildingMap[shardGroup.Name]; reserved {
		return true, nil
	}

	if count >= int(limit) {
		return false, nil
	}

	c.inProgressRebuildingMap[shardGroup.Name] = time.Now()
	return true, nil
}

// ecSlotStateToShardState maps an SPDK EcSlotState proto enum to a Longhorn ShardState.
func ecSlotStateToShardState(s spdkrpc.EcSlotState) longhorn.ShardState {
	switch s {
	case spdkrpc.EcSlotState_EC_SLOT_STATE_NORMAL:
		return longhorn.ShardStateNormal
	case spdkrpc.EcSlotState_EC_SLOT_STATE_FAILED:
		return longhorn.ShardStateFailed
	case spdkrpc.EcSlotState_EC_SLOT_STATE_REPLACING:
		return longhorn.ShardStateReplacing
	default:
		return longhorn.ShardStateFailed
	}
}

func (c *ShardGroupController) isResponsibleFor(shardGroup *longhorn.ShardGroup) bool {
	return isControllerResponsibleFor(c.controllerID, c.ds, shardGroup.Name, shardGroup.Spec.NodeID, shardGroup.Status.OwnerID)
}

func getLoggerForShardGroup(logger logrus.FieldLogger, shardGroup *longhorn.ShardGroup) *logrus.Entry {
	return logger.WithFields(logrus.Fields{
		"shardGroup": shardGroup.Name,
		"volume":     shardGroup.Spec.VolumeName,
	})
}
