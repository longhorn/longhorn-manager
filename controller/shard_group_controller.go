package controller

import (
	"fmt"
	"reflect"
	"strconv"
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

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

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

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-shard-group-controller"}),
	}

	var err error
	if _, err = ds.ShardGroupInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueShardGroup,
		UpdateFunc: func(old, cur interface{}) { c.enqueueShardGroup(cur) },
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

	// Step 3: fetch the shard list (after syncShards + syncECHealth), schedule any
	// unplaced shards, then run the single per-shard reconcile loop.
	shards, err := c.ds.ListShardsByShardGroup(shardGroup.Name)
	if err != nil {
		return err
	}
	rctx.shards = shards

	for _, shard := range shards {
		if err := c.reconcileShard(rctx, shard); err != nil {
			return err
		}
	}

	// Step 5: derive ShardGroup status from the current shard snapshot.
	return c.syncStatus(rctx)
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
	default:
		return nil
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

// cleanupShardGroup deletes all owned Shard CRs and removes the finalizer once done.
func (c *ShardGroupController) cleanupShardGroup(rctx *sgReconcileCtx) error {
	shardGroup := rctx.shardGroup
	log := rctx.log

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

func (c *ShardGroupController) isResponsibleFor(shardGroup *longhorn.ShardGroup) bool {
	return isControllerResponsibleFor(c.controllerID, c.ds, shardGroup.Name, shardGroup.Spec.NodeID, shardGroup.Status.OwnerID)
}

func getLoggerForShardGroup(logger logrus.FieldLogger, shardGroup *longhorn.ShardGroup) *logrus.Entry {
	return logger.WithFields(logrus.Fields{
		"shardGroup": shardGroup.Name,
		"volume":     shardGroup.Spec.VolumeName,
	})
}
