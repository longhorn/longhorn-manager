package controller

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
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
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	lhns "github.com/longhorn/go-common-libs/ns"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type OrphanController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced
}

func NewOrphanController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) (*OrphanController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	oc := &OrphanController{
		baseController: newBaseController("longhorn-orphan", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-orphan-controller"}),
	}

	var err error
	if _, err = ds.OrphanInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    oc.enqueueOrphan,
		UpdateFunc: func(old, cur interface{}) { oc.enqueueOrphan(cur) },
		DeleteFunc: oc.enqueueOrphan,
	}); err != nil {
		return nil, err
	}
	oc.cacheSyncs = append(oc.cacheSyncs, ds.OrphanInformer.HasSynced)

	if _, err = ds.NodeInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(cur interface{}) { oc.enqueueForLonghornNode(cur) },
		UpdateFunc: func(old, cur interface{}) { oc.enqueueForLonghornNode(cur) },
		DeleteFunc: func(cur interface{}) { oc.enqueueForLonghornNode(cur) },
	}, 0); err != nil {
		return nil, err
	}
	oc.cacheSyncs = append(oc.cacheSyncs, ds.NodeInformer.HasSynced)

	if _, err = ds.InstanceManagerInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(cur interface{}) { oc.enqueueForInstanceManager(cur) },
		UpdateFunc: func(old, cur interface{}) { oc.enqueueForInstanceManager(cur) },
		DeleteFunc: func(cur interface{}) { oc.enqueueForInstanceManager(cur) },
	}, 0); err != nil {
		return nil, err
	}
	oc.cacheSyncs = append(oc.cacheSyncs, ds.InstanceManagerInformer.HasSynced)

	return oc, nil
}

func (oc *OrphanController) enqueueOrphan(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object %#v: %v", obj, err))
		return
	}

	oc.queue.Add(key)
}

func (oc *OrphanController) enqueueForLonghornNode(obj interface{}) {
	node, ok := obj.(*longhorn.Node)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}
		// use the last known state, to enqueue, dependent objects
		node, ok = deletedState.Obj.(*longhorn.Node)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	orphans, err := oc.ds.ListOrphansByNodeRO(node.Name)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to list orphans on node %v since %v", node.Name, err))
		return
	}

	for _, orphan := range orphans {
		oc.enqueueOrphan(orphan)
	}
}

func (oc *OrphanController) enqueueForInstanceManager(obj interface{}) {
	im, ok := obj.(*longhorn.InstanceManager)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}
		// use the last known state, to enqueue, dependent objects
		im, ok = deletedState.Obj.(*longhorn.InstanceManager)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	orphans, err := oc.ds.ListOrphansByNodeRO(im.Spec.NodeID)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to list orphans on instance manager %v since %v", im.Name, err))
		return
	}

	for _, orphan := range orphans {
		oc.enqueueOrphan(orphan)
	}
}

func (oc *OrphanController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer oc.queue.ShutDown()

	oc.logger.Info("Starting Longhorn Orphan controller")
	defer oc.logger.Info("Shut down Longhorn Orphan controller")

	if !cache.WaitForNamedCacheSync(oc.name, stopCh, oc.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(oc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (oc *OrphanController) worker() {
	for oc.processNextWorkItem() {
	}
}

func (oc *OrphanController) processNextWorkItem() bool {
	key, quit := oc.queue.Get()
	if quit {
		return false
	}
	defer oc.queue.Done(key)
	err := oc.syncOrphan(key.(string))
	oc.handleErr(err, key)
	return true
}

func (oc *OrphanController) handleErr(err error, key interface{}) {
	if err == nil {
		oc.queue.Forget(key)
		return
	}

	log := oc.logger.WithField("orphan", key)
	if oc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn orphan")
		oc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn orphan out of the queue")
	oc.queue.Forget(key)
}

func (oc *OrphanController) syncOrphan(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync orphan %v", key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != oc.namespace {
		return nil
	}
	return oc.reconcile(name)
}

func (oc *OrphanController) reconcile(orphanName string) (err error) {
	orphan, err := oc.ds.GetOrphan(orphanName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	log := getLoggerForOrphan(oc.logger, orphan)

	if !oc.isResponsibleFor(orphan) {
		return nil
	}

	if orphan.Status.OwnerID != oc.controllerID {
		orphan.Status.OwnerID = oc.controllerID
		orphan, err = oc.ds.UpdateOrphanStatus(orphan)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("Orphan got new owner %v", oc.controllerID)
	}

	if !orphan.DeletionTimestamp.IsZero() {
		isCleanupComplete, err := oc.cleanupOrphanedResource(orphan)
		if isCleanupComplete {
			return oc.ds.RemoveFinalizerForOrphan(orphan)
		}
		return err
	}

	existingOrphan := orphan.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if reflect.DeepEqual(existingOrphan.Status, orphan.Status) {
			return
		}
		if _, err := oc.ds.UpdateOrphanStatus(orphan); err != nil && apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue %v due to conflict", orphanName)
			oc.enqueueOrphan(orphan)
		}
	}()

	if err := oc.updateConditions(orphan); err != nil {
		return errors.Wrapf(err, "failed to update conditions for orphan %v", orphan.Name)
	}

	return nil
}

func getLoggerForOrphan(logger logrus.FieldLogger, orphan *longhorn.Orphan) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"orphan": orphan.Name,
		},
	)
}

func (oc *OrphanController) isResponsibleFor(orphan *longhorn.Orphan) bool {
	return isControllerResponsibleFor(oc.controllerID, oc.ds, orphan.Name, orphan.Spec.NodeID, orphan.Status.OwnerID)
}

func (oc *OrphanController) cleanupOrphanedResource(orphan *longhorn.Orphan) (isCleanupComplete bool, err error) {
	log := getLoggerForOrphan(oc.logger, orphan)

	defer func() {
		if err == nil {
			return
		}

		err = errors.Wrapf(err, "failed to delete %v orphan %v", orphan.Spec.Type, orphan.Name)
		orphan.Status.Conditions = types.SetCondition(orphan.Status.Conditions,
			longhorn.OrphanConditionTypeError, longhorn.ConditionStatusTrue, "", err.Error())
	}()

	// Make sure if the orphan nodeID and controller ID are same.
	// If NO, just delete the orphan resource object and don't touch the data.
	if orphan.Spec.NodeID != oc.controllerID {
		log.WithFields(logrus.Fields{
			"orphanType": orphan.Spec.Type,
			"orphanName": orphan.Name,
			"orphanNode": orphan.Spec.NodeID,
		}).Infof("Orphan does not belong to this controller. Skipping resource cleanup")
		return true, nil
	}

	switch orphan.Spec.Type {
	case longhorn.OrphanTypeEngineInstance:
		isCleanupComplete, err = oc.cleanupOrphanedEngineInstance(orphan)
	case longhorn.OrphanTypeReplicaInstance:
		isCleanupComplete, err = oc.cleanupOrphanedReplicaInstance(orphan)
	case longhorn.OrphanTypeReplicaDataStore:
		if types.GetCondition(orphan.Status.Conditions, longhorn.OrphanConditionTypeDataCleanable).Status !=
			longhorn.ConditionStatusTrue {
			log.Infof("Only delete orphan %v resource object and do not delete the orphaned data store", orphan.Name)
			return true, nil
		}
		err = oc.deleteOrphanedReplicaDataStore(orphan)
		if err == nil || (err != nil && apierrors.IsNotFound(err)) {
			isCleanupComplete = true
			err = nil
		}
	default:
		return false, fmt.Errorf("unknown orphan type %v to clear orphan %v", orphan.Spec.Type, orphan.Name)
	}

	return isCleanupComplete, err
}

func (oc *OrphanController) cleanupOrphanedEngineInstance(orphan *longhorn.Orphan) (isCleanupComplete bool, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "cannot cleanup orphan engine instance %v", orphan.Name)
		}
	}()

	instance, deType, err := oc.extractOrphanedInstanceInfo(orphan)
	if err != nil {
		return false, err
	}

	var spec *longhorn.InstanceSpec
	if engineCR, err := oc.ds.GetEngineRO(instance); err != nil {
		if !apierrors.IsNotFound(err) {
			return false, err
		}
		spec = nil
	} else {
		spec = &engineCR.Spec.InstanceSpec
	}
	return oc.cleanupOrphanedInstance(orphan, instance, longhorn.InstanceManagerTypeEngine, deType, spec)
}

func (oc *OrphanController) cleanupOrphanedReplicaInstance(orphan *longhorn.Orphan) (isCleanupComplete bool, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "cannot cleanup orphan replica instance %v", orphan.Name)
		}
	}()

	instance, deType, err := oc.extractOrphanedInstanceInfo(orphan)
	if err != nil {
		return false, err
	}

	var spec *longhorn.InstanceSpec
	if replicaCR, err := oc.ds.GetReplicaRO(instance); err != nil {
		if !apierrors.IsNotFound(err) {
			return false, err
		}
		spec = nil
	} else {
		spec = &replicaCR.Spec.InstanceSpec
	}
	return oc.cleanupOrphanedInstance(orphan, instance, longhorn.InstanceManagerTypeReplica, deType, spec)
}

func (oc *OrphanController) cleanupOrphanedInstance(orphan *longhorn.Orphan, instance string, imType longhorn.InstanceManagerType, deType longhorn.DataEngineType, instanceCRSpec *longhorn.InstanceSpec) (isCleanupComplete bool, err error) {
	if instanceCRSpec != nil && instanceCRSpec.NodeID == oc.controllerID {
		// Instance is scheduled back to current node and is no longer considered orphaned.
		// Finalize the orphan CR directly without terminating the instance process.
		return true, nil
	}

	im, err := oc.ds.GetRunningInstanceManagerByNodeRO(oc.controllerID, deType)
	if err != nil {
		oc.logger.WithError(err).Infof("No running instance manager for node %v for deleting orphan instance %v", oc.controllerID, orphan.Name)
		return true, nil
	}
	imc, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return false, err
	}
	defer func() {
		if closeErr := imc.Close(); closeErr != nil {
			oc.logger.WithError(closeErr).Error("failed to close instance manager client")
		}
	}()

	instanceExist := types.GetCondition(orphan.Status.Conditions, longhorn.OrphanConditionTypeInstanceState).Status == longhorn.ConditionStatusTrue
	if instanceExist {
		if err = oc.deleteInstance(imc, instance, imType, deType); err != nil {
			return false, err
		}
	}
	return oc.confirmOrphanInstanceCleanup(imc, instance, imType, deType)
}

func (oc *OrphanController) extractOrphanedInstanceInfo(orphan *longhorn.Orphan) (name string, dataEngineType longhorn.DataEngineType, err error) {
	name, ok := orphan.Spec.Parameters[longhorn.OrphanInstanceName]
	if !ok {
		return "", "", fmt.Errorf("failed to get instance name for instance orphan %v", orphan.Name)
	}

	dataEngineTypeStr, ok := orphan.Spec.Parameters[longhorn.OrphanDataEngineType]
	if !ok {
		return "", "", fmt.Errorf("failed to get data engine type for instance orphan %v", orphan.Name)
	}
	switch dataEngineType = longhorn.DataEngineType(dataEngineTypeStr); dataEngineType {
	case longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2:
		// supported data engine type
	default:
		return "", "", fmt.Errorf("unknown data engine type %v for instance orphan %v", dataEngineTypeStr, orphan.Name)
	}

	return name, dataEngineType, nil
}

func (oc *OrphanController) deleteInstance(imc *engineapi.InstanceManagerClient, instanceName string, instanceKind longhorn.InstanceManagerType, engineType longhorn.DataEngineType) (err error) {
	// There is a delay between deletion initiation and state/InstanceManager update,
	// this function may be called multiple times before given instance exits.

	oc.logger.Infof("Orphan controller deleting instance %v", instanceName)

	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "cannot delete instance %v", instanceName)
		}
	}()

	err = imc.InstanceDelete(engineType, instanceName, string(instanceKind), "", false)
	if err != nil && !types.ErrorIsNotFound(err) {
		return err
	}

	return nil
}

func (oc *OrphanController) confirmOrphanInstanceCleanup(imc *engineapi.InstanceManagerClient, instanceName string, imType longhorn.InstanceManagerType, engineType longhorn.DataEngineType) (isCleanupComplete bool, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "cannot confirm cleanup result of instance %v", instanceName)
		}
	}()

	_, err = imc.InstanceGet(engineType, instanceName, string(imType))
	switch {
	case err == nil:
		// Instance still exists - cleanup not complete
		// Cleanup will continue after the instance state update.
		return false, nil
	case types.ErrorIsNotFound(err):
		// instance not found - cleanup completed.
		return true, nil
	default:
		// Unexpected error - cleanup status unknown.
		return false, err
	}
}

func (oc *OrphanController) deleteOrphanedReplicaDataStore(orphan *longhorn.Orphan) error {
	oc.logger.Infof("Deleting orphan %v replica data store %v in disk %v on node %v",
		orphan.Name, orphan.Spec.Parameters[longhorn.OrphanDataName],
		orphan.Spec.Parameters[longhorn.OrphanDiskPath], orphan.Status.OwnerID)

	diskType, ok := orphan.Spec.Parameters[longhorn.OrphanDiskType]
	if !ok {
		return fmt.Errorf("failed to get disk type for orphan %v", orphan.Name)
	}

	switch longhorn.DiskType(diskType) {
	case longhorn.DiskTypeFilesystem:
		diskPath := orphan.Spec.Parameters[longhorn.OrphanDiskPath]
		replicaDirectoryName := orphan.Spec.Parameters[longhorn.OrphanDataName]
		err := lhns.DeletePath(filepath.Join(diskPath, "replicas", replicaDirectoryName))
		return errors.Wrapf(err, "failed to delete orphan replica directory %v in disk %v", replicaDirectoryName, diskPath)
	case longhorn.DiskTypeBlock:
		return oc.DeleteV2ReplicaStore(orphan.Spec.Parameters[longhorn.OrphanDiskName], orphan.Spec.Parameters[longhorn.OrphanDiskUUID], "", orphan.Spec.Parameters[longhorn.OrphanDataName])
	default:
		return fmt.Errorf("unknown disk type %v for orphan %v", diskType, orphan.Name)
	}
}

func (oc *OrphanController) DeleteV2ReplicaStore(diskName, diskUUID, diskDriver, replicaInstanceName string) (err error) {
	logrus.Infof("Deleting SPDK replica store %v on disk %v on node %v", replicaInstanceName, diskUUID, oc.controllerID)

	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "cannot delete v2 replica store %v", replicaInstanceName)
		}
	}()

	im, err := oc.ds.GetRunningInstanceManagerByNodeRO(oc.controllerID, longhorn.DataEngineTypeV2)
	if err != nil {
		return errors.Wrapf(err, "failed to get running instance manager for node %v for deleting v2 replica store %v", oc.controllerID, replicaInstanceName)
	}

	c, err := engineapi.NewDiskServiceClient(im, oc.logger)
	if err != nil {
		return err
	}
	defer c.Close()

	err = c.DiskReplicaInstanceDelete(string(longhorn.DiskTypeBlock), diskName, diskUUID, diskDriver, replicaInstanceName)
	if err != nil && !types.ErrorIsNotFound(err) {
		return err
	}

	return nil
}

func (oc *OrphanController) updateConditions(orphan *longhorn.Orphan) error {
	switch orphan.Spec.Type {
	case longhorn.OrphanTypeEngineInstance, longhorn.OrphanTypeReplicaInstance:
		// No condition update needed. Instance state is tracked by instance manager monitor.
	case longhorn.OrphanTypeReplicaDataStore:
		if err := oc.updateDataCleanableCondition(orphan); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown orphan type %v to update conditions on orphan %v", orphan.Spec.Type, orphan.Name)
	}

	if types.GetCondition(orphan.Status.Conditions, longhorn.OrphanConditionTypeError).Status != longhorn.ConditionStatusTrue {
		orphan.Status.Conditions = types.SetCondition(orphan.Status.Conditions, longhorn.OrphanConditionTypeError, longhorn.ConditionStatusFalse, "", "")
	}

	return nil
}

// If the DataCleanable condition status is false, then the deletion of the orphan CRD won't delete the on-disk data
func (oc *OrphanController) updateDataCleanableCondition(orphan *longhorn.Orphan) (err error) {
	reason := ""

	defer func() {
		if err != nil {
			return
		}

		status := longhorn.ConditionStatusTrue
		if reason != "" {
			status = longhorn.ConditionStatusFalse
		}

		orphan.Status.Conditions = types.SetCondition(orphan.Status.Conditions, longhorn.OrphanConditionTypeDataCleanable, status, reason, "")
	}()

	isUnavailable, err := oc.ds.IsNodeDownOrDeletedOrMissingManager(orphan.Spec.NodeID)
	if err != nil {
		return errors.Wrapf(err, "failed to check node down or missing manager for node %v", orphan.Spec.NodeID)
	}
	if isUnavailable {
		reason = longhorn.OrphanConditionTypeDataCleanableReasonNodeUnavailable
		return nil
	}

	node, err := oc.ds.GetNode(orphan.Spec.NodeID)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get node %v", orphan.Spec.NodeID)
		}
		reason = longhorn.OrphanConditionTypeDataCleanableReasonNodeUnavailable
		return nil
	}

	if node.Spec.EvictionRequested {
		reason = longhorn.OrphanConditionTypeDataCleanableReasonNodeEvicted
		return nil
	}

	if orphan.Spec.Type == longhorn.OrphanTypeReplicaDataStore {
		reason = oc.checkOrphanedReplicaDataCleanable(node, orphan)
	}

	return nil
}

func (oc *OrphanController) checkOrphanedReplicaDataCleanable(node *longhorn.Node, orphan *longhorn.Orphan) string {
	diskName, err := oc.ds.GetReadyDisk(node.Name, orphan.Spec.Parameters[longhorn.OrphanDiskUUID])
	if err != nil {
		if strings.Contains(err.Error(), "cannot find the ready disk") {
			return longhorn.OrphanConditionTypeDataCleanableReasonDiskInvalid
		}

		return ""
	}

	disk := node.Spec.Disks[diskName]

	if diskName != orphan.Spec.Parameters[longhorn.OrphanDiskName] ||
		disk.Path != orphan.Spec.Parameters[longhorn.OrphanDiskPath] {
		return longhorn.OrphanConditionTypeDataCleanableReasonDiskChanged
	}

	if disk.EvictionRequested {
		return longhorn.OrphanConditionTypeDataCleanableReasonDiskEvicted
	}

	return ""
}
