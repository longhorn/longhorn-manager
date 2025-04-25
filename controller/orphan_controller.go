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
		if !datastore.ErrorIsNotFound(err) {
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
			if datastore.ErrorIsConflict(errors.Cause(err)) {
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
		if _, err := oc.ds.UpdateOrphanStatus(orphan); err != nil && datastore.ErrorIsConflict(errors.Cause(err)) {
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

	// Make sure if the orphan nodeID and controller ID are the same.
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
	case longhorn.OrphanTypeReplicaData:
		if types.GetCondition(orphan.Status.Conditions, longhorn.OrphanConditionTypeDataCleanable).Status !=
			longhorn.ConditionStatusTrue {
			log.Infof("Only delete orphan %v resource object and do not delete the orphaned data store", orphan.Name)
			return true, nil
		}
		err = oc.deleteOrphanedReplicaDataStore(orphan)
		if err == nil || (err != nil && datastore.ErrorIsNotFound(err)) {
			isCleanupComplete = true
			err = nil
		}
	default:
		return false, fmt.Errorf("unknown orphan type %v to clean up orphaned resource for %v", orphan.Spec.Type, orphan.Name)
	}

	return isCleanupComplete, err
}

func (oc *OrphanController) cleanupOrphanedEngineInstance(orphan *longhorn.Orphan) (isCleanupComplete bool, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "failed to cleanup orphan engine instance %v", orphan.Name)
		}
	}()

	instance, imName, err := oc.extractOrphanedInstanceInfo(orphan)
	if err != nil {
		return false, err
	}

	var spec *longhorn.InstanceSpec
	if engineCR, err := oc.ds.GetEngineRO(instance); err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return false, err
		}
		spec = nil
	} else {
		spec = &engineCR.Spec.InstanceSpec
	}
	oc.cleanupOrphanedInstance(orphan, instance, imName, longhorn.InstanceManagerTypeEngine, spec)
	return true, nil
}

func (oc *OrphanController) cleanupOrphanedReplicaInstance(orphan *longhorn.Orphan) (isCleanupComplete bool, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "failed to cleanup orphan replica instance %v", orphan.Name)
		}
	}()

	instance, imName, err := oc.extractOrphanedInstanceInfo(orphan)
	if err != nil {
		return false, err
	}

	var spec *longhorn.InstanceSpec
	if replicaCR, err := oc.ds.GetReplicaRO(instance); err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return false, err
		}
		spec = nil
	} else {
		spec = &replicaCR.Spec.InstanceSpec
	}
	oc.cleanupOrphanedInstance(orphan, instance, imName, longhorn.InstanceManagerTypeReplica, spec)
	return true, nil
}

func (oc *OrphanController) extractOrphanedInstanceInfo(orphan *longhorn.Orphan) (name, instanceManager string, err error) {
	name, ok := orphan.Spec.Parameters[longhorn.OrphanInstanceName]
	if !ok {
		return "", "", fmt.Errorf("failed to get instance name for instance orphan %v", orphan.Name)
	}

	instanceManager, ok = orphan.Spec.Parameters[longhorn.OrphanInstanceManager]
	if !ok {
		return "", "", fmt.Errorf("failed to get instance manager for instance orphan %v", orphan.Name)
	}

	switch orphan.Spec.DataEngine {
	case longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2:
		// supported data engine type
	default:
		return "", "", fmt.Errorf("unknown data engine type %v for instance orphan %v", orphan.Spec.DataEngine, orphan.Name)
	}

	return name, instanceManager, nil
}

func (oc *OrphanController) cleanupOrphanedInstance(orphan *longhorn.Orphan, instance, imName string, imType longhorn.InstanceManagerType, instanceCRSpec *longhorn.InstanceSpec) {
	if instanceCRSpec != nil && instanceCRSpec.NodeID == orphan.Spec.NodeID {
		oc.logger.Infof("Orphan instance %v is scheduled back to current node %v. Skip cleaning up the instance resource and finalize the orphan CR.", instance, orphan.Spec.NodeID)
		return
	}

	// If the instance manager client is unavailable or failed to delete the instance, continue finalizing the orphan.
	// Later if the orphaned instance is still reachable, the orphan will be recreated.
	imc, err := oc.getRunningInstanceManagerClientForOrphan(orphan, imName)
	if err != nil {
		oc.logger.WithError(err).Warnf("Failed to delete orphan instance %v due to instance manager client initialization failure. Continue to finalize orphan %v", instance, orphan.Name)
		return
	} else if imc == nil {
		oc.logger.WithField("orphanInstanceNode", orphan.Spec.NodeID).Warnf("No running instance manager for deleting orphan instance %v", orphan.Name)
		return
	}
	defer func() {
		if closeErr := imc.Close(); closeErr != nil {
			oc.logger.WithError(closeErr).Error("Failed to close instance manager client")
		}
	}()

	err = imc.InstanceDelete(orphan.Spec.DataEngine, instance, string(imType), "", false)
	if err != nil && !types.ErrorIsNotFound(err) {
		oc.logger.WithError(err).Warnf("Failed to delete orphan instance %v. Continue to finalize orphan %v", instance, orphan.Name)
	}
}

func (oc *OrphanController) getRunningInstanceManagerClientForOrphan(orphan *longhorn.Orphan, imName string) (*engineapi.InstanceManagerClient, error) {
	im, err := oc.ds.GetRunningInstanceManagerByNodeRO(orphan.Spec.NodeID, orphan.Spec.DataEngine)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if im.Name != imName {
		oc.logger.WithField("instanceManager", imName).Warnf("Orphan instance %v is not managed by current running instance manager %v", orphan.Name, im.Name)
		return nil, nil
	}
	return engineapi.NewInstanceManagerClient(im, false)
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
		return oc.DeleteV2ReplicaInstance(orphan.Spec.Parameters[longhorn.OrphanDiskName], orphan.Spec.Parameters[longhorn.OrphanDiskUUID], "", orphan.Spec.Parameters[longhorn.OrphanDataName])
	default:
		return fmt.Errorf("unknown disk type %v for orphan %v", diskType, orphan.Name)
	}
}

func (oc *OrphanController) DeleteV2ReplicaInstance(diskName, diskUUID, diskDriver, replicaInstanceName string) (err error) {
	logrus.Infof("Deleting SPDK replica instance %v on disk %v on node %v", replicaInstanceName, diskUUID, oc.controllerID)

	defer func() {
		err = errors.Wrapf(err, "cannot delete v2 replica instance %v", replicaInstanceName)
	}()

	im, err := oc.ds.GetRunningInstanceManagerByNodeRO(oc.controllerID, longhorn.DataEngineTypeV2)
	if err != nil {
		return errors.Wrapf(err, "failed to get running instance manager for node %v for deleting v2 replica instance %v", oc.controllerID, replicaInstanceName)
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
	case longhorn.OrphanTypeEngineInstance:
		if err := oc.updateInstanceStateCondition(orphan, longhorn.InstanceTypeEngine); err != nil {
			return err
		}
	case longhorn.OrphanTypeReplicaInstance:
		if err := oc.updateInstanceStateCondition(orphan, longhorn.InstanceTypeReplica); err != nil {
			return err
		}
	case longhorn.OrphanTypeReplicaData:
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

// If the InstanceState condition status is true, the corresponding instance is still exist in instance manager
func (oc *OrphanController) updateInstanceStateCondition(orphan *longhorn.Orphan, instanceType longhorn.InstanceType) (err error) {
	var instanceState longhorn.InstanceState
	defer func() {
		if err != nil {
			return
		}

		status := longhorn.ConditionStatusTrue
		if instanceState == longhorn.InstanceStateTerminated {
			status = longhorn.ConditionStatusFalse
		}

		orphan.Status.Conditions = types.SetCondition(orphan.Status.Conditions, longhorn.OrphanConditionTypeInstanceExist, status, string(instanceState), "")
	}()

	instanceName, instanceManager, err := oc.extractOrphanedInstanceInfo(orphan)
	im, err := oc.ds.GetInstanceManager(instanceManager)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			oc.logger.WithError(err).Infof("No instance manager for node %v for update instance state of orphan instance %v", oc.controllerID, orphan.Name)
			instanceState = longhorn.InstanceStateTerminated
			return nil
		}
		return err
	}
	imc, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return errors.Wrapf(err, "failed to check node down or missing manager for node %v", orphan.Spec.NodeID)
	}
	defer func() {
		if closeErr := imc.Close(); closeErr != nil {
			oc.logger.WithError(closeErr).Error("Failed to close instance manager client")
		}
	}()

	instance, err := imc.InstanceGet(orphan.Spec.DataEngine, instanceName, string(instanceType))
	switch {
	case err != nil:
		return errors.Wrapf(err, "failed to get instance %v", instanceName)
	case instance == nil:
		instanceState = longhorn.InstanceStateTerminated
	default:
		instanceState = instance.Status.State
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
		if !datastore.ErrorIsNotFound(err) {
			return fmt.Errorf("failed to get node %v", orphan.Spec.NodeID)
		}
		reason = longhorn.OrphanConditionTypeDataCleanableReasonNodeUnavailable
		return nil
	}

	if node.Spec.EvictionRequested {
		reason = longhorn.OrphanConditionTypeDataCleanableReasonNodeEvicted
		return nil
	}

	if orphan.Spec.Type == longhorn.OrphanTypeReplicaData {
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
