package controller

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type DiskScheduleController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced

	backoff *flowcontrol.Backoff
}

func NewDiskScheduleController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	namespace,
	controllerID string,
) (*DiskScheduleController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	c := &DiskScheduleController{
		baseController: newBaseController("longhorn-disk-schedule", logger),

		ds:           ds,
		namespace:    namespace,
		controllerID: controllerID,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-disk-schedule-controller"}),

		backoff: flowcontrol.NewBackOff(time.Minute, time.Minute*3),
	}

	var err error
	if _, err = ds.DiskScheduleInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueDiskSchedule,
		UpdateFunc: func(old, cur any) { c.enqueueDiskSchedule(cur) },
		DeleteFunc: c.enqueueDiskSchedule,
	}); err != nil {
		return nil, err
	}
	c.cacheSyncs = append(c.cacheSyncs, ds.DiskScheduleInformer.HasSynced)

	if _, err = ds.ReplicaInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: c.enqueueReplicaDelete,
	}); err != nil {
		return nil, err
	}
	c.cacheSyncs = append(c.cacheSyncs, ds.ReplicaInformer.HasSynced)

	if _, err = ds.BackingImageInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: c.enqueueBackingImageDelete,
	}); err != nil {
		return nil, err
	}
	c.cacheSyncs = append(c.cacheSyncs, ds.BackingImageInformer.HasSynced)

	return c, nil
}

// Run launches the single-thread worker of disk schedule controller.
func (dsc *DiskScheduleController) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer dsc.queue.ShutDown()

	dsc.logger.Info("Starting Longhorn disk schedule controller")
	defer dsc.logger.Info("Shut down Longhorn disk schedule controller")

	if !cache.WaitForNamedCacheSync(dsc.name, stopCh, dsc.cacheSyncs...) {
		return
	}

	// disk schedule controller prevents data racing by single-thread event handler
	go wait.Until(dsc.worker, time.Second, stopCh)

	<-stopCh
}

func (dsc *DiskScheduleController) worker() {
	for dsc.processNextWorkItem() {
	}
}

func (dsc *DiskScheduleController) processNextWorkItem() bool {
	key, quit := dsc.queue.Get()

	if quit {
		return false
	}
	defer dsc.queue.Done(key)

	err := dsc.syncDiskSchedule(key.(string))
	dsc.handleErr(err, key)

	return true
}

func (dsc *DiskScheduleController) handleErr(err error, key any) {
	if err == nil {
		dsc.queue.Forget(key)
		return
	}

	log := dsc.logger.WithField("DiskSchedule", key)
	if dsc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn disk schedule")
		dsc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn disk schedule out of the queue")
	dsc.queue.Forget(key)
}

func (dsc *DiskScheduleController) syncDiskSchedule(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != dsc.namespace {
		// Not ours, don't do anything
		return nil
	}
	return dsc.reconcile(name)
}

func (dsc *DiskScheduleController) reconcile(diskScheduleName string) (err error) {
	diskSchedule, err := dsc.ds.GetDiskSchedule(diskScheduleName)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return err
		}
		return nil
	}

	log := getLoggerForDiskSchedule(dsc.logger, diskSchedule)

	if !diskSchedule.DeletionTimestamp.IsZero() {
		return dsc.ds.RemoveFinalizerForDiskSchedule(diskSchedule)
	}

	// disk schedule is controlled only by the owner of the disk
	if diskSchedule.Spec.NodeID != dsc.controllerID {
		return nil
	}

	existingDiskSchedule := diskSchedule.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if !reflect.DeepEqual(existingDiskSchedule.Spec, diskSchedule.Spec) {
			if _, err := dsc.ds.UpdateDiskSchedule(diskSchedule); err != nil && apierrors.IsConflict(errors.Cause(err)) {
				log.WithError(err).Debugf("Requeue %v due to spec conflict", diskScheduleName)
				dsc.enqueueDiskSchedule(diskSchedule)
			}
		}
		if reflect.DeepEqual(existingDiskSchedule.Status, diskSchedule.Status) {
			return
		}
		if _, updateStatusErr := dsc.ds.UpdateDiskScheduleStatus(diskSchedule); updateStatusErr != nil && datastore.ErrorIsConflict(errors.Cause(updateStatusErr)) {
			log.WithError(updateStatusErr).Debugf("Requeue %v due to status conflict", diskScheduleName)
			dsc.enqueueDiskSchedule(diskSchedule)
		}
	}()

	if diskSchedule.Status.Replicas == nil {
		diskSchedule.Status.Replicas = make(map[string]*longhorn.DiskScheduledResourcesStatus)
	}
	if diskSchedule.Status.BackingImages == nil {
		diskSchedule.Status.BackingImages = make(map[string]*longhorn.DiskScheduledResourcesStatus)
	}
	if dsc.isDiskScheduleInitialing(diskSchedule) {
		// blindly record the allocation for existing resource
		dsc.syncFromAllocatedResources(diskSchedule)
	}

	if err := dsc.syncResourceDeallocation(diskSchedule); err != nil {
		return err
	}

	if err := dsc.syncResourceAllocation(diskSchedule); err != nil {
		return err
	}

	if err := dsc.updateConditions(diskSchedule); err != nil {
		return errors.Wrapf(err, "failed to update conditions for disk schedule %v", diskSchedule.Name)
	}

	return nil
}

func getLoggerForDiskSchedule(logger logrus.FieldLogger, diskSchedule *longhorn.DiskSchedule) *logrus.Entry {
	log := logger.WithFields(
		logrus.Fields{
			"diskUUID": diskSchedule.Name,
			"diskName": diskSchedule.Spec.Name,
			"nodeID":   diskSchedule.Spec.NodeID,
		},
	)
	return log
}

func (dsc *DiskScheduleController) enqueueDiskSchedule(obj any) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	dsc.queue.Add(key)
}

func (dsc *DiskScheduleController) enqueueReplicaDelete(obj any) {
	replica, typeErr := eventObjToTypedObj[longhorn.Replica](obj)
	if typeErr != nil {
		utilruntime.HandleError(typeErr)
		return
	}

	if replica.Spec.DiskID == "" {
		return
	}

	diskSchedule, err := dsc.ds.GetDiskScheduleRO(replica.Spec.DiskID)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("failed to get disk schedule %v for replica %v: %v ", replica.Spec.DiskID, replica.Name, err))
		}
		return
	}
	dsc.enqueueDiskSchedule(diskSchedule)
}

func (dsc *DiskScheduleController) enqueueBackingImageDelete(obj any) {
	bi, typeErr := eventObjToTypedObj[longhorn.BackingImage](obj)
	if typeErr != nil {
		utilruntime.HandleError(typeErr)
		return
	}

	diskUUIDs := make(map[string]struct{})
	for diskUUID := range bi.Spec.DiskFileSpecMap {
		diskUUIDs[diskUUID] = struct{}{}
	}
	for diskUUID := range bi.Status.DiskFileStatusMap {
		diskUUIDs[diskUUID] = struct{}{}
	}

	for diskUUID := range diskUUIDs {
		diskSchedule, err := dsc.ds.GetDiskScheduleRO(diskUUID)
		if err != nil {
			if !datastore.ErrorIsNotFound(err) {
				utilruntime.HandleError(fmt.Errorf("failed to get disk schedule %v for backing image %v: %v ", diskUUID, bi.Name, err))
			}
			return
		}
		dsc.enqueueDiskSchedule(diskSchedule)
	}
}

func (dsc *DiskScheduleController) isDiskScheduleInitialing(diskSchedule *longhorn.DiskSchedule) bool {
	condition := types.GetCondition(diskSchedule.Status.Conditions, longhorn.DiskScheduleConditionTypeReady)
	return condition.Status == longhorn.ConditionStatusUnknown
}

// syncFromAllocatedResources blindly record the allocation for existing resource
func (dsc *DiskScheduleController) syncFromAllocatedResources(diskSchedule *longhorn.DiskSchedule) {
	dsc.logger.Infof("Syncing disk schedule %v from allocated resources", diskSchedule.Name)
	logger := getLoggerForDiskSchedule(dsc.logger, diskSchedule)

	// sync state from existing replicas
	for rName, size := range diskSchedule.Spec.Replicas {
		logger.Infof("Schedule existing replica %v on disk", rName)
		diskSchedule.Status.Replicas[rName] = &longhorn.DiskScheduledResourcesStatus{
			State: longhorn.DiskScheduledStateScheduled,
			Size:  size,
		}
	}

	// sync state from existing backing images
	for biName, size := range diskSchedule.Spec.BackingImages {
		logger.Infof("Schedule existing backing image %v on disk", biName)
		diskSchedule.Status.BackingImages[biName] = &longhorn.DiskScheduledResourcesStatus{
			State: longhorn.DiskScheduledStateScheduled,
			Size:  size,
		}
	}
}

func (dsc *DiskScheduleController) syncResourceDeallocation(diskSchedule *longhorn.DiskSchedule) error {
	// clean up resource requirement maps
	cleanUpRequirement := func(specMap map[string]int64, isResourceDeleted func(name string) (isDeleted bool, err error)) error {
		for name := range specMap {
			isDeleted, err := isResourceDeleted(name)
			if err != nil {
				return err
			}
			if isDeleted {
				delete(specMap, name)
			}
		}
		return nil
	}

	err := cleanUpRequirement(diskSchedule.Spec.Replicas, func(rName string) (bool, error) {
		replica, rErr := dsc.ds.GetReplicaRO(rName)
		if rErr != nil {
			if datastore.ErrorIsNotFound(rErr) {
				return true, nil
			}
			return false, rErr
		}
		return !replica.DeletionTimestamp.IsZero(), nil
	})
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("failed to check replica deallocation for disk schedule %v", diskSchedule.Name))
	}
	err = cleanUpRequirement(diskSchedule.Spec.BackingImages, func(biName string) (bool, error) {
		bi, biErr := dsc.ds.GetBackingImage(biName)
		if biErr != nil {
			if datastore.ErrorIsNotFound(biErr) {
				return true, nil
			}
			return false, biErr
		}
		return !bi.DeletionTimestamp.IsZero(), nil
	})
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("failed to check backing image deallocation for disk schedule %v", diskSchedule.Name))
	}

	// clean up resource status maps
	syncResourceStatus := func(specMap map[string]int64, statusMap map[string]*longhorn.DiskScheduledResourcesStatus) (scheduledSize int64) {
		scheduledSize = 0
		for resource, status := range statusMap {
			_, exist := specMap[resource]
			if exist {
				scheduledSize += status.Size
			} else {
				delete(statusMap, resource)
			}
		}
		return scheduledSize
	}

	scheduledReplicaSize := syncResourceStatus(diskSchedule.Spec.Replicas, diskSchedule.Status.Replicas)
	scheduledBackingImageSize := syncResourceStatus(diskSchedule.Spec.BackingImages, diskSchedule.Status.BackingImages)
	diskSchedule.Status.StorageScheduled = scheduledReplicaSize + scheduledBackingImageSize

	return nil
}

func (dsc *DiskScheduleController) syncResourceAllocation(diskSchedule *longhorn.DiskSchedule) error {
	node, err := dsc.ds.GetNodeRO(diskSchedule.Spec.NodeID)
	if err != nil {
		return errors.Wrapf(err, "failed to get node %s for disk schedule %v", diskSchedule.Spec.NodeID, diskSchedule.Name)
	}
	diskSpec, exist := node.Spec.Disks[diskSchedule.Spec.Name]
	if !exist {
		dsc.logger.Infof("no disk spec for disk schedule %v, skip resource allocation", diskSchedule.Name)
		return nil
	}
	diskStatus, exist := node.Status.DiskStatus[diskSchedule.Spec.Name]
	if !exist {
		dsc.logger.Infof("no disk status for disk schedule %v, skip resource allocation", diskSchedule.Name)
		return nil
	}

	if !node.Spec.AllowScheduling || !diskSpec.AllowScheduling {
		// no new allocation is allowed
		return nil
	}

	type resourceItem struct {
		kind      string
		name      string
		specMap   map[string]int64
		statusMap map[string]*longhorn.DiskScheduledResourcesStatus
	}

	// randomize reconciliation order to prevent starvation
	resourceList := make([]resourceItem, 0, len(diskSchedule.Spec.Replicas)+len(diskSchedule.Spec.BackingImages))
	for name := range diskSchedule.Spec.Replicas {
		resourceList = append(resourceList, resourceItem{
			kind:      "Replica",
			name:      name,
			specMap:   diskSchedule.Spec.Replicas,
			statusMap: diskSchedule.Status.Replicas,
		})
	}
	for name := range diskSchedule.Spec.BackingImages {
		resourceList = append(resourceList, resourceItem{
			kind:      "BackingImage",
			name:      name,
			specMap:   diskSchedule.Spec.BackingImages,
			statusMap: diskSchedule.Status.BackingImages,
		})
	}
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	rnd.Shuffle(len(resourceList), func(i, j int) { resourceList[i], resourceList[j] = resourceList[j], resourceList[i] })

	// reconcile size change for each resource
	accScheduledSize := diskSchedule.Status.StorageScheduled
	for _, resource := range resourceList {
		resourceLogger := dsc.getLoggerForResource(diskSchedule, resource.kind, resource.name)

		requiredSize := resource.specMap[resource.name]
		status := resource.statusMap[resource.name]
		if status == nil {
			status = &longhorn.DiskScheduledResourcesStatus{
				Size: 0,
			}
			resource.statusMap[resource.name] = status
		}
		if requiredSize == status.Size {
			// already reconciled
			continue
		}
		schedulableMaximum := diskStatus.StorageMaximum - diskSpec.StorageReserved
		result, updateSize, updateAccSize := dsc.reconcileResourceAllocation(resourceLogger, accScheduledSize, schedulableMaximum, status.Size, requiredSize)
		status.Size = updateSize
		status.State = result
		accScheduledSize = updateAccSize
	}
	diskSchedule.Status.StorageScheduled = accScheduledSize

	return nil
}

func (dsc *DiskScheduleController) reconcileResourceAllocation(logger *logrus.Entry, accScheduledSize, schedulableMaximum, scheduledSize, requireSize int64) (result longhorn.DiskScheduleState, newScheduledSize, newAccScheduledSize int64) {
	logger = logger.WithFields(logrus.Fields{
		"accScheduledSize":   accScheduledSize,
		"schedulableMaximum": schedulableMaximum,
		"scheduledSize":      scheduledSize,
		"requireSize":        requireSize,
	})

	// TODO: blindly accept size reduction for now. Might consider capacity consumed by components outside from Longhorn.
	isCancelingExpansion := requireSize <= scheduledSize

	requireSizeDiff := requireSize - scheduledSize
	newAccScheduledSize = accScheduledSize + requireSizeDiff
	if isCancelingExpansion {
		logger.Info("Accept resource reduction")
		return longhorn.DiskScheduledStateScheduled, requireSize, newAccScheduledSize
	} else if newAccScheduledSize <= schedulableMaximum {
		logger.Info("Accept resource allocation")
		return longhorn.DiskScheduledStateScheduled, requireSize, newAccScheduledSize
	}
	logger.Info("Reject resource allocation")
	return longhorn.DiskScheduledStateRejected, scheduledSize, accScheduledSize
}

func (dsc *DiskScheduleController) updateConditions(diskSchedule *longhorn.DiskSchedule) error {
	diskSchedule.Status.Conditions = types.SetCondition(diskSchedule.Status.Conditions, longhorn.DiskScheduleConditionTypeReady, longhorn.ConditionStatusTrue, "", "")
	return nil
}

func (dsc *DiskScheduleController) getLoggerForResource(diskSchedule *longhorn.DiskSchedule, kind, name string) *logrus.Entry {
	return dsc.logger.WithFields(logrus.Fields{
		"diskSchedule": diskSchedule.Name,
		"node":         diskSchedule.Spec.NodeID,
		"resourceKind": kind,
		"resourceName": name,
	})
}
