package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
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
	imapi "github.com/longhorn/longhorn-instance-manager/pkg/api"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type ReplicaController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of replica
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced

	instanceHandler *InstanceHandler

	rebuildingLock          *sync.Mutex
	inProgressRebuildingMap map[string]struct{}

	proxyConnCounter util.Counter
}

func NewReplicaController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	namespace string, controllerID string,
	proxyConnCounter util.Counter) (*ReplicaController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	rc := &ReplicaController{
		baseController: newBaseController("longhorn-replica", logger),

		namespace:    namespace,
		controllerID: controllerID,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-replica-controller"}),

		ds: ds,

		proxyConnCounter: proxyConnCounter,

		rebuildingLock:          &sync.Mutex{},
		inProgressRebuildingMap: map[string]struct{}{},
	}
	rc.instanceHandler = NewInstanceHandler(ds, rc, rc.eventRecorder)

	var err error
	if _, err = ds.ReplicaInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: rc.enqueueReplica,
		UpdateFunc: func(old, cur interface{}) {
			rc.enqueueReplica(cur)

			oldR := old.(*longhorn.Replica)
			curR := cur.(*longhorn.Replica)
			if IsRebuildingReplica(oldR) && !IsRebuildingReplica(curR) {
				rc.enqueueAllRebuildingReplicaOnCurrentNode()
			}
		},
		DeleteFunc: rc.enqueueReplica,
	}); err != nil {
		return nil, err
	}
	rc.cacheSyncs = append(rc.cacheSyncs, ds.ReplicaInformer.HasSynced)

	if _, err = ds.InstanceManagerInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    rc.enqueueInstanceManagerChange,
		UpdateFunc: func(old, cur interface{}) { rc.enqueueInstanceManagerChange(cur) },
		DeleteFunc: rc.enqueueInstanceManagerChange,
	}, 0); err != nil {
		return nil, err
	}
	rc.cacheSyncs = append(rc.cacheSyncs, ds.InstanceManagerInformer.HasSynced)

	if _, err = ds.NodeInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    rc.enqueueNodeAddOrDelete,
		UpdateFunc: rc.enqueueNodeChange,
		DeleteFunc: rc.enqueueNodeAddOrDelete,
	}, 0); err != nil {
		return nil, err
	}
	rc.cacheSyncs = append(rc.cacheSyncs, ds.NodeInformer.HasSynced)

	if _, err = ds.BackingImageInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    rc.enqueueBackingImageChange,
		UpdateFunc: func(old, cur interface{}) { rc.enqueueBackingImageChange(cur) },
		DeleteFunc: rc.enqueueBackingImageChange,
	}, 0); err != nil {
		return nil, err
	}
	rc.cacheSyncs = append(rc.cacheSyncs, ds.BackingImageInformer.HasSynced)

	if _, err = ds.SettingInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, cur interface{}) { rc.enqueueSettingChange(cur) },
	}, 0); err != nil {
		return nil, err
	}
	rc.cacheSyncs = append(rc.cacheSyncs, ds.SettingInformer.HasSynced)

	return rc, nil
}

func (rc *ReplicaController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer rc.queue.ShutDown()

	rc.logger.Info("Starting Longhorn replica controller")
	defer rc.logger.Info("Shut down Longhorn replica controller")

	if !cache.WaitForNamedCacheSync("longhorn replicas", stopCh, rc.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(rc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (rc *ReplicaController) worker() {
	for rc.processNextWorkItem() {
	}
}

func (rc *ReplicaController) processNextWorkItem() bool {
	key, quit := rc.queue.Get()

	if quit {
		return false
	}
	defer rc.queue.Done(key)

	err := rc.syncReplica(key.(string))
	rc.handleErr(err, key)

	return true
}

func (rc *ReplicaController) handleErr(err error, key interface{}) {
	if err == nil {
		rc.queue.Forget(key)
		return
	}

	log := rc.logger.WithField("Replica", key)
	if rc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn replica")
		rc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn replica out of the queue")
	rc.queue.Forget(key)
}

func getLoggerForReplica(logger logrus.FieldLogger, r *longhorn.Replica) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"replica": r.Name,
			"nodeID":  r.Spec.NodeID,
			"ownerID": r.Status.OwnerID,
		},
	)
}

func (rc *ReplicaController) syncReplica(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync replica for %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != rc.namespace {
		// Not ours, don't do anything
		return nil
	}

	replica, err := rc.ds.GetReplica(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "failed to get replica")
	}
	dataPath := types.GetReplicaDataPath(replica.Spec.DiskPath, replica.Spec.DataDirectoryName)

	log := getLoggerForReplica(rc.logger, replica)

	isResponsible, err := rc.isResponsibleFor(replica)
	if err != nil {
		return err
	}
	if !isResponsible {
		return nil
	}
	if replica.Status.OwnerID != rc.controllerID {
		replica.Status.OwnerID = rc.controllerID
		replica, err = rc.ds.UpdateReplicaStatus(replica)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("Replica got new owner %v", rc.controllerID)
	}

	if replica.DeletionTimestamp != nil {
		if err := rc.DeleteInstance(replica); err != nil {
			return errors.Wrapf(err, "failed to cleanup the related replica instance before deleting replica %v", replica.Name)
		}

		rs, err := rc.ds.ListVolumeReplicasRO(replica.Spec.VolumeName)
		if err != nil {
			return errors.Wrapf(err, "failed to list replicas of the volume before deleting replica %v", replica.Name)
		}

		if replica.Spec.NodeID != "" && replica.Spec.NodeID != rc.controllerID {
			log.Warn("Failed to cleanup replica's data because the replica's data is not on this node")
		} else if replica.Spec.NodeID != "" {
			if types.IsDataEngineV1(replica.Spec.DataEngine) {
				// Clean up the data directory if this is the active replica or if this inactive replica is the only one
				// using it.
				if (replica.Spec.Active || !hasMatchingReplica(replica, rs)) && dataPath != "" {
					// prevent accidentally deletion
					if !strings.Contains(filepath.Base(filepath.Clean(dataPath)), "-") {
						return fmt.Errorf("%v doesn't look like a replica data path", dataPath)
					}
					log.Info("Cleaning up replica")
					if err := lhns.DeleteDirectory(dataPath); err != nil {
						return errors.Wrapf(err, "cannot cleanup after replica %v at %v", replica.Name, dataPath)
					}
				} else {
					log.Info("Didn't cleanup replica since it's not the active one for the path or the path is empty")
				}
			}
		}

		return rc.ds.RemoveFinalizerForReplica(replica)
	}

	existingReplica := replica.DeepCopy()
	defer func() {
		// we're going to update replica assume things changes
		if err == nil && !reflect.DeepEqual(existingReplica.Status, replica.Status) {
			_, err = rc.ds.UpdateReplicaStatus(replica)
		}
		// requeue if it's conflict
		if apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue %v due to conflict", key)
			rc.enqueueReplica(replica)
			err = nil
		}
	}()

	// Deprecated and no longer used by Longhorn, but maybe someone's external tooling uses it? Remove in v1.7.0.
	replica.Status.EvictionRequested = replica.Spec.EvictionRequested // nolint: staticcheck

	return rc.instanceHandler.ReconcileInstanceState(replica, &replica.Spec.InstanceSpec, &replica.Status.InstanceStatus)
}

func (rc *ReplicaController) enqueueReplica(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	rc.queue.Add(key)
}

func (rc *ReplicaController) CreateInstance(obj interface{}, isInstanceOnRemoteNode bool) (*longhorn.InstanceProcess, error) {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		return nil, fmt.Errorf("invalid object for replica instance creation: %v", obj)
	}

	dataPath := types.GetReplicaDataPath(r.Spec.DiskPath, r.Spec.DataDirectoryName)
	if r.Spec.NodeID == "" || dataPath == "" || r.Spec.DiskID == "" || r.Spec.VolumeSize == 0 {
		return nil, fmt.Errorf("missing parameters for replica instance creation: %v", r)
	}

	var err error
	backingImagePath := ""
	if r.Spec.BackingImage != "" {
		if backingImagePath, err = rc.GetBackingImagePathForReplicaStarting(r); err != nil {
			r.Status.Conditions = types.SetCondition(r.Status.Conditions, longhorn.ReplicaConditionTypeWaitForBackingImage,
				longhorn.ConditionStatusTrue, longhorn.ReplicaConditionReasonWaitForBackingImageFailed, err.Error())
			return nil, err
		}
		if backingImagePath == "" {
			r.Status.Conditions = types.SetCondition(r.Status.Conditions, longhorn.ReplicaConditionTypeWaitForBackingImage,
				longhorn.ConditionStatusTrue, longhorn.ReplicaConditionReasonWaitForBackingImageWaiting, "")
			return nil, nil
		}
	}

	r.Status.Conditions = types.SetCondition(r.Status.Conditions, longhorn.ReplicaConditionTypeWaitForBackingImage,
		longhorn.ConditionStatusFalse, "", "")

	if IsRebuildingReplica(r) {
		canStart, err := rc.CanStartRebuildingReplica(r)
		if err != nil {
			return nil, err
		}
		if !canStart {
			return nil, nil
		}
	}

	im, err := rc.ds.GetInstanceManagerByInstanceRO(obj, isInstanceOnRemoteNode)
	if err != nil {
		return nil, err
	}

	c, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	v, err := rc.ds.GetVolumeRO(r.Spec.VolumeName)
	if err != nil {
		return nil, err
	}

	cliAPIVersion, err := rc.ds.GetDataEngineImageCLIAPIVersion(r.Spec.Image, r.Spec.DataEngine)
	if err != nil {
		return nil, err
	}

	diskName, err := rc.getDiskNameFromUUID(r)
	if err != nil {
		return nil, err
	}

	return c.ReplicaInstanceCreate(&engineapi.ReplicaInstanceCreateRequest{
		Replica:             r,
		DiskName:            diskName,
		DataPath:            dataPath,
		BackingImagePath:    backingImagePath,
		DataLocality:        v.Spec.DataLocality,
		ImIP:                im.Status.IP,
		EngineCLIAPIVersion: cliAPIVersion,
	})
}

func (rc *ReplicaController) getDiskNameFromUUID(r *longhorn.Replica) (string, error) {
	node, err := rc.ds.GetNodeRO(rc.controllerID)
	if err != nil {
		return "", err
	}
	for _, disk := range node.Status.DiskStatus {
		if disk.DiskUUID == r.Spec.DiskID {
			return disk.DiskName, nil
		}
	}
	return "", fmt.Errorf("cannot find disk name for replica %v", r.Name)
}

func (rc *ReplicaController) GetBackingImagePathForReplicaStarting(r *longhorn.Replica) (string, error) {
	log := getLoggerForReplica(rc.logger, r)

	bi, err := rc.ds.GetBackingImage(r.Spec.BackingImage)
	if err != nil {
		return "", err
	}
	if bi.Status.UUID == "" {
		log.Warnf("The requested backing image %v has not been initialized, UUID is empty", bi.Name)
		return "", nil
	}
	if bi.Spec.DiskFileSpecMap == nil {
		log.Warnf("The requested backing image %v has not started disk file handling", bi.Name)
		return "", nil
	}
	if _, exists := bi.Spec.DiskFileSpecMap[r.Spec.DiskID]; !exists {
		res, err := rc.ds.CanPutBackingImageOnDisk(bi, r.Spec.DiskID)
		if err != nil {
			log.WithError(err).Warnf("Failed to check if backing image %v can be put on disk %v", bi.Name, r.Spec.DiskID)
		}
		if !res {
			return "", fmt.Errorf("The backing image %v can not be put on the disk %v", bi.Name, r.Spec.DiskID)
		}
		bi.Spec.DiskFileSpecMap[r.Spec.DiskID] = &longhorn.BackingImageDiskFileSpec{}
		log.Infof("Asking backing image %v to download file to node %v disk %v", bi.Name, r.Spec.NodeID, r.Spec.DiskID)
		if _, err := rc.ds.UpdateBackingImage(bi); err != nil {
			return "", err
		}
		return "", nil
	}
	if fileStatus, exists := bi.Status.DiskFileStatusMap[r.Spec.DiskID]; !exists || fileStatus.State != longhorn.BackingImageStateReady {
		currentBackingImageState := ""
		if fileStatus != nil {
			currentBackingImageState = string(fileStatus.State)
		}
		log.Infof("Waiting for backing image %v to download file to node %v disk %v, the current state is %v", bi.Name, r.Spec.NodeID, r.Spec.DiskID, currentBackingImageState)
		return "", nil
	}
	return types.GetBackingImagePathForReplicaManagerContainer(r.Spec.DiskPath, r.Spec.BackingImage, bi.Status.UUID), nil
}

func IsRebuildingReplica(r *longhorn.Replica) bool {
	return r.Spec.RebuildRetryCount != 0 && r.Spec.HealthyAt == "" && r.Spec.FailedAt == ""
}

func (rc *ReplicaController) CanStartRebuildingReplica(r *longhorn.Replica) (bool, error) {
	log := getLoggerForReplica(rc.logger, r)

	concurrentRebuildingLimit, err := rc.ds.GetSettingAsInt(types.SettingNameConcurrentReplicaRebuildPerNodeLimit)
	if err != nil {
		return false, err
	}

	// If the concurrent value is 0, Longhorn will rely on
	// skipping replica replenishment rather than blocking instance launching here to disable the rebuilding.
	// Otherwise, the newly created replicas will keep hanging up there.
	if concurrentRebuildingLimit < 1 {
		return true, nil
	}

	// This is the only place in which the controller will operate
	// the in progress rebuilding replica map. Then the main reconcile loop
	// and the normal replicas will not be affected by the locking.
	rc.rebuildingLock.Lock()
	defer rc.rebuildingLock.Unlock()

	rsMap := map[string]*longhorn.Replica{}
	rs, err := rc.ds.ListReplicasByNodeRO(r.Spec.NodeID)
	if err != nil {
		return false, err
	}
	for _, replicaOnTheSameNode := range rs {
		rsMap[replicaOnTheSameNode.Name] = replicaOnTheSameNode
		// Just in case, this means the replica controller will try to recall
		// in-progress rebuilding replicas even if the longhorn manager pod is restarted.
		if IsRebuildingReplica(replicaOnTheSameNode) &&
			(replicaOnTheSameNode.Status.CurrentState == longhorn.InstanceStateStarting ||
				replicaOnTheSameNode.Status.CurrentState == longhorn.InstanceStateRunning) {
			rc.inProgressRebuildingMap[replicaOnTheSameNode.Name] = struct{}{}
		}
	}

	// Clean up the entries when the corresponding replica is no longer a
	// rebuilding one.
	for inProgressReplicaName := range rc.inProgressRebuildingMap {
		if inProgressReplicaName == r.Name {
			return true, nil
		}
		replicaOnTheSameNode, exists := rsMap[inProgressReplicaName]
		if !exists {
			delete(rc.inProgressRebuildingMap, inProgressReplicaName)
			continue
		}
		if !IsRebuildingReplica(replicaOnTheSameNode) {
			delete(rc.inProgressRebuildingMap, inProgressReplicaName)
		}
	}

	if len(rc.inProgressRebuildingMap) >= int(concurrentRebuildingLimit) {
		log.Warnf("Replica rebuildings for %+v are in progress on this node, which reaches or exceeds the concurrent limit value %v",
			rc.inProgressRebuildingMap, concurrentRebuildingLimit)
		return false, nil
	}

	rc.inProgressRebuildingMap[r.Name] = struct{}{}

	return true, nil
}

func (rc *ReplicaController) DeleteInstance(obj interface{}) (err error) {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		return fmt.Errorf("invalid object for replica instance deletion: %v", obj)
	}
	log := getLoggerForReplica(rc.logger, r)

	var im *longhorn.InstanceManager
	// Not assigned or not updated, try best to delete
	if r.Status.InstanceManagerName == "" {
		if r.Spec.NodeID == "" {
			log.Warnf("Replica %v does not set instance manager name and node ID, will skip the actual instance deletion", r.Name)
			return nil
		}
		im, err = rc.ds.GetInstanceManagerByInstance(obj, false)
		if err != nil {
			log.WithError(err).Warnf("Failed to detect instance manager for replica %v, will skip the actual instance deletion", r.Name)
			return nil
		}
		log.Infof("Cleaning up the instance for replica %v in instance manager %v", r.Name, im.Name)
	} else {
		im, err = rc.ds.GetInstanceManager(r.Status.InstanceManagerName)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			// The related node may be directly deleted.
			log.Warnf("The replica instance manager %v is gone during the replica instance %v deletion. Will do nothing for the deletion", r.Status.InstanceManagerName, r.Name)
			return nil
		}
	}

	if shouldSkip, skipReason := shouldSkipReplicaDeletion(im.Status.CurrentState); shouldSkip {
		log.Infof("Skipping deleting replica %v since %s", r.Name, skipReason)
		return nil
	}

	isDelinquent, err := rc.ds.IsNodeDelinquent(im.Spec.NodeID, r.Spec.VolumeName)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			log.WithError(err).Warnf("Failed to delete replica process %v", r.Name)
			if canIgnore, ignoreReason := canIgnoreReplicaDeletionFailure(im.Status.CurrentState,
				isDelinquent); canIgnore {
				log.Warnf("Ignored the failure to delete replica process %v because %s", r.Name, ignoreReason)
				err = nil
			}
		}
	}()

	c, err := engineapi.NewInstanceManagerClient(im, true)
	if err != nil {
		return err
	}
	defer c.Close()

	// No need to delete the instance if the replica is backed by a SPDK lvol
	cleanupRequired := false
	if canDeleteInstance(r) {
		cleanupRequired = true
	}

	if types.IsDataEngineV2(r.Spec.DataEngine) && r.Spec.FailedAt != "" {
		upgradeRequested, err := rc.ds.IsNodeDataEngineUpgradeRequested(r.Spec.NodeID)
		if err != nil {
			return err
		}
		if upgradeRequested {
			log.Infof("Deleting failed replica instance %v from its engine instance without cleanup since the node %v is requested to upgrade data engine",
				r.Name, r.Spec.NodeID)
			err = rc.removeFailedReplicaInstanceFromEngineInstance(r)
			if err != nil {
				return errors.Wrapf(err, "failed to remove failed replica instance %v from engine instance", r.Name)
			}
		}
	}

	log.WithField("cleanupRequired", cleanupRequired).Infof("Deleting replica instance on disk %v", r.Spec.DiskPath)

	err = c.InstanceDelete(r.Spec.DataEngine, r.Name, string(longhorn.InstanceManagerTypeReplica), r.Spec.DiskID, cleanupRequired)
	if err != nil && !types.ErrorIsNotFound(err) {
		return err
	}

	if err := deleteUnixSocketFile(r.Spec.VolumeName); err != nil && !types.ErrorIsNotFound(err) {
		log.WithError(err).Warnf("Failed to delete unix-domain-socket file for volume %v", r.Spec.VolumeName)
	}

	// Directly remove the instance from the map. Best effort.
	if im.Status.APIVersion == engineapi.IncompatibleInstanceManagerAPIVersion {
		delete(im.Status.InstanceReplicas, r.Name)
		delete(im.Status.Instances, r.Name) // nolint: staticcheck
		if _, err := rc.ds.UpdateInstanceManagerStatus(im); err != nil {
			return err
		}
	}

	return nil
}

func (rc *ReplicaController) removeFailedReplicaInstanceFromEngineInstance(r *longhorn.Replica) error {
	e, err := rc.ds.GetVolumeCurrentEngine(r.Spec.VolumeName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	engineCliClient, err := GetBinaryClientForEngine(e, &engineapi.EngineCollection{}, e.Status.CurrentImage)
	if err != nil {
		return err
	}

	engineClientProxy, err := engineapi.GetCompatibleClient(e, engineCliClient, rc.ds, rc.logger, rc.proxyConnCounter)
	if err != nil {
		return err
	}
	defer engineClientProxy.Close()

	if err := engineClientProxy.ReplicaRemove(e, "", r.Name); err != nil {
		if strings.Contains(err.Error(), "cannot find replica") {
			return nil
		}
		return errors.Wrapf(err, "failed to remove failed replica %v from engine instance", r.Name)
	}

	return nil
}

func canDeleteInstance(r *longhorn.Replica) bool {
	return types.IsDataEngineV1(r.Spec.DataEngine) ||
		(types.IsDataEngineV2(r.Spec.DataEngine) && r.DeletionTimestamp != nil)
}

func deleteUnixSocketFile(volumeName string) error {
	return os.RemoveAll(filepath.Join(types.UnixDomainSocketDirectoryOnHost, volumeName+filepath.Ext(".sock")))
}

func (rc *ReplicaController) SuspendInstance(obj interface{}) error {
	return nil
}

func (rc *ReplicaController) ResumeInstance(obj interface{}) error {
	return nil
}

func (rc *ReplicaController) SwitchOverTarget(obj interface{}) error {
	return nil
}

func (rc *ReplicaController) DeleteTarget(obj interface{}) error {
	return nil
}

func (rc *ReplicaController) RequireRemoteTargetInstance(obj interface{}) (bool, error) {
	return false, nil
}

func (rc *ReplicaController) IsEngine(obj interface{}) bool {
	_, ok := obj.(*longhorn.Engine)
	return ok
}

func (rc *ReplicaController) GetInstance(obj interface{}, isInstanceOnRemoteNode bool) (*longhorn.InstanceProcess, error) {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		return nil, fmt.Errorf("invalid object for replica instance get: %v", obj)
	}

	var (
		im  *longhorn.InstanceManager
		err error
	)
	if r.Status.InstanceManagerName == "" {
		im, err = rc.ds.GetInstanceManagerByInstanceRO(obj, isInstanceOnRemoteNode)
		if err != nil {
			return nil, err
		}
	} else {
		im, err = rc.ds.GetInstanceManagerRO(r.Status.InstanceManagerName)
		if err != nil {
			return nil, err
		}
	}

	c, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	instance, err := c.InstanceGet(r.Spec.DataEngine, r.Name, string(longhorn.InstanceManagerTypeReplica))
	if err != nil {
		return nil, err
	}

	if types.IsDataEngineV2(instance.Spec.DataEngine) {
		if instance.Status.State == longhorn.InstanceStateStopped {
			return nil, fmt.Errorf("instance %v is stopped", instance.Spec.Name)
		}
	}

	return instance, nil
}

func (rc *ReplicaController) LogInstance(ctx context.Context, obj interface{}) (*engineapi.InstanceManagerClient, *imapi.LogStream, error) {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		return nil, nil, fmt.Errorf("invalid object for replica instance log: %v", obj)
	}

	im, err := rc.ds.GetInstanceManagerRO(r.Status.InstanceManagerName)
	if err != nil {
		return nil, nil, err
	}

	c, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return nil, nil, err
	}

	// TODO: #2441 refactor this when we do the resource monitoring refactor
	stream, err := c.InstanceLog(ctx, r.Spec.DataEngine, r.Name, string(longhorn.InstanceManagerTypeReplica))
	return c, stream, err
}

func (rc *ReplicaController) enqueueInstanceManagerChange(obj interface{}) {
	im, isInstanceManager := obj.(*longhorn.InstanceManager)
	if !isInstanceManager {
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

	imType, err := datastore.CheckInstanceManagerType(im)
	if err != nil || (imType != longhorn.InstanceManagerTypeReplica && imType != longhorn.InstanceManagerTypeAllInOne) {
		return
	}

	// replica's NodeID won't change, don't need to check instance manager
	replicasRO, err := rc.ds.ListReplicasByNodeRO(im.Spec.NodeID)
	if err != nil {
		getLoggerForInstanceManager(rc.logger, im).Warn("Failed to list replicas on node")
		return
	}

	for _, r := range replicasRO {
		rc.enqueueReplica(r)
	}

}

func (rc *ReplicaController) enqueueNodeAddOrDelete(obj interface{}) {
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

	for diskName := range node.Spec.Disks {
		if diskStatus, existed := node.Status.DiskStatus[diskName]; existed {
			for replicaName := range diskStatus.ScheduledReplica {
				if replica, err := rc.ds.GetReplicaRO(replicaName); err == nil {
					rc.enqueueReplica(replica)
				}
			}
		}
	}

}

func (rc *ReplicaController) enqueueNodeChange(oldObj, currObj interface{}) {
	oldNode, ok := oldObj.(*longhorn.Node)
	if !ok {
		deletedState, ok := oldObj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", oldObj))
			return
		}

		// use the last known state, to enqueue, dependent objects
		oldNode, ok = deletedState.Obj.(*longhorn.Node)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	currNode, ok := currObj.(*longhorn.Node)
	if !ok {
		deletedState, ok := currObj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", currObj))
			return
		}

		// use the last known state, to enqueue, dependent objects
		currNode, ok = deletedState.Obj.(*longhorn.Node)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	// if a node or disk changes its EvictionRequested, enqueue all replicas on that node/disk
	evictionRequestedChangeOnNodeLevel := currNode.Spec.EvictionRequested != oldNode.Spec.EvictionRequested
	for diskName, newDiskSpec := range currNode.Spec.Disks {
		oldDiskSpec, ok := oldNode.Spec.Disks[diskName]
		evictionRequestedChangeOnDiskLevel := !ok || (newDiskSpec.EvictionRequested != oldDiskSpec.EvictionRequested)
		if diskStatus, existed := currNode.Status.DiskStatus[diskName]; existed && (evictionRequestedChangeOnNodeLevel || evictionRequestedChangeOnDiskLevel) {
			for replicaName := range diskStatus.ScheduledReplica {
				if replica, err := rc.ds.GetReplica(replicaName); err == nil {
					rc.enqueueReplica(replica)
				}
			}
		}
	}

}

func (rc *ReplicaController) enqueueBackingImageChange(obj interface{}) {
	backingImage, ok := obj.(*longhorn.BackingImage)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}
		// use the last known state, to enqueue, dependent objects
		backingImage, ok = deletedState.Obj.(*longhorn.BackingImage)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	replicas, err := rc.ds.ListReplicasByNodeRO(rc.controllerID)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to list replicas on node %v for backing image %v: %v", rc.controllerID, backingImage.Name, err))
		return
	}
	for diskUUID := range backingImage.Status.DiskFileStatusMap {
		for _, r := range replicas {
			if r.Spec.DiskID == diskUUID && r.Spec.BackingImage == backingImage.Name {
				rc.enqueueReplica(r)
			}
		}
	}

}

func (rc *ReplicaController) enqueueSettingChange(obj interface{}) {
	setting, ok := obj.(*longhorn.Setting)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to enqueue, dependent objects
		setting, ok = deletedState.Obj.(*longhorn.Setting)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	if types.SettingName(setting.Name) != types.SettingNameConcurrentReplicaRebuildPerNodeLimit {
		return
	}

	rc.enqueueAllRebuildingReplicaOnCurrentNode()

}

func (rc *ReplicaController) enqueueAllRebuildingReplicaOnCurrentNode() {
	replicas, err := rc.ds.ListReplicasByNodeRO(rc.controllerID)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to list rebuilding replicas on current node %v: %v",
			rc.controllerID, err))
		return
	}
	for _, r := range replicas {
		if IsRebuildingReplica(r) {
			rc.enqueueReplica(r)
		}
	}
}

func (rc *ReplicaController) isResponsibleFor(r *longhorn.Replica) (bool, error) {
	// If a regular RWX is delinquent, try to switch ownership quickly to the owner node of the share manager CR
	isOwnerNodeDelinquent, err := rc.ds.IsNodeDelinquent(r.Status.OwnerID, r.Spec.VolumeName)
	if err != nil {
		return false, err
	}
	isSpecNodeDelinquent, err := rc.ds.IsNodeDelinquent(r.Spec.NodeID, r.Spec.VolumeName)
	if err != nil {
		return false, err
	}
	preferredOwnerID := r.Spec.NodeID
	if isOwnerNodeDelinquent || isSpecNodeDelinquent {
		sm, err := rc.ds.GetShareManager(r.Spec.VolumeName)
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		if sm != nil {
			preferredOwnerID = sm.Status.OwnerID
		}
	}

	return isControllerResponsibleFor(rc.controllerID, rc.ds, r.Name, preferredOwnerID, r.Status.OwnerID), nil
}

func hasMatchingReplica(replica *longhorn.Replica, replicas map[string]*longhorn.Replica) bool {
	for _, r := range replicas {
		if r.Name != replica.Name &&
			r.Spec.NodeID == replica.Spec.NodeID &&
			r.Spec.DiskID == replica.Spec.DiskID &&
			r.Spec.DiskPath == replica.Spec.DiskPath &&
			r.Spec.DataDirectoryName == replica.Spec.DataDirectoryName {
			return true
		}
	}
	return false
}

func shouldSkipReplicaDeletion(imState longhorn.InstanceManagerState) (canSkip bool, reason string) {
	// If the instance manager is in an unknown state, we should at least attempt instance deletion.
	if imState == longhorn.InstanceManagerStateRunning || imState == longhorn.InstanceManagerStateUnknown {
		return false, ""
	}

	return true, fmt.Sprintf("instance manager is in %v state", imState)
}

func canIgnoreReplicaDeletionFailure(imState longhorn.InstanceManagerState, isDelinquent bool) (canIgnore bool, reason string) {
	// Instance deletion is always best effort for an unknown instance manager.
	if imState == longhorn.InstanceManagerStateUnknown {
		return true, fmt.Sprintf("instance manager is in %v state", imState)
	}

	if isDelinquent {
		return true, "the RWX volume is delinquent"
	}

	return false, ""
}
