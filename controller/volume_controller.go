package controller

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	batchv1 "k8s.io/api/batch/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/backupstore"
	imutil "github.com/longhorn/longhorn-instance-manager/pkg/util"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/scheduler"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
)

var (
	ownerKindVolume = longhorn.SchemeGroupVersion.WithKind("Volume").String()

	RetryInterval = 100 * time.Millisecond
	RetryCounts   = 20

	AutoSalvageTimeLimit = 1 * time.Minute
)

const (
	CronJobBackoffLimit               = 3
	CronJobSuccessfulJobsHistoryLimit = 1
	VolumeSnapshotsWarningThreshold   = 100

	LastAppliedCronJobSpecAnnotationKeySuffix = "last-applied-cronjob-spec"
)

type VolumeController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID   string
	ManagerImage   string
	ServiceAccount string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	vStoreSynced  cache.InformerSynced
	eStoreSynced  cache.InformerSynced
	rStoreSynced  cache.InformerSynced
	smStoreSynced cache.InformerSynced

	scheduler *scheduler.ReplicaScheduler

	backoff *flowcontrol.Backoff

	// for unit test
	nowHandler func() string
}

func NewVolumeController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	volumeInformer lhinformers.VolumeInformer,
	engineInformer lhinformers.EngineInformer,
	replicaInformer lhinformers.ReplicaInformer,
	shareManagerInformer lhinformers.ShareManagerInformer,
	kubeClient clientset.Interface,
	namespace, controllerID, serviceAccount string,
	managerImage string) *VolumeController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	vc := &VolumeController{
		baseController: newBaseController("longhorn-volume", logger),

		ds:             ds,
		namespace:      namespace,
		controllerID:   controllerID,
		ManagerImage:   managerImage,
		ServiceAccount: serviceAccount,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-volume-controller"}),

		vStoreSynced:  volumeInformer.Informer().HasSynced,
		eStoreSynced:  engineInformer.Informer().HasSynced,
		rStoreSynced:  replicaInformer.Informer().HasSynced,
		smStoreSynced: shareManagerInformer.Informer().HasSynced,

		backoff: flowcontrol.NewBackOff(time.Minute, time.Minute*3),

		nowHandler: util.Now,
	}

	vc.scheduler = scheduler.NewReplicaScheduler(ds)

	volumeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    vc.enqueueVolume,
		UpdateFunc: func(old, cur interface{}) { vc.enqueueVolume(cur) },
		DeleteFunc: vc.enqueueVolume,
	})
	engineInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    vc.enqueueControlleeChange,
		UpdateFunc: func(old, cur interface{}) { vc.enqueueControlleeChange(cur) },
		DeleteFunc: vc.enqueueControlleeChange,
	}, 0)
	replicaInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    vc.enqueueControlleeChange,
		UpdateFunc: func(old, cur interface{}) { vc.enqueueControlleeChange(cur) },
		DeleteFunc: vc.enqueueControlleeChange,
	}, 0)
	shareManagerInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    vc.enqueueVolumesForShareManager,
		UpdateFunc: func(old, cur interface{}) { vc.enqueueVolumesForShareManager(cur) },
		DeleteFunc: vc.enqueueVolumesForShareManager,
	}, 0)
	return vc
}

func (vc *VolumeController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer vc.queue.ShutDown()

	vc.logger.Infof("Start Longhorn volume controller")
	defer vc.logger.Infof("Shutting down Longhorn volume controller")

	if !cache.WaitForNamedCacheSync("longhorn engines", stopCh, vc.vStoreSynced, vc.eStoreSynced, vc.rStoreSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(vc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (vc *VolumeController) worker() {
	for vc.processNextWorkItem() {
	}
}

func (vc *VolumeController) processNextWorkItem() bool {
	key, quit := vc.queue.Get()

	if quit {
		return false
	}
	defer vc.queue.Done(key)

	err := vc.syncVolume(key.(string))
	vc.handleErr(err, key)

	return true
}

func (vc *VolumeController) handleErr(err error, key interface{}) {
	if err == nil {
		vc.queue.Forget(key)
		return
	}

	if vc.queue.NumRequeues(key) < maxRetries {
		vc.logger.WithError(err).Warnf("Error syncing Longhorn volume %v", key)
		vc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	vc.logger.WithError(err).Warnf("Dropping Longhorn volume %v out of the queue", key)
	vc.queue.Forget(key)
}

func getLoggerForVolume(logger logrus.FieldLogger, v *longhorn.Volume) *logrus.Entry {
	log := logger.WithFields(
		logrus.Fields{
			"volume":     v.Name,
			"frontend":   v.Spec.Frontend,
			"state":      v.Status.State,
			"owner":      v.Status.OwnerID,
			"accessMode": v.Spec.AccessMode,
			"migratable": v.Spec.Migratable,
		},
	)

	if v.Spec.AccessMode == types.AccessModeReadWriteMany {
		log = log.WithFields(
			logrus.Fields{
				"shareState":    v.Status.ShareState,
				"shareEndpoint": v.Status.ShareEndpoint,
			})
	}

	return log
}

func (vc *VolumeController) syncVolume(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != vc.namespace {
		// Not ours, don't do anything
		return nil
	}

	volume, err := vc.ds.GetVolume(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			vc.logger.WithField("volume", name).Debug("Longhorn volume has been deleted")
			return nil
		}
		return err
	}

	log := getLoggerForVolume(vc.logger, volume)

	defaultEngineImage, err := vc.ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if err != nil {
		return err
	}
	isResponsible, err := vc.isResponsibleFor(volume, defaultEngineImage)
	if err != nil {
		return err
	}
	if !isResponsible {
		return nil
	}

	if volume.Status.OwnerID != vc.controllerID {
		volume.Status.OwnerID = vc.controllerID
		volume, err = vc.ds.UpdateVolumeStatus(volume)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Debugf("Volume got new owner %v", vc.controllerID)
	}

	engines, err := vc.ds.ListVolumeEngines(volume.Name)
	if err != nil {
		return err
	}
	replicas, err := vc.ds.ListVolumeReplicas(volume.Name)
	if err != nil {
		return err
	}

	if volume.DeletionTimestamp != nil {
		if volume.Status.State != types.VolumeStateDeleting {
			volume.Status.State = types.VolumeStateDeleting
			volume, err = vc.ds.UpdateVolumeStatus(volume)
			if err != nil {
				return err
			}
			vc.eventRecorder.Eventf(volume, v1.EventTypeNormal, EventReasonDelete, "Deleting volume %v", volume.Name)
		}

		if volume.Spec.AccessMode == types.AccessModeReadWriteMany {
			log.Info("Removing share manager for deleted volume")
			if err := vc.ds.DeleteShareManager(volume.Name); err != nil && !datastore.ErrorIsNotFound(err) {
				return err
			}
		}

		for _, job := range volume.Spec.RecurringJobs {
			if err := vc.ds.DeleteCronJob(types.GetCronJobNameForVolumeAndJob(volume.Name, job.Name)); err != nil {
				return err
			}
		}

		for _, e := range engines {
			if e.DeletionTimestamp == nil {
				if err := vc.ds.DeleteEngine(e.Name); err != nil {
					return err
				}
			}
		}
		for _, r := range replicas {
			if r.DeletionTimestamp == nil {
				if err := vc.ds.DeleteReplica(r.Name); err != nil {
					return err
				}
			}
		}

		kubeStatus := volume.Status.KubernetesStatus

		if kubeStatus.PVName != "" {
			if err := vc.ds.DeletePersistentVolume(kubeStatus.PVName); err != nil {
				if !datastore.ErrorIsNotFound(err) {
					return err
				}
			}
		}

		if kubeStatus.PVCName != "" && kubeStatus.LastPVCRefAt == "" {
			if err := vc.ds.DeletePersistentVolumeClaim(kubeStatus.Namespace, kubeStatus.PVCName); err != nil {
				if !datastore.ErrorIsNotFound(err) {
					return err
				}
			}
		}

		// now replicas and engines have been marked for deletion

		if engines, err := vc.ds.ListVolumeEngines(volume.Name); err != nil {
			return err
		} else if len(engines) > 0 {
			return nil
		}
		if replicas, err := vc.ds.ListVolumeReplicas(volume.Name); err != nil {
			return err
		} else if len(replicas) > 0 {
			return nil
		}
		// now replicas and engines are deleted

		return vc.ds.RemoveFinalizerForVolume(volume)
	}

	existingVolume := volume.DeepCopy()
	existingEngines := map[string]*longhorn.Engine{}
	for k, e := range engines {
		existingEngines[k] = e.DeepCopy()
	}
	existingReplicas := map[string]*longhorn.Replica{}
	for k, r := range replicas {
		existingReplicas[k] = r.DeepCopy()
	}
	defer func() {
		var lastErr error
		// create/delete engine/replica has been handled already
		// so we only need to worry about entries in the current list
		for k, r := range replicas {
			if existingReplicas[k] == nil ||
				!reflect.DeepEqual(existingReplicas[k].Spec, r.Spec) {
				if _, err := vc.ds.UpdateReplica(r); err != nil {
					lastErr = err
				}
			}
		}
		// stop updating if replicas weren't fully updated
		if lastErr == nil {
			for k, e := range engines {
				if existingEngines[k] == nil ||
					!reflect.DeepEqual(existingEngines[k].Spec, e.Spec) {
					if _, err := vc.ds.UpdateEngine(e); err != nil {
						lastErr = err
					}
				}
			}
		}
		// stop updating if engines and replicas weren't fully updated
		if lastErr == nil {
			// Make sure that we don't update condition's LastTransitionTime if the condition's values hasn't changed
			handleConditionLastTransitionTime(&existingVolume.Status, &volume.Status)
			if !reflect.DeepEqual(existingVolume.Status, volume.Status) {
				// reuse err
				_, err = vc.ds.UpdateVolumeStatus(volume)
			}
		}
		// requeue if it's conflict
		if apierrors.IsConflict(errors.Cause(err)) || apierrors.IsConflict(errors.Cause(lastErr)) {
			log.Debugf("Requeue volume due to error %v or %v", err, lastErr)
			vc.enqueueVolume(volume)
			err = nil
		}
	}()

	engine, err := vc.getCurrentEngine(volume, engines)
	if err != nil {
		return err
	}
	if len(engines) <= 1 {
		if err := vc.ReconcileEngineReplicaState(volume, engine, replicas); err != nil {
			return err
		}

		if err := vc.updateRecurringJobs(volume); err != nil {
			return err
		}

		if err := vc.upgradeEngineForVolume(volume, engine, replicas); err != nil {
			return err
		}
	}

	if err := vc.processMigration(volume, engines, replicas); err != nil {
		return err
	}

	if err := vc.ReconcileShareManagerState(volume); err != nil {
		return err
	}

	if err := vc.ReconcileVolumeState(volume, engines, replicas); err != nil {
		return err
	}

	if err := vc.cleanupReplicas(volume, engines, replicas); err != nil {
		return err
	}

	return nil
}

// handleConditionLastTransitionTime rollback to the existing condition object if condition's values hasn't changed
func handleConditionLastTransitionTime(existingStatus, newStatus *types.VolumeStatus) {
	for conditionType, condition := range newStatus.Conditions {
		existingCondition, ok := existingStatus.Conditions[conditionType]
		if ok &&
			existingCondition.Status == condition.Status &&
			existingCondition.Reason == condition.Reason &&
			existingCondition.Message == condition.Message {
			newStatus.Conditions[conditionType] = existingCondition
		}
	}
}

func (vc *VolumeController) getCurrentEngine(v *longhorn.Volume, es map[string]*longhorn.Engine) (*longhorn.Engine, error) {
	if len(es) == 0 {
		return nil, nil
	}

	if len(es) == 1 {
		for _, e := range es {
			return e, nil
		}
	}

	// len(es) > 1
	node := v.Status.CurrentNodeID
	if node != "" {
		for _, e := range es {
			if e.Spec.NodeID == node {
				return e, nil
			}
		}
		return nil, fmt.Errorf("BUG: multiple engines detected but none matched volume %v attached node %v", v.Name, node)
	}
	return nil, fmt.Errorf("BUG: multiple engines detected when volume %v is detached", v.Name)
}

// EvictReplicas do creating one more replica for eviction, if requested
func (vc *VolumeController) EvictReplicas(v *longhorn.Volume,
	e *longhorn.Engine, rs map[string]*longhorn.Replica, healthyCount int) (err error) {
	log := getLoggerForVolume(vc.logger, v)

	for _, replica := range rs {
		if replica.Status.EvictionRequested == true &&
			healthyCount == v.Spec.NumberOfReplicas {
			if err := vc.replenishReplicas(v, e, rs, ""); err != nil {
				log.WithError(err).Error("Failed to create new replica for replica eviction")
				vc.eventRecorder.Eventf(v, v1.EventTypeWarning,
					EventReasonFailedEviction,
					"volume %v failed to create one more replica", v.Name)
				return err
			}
			log.Debug("Creating one more replica for eviction")
			break
		}
	}

	return nil
}

// ReconcileEngineReplicaState will get the current main engine e.Status.ReplicaModeMap, e.Status.RestoreStatus,
// and e.Status.purgeStatus then update v and rs accordingly.
func (vc *VolumeController) ReconcileEngineReplicaState(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to reconcile engine/replica state for %v", v.Name)
		if v.Status.Robustness != types.VolumeRobustnessDegraded {
			v.Status.LastDegradedAt = ""
		}
	}()
	if e == nil {
		return nil
	}

	if e.Status.CurrentState == types.InstanceStateUnknown {
		if v.Status.Robustness != types.VolumeRobustnessUnknown {
			v.Status.Robustness = types.VolumeRobustnessUnknown
			vc.eventRecorder.Eventf(v, v1.EventTypeWarning, EventReasonUnknown, "volume %v robustness is unknown", v.Name)
		}
		return nil
	}
	if e.Status.CurrentState != types.InstanceStateRunning {
		return nil
	}

	// wait for monitoring to start
	if e.Status.ReplicaModeMap == nil {
		return nil
	}

	replicaList := []*longhorn.Replica{}
	for _, r := range rs {
		replicaList = append(replicaList, r)
	}

	restoreStatusMap := map[string]*types.RestoreStatus{}
	for addr, status := range e.Status.RestoreStatus {
		rName := datastore.ReplicaAddressToReplicaName(addr, replicaList)
		if _, exists := rs[rName]; exists {
			restoreStatusMap[rName] = status
		}
	}

	purgeStatusMap := map[string]*types.PurgeStatus{}
	for addr, status := range e.Status.PurgeStatus {
		rName := datastore.ReplicaAddressToReplicaName(addr, replicaList)
		if _, exists := rs[rName]; exists {
			purgeStatusMap[rName] = status
		}
	}

	// 1. remove ERR replicas
	// 2. count RW replicas
	healthyCount := 0
	for rName, mode := range e.Status.ReplicaModeMap {
		r := rs[rName]
		if r == nil {
			continue
		}
		restoreStatus := restoreStatusMap[rName]
		purgeStatus := purgeStatusMap[rName]
		if mode == types.ReplicaModeERR ||
			(restoreStatus != nil && restoreStatus.Error != "") ||
			(purgeStatus != nil && purgeStatus.Error != "") {
			if restoreStatus != nil && restoreStatus.Error != "" {
				vc.eventRecorder.Eventf(v, v1.EventTypeWarning, EventReasonFailedRestore, "replica %v failed the restore: %s", r.Name, restoreStatus.Error)
			}
			if purgeStatus != nil && purgeStatus.Error != "" {
				vc.eventRecorder.Eventf(v, v1.EventTypeWarning, EventReasonFailedSnapshotPurge, "replica %v failed the snapshot purge: %s", r.Name, purgeStatus.Error)
			}
			if r.Spec.FailedAt == "" {
				r.Spec.FailedAt = vc.nowHandler()
				e.Spec.LogRequested = true
				r.Spec.LogRequested = true
			}
			r.Spec.DesireState = types.InstanceStateStopped
		} else if mode == types.ReplicaModeRW {
			// record once replica became healthy, so if it
			// failed in the future, we can tell it apart
			// from replica failed during rebuilding
			if r.Spec.HealthyAt == "" {
				vc.backoff.DeleteEntry(r.Name)
				r.Spec.HealthyAt = vc.nowHandler()
				r.Spec.RebuildRetryCount = 0
			}
			healthyCount++
		}
	}
	// If a replica failed at attaching stage,
	// there is no record in e.Status.ReplicaModeMap
	for _, r := range rs {
		if r.Spec.FailedAt == "" && r.Status.CurrentState == types.InstanceStateError {
			e.Spec.LogRequested = true
			r.Spec.LogRequested = true
			r.Spec.FailedAt = vc.nowHandler()
			r.Spec.DesireState = types.InstanceStateStopped
		}
	}
	oldRobustness := v.Status.Robustness
	if healthyCount == 0 { // no healthy replica exists, going to faulted
		// ReconcileVolumeState() will deal with the faulted case
		return nil
	} else if healthyCount >= v.Spec.NumberOfReplicas {
		v.Status.Robustness = types.VolumeRobustnessHealthy
		if oldRobustness == types.VolumeRobustnessDegraded {
			vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonHealthy, "volume %v became healthy", v.Name)
		}

		// Evict replicas for the volume
		if err := vc.EvictReplicas(v, e, rs, healthyCount); err != nil {
			return err
		}

		// Migrate local replica when Data Locality is on
		// We turn off data locality while doing auto-attaching or restoring (e.g. frontend is disabled)
		if v.Status.State == types.VolumeStateAttached && !v.Status.FrontendDisabled &&
			!isDataLocalityDisabled(v) && !hasLocalReplicaOnSameNodeAsEngine(e, rs) {
			if err := vc.replenishReplicas(v, e, rs, e.Spec.NodeID); err != nil {
				return err
			}
		}

	} else { // healthyCount < v.Spec.NumberOfReplicas
		v.Status.Robustness = types.VolumeRobustnessDegraded
		if oldRobustness != types.VolumeRobustnessDegraded {
			v.Status.LastDegradedAt = vc.nowHandler()
			vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonDegraded, "volume %v became degraded", v.Name)
		}

		cliAPIVersion, err := vc.ds.GetEngineImageCLIAPIVersion(e.Status.CurrentImage)
		if err != nil {
			return err
		}
		// Rebuild is not supported when:
		//   1. the volume is old restore/DR volumes.
		//   2. the volume is expanding size.
		if !((v.Status.IsStandby || v.Status.RestoreRequired) && cliAPIVersion < engineapi.CLIVersionFour) &&
			v.Spec.Size == e.Status.CurrentSize {
			if err := vc.replenishReplicas(v, e, rs, ""); err != nil {
				return err
			}
		}
		// replicas will be started by ReconcileVolumeState() later
	}
	return nil
}

func getHealthyReplicaCount(rs map[string]*longhorn.Replica) int {
	count := 0
	for _, r := range rs {
		if r.Spec.FailedAt == "" && r.Spec.HealthyAt != "" {
			count++
		}
	}
	return count
}

func getFailedReplicaCount(rs map[string]*longhorn.Replica) int {
	count := 0
	for _, r := range rs {
		if r.Spec.FailedAt != "" {
			count++
		}
	}
	return count
}

func (vc *VolumeController) hasReplicaEvictionRequested(rs map[string]*longhorn.Replica) bool {
	for _, r := range rs {
		if r.Status.EvictionRequested == true {
			return true
		}
	}

	return false
}

func (vc *VolumeController) cleanupReplicas(v *longhorn.Volume, es map[string]*longhorn.Engine, rs map[string]*longhorn.Replica) error {
	// TODO: I don't think it's a good idea to cleanup replicas during a migration or engine image update
	// 	since the getHealthyReplicaCount function doesn't differentiate between replicas of different engines
	// 	then during cleanupExtraHealthyReplicas the condition `healthyCount > v.Spec.NumberOfReplicas` will be true
	//  which can lead to incorrect deletion of replicas.
	if vc.isVolumeMigrating(v) || vc.isVolumeUpgrading(v) {
		return nil
	}

	e, err := vc.getCurrentEngine(v, es)
	if err != nil {
		return err
	}

	if err := vc.cleanupCorruptedOrStaleReplicas(v, rs); err != nil {
		return err
	}

	if err := vc.cleanupFailedToScheduledReplicas(v, rs); err != nil {
		return err
	}

	if err := vc.cleanupExtraHealthyReplicas(v, e, rs); err != nil {
		return err
	}
	return nil
}

func (vc *VolumeController) cleanupCorruptedOrStaleReplicas(v *longhorn.Volume, rs map[string]*longhorn.Replica) error {
	healthyCount := getHealthyReplicaCount(rs)
	cleanupLeftoverReplicas := !vc.isVolumeUpgrading(v) && !vc.isVolumeMigrating(v)
	log := getLoggerForVolume(vc.logger, v)

	for _, r := range rs {
		if cleanupLeftoverReplicas {
			if !r.Spec.Active {
				// Leftover by live upgrade. Successful or not, there are replicas left to clean up
				if err := vc.deleteReplica(r, rs); err != nil {
					return err
				}
				continue
			} else if r.Spec.EngineImage != v.Spec.EngineImage {
				// r.Spec.Active shouldn't be set for the leftover replicas, something must wrong
				log.WithField("replica", r.Name).Errorf("BUG: replica engine image %v is different from volume engine image %v, "+
					"but replica spec.Active has been set", r.Spec.EngineImage, v.Spec.EngineImage)
			}
		}

		if r.Spec.FailedAt == "" {
			continue
		}

		if r.DeletionTimestamp != nil {
			continue
		}
		staled := false
		if v.Spec.StaleReplicaTimeout > 0 && util.TimestampAfterTimeout(r.Spec.FailedAt,
			time.Duration(int64(v.Spec.StaleReplicaTimeout*60))*time.Second) {

			staled = true
		}

		// 1. failed for multiple times or failed at rebuilding (`Spec.RebuildRetryCount` of a newly created rebuilding replica is `FailedReplicaMaxRetryCount`) before ever became healthy/ mode RW,
		// 2. failed too long ago, became stale and unnecessary to keep around, unless we don't have any healthy replicas
		if (r.Spec.RebuildRetryCount >= scheduler.FailedReplicaMaxRetryCount) || (healthyCount != 0 && staled) {
			log.WithField("replica", r.Name).Info("Cleaning up corrupted, staled replica")
			if err := vc.deleteReplica(r, rs); err != nil {
				return errors.Wrapf(err, "cannot cleanup staled replica %v", r.Name)
			}
		}
	}

	return nil
}

func (vc *VolumeController) cleanupFailedToScheduledReplicas(v *longhorn.Volume, rs map[string]*longhorn.Replica) (err error) {
	healthyCount := getHealthyReplicaCount(rs)
	hasEvictionRequestedReplicas := vc.hasReplicaEvictionRequested(rs)

	if healthyCount >= v.Spec.NumberOfReplicas {
		for _, r := range rs {
			if !hasEvictionRequestedReplicas {
				if r.Spec.HealthyAt == "" && r.Spec.FailedAt == "" && r.Spec.NodeID == "" &&
					(isDataLocalityDisabled(v) || r.Spec.HardNodeAffinity != v.Status.CurrentNodeID) {
					logrus.Infof("Cleaning up failed to scheduled replica %v", r.Name)
					if err := vc.deleteReplica(r, rs); err != nil {
						return errors.Wrapf(err, "cannot cleanup failed to scheduled replica %v", r.Name)
					}
				}
			}
		}
	}

	return nil
}

func (vc *VolumeController) cleanupExtraHealthyReplicas(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) (err error) {
	healthyCount := getHealthyReplicaCount(rs)

	if healthyCount > v.Spec.NumberOfReplicas {
		for _, r := range rs {
			// Clean up eviction requested replica
			if r.Status.EvictionRequested {
				if err := vc.deleteReplica(r, rs); err != nil {
					vc.eventRecorder.Eventf(v, v1.EventTypeWarning,
						EventReasonFailedEviction,
						"volume %v failed to evict replica %v",
						v.Name, r.Name)
					return err
				}
				logrus.Debugf("Evicted replica %v", r.Name)
				return nil
			}
		}
	}

	// Clean up extra healthy replica with data-locality finished
	healthyCount = getHealthyReplicaCount(rs)

	if healthyCount > v.Spec.NumberOfReplicas &&
		!isDataLocalityDisabled(v) &&
		hasLocalReplicaOnSameNodeAsEngine(e, rs) {
		rNames, err := vc.getPreferredReplicaCandidatesForDeletion(rs)
		if err != nil {
			return err
		}

		// Randomly delete extra non-local healthy replicas in the preferred candidate list rNames
		// Sometime cleanupExtraHealthyReplicas() is called more than once with the same input (v,e,rs).
		// To make the deleting operation idempotent and prevent deleting more replica than needed,
		// we always delete the replica with the smallest name.
		sort.Strings(rNames)
		for _, rName := range rNames {
			r := rs[rName]
			if r.Spec.NodeID != e.Spec.NodeID {
				return vc.deleteReplica(r, rs)
			}
		}
	}

	return nil
}

// ReconcileVolumeState handles the attaching and detaching of volume
func (vc *VolumeController) ReconcileVolumeState(v *longhorn.Volume, es map[string]*longhorn.Engine, rs map[string]*longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to reconcile volume state for %v", v.Name)
	}()

	log := getLoggerForVolume(vc.logger, v)

	e, err := vc.getCurrentEngine(v, es)
	if err != nil {
		return err
	}

	newVolume := false
	if v.Status.CurrentImage == "" {
		v.Status.CurrentImage = v.Spec.EngineImage
	}

	if err := vc.checkAndInitVolumeRestore(v); err != nil {
		return err
	}
	if err := vc.updateRequestedBackupForVolumeRestore(v, e); err != nil {
		return err
	}

	if v.Status.CurrentNodeID == "" {
		// `PendingNodeID != ""` means the engine dead unexpectedly.
		// The volume will be detached by cleaning up `CurrentNodeID` then be reattached by `v.Status.CurrentNodeID = v.Status.PendingNodeID`.
		// Hence this auto attachment logic shouldn't set `CurrentNodeID` in this case.
		if v.Status.PendingNodeID == "" && v.Status.Robustness != types.VolumeRobustnessFaulted {
			v.Status.CurrentNodeID = v.Spec.NodeID
		}
	} else if v.Spec.NodeID != "" && v.Spec.NodeID != v.Status.CurrentNodeID {
		// in the case where the currentNode id upgrade from the migration failed
		// we can check if we have a current active engine and if it's on v.Spec
		// then it's safe to switch the current node id to v.Spec
		// TODO: we still need to rework all of this, so that the user can just specify a nodeID
		// 	instead of having to go through the detach/attach cycle, like in the case of the share-manager
		if e != nil && e.Spec.NodeID == v.Spec.NodeID && e.DeletionTimestamp == nil {
			vc.logger.Infof("switching current node id from %v to %v since we have an engine on that node already", v.Status.CurrentNodeID, v.Spec.NodeID)
			v.Status.CurrentNodeID = v.Spec.NodeID

			// ensure that the replicas are marked as active
			if err := vc.switchActiveReplicas(rs, func(r *longhorn.Replica, engineName string) bool {
				return r.Spec.EngineName == engineName
			}, e.Name); err != nil {
				return err
			}
		}

		return fmt.Errorf("volume %v has already attached to node %v, but asked to attach to node %v", v.Name, v.Status.CurrentNodeID, v.Spec.NodeID)
	}

	if e == nil {
		// first time creation
		e, err = vc.createEngine(v)
		if err != nil {
			return err
		}
		newVolume = true
		es[e.Name] = e
	}

	if len(e.Status.Snapshots) != 0 {
		actualSize := int64(0)
		for _, snapshot := range e.Status.Snapshots {
			size, err := util.ConvertSize(snapshot.Size)
			if err != nil {
				log.WithField("snapshot", snapshot.Name).WithError(err).Errorf("BUG: unable to parse snapshot size %v", snapshot.Size)
				// continue checking for other snapshots
			}
			actualSize += size
		}
		v.Status.ActualSize = actualSize
	}

	if len(e.Status.Snapshots) > VolumeSnapshotsWarningThreshold {
		v.Status.Conditions = types.SetCondition(v.Status.Conditions,
			types.VolumeConditionTypeTooManySnapshots, types.ConditionStatusTrue,
			types.VolumeConditionReasonTooManySnapshots, fmt.Sprintf("Snapshots count is %v over the warning threshold %v", len(e.Status.Snapshots), VolumeSnapshotsWarningThreshold))
	} else {
		v.Status.Conditions = types.SetCondition(v.Status.Conditions,
			types.VolumeConditionTypeTooManySnapshots, types.ConditionStatusFalse,
			"", "")
	}

	if len(rs) == 0 {
		// first time creation
		if err := vc.replenishReplicas(v, e, rs, ""); err != nil {
			return err
		}
	}

	needReplicaLogs := false
	for _, r := range rs {
		if r.Spec.LogRequested && r.Status.LogFetched {
			r.Spec.LogRequested = false
		}
		needReplicaLogs = needReplicaLogs || r.Spec.LogRequested
		rs[r.Name] = r
	}
	if e.Spec.LogRequested && e.Status.LogFetched && !needReplicaLogs {
		e.Spec.LogRequested = false
	}

	scheduled := true
	for _, r := range rs {
		// check whether the replica need to be scheduled
		if r.Spec.NodeID != "" {
			continue
		}
		scheduledReplica, err := vc.scheduler.ScheduleReplica(r, rs, v)
		if err != nil {
			return err
		}
		if scheduledReplica == nil {
			if r.Spec.HardNodeAffinity == "" {
				log.WithField("replica", r.Name).Error("unable to schedule replica")
				v.Status.Conditions = types.SetCondition(v.Status.Conditions,
					types.VolumeConditionTypeScheduled, types.ConditionStatusFalse,
					types.VolumeConditionReasonReplicaSchedulingFailure, "")
			} else {
				log.WithField("replica", r.Name).Errorf("unable to schedule replica of volume with HardNodeAffinity = %v", r.Spec.HardNodeAffinity)
				v.Status.Conditions = types.SetCondition(v.Status.Conditions,
					types.VolumeConditionTypeScheduled, types.ConditionStatusFalse,
					types.VolumeConditionReasonLocalReplicaSchedulingFailure, "")
			}
			scheduled = false
		} else {
			rs[r.Name] = scheduledReplica
		}
	}
	if scheduled {
		v.Status.Conditions = types.SetCondition(v.Status.Conditions,
			types.VolumeConditionTypeScheduled, types.ConditionStatusTrue, "", "")
	} else if v.Status.CurrentNodeID == "" {
		allowCreateDegraded, err := vc.ds.GetSettingAsBool(types.SettingNameAllowVolumeCreationWithDegradedAvailability)
		if err != nil {
			return err
		}
		if allowCreateDegraded {
			atLeastOneReplicaAvailable := false
			for _, r := range rs {
				if r.Spec.NodeID != "" && r.Spec.FailedAt == "" {
					atLeastOneReplicaAvailable = true
					break
				}
			}
			if atLeastOneReplicaAvailable {
				v.Status.Conditions = types.SetCondition(v.Status.Conditions,
					types.VolumeConditionTypeScheduled, types.ConditionStatusTrue, "",
					"Reset schedulable due to allow volume creation with degraded availability")
				scheduled = true
			}
		}
	}

	if err := vc.reconcileVolumeSize(v, e, rs); err != nil {
		return err
	}

	if err := vc.checkForAutoAttachment(v, e, rs, scheduled); err != nil {
		return err
	}
	if err := vc.checkForAutoDetachment(v, e, rs); err != nil {
		return err
	}

	// The frontend should be disabled for auto attached volumes.
	// The exception is that the frontend should be enabled for the block device expansion during the offline expansion.
	if v.Spec.NodeID == "" && v.Status.CurrentNodeID != "" {
		v.Status.FrontendDisabled = true
	} else {
		v.Status.FrontendDisabled = v.Spec.DisableFrontend
	}

	// Clear SalvageRequested flag if SalvageExecuted flag has been set.
	if e.Spec.SalvageRequested && e.Status.SalvageExecuted {
		e.Spec.SalvageRequested = false
	}

	isAutoSalvageNeeded := getHealthyReplicaCount(rs) == 0 && getFailedReplicaCount(rs) > 0
	if isAutoSalvageNeeded {
		v.Status.Robustness = types.VolumeRobustnessFaulted
		v.Status.CurrentNodeID = ""

		autoSalvage, err := vc.ds.GetSettingAsBool(types.SettingNameAutoSalvage)
		if err != nil {
			return err
		}
		// To make sure that we don't miss the `isAutoSalvageNeeded` event, This IF statement makes sure the `e.Spec.SalvageRequested=true`
		// persist in ETCD before Longhorn salvages the failed replicas in the IF statement below it.
		// More explanation: when all replicas fails, Longhorn tries to set `e.Spec.SalvageRequested=true`
		// and try to detach the volume by setting `v.Status.CurrentNodeID = ""`.
		// Because at the end of volume syncVolume(), Longhorn updates CRs in the order: replicas, engine, volume,
		// when volume changes from v.Status.State == types.VolumeStateAttached to v.Status.State == types.VolumeStateDetached,
		// we know that volume RS has been updated and therefore the engine RS also has been updated and persisted in ETCD.
		// At this moment, Longhorn goes into the IF statement below this IF statement and salvage all replicas.
		if autoSalvage && !v.Status.IsStandby && !v.Status.RestoreRequired {
			// Since all replica failed and autoSalvage is enable, mark engine controller salvage requested
			e.Spec.SalvageRequested = true
			log.Infof("All replicas are failed, set engine salvageRequested to %v", e.Spec.SalvageRequested)
		}
		// make sure the volume is detached before automatically salvage replicas
		if autoSalvage && v.Status.State == types.VolumeStateDetached && !v.Status.IsStandby && !v.Status.RestoreRequired {
			// There is no need to auto salvage (and reattach) a volume on an unavailable node
			// If we return error we will loop forever, instead we can do auto salvage even if the volume should be detached
			// since all auto salvage does is unset the failure state for the replicas
			// on a single replica volume, if the replica is on a failed node, it will be skipped below
			// and the volume will remain in the faulted state but as soon as the node of the replica comes back
			// we can do the auto salvage operation and set the Robustness to Unknown.
			shouldBeAttached := v.Spec.NodeID != ""
			isNodeDownOrDeleted, err := vc.ds.IsNodeDownOrDeleted(v.Spec.NodeID)
			if err != nil && shouldBeAttached {
				return err
			}
			if !isNodeDownOrDeleted || !shouldBeAttached {
				lastFailedAt := time.Time{}
				failedUsableReplicas := map[string]*longhorn.Replica{}
				dataExists := false

				for _, r := range rs {
					if r.Spec.HealthyAt == "" {
						continue
					}
					dataExists = true
					if r.Spec.NodeID == "" || r.Spec.DiskID == "" {
						continue
					}
					if isDownOrDeleted, err := vc.ds.IsNodeDownOrDeleted(r.Spec.NodeID); err != nil {
						log.WithField("replica", r.Name).WithError(err).Errorf("Unable to check if node %v is still running for failed replica", r.Spec.NodeID)
						continue
					} else if isDownOrDeleted {
						continue
					}
					node, err := vc.ds.GetNode(r.Spec.NodeID)
					if err != nil {
						log.WithField("replica", r.Name).WithError(err).Errorf("Unable to get node %v for failed replica", r.Spec.NodeID)
					}
					diskSchedulable := false
					for _, diskStatus := range node.Status.DiskStatus {
						if diskStatus.DiskUUID == r.Spec.DiskID {
							if types.GetCondition(diskStatus.Conditions, types.DiskConditionTypeSchedulable).Status == types.ConditionStatusTrue {
								diskSchedulable = true
								break
							}
						}
					}
					if !diskSchedulable {
						continue
					}
					failedAt, err := util.ParseTime(r.Spec.FailedAt)
					if err != nil {
						log.WithField("replica", r.Name).WithError(err).Error("Unable to parse FailedAt timestamp for replica")
						continue
					}
					if failedAt.After(lastFailedAt) {
						lastFailedAt = failedAt
					}
					// all failedUsableReplica contains data
					failedUsableReplicas[r.Name] = r
				}
				if !dataExists {
					log.Warn("Cannot auto salvage volume: no data exists")
				} else {
					// This salvage is for revision counter enabled case
					salvaged := false
					// Bring up the replicas for auto-salvage
					for _, r := range failedUsableReplicas {
						if util.TimestampWithinLimit(lastFailedAt, r.Spec.FailedAt, AutoSalvageTimeLimit) {
							r.Spec.FailedAt = ""
							log.WithField("replica", r.Name).Warn("Automatically salvaging volume replica")
							msg := fmt.Sprintf("Replica %v of volume %v will be automatically salvaged", r.Name, v.Name)
							vc.eventRecorder.Event(v, v1.EventTypeWarning, EventReasonAutoSalvaged, msg)
							salvaged = true
						}
					}
					if salvaged {
						// remount the reattached volume later if possible
						// For the auto-salvaged volume, `v.Status.CurrentNodeID` is empty but `v.Spec.NodeID` shouldn't be empty.
						// There shouldn't be any problems if v.Spec.NodeID is empty, since the volume is desired to be detached
						// so we just unset PendingNodeID.
						v.Status.PendingNodeID = v.Spec.NodeID
						v.Status.RemountRequestedAt = vc.nowHandler()
						msg := fmt.Sprintf("Volume %v requested remount at %v", v.Name, v.Status.RemountRequestedAt)
						vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonRemount, msg)
						v.Status.Robustness = types.VolumeRobustnessUnknown
					}
				}
			}
		}
	} else { // !isAutoSalvageNeeded
		if v.Status.Robustness == types.VolumeRobustnessFaulted {
			v.Status.Robustness = types.VolumeRobustnessUnknown
			// The volume was faulty and there are usable replicas.
			// Therefore, we set RemountRequestedAt so that KubernetesPodController restarts the workload pod
			v.Status.RemountRequestedAt = vc.nowHandler()
			msg := fmt.Sprintf("Volume %v requested remount at %v", v.Name, v.Status.RemountRequestedAt)
			vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonRemount, msg)
		}

		// reattach volume if detached unexpected and there are still healthy replicas
		if e.Status.CurrentState == types.InstanceStateError && v.Status.CurrentNodeID != "" {
			v.Status.PendingNodeID = v.Status.CurrentNodeID
			log.Warn("Engine of volume dead unexpectedly, reattach the volume")
			msg := fmt.Sprintf("Engine of volume %v dead unexpectedly, reattach the volume", v.Name)
			vc.eventRecorder.Event(v, v1.EventTypeWarning, EventReasonDetachedUnexpectly, msg)
			e.Spec.LogRequested = true
			for _, r := range rs {
				if r.Status.CurrentState == types.InstanceStateRunning {
					r.Spec.LogRequested = true
					rs[r.Name] = r
				}
			}
			v.Status.Robustness = types.VolumeRobustnessFaulted
			v.Status.CurrentNodeID = ""
		}
	}

	oldState := v.Status.State
	if v.Status.CurrentNodeID == "" {
		// the final state will be determined at the end of the clause
		if newVolume {
			v.Status.State = types.VolumeStateCreating
		} else if v.Status.State != types.VolumeStateDetached {
			v.Status.State = types.VolumeStateDetaching
		}

		v.Status.Conditions = types.SetCondition(v.Status.Conditions,
			types.VolumeConditionTypeRestore, types.ConditionStatusFalse, "", "")

		if v.Status.Robustness != types.VolumeRobustnessFaulted {
			v.Status.Robustness = types.VolumeRobustnessUnknown
		} else {
			if v.Status.RestoreRequired || v.Status.IsStandby {
				v.Status.Conditions = types.SetCondition(v.Status.Conditions,
					types.VolumeConditionTypeRestore, types.ConditionStatusFalse, types.VolumeConditionReasonRestoreFailure, "All replica restore failed and the volume became Faulted")
			}
		}

		// check if any replica has been RW yet
		dataExists := false
		for _, r := range rs {
			if r.Spec.HealthyAt != "" {
				dataExists = true
				break
			}
		}
		// stop rebuilding
		if dataExists {
			for _, r := range rs {
				if r.Spec.HealthyAt == "" && r.Spec.FailedAt == "" {
					r.Spec.FailedAt = vc.nowHandler()
					r.Spec.DesireState = types.InstanceStateStopped
					rs[r.Name] = r
				}
			}
		}
		if e.Spec.DesireState != types.InstanceStateStopped || e.Spec.NodeID != "" {
			if v.Status.Robustness == types.VolumeRobustnessFaulted {
				e.Spec.LogRequested = true
			}
			// Prevent this field from being unset when restore/DR volumes crash unexpectedly.
			if !v.Status.RestoreRequired && !v.Status.IsStandby {
				e.Spec.BackupVolume = ""
			}
			e.Spec.RequestedBackupRestore = ""
			e.Spec.NodeID = ""
			e.Spec.DesireState = types.InstanceStateStopped
		}
		// must make sure engine stopped first before stopping replicas
		// otherwise we may corrupt the data
		if e.Status.CurrentState != types.InstanceStateStopped {
			return nil
		}

		allReplicasStopped := true
		for _, r := range rs {
			if r.Spec.DesireState != types.InstanceStateStopped {
				if v.Status.Robustness == types.VolumeRobustnessFaulted {
					r.Spec.LogRequested = true
				}
				r.Spec.DesireState = types.InstanceStateStopped
				rs[r.Name] = r
			}
			if r.Status.CurrentState != types.InstanceStateStopped {
				allReplicasStopped = false
			}
		}
		if !allReplicasStopped {
			return nil
		}

		v.Status.State = types.VolumeStateDetached
		if oldState != v.Status.State {
			vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonDetached, "volume %v has been detached", v.Name)
		}
		// Automatic reattach the volume if PendingNodeID was set
		// TODO: need to revisit for CurrentNodeID and PendingNodeID update
		// The reason is usually when CurrentNodeID is set, the volume
		// should be in attaching or attached state, currently
		// CurrentNodeID is set when volume is in detached state.
		// Implicitly assume v.status.CurrentNodeID has been the final
		// state of this sync loop if this code block is reached.
		// Hence updating v.status.CurrentNodeID here is weird.
		if v.Status.PendingNodeID != "" {
			if isReady, err := vc.ds.CheckEngineImageReadiness(v.Status.CurrentImage, v.Status.PendingNodeID); !isReady {
				log.WithError(err).Warnf("skip auto attach because current image %v is not ready", v.Status.CurrentImage)
			} else {
				v.Status.CurrentNodeID = v.Status.PendingNodeID
				v.Status.PendingNodeID = ""
			}
		}

	} else {
		// wait for offline engine upgrade to finish
		if v.Status.State == types.VolumeStateDetached && v.Status.CurrentImage != v.Spec.EngineImage {
			log.Debug("Wait for offline volume upgrade to finish")
			return nil
		}
		// if engine was running, then we are attached already
		// (but we may still need to start rebuilding replicas)
		if e.Status.CurrentState != types.InstanceStateRunning && e.Status.CurrentState != types.InstanceStateUnknown {
			// the final state will be determined at the end of the clause
			v.Status.State = types.VolumeStateAttaching
			e.Spec.UpgradedReplicaAddressMap = map[string]string{}
		}

		isCLIAPIVersionOne := false
		if v.Status.CurrentImage != "" {
			isCLIAPIVersionOne, err = vc.ds.IsEngineImageCLIAPIVersionOne(v.Status.CurrentImage)
			if err != nil {
				return err
			}
		}

		for _, r := range rs {
			// Don't attempt to start the replica or do anything else if it hasn't been scheduled.
			if r.Spec.NodeID == "" {
				continue
			}
			canIMLaunchReplica, err := vc.canInstanceManagerLaunchReplica(r)
			if err != nil {
				return err
			}
			if !canIMLaunchReplica {
				if r.Spec.FailedAt == "" {
					r.Spec.FailedAt = vc.nowHandler()
				}
				r.Spec.DesireState = types.InstanceStateStopped
			} else if r.Spec.FailedAt == "" && r.Spec.EngineImage == v.Status.CurrentImage {
				if r.Status.CurrentState == types.InstanceStateStopped {
					r.Spec.DesireState = types.InstanceStateRunning
				}
			}
			rs[r.Name] = r
		}
		replicaAddressMap := map[string]string{}
		for _, r := range rs {
			// Ignore unscheduled replicas
			if r.Spec.NodeID == "" {
				continue
			}
			if r.Spec.EngineImage != v.Status.CurrentImage {
				continue
			}
			if r.Spec.EngineName != e.Name {
				continue
			}
			if r.Spec.FailedAt != "" {
				continue
			}
			if r.Status.CurrentState == types.InstanceStateError {
				continue
			}
			// wait for all potentially healthy replicas become running
			if r.Status.CurrentState != types.InstanceStateRunning {
				return nil
			}
			if r.Status.IP == "" {
				log.WithField("replica", r.Name).Error("BUG: replica is running but IP is empty")
				continue
			}
			if r.Status.Port == 0 {
				// Do not skip this replica if its engine image is CLIAPIVersion 1.
				if !isCLIAPIVersionOne {
					log.WithField("replica", r.Name).Error("BUG: replica is running but Port is empty")
					continue
				}
			}
			replicaAddressMap[r.Name] = imutil.GetURL(r.Status.IP, r.Status.Port)
		}
		if len(replicaAddressMap) == 0 {
			return fmt.Errorf("no healthy or scheduled replica for starting")
		}

		if e.Spec.NodeID != "" && e.Spec.NodeID != v.Status.CurrentNodeID {
			return fmt.Errorf("engine is on node %v vs volume on %v, must detach first",
				e.Spec.NodeID, v.Status.CurrentNodeID)
		}
		e.Spec.NodeID = v.Status.CurrentNodeID
		e.Spec.ReplicaAddressMap = replicaAddressMap
		e.Spec.DesireState = types.InstanceStateRunning
		// The volume may be activated
		e.Spec.DisableFrontend = v.Status.FrontendDisabled
		e.Spec.Frontend = v.Spec.Frontend
		// wait for engine to be up
		if e.Status.CurrentState != types.InstanceStateRunning {
			return nil
		}

		if e.Spec.RequestedBackupRestore != "" {
			v.Status.Conditions = types.SetCondition(v.Status.Conditions,
				types.VolumeConditionTypeRestore, types.ConditionStatusTrue, types.VolumeConditionReasonRestoreInProgress, "")
		}

		if v.Status.ExpansionRequired {
			// The engine expansion is complete
			if v.Spec.Size == e.Status.CurrentSize {
				if v.Spec.Frontend == types.VolumeFrontendBlockDev {
					log.Info("Prepare to start frontend and expand the file system for volume")
					// Here is the exception that the frontend is enabled but the volume is auto attached.
					v.Status.FrontendDisabled = false
					e.Spec.DisableFrontend = false
					// Wait for the frontend to be up after e.Spec.DisableFrontend getting changed.
					// If the frontend is up, the engine endpoint won't be empty.
					if e.Status.Endpoint != "" {
						// Best effort. We don't know if there is a file system built in the volume.
						if err := util.ExpandFileSystem(v.Name); err != nil {
							log.WithError(err).Warn("Failed to expand the file system for volume")
						} else {
							log.Info("Succeeded to expand the file system for volume")
						}
						v.Status.ExpansionRequired = false
					}
				} else {
					log.Info("Expanding file system is not supported for volume frontend")
					v.Status.ExpansionRequired = false
				}
				vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonSucceededExpansion,
					"Succeeds to expand the volume %v to size %v, will automatically detach it if it's not DR volume", v.Name, e.Status.CurrentSize)
			}
		}

		v.Status.State = types.VolumeStateAttached
		if oldState != v.Status.State {
			vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonAttached, "volume %v has been attached to %v", v.Name, v.Status.CurrentNodeID)
		}
	}
	return nil
}

func (vc *VolumeController) canInstanceManagerLaunchReplica(r *longhorn.Replica) (bool, error) {
	nodeDown, err := vc.ds.IsNodeDownOrDeleted(r.Spec.NodeID)
	if err != nil {
		return false, errors.Wrapf(err, "fail checking IsNodeDownOrDeleted %v", r.Spec.NodeID)
	}
	if nodeDown {
		return false, nil
	}
	// Replica already had IM
	if r.Status.InstanceManagerName != "" {
		return true, nil
	}
	defaultIM, err := vc.ds.GetInstanceManagerByInstance(r)
	if err != nil {
		return false, errors.Wrapf(err, "cannot find instance manager for replica %v", r.Name)
	}
	return defaultIM.Status.CurrentState == types.InstanceManagerStateRunning ||
		defaultIM.Status.CurrentState == types.InstanceManagerStateStarting, nil
}

func (vc *VolumeController) getPreferredReplicaCandidatesForDeletion(rs map[string]*longhorn.Replica) ([]string, error) {
	diskToReplicaMap := make(map[string][]string)
	nodeToReplicaMap := make(map[string][]string)
	zoneToReplicaMap := make(map[string][]string)

	nodeList, err := vc.ds.ListNodes()
	if err != nil {
		return nil, err
	}

	for _, r := range rs {
		diskToReplicaMap[r.Spec.NodeID+r.Spec.DiskID] = append(diskToReplicaMap[r.Spec.NodeID+r.Spec.DiskID], r.Name)
		nodeToReplicaMap[r.Spec.NodeID] = append(nodeToReplicaMap[r.Spec.NodeID], r.Name)
		if node, ok := nodeList[r.Spec.NodeID]; ok {
			zoneToReplicaMap[node.Status.Zone] = append(zoneToReplicaMap[node.Status.Zone], r.Name)
		}
	}

	var deletionCandidates []string

	// prefer to delete replicas on the same disk
	deletionCandidates = findValueWithBiggestLength(diskToReplicaMap)
	if len(deletionCandidates) > 1 {
		return deletionCandidates, nil
	}

	// if all replicas are on different disks, prefer to delete replicas on the same node
	deletionCandidates = findValueWithBiggestLength(nodeToReplicaMap)
	if len(deletionCandidates) > 1 {
		return deletionCandidates, nil
	}

	// if all replicas are on different nodes, prefer to delete replicas on the same zone
	deletionCandidates = findValueWithBiggestLength(zoneToReplicaMap)
	if len(deletionCandidates) > 1 {
		return deletionCandidates, nil
	}

	// if all replicas are on different zones, return all replicas' names in the input RS
	deletionCandidates = make([]string, 0, len(rs))
	for rName := range rs {
		deletionCandidates = append(deletionCandidates, rName)
	}
	return deletionCandidates, nil
}

func findValueWithBiggestLength(m map[string][]string) []string {
	targetKey, currentMax := "", 0
	for k, v := range m {
		if len(v) > currentMax {
			targetKey, currentMax = k, len(v)
		}
	}
	return m[targetKey]
}

func isDataLocalityDisabled(v *longhorn.Volume) bool {
	return string(v.Spec.DataLocality) == "" || v.Spec.DataLocality == types.DataLocalityDisabled
}

// hasLocalReplicaOnSameNodeAsEngine returns true if one of the following condition is satisfied:
// 1. there exist a replica on the same node as engine
// 2. there exist a replica with HardNodeAffinity set to engine's NodeID
func hasLocalReplicaOnSameNodeAsEngine(e *longhorn.Engine, rs map[string]*longhorn.Replica) bool {
	for _, r := range rs {
		if e.Spec.NodeID != "" && (r.Spec.NodeID == e.Spec.NodeID || r.Spec.HardNodeAffinity == e.Spec.NodeID) {
			return true
		}
	}
	return false
}

// replenishReplicas will keep replicas count to v.Spec.NumberOfReplicas
// It will count all the potentially usable replicas, since some replicas maybe
// blank or in rebuilding state
func (vc *VolumeController) replenishReplicas(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica, hardNodeAffinity string) error {
	disableReplicaRebuild, err := vc.ds.GetSettingAsBool(types.SettingNameDisableReplicaRebuild)
	if err != nil {
		return err
	}

	// If disabled replica rebuild, skip all the rebuild except first time creation.
	if (len(rs) != 0) && (disableReplicaRebuild == true) {
		return nil
	}

	if vc.isVolumeMigrating(v) {
		return nil
	}

	if (len(rs) != 0) && v.Status.State != types.VolumeStateAttached {
		return nil
	}

	if e == nil {
		return fmt.Errorf("BUG: replenishReplica needs a valid engine")
	}

	// To prevent duplicate IP for different replicas cause problem
	// Wait for engine to:
	// 1. make sure the existing healthy replicas have shown up in engine.spec.ReplicaAddressMap
	// 2. has recognized all the replicas from the spec.ReplicaAddressMap in the status.ReplicaModeMap
	// 3. has cleaned up the extra entries in the status.ReplicaModeMap
	// https://github.com/longhorn/longhorn/issues/687
	if !vc.hasEngineStatusSynced(e, rs) {
		return nil
	}

	if currentRebuilding := getRebuildingReplicaCount(e); currentRebuilding != 0 {
		return nil
	}

	log := getLoggerForVolume(vc.logger, v)

	replenishCount := vc.getReplenishReplicasCount(v, rs)

	newVolume := len(rs) == 0

	// For regular rebuild case or data locality case, rebuild one replica at a time
	if (!newVolume && replenishCount > 0) || hardNodeAffinity != "" {
		replenishCount = 1
	}

	for i := 0; i < replenishCount; i++ {
		reusableFailedReplica, err := vc.scheduler.CheckAndReuseFailedReplica(rs, v, hardNodeAffinity)
		if err != nil {
			return errors.Wrapf(err, "failed to reuse a failed replica during replica replenishment")
		}
		if reusableFailedReplica != nil {
			if !vc.backoff.IsInBackOffSinceUpdate(reusableFailedReplica.Name, time.Now()) {
				log.Debugf("Failed replica %v will be reused during rebuilding", reusableFailedReplica.Name)
				reusableFailedReplica.Spec.FailedAt = ""
				reusableFailedReplica.Spec.HealthyAt = ""
				reusableFailedReplica.Spec.RebuildRetryCount++
				vc.backoff.Next(reusableFailedReplica.Name, time.Now())
				rs[reusableFailedReplica.Name] = reusableFailedReplica
				continue
			}
			log.Debugf("Cannot reuse failed replica %v immediately, backoff period is %v now",
				reusableFailedReplica.Name, vc.backoff.Get(reusableFailedReplica.Name).Seconds())
		}
		if vc.scheduler.RequireNewReplica(rs, v, hardNodeAffinity) {
			if err := vc.createReplica(v, e, rs, hardNodeAffinity, !newVolume); err != nil {
				return err
			}
		}
	}
	return nil
}

func getRebuildingReplicaCount(e *longhorn.Engine) int {
	rebuilding := 0
	replicaExists := make(map[string]bool)
	// replicas are currently rebuilding
	for replica, mode := range e.Status.ReplicaModeMap {
		replicaExists[replica] = true
		if mode == types.ReplicaModeWO {
			rebuilding++
		}
	}
	// replicas are to be rebuilt
	for replica := range e.Status.CurrentReplicaAddressMap {
		if !replicaExists[replica] {
			rebuilding++
		}
	}
	return rebuilding
}

func (vc *VolumeController) getReplenishReplicasCount(v *longhorn.Volume, rs map[string]*longhorn.Replica) int {
	usableCount := 0
	for _, r := range rs {
		// The failed to schedule local replica shouldn't be counted
		if !isDataLocalityDisabled(v) && r.Spec.HealthyAt == "" && r.Spec.FailedAt == "" && r.Spec.NodeID == "" &&
			v.Status.CurrentNodeID != "" && r.Spec.HardNodeAffinity == v.Status.CurrentNodeID {
			continue
		}
		// Skip the replica has been requested eviction.
		if r.Spec.FailedAt == "" && (!r.Status.EvictionRequested) {
			usableCount++
		}
	}

	if v.Spec.NumberOfReplicas < usableCount {
		return 0
	}
	return v.Spec.NumberOfReplicas - usableCount
}

func (vc *VolumeController) hasEngineStatusSynced(e *longhorn.Engine, rs map[string]*longhorn.Replica) bool {
	connectedReplicaCount := 0
	for _, r := range rs {
		if r.Spec.FailedAt == "" && r.Spec.NodeID != "" {
			connectedReplicaCount++
		}
	}

	if len(e.Spec.ReplicaAddressMap) != connectedReplicaCount {
		return false
	}
	if len(e.Spec.ReplicaAddressMap) != len(e.Status.ReplicaModeMap) {
		return false
	}
	for rName := range e.Spec.ReplicaAddressMap {
		mode, exists := e.Status.ReplicaModeMap[rName]
		if !exists {
			return false
		}
		if mode == types.ReplicaModeERR {
			return false
		}
	}
	return true
}

func (vc *VolumeController) upgradeEngineForVolume(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) error {
	var err error

	if !vc.isVolumeUpgrading(v) {
		// it must be a rollback
		if e != nil && e.Spec.EngineImage != v.Spec.EngineImage {
			e.Spec.EngineImage = v.Spec.EngineImage
			e.Spec.UpgradedReplicaAddressMap = map[string]string{}
		}
		return nil
	}

	// If volume is detached accidentally during the live upgrade,
	// the live upgrade info and the inactive replicas are meaningless.
	if v.Status.State == types.VolumeStateDetached {
		if e.Spec.EngineImage != v.Spec.EngineImage {
			e.Spec.EngineImage = v.Spec.EngineImage
			e.Spec.UpgradedReplicaAddressMap = map[string]string{}
		}
		for _, r := range rs {
			if r.Spec.EngineImage != v.Spec.EngineImage {
				r.Spec.EngineImage = v.Spec.EngineImage
				rs[r.Name] = r
			}
			if !r.Spec.Active {
				if err := vc.deleteReplica(r, rs); err != nil {
					return err
				}
			}
		}
		// TODO current replicas should be calculated by checking if there is
		// any other image exists except for the v.Spec.EngineImage
		v.Status.CurrentImage = v.Spec.EngineImage
		return nil
	}

	// only start live upgrade if volume is healthy
	if v.Status.State != types.VolumeStateAttached || v.Status.Robustness != types.VolumeRobustnessHealthy {
		return nil
	}

	log := getLoggerForVolume(vc.logger, v)

	volumeAndReplicaNodes := []string{v.Status.CurrentNodeID}
	for _, r := range rs {
		volumeAndReplicaNodes = append(volumeAndReplicaNodes, r.Spec.NodeID)
	}

	oldImage, err := vc.getEngineImage(v.Status.CurrentImage)
	if err != nil {
		log.WithError(err).Warnf("Cannot get engine image %v for live upgrade", v.Status.CurrentImage)
		return nil
	}

	if isReady, err := vc.ds.CheckEngineImageReadiness(oldImage.Spec.Image, volumeAndReplicaNodes...); !isReady {
		log.WithError(err).Warnf("Engine live upgrade from %v, but the image wasn't ready", oldImage.Spec.Image)
		return nil
	}
	newImage, err := vc.getEngineImage(v.Spec.EngineImage)
	if err != nil {
		log.WithError(err).Warnf("Cannot get engine image %v for live upgrade", v.Spec.EngineImage)
		return nil
	}
	if isReady, err := vc.ds.CheckEngineImageReadiness(newImage.Spec.Image, volumeAndReplicaNodes...); !isReady {
		log.WithError(err).Warnf("Engine live upgrade to %v, but the image wasn't ready", newImage.Spec.Image)
		return nil
	}

	if oldImage.Status.GitCommit == newImage.Status.GitCommit {
		log.Warnf("Engine image %v and %v are identical, delay upgrade until detach for volume", oldImage.Spec.Image, newImage.Spec.Image)
		return nil
	}

	if oldImage.Status.ControllerAPIVersion > newImage.Status.ControllerAPIVersion ||
		oldImage.Status.ControllerAPIVersion < newImage.Status.ControllerAPIMinVersion {
		log.Warnf("Unable to live upgrade from %v to %v: the old controller version %v "+
			"is not compatible with the new controller version %v and the new controller minimal version %v",
			oldImage.Spec.Image, newImage.Spec.Image,
			oldImage.Status.ControllerAPIVersion, newImage.Status.ControllerAPIVersion, newImage.Status.ControllerAPIMinVersion)
		return nil
	}

	unknownReplicas := map[string]*longhorn.Replica{}
	dataPathToOldRunningReplica := map[string]*longhorn.Replica{}
	dataPathToNewReplica := map[string]*longhorn.Replica{}
	for _, r := range rs {
		dataPath := types.GetReplicaDataPath(r.Spec.DiskPath, r.Spec.DataDirectoryName)
		if r.Spec.EngineImage == v.Status.CurrentImage && r.Status.CurrentState == types.InstanceStateRunning && r.Spec.HealthyAt != "" {
			dataPathToOldRunningReplica[dataPath] = r
		} else if r.Spec.EngineImage == v.Spec.EngineImage {
			dataPathToNewReplica[dataPath] = r
		} else {
			log.Warnf("Found unknown replica with image %v for live upgrade", r.Spec.EngineImage)
			unknownReplicas[r.Name] = r
		}
	}

	// Skip checking and creating new replicas for the 2 cases:
	//   1. Volume is degraded.
	//   2. The new replicas is activated and all old replicas are already purged.
	if len(dataPathToOldRunningReplica) >= v.Spec.NumberOfReplicas {
		if err := vc.createAndStartMatchingReplicas(v, rs, dataPathToOldRunningReplica, dataPathToNewReplica, func(r *longhorn.Replica, engineImage string) {
			r.Spec.EngineImage = engineImage
		}, v.Spec.EngineImage); err != nil {
			return err
		}
	}

	if e.Spec.EngineImage != v.Spec.EngineImage {
		replicaAddressMap := map[string]string{}
		for _, r := range dataPathToNewReplica {
			// wait for all potentially healthy replicas become running
			if r.Status.CurrentState != types.InstanceStateRunning {
				return nil
			}
			if r.Status.IP == "" {
				log.WithField("replica", r.Name).Error("BUG: replica is running but IP is empty")
				continue
			}
			if r.Status.Port == 0 {
				log.WithField("replica", r.Name).Error("BUG: replica is running but port is 0")
				continue
			}
			replicaAddressMap[r.Name] = imutil.GetURL(r.Status.IP, r.Status.Port)
		}
		// Only upgrade e.Spec.EngineImage if there are enough new upgraded replica.
		// This prevent the deadlock in the case that an upgrade from engine image
		// is followed immediately by an other upgrade.
		// More specifically, after the 1st upgrade, e.Status.ReplicaModeMap empty.
		// Therefore, dataPathToOldRunningReplica, dataPathToOldRunningReplica, and replicaAddressMap are also empty.
		// Now, if we set e.Spec.UpgradedReplicaAddressMap to an empty map in the second upgrade,
		// the second engine upgrade will be blocked since len(e.Spec.UpgradedReplicaAddressMap) == 0.
		// On the other hand, the engine controller blocks the engine's status from being refreshed
		// and keep the e.Status.ReplicaModeMap to be empty map. The system enter a deadlock for the volume.
		if len(replicaAddressMap) == v.Spec.NumberOfReplicas {
			e.Spec.UpgradedReplicaAddressMap = replicaAddressMap
			e.Spec.EngineImage = v.Spec.EngineImage
		}
	}
	if e.Status.CurrentImage != v.Spec.EngineImage ||
		e.Status.CurrentState != types.InstanceStateRunning {
		return nil
	}

	if err := vc.switchActiveReplicas(rs, func(r *longhorn.Replica, engineImage string) bool {
		return r.Spec.EngineImage == engineImage
	}, v.Spec.EngineImage); err != nil {
		return err
	}

	e.Spec.ReplicaAddressMap = e.Spec.UpgradedReplicaAddressMap
	e.Spec.UpgradedReplicaAddressMap = map[string]string{}
	// cleanupCorruptedOrStaleReplicas() will take care of old replicas
	log.Infof("Engine %v has been upgraded from %v to %v", e.Name, v.Status.CurrentImage, v.Spec.EngineImage)
	v.Status.CurrentImage = v.Spec.EngineImage

	return nil
}

func (vc *VolumeController) updateRequestedBackupForVolumeRestore(v *longhorn.Volume, e *longhorn.Engine) (err error) {
	if e == nil {
		return nil
	}

	if v.Spec.FromBackup == "" {
		return nil
	}
	if !v.Status.RestoreRequired && !v.Status.IsStandby {
		return nil
	}

	if v.Status.LastBackup != "" && v.Status.LastBackup != e.Spec.RequestedBackupRestore {
		e.Spec.RequestedBackupRestore = v.Status.LastBackup
	}

	return nil
}

func (vc *VolumeController) checkAndInitVolumeRestore(v *longhorn.Volume) error {
	log := getLoggerForVolume(vc.logger, v)

	if v.Spec.FromBackup == "" || v.Status.RestoreInitiated {
		return nil
	}

	backupTarget, err := manager.GenerateBackupTarget(vc.ds)
	if err != nil {
		return err
	}
	backup, err := backupTarget.GetBackup(v.Spec.FromBackup)
	if err != nil {
		return fmt.Errorf("cannot get backup %v: %v", v.Spec.FromBackup, err)
	}
	if backup == nil {
		return fmt.Errorf("cannot find backup %v of volume %v", v.Spec.FromBackup, v.Name)
	}

	size, err := util.ConvertSize(backup.Size)
	if err != nil {
		return fmt.Errorf("cannot get the size of backup %v: %v", v.Spec.FromBackup, err)
	}
	v.Status.ActualSize = size

	// If KubernetesStatus is set on Backup, restore it.
	kubeStatus := &types.KubernetesStatus{}
	if statusJSON, ok := backup.Labels[types.KubernetesStatusLabel]; ok {
		if err := json.Unmarshal([]byte(statusJSON), kubeStatus); err != nil {
			log.WithError(err).Warnf("Ignore KubernetesStatus JSON for backup %v", backup.Name)
		} else {
			// We were able to unmarshal KubernetesStatus. Set the Ref fields.
			if kubeStatus.PVCName != "" && kubeStatus.LastPVCRefAt == "" {
				kubeStatus.LastPVCRefAt = backup.SnapshotCreated
			}
			if len(kubeStatus.WorkloadsStatus) != 0 && kubeStatus.LastPodRefAt == "" {
				kubeStatus.LastPodRefAt = backup.SnapshotCreated
			}

			// Do not restore the PersistentVolume fields.
			kubeStatus.PVName = ""
			kubeStatus.PVStatus = ""
		}
	}
	v.Status.KubernetesStatus = *kubeStatus

	if v.Spec.Standby {
		v.Status.IsStandby = true
	}

	v.Status.RestoreRequired = true
	v.Status.RestoreInitiated = true

	return nil
}

func (vc *VolumeController) reconcileVolumeSize(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) error {
	log := getLoggerForVolume(vc.logger, v)

	if e == nil || rs == nil {
		return nil
	}
	if e.Spec.VolumeSize == v.Spec.Size {
		return nil
	}

	// The expansion is canceled or hasn't been started
	if e.Status.CurrentSize == v.Spec.Size {
		v.Status.ExpansionRequired = false
		vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonCanceledExpansion,
			"Canceled expanding the volume %v, will automatically detach it", v.Name)
	} else {
		if err := vc.scheduler.CheckReplicasSizeExpansion(v, e.Spec.VolumeSize, v.Spec.Size); err != nil {
			log.Debugf("cannot start volume expansion: %v", err)
			return nil
		}
		log.Infof("Start volume expand from size %v to size %v", e.Spec.VolumeSize, v.Spec.Size)
		v.Status.ExpansionRequired = true
	}

	e.Spec.VolumeSize = v.Spec.Size
	for _, r := range rs {
		r.Spec.VolumeSize = v.Spec.Size
	}

	return nil
}

func (vc *VolumeController) checkForAutoAttachment(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica, scheduled bool) error {
	if v.Spec.NodeID != "" || v.Status.CurrentNodeID != "" {
		return nil
	}
	if !(v.Status.State == "" || v.Status.State == types.VolumeStateDetached) {
		return nil
	}
	// Do not intervene the auto reattachment workflow during the engine crashing and volume recovery.
	if v.Status.PendingNodeID != "" {
		return nil
	}
	// It's meaningless to do auto attachment if the volume scheduling fails
	if !scheduled {
		return nil
	}

	// Do auto attachment for:
	//   1. restoring/DR volumes.
	//   2. offline expansion.
	//   3. Eviction requested on this volume.
	if v.Status.RestoreRequired || v.Status.IsStandby ||
		v.Status.ExpansionRequired || vc.hasReplicaEvictionRequested(rs) {
		// Should use vc.controllerID or v.Status.OwnerID as CurrentNodeID,
		// otherwise they may be not equal
		v.Status.CurrentNodeID = v.Status.OwnerID
	}

	return nil
}

func (vc *VolumeController) checkForAutoDetachment(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) error {
	log := getLoggerForVolume(vc.logger, v)

	if v.Spec.NodeID != "" || v.Status.CurrentNodeID == "" || e == nil {
		return nil
	}

	if v.Status.ExpansionRequired {
		return nil
	}

	// Don't do auto-detachment if the eviction is going on.
	if vc.hasReplicaEvictionRequested(rs) {
		return nil
	}

	// Do auto-detachment for non-restore/DR volumes.
	if !v.Status.RestoreRequired && !v.Status.IsStandby {
		v.Status.CurrentNodeID = ""
		return nil
	}

	// Do auto-detachment for restore/DR volumes.
	if v.Status.CurrentNodeID != v.Status.OwnerID {
		log.Info("Found the restore/DR volume node is down, will detach it first then re-attach it to restart the restoring")
		v.Status.CurrentNodeID = ""
		return nil
	}
	// Can automatically detach/activate the restore/DR volume on the running node if the following conditions are satisfied:
	// 1) The restored backup is up-to-date;
	// 2) The volume is no longer a DR volume;
	// 3) The restore/DR volume is
	//   3.1) using the old engine image. And it's still running.
	//	 3.2) or using the latest engine image without purging snapshots. And
	//	   3.2.1) it's state `Healthy`;
	//	   3.2.2) or it's state `Degraded` with all the scheduled replica included in the engine
	cliAPIVersion, err := vc.ds.GetEngineImageCLIAPIVersion(v.Status.CurrentImage)
	if err != nil {
		return err
	}
	isPurging := false
	for _, status := range e.Status.PurgeStatus {
		if status.IsPurging {
			isPurging = true
			break
		}
	}
	if !(e.Spec.RequestedBackupRestore != "" && e.Spec.RequestedBackupRestore == e.Status.LastRestoredBackup &&
		!v.Spec.Standby) {
		return nil
	}
	allScheduledReplicasIncluded := true
	for _, r := range rs {
		// skip unscheduled replicas
		if r.Spec.NodeID == "" {
			continue
		}
		if isDownOrDeleted, err := vc.ds.IsNodeDownOrDeleted(r.Spec.NodeID); err != nil {
			return err
		} else if isDownOrDeleted {
			continue
		}
		if mode := e.Status.ReplicaModeMap[r.Name]; mode != types.ReplicaModeRW {
			allScheduledReplicasIncluded = false
			break
		}
	}
	if (cliAPIVersion >= engineapi.CLIVersionFour && !isPurging && (v.Status.Robustness == types.VolumeRobustnessHealthy || v.Status.Robustness == types.VolumeRobustnessDegraded) && allScheduledReplicasIncluded) ||
		(cliAPIVersion < engineapi.CLIVersionFour && (v.Status.Robustness == types.VolumeRobustnessHealthy || v.Status.Robustness == types.VolumeRobustnessDegraded)) {
		log.Info("Prepare to do auto detachment for restore/DR volume")
		v.Status.CurrentNodeID = ""
		v.Status.IsStandby = false
		v.Status.RestoreRequired = false
	}

	return nil
}

func (vc *VolumeController) getInfoFromBackupURL(v *longhorn.Volume) (string, string, error) {
	if v.Spec.FromBackup == "" {
		return "", "", nil
	}

	backupVolumeName, err := backupstore.GetVolumeFromBackupURL(v.Spec.FromBackup)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to get backup volume name from backupURL %v", v.Spec.FromBackup)
	}

	backupName, err := backupstore.GetBackupFromBackupURL(v.Spec.FromBackup)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to get backup name from backupURL %v", v.Spec.FromBackup)
	}

	return backupVolumeName, backupName, nil

}

func (vc *VolumeController) createEngine(v *longhorn.Volume) (*longhorn.Engine, error) {
	log := getLoggerForVolume(vc.logger, v)

	engine := &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{
			Name:            types.GenerateEngineNameForVolume(v.Name),
			OwnerReferences: datastore.GetOwnerReferencesForVolume(v),
		},
		Spec: types.EngineSpec{
			InstanceSpec: types.InstanceSpec{
				VolumeName:  v.Name,
				VolumeSize:  v.Spec.Size,
				EngineImage: v.Status.CurrentImage,
				DesireState: types.InstanceStateStopped,
			},
			Frontend:                  v.Spec.Frontend,
			ReplicaAddressMap:         map[string]string{},
			UpgradedReplicaAddressMap: map[string]string{},
			RevisionCounterDisabled:   v.Spec.RevisionCounterDisabled,
		},
	}

	if v.Spec.FromBackup != "" {
		backupVolumeName, backupName, err := vc.getInfoFromBackupURL(v)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get backup volume when creating engine object of restored volume %v", v.Name)
		}
		engine.Spec.BackupVolume = backupVolumeName
		engine.Spec.RequestedBackupRestore = backupName

		log.Debugf("Created engine %v for restored volume, BackupVolume is %v, RequestedBackupRestore is %v",
			engine.Name, engine.Spec.BackupVolume, engine.Spec.RequestedBackupRestore)
	}

	return vc.ds.CreateEngine(engine)
}

func (vc *VolumeController) createReplica(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica, hardNodeAffinity string, isRebuildingReplica bool) error {
	log := getLoggerForVolume(vc.logger, v)

	replica := &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name:            types.GenerateReplicaNameForVolume(v.Name),
			OwnerReferences: datastore.GetOwnerReferencesForVolume(v),
		},
		Spec: types.ReplicaSpec{
			InstanceSpec: types.InstanceSpec{
				VolumeName:  v.Name,
				VolumeSize:  v.Spec.Size,
				EngineImage: v.Status.CurrentImage,
				DesireState: types.InstanceStateStopped,
			},
			EngineName:              e.Name,
			Active:                  true,
			BackingImage:            v.Spec.BackingImage,
			HardNodeAffinity:        hardNodeAffinity,
			RevisionCounterDisabled: v.Spec.RevisionCounterDisabled,
		},
	}
	if isRebuildingReplica {
		log.Debugf("A new replica %v will be replenished during rebuilding", replica.Name)
		// Prevent this new replica from being reused after rebuilding failure.
		replica.Spec.RebuildRetryCount = scheduler.FailedReplicaMaxRetryCount
	}

	replica, err := vc.ds.CreateReplica(replica)
	if err != nil {
		return err
	}
	rs[replica.Name] = replica

	return nil
}

func (vc *VolumeController) duplicateReplica(r *longhorn.Replica, v *longhorn.Volume) *longhorn.Replica {
	replica := &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name:            types.GenerateReplicaNameForVolume(r.Spec.VolumeName),
			OwnerReferences: datastore.GetOwnerReferencesForVolume(v),
		},
		Spec: r.DeepCopy().Spec,
	}
	return replica
}

func (vc *VolumeController) enqueueVolume(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	vc.queue.Add(key)
}

func (vc *VolumeController) enqueueControlleeChange(obj interface{}) {
	if deletedState, ok := obj.(*cache.DeletedFinalStateUnknown); ok {
		obj = deletedState.Obj
	}

	metaObj, err := meta.Accessor(obj)
	if err != nil {
		vc.logger.WithError(err).Warnf("BUG: cannot convert obj %v to metav1.Object", obj)
		return
	}
	ownerRefs := metaObj.GetOwnerReferences()
	for _, ref := range ownerRefs {
		namespace := metaObj.GetNamespace()
		vc.ResolveRefAndEnqueue(namespace, &ref)
		return
	}
}

func (vc *VolumeController) ResolveRefAndEnqueue(namespace string, ref *metav1.OwnerReference) {
	if ref.Kind != types.LonghornKindVolume {
		// TODO: Will stop checking this wrong reference kind after all Longhorn components having used the new kinds
		if ref.Kind != ownerKindVolume {
			return
		}
	}
	volume, err := vc.ds.GetVolume(ref.Name)
	if err != nil {
		return
	}
	if volume.UID != ref.UID {
		// The controller we found with this Name is not the same one that the
		// OwnerRef points to.
		return
	}
	vc.enqueueVolume(volume)
}

func (vc *VolumeController) createCronJob(v *longhorn.Volume, job *types.RecurringJob) (*batchv1beta1.CronJob, error) {
	backoffLimit := int32(CronJobBackoffLimit)
	successfulJobsHistoryLimit := int32(CronJobSuccessfulJobsHistoryLimit)
	cmd := []string{
		"longhorn-manager", "-d",
		"snapshot", v.Name,
		"--manager-url", types.GetDefaultManagerURL(),
		"--snapshot-name", job.Name,
		"--labels", types.RecurringJobLabel + "=" + job.Name,
		"--retain", strconv.Itoa(job.Retain),
	}
	for key, val := range job.Labels {
		cmd = append(cmd, "--labels", key+"="+val)
	}
	if job.Task == types.RecurringJobTypeBackup {
		cmd = append(cmd, "--backup")
	}
	tolerations, err := vc.ds.GetSettingTaintToleration()
	if err != nil {
		return nil, err
	}
	priorityClass, err := vc.ds.GetSetting(types.SettingNamePriorityClass)
	if err != nil {
		return nil, err
	}
	nodeSelector, err := vc.ds.GetSettingSystemManagedComponentsNodeSelector()
	if err != nil {
		return nil, err
	}
	// for mounting inside container
	privilege := true
	cronJob := &batchv1beta1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:            types.GetCronJobNameForVolumeAndJob(v.Name, job.Name),
			Namespace:       vc.namespace,
			Labels:          types.GetCronJobLabels(v.Name, job),
			OwnerReferences: datastore.GetOwnerReferencesForVolume(v),
		},
		Spec: batchv1beta1.CronJobSpec{
			Schedule:                   job.Cron,
			ConcurrencyPolicy:          batchv1beta1.ForbidConcurrent,
			SuccessfulJobsHistoryLimit: &successfulJobsHistoryLimit,
			JobTemplate: batchv1beta1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					BackoffLimit: &backoffLimit,
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name:   types.GetCronJobNameForVolumeAndJob(v.Name, job.Name),
							Labels: types.GetCronJobPodLabels(v.Name, job),
						},
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:    types.GetCronJobNameForVolumeAndJob(v.Name, job.Name),
									Image:   vc.ManagerImage,
									Command: cmd,
									SecurityContext: &v1.SecurityContext{
										Privileged: &privilege,
									},
									Env: []v1.EnvVar{
										{
											Name: "POD_NAMESPACE",
											ValueFrom: &v1.EnvVarSource{
												FieldRef: &v1.ObjectFieldSelector{
													FieldPath: "metadata.namespace",
												},
											},
										},
									},
									VolumeMounts: []v1.VolumeMount{
										{
											Name:      "engine-binaries",
											MountPath: types.EngineBinaryDirectoryOnHost,
										},
									},
								},
							},
							Volumes: []v1.Volume{
								{
									Name: "engine-binaries",
									VolumeSource: v1.VolumeSource{
										HostPath: &v1.HostPathVolumeSource{
											Path: types.EngineBinaryDirectoryOnHost,
										},
									},
								},
							},
							ServiceAccountName: vc.ServiceAccount,
							RestartPolicy:      v1.RestartPolicyOnFailure,
							Tolerations:        util.GetDistinctTolerations(tolerations),
							NodeSelector:       nodeSelector,
							PriorityClassName:  priorityClass.Value,
						},
					},
				},
			},
		},
	}
	return cronJob, nil
}

func (vc *VolumeController) updateRecurringJobs(v *longhorn.Volume) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to update recurring jobs for %v", v.Name)
	}()

	// the cronjobs are RO in the map, but not the map itself
	appliedCronJobROs, err := vc.ds.ListVolumeCronJobROs(v.Name)
	if err != nil {
		return err
	}

	desiredCronJobs := make(map[string]*batchv1beta1.CronJob)
	if !vc.shouldDeleteRecurringJobs(v) {
		for _, job := range v.Spec.RecurringJobs {
			cronJob, err := vc.createCronJob(v, &job)
			if err != nil {
				return err
			}
			desiredCronJobs[cronJob.Name] = cronJob
		}
	}

	for name, cronJob := range desiredCronJobs {
		appliedCronJob := appliedCronJobROs[name]
		cronJobSpecB, err := json.Marshal(cronJob)
		if err != nil {
			return errors.Wrapf(err, "fail to updateRecurringJobs %v", cronJob.GetName())
		}
		cronJobSpec := string(cronJobSpecB)

		if appliedCronJob == nil {
			util.SetAnnotation(cronJob, types.GetLonghornLabelKey(LastAppliedCronJobSpecAnnotationKeySuffix), cronJobSpec)
			_, err := vc.ds.CreateVolumeCronJob(v.Name, cronJob)
			if err != nil {
				return err
			}
			continue
		}

		lastAppliedSpec, err := util.GetAnnotation(appliedCronJob, types.GetLonghornLabelKey(LastAppliedCronJobSpecAnnotationKeySuffix))
		if err != nil {
			return err
		}
		if lastAppliedSpec == cronJobSpec {
			continue
		}

		util.SetAnnotation(cronJob, types.GetLonghornLabelKey(LastAppliedCronJobSpecAnnotationKeySuffix), cronJobSpec)
		if _, err := vc.ds.UpdateVolumeCronJob(v.Name, cronJob); err != nil {
			return err
		}
	}

	for name, job := range appliedCronJobROs {
		if desiredCronJobs[name] == nil {
			if err := vc.ds.DeleteCronJob(name); err != nil {
				return err
			}
			continue
		}

		// TODO: Will Remove this reference kind correcting after all Longhorn components having used the new kinds
		if len(job.OwnerReferences) < 1 || job.OwnerReferences[0].Kind != types.LonghornKindVolume {
			job.OwnerReferences = datastore.GetOwnerReferencesForVolume(v)
			job, err = vc.kubeClient.BatchV1beta1().CronJobs(vc.namespace).Update(job)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (vc *VolumeController) shouldDeleteRecurringJobs(v *longhorn.Volume) bool {
	allowRecurringJobWhileVolumeDetached, err := vc.ds.GetSettingAsBool(types.SettingNameAllowRecurringJobWhileVolumeDetached)
	if err != nil {
		vc.logger.WithError(err).Warn("error getting allow-recurring-backup-while-volume-detached setting")
	}

	if v.Status.State == types.VolumeStateAttached || allowRecurringJobWhileVolumeDetached {
		return false
	}
	return true
}

func (vc *VolumeController) isVolumeUpgrading(v *longhorn.Volume) bool {
	return v.Status.CurrentImage != v.Spec.EngineImage
}

func (vc *VolumeController) isVolumeMigrating(v *longhorn.Volume) bool {
	return v.Spec.MigrationNodeID != ""
}

func (vc *VolumeController) getEngineImage(image string) (*longhorn.EngineImage, error) {
	name := types.GetEngineImageChecksumName(image)
	img, err := vc.ds.GetEngineImage(name)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get engine image %v", image)
	}
	return img, nil
}

func (vc *VolumeController) getCurrentEngineAndExtras(v *longhorn.Volume, es map[string]*longhorn.Engine) (currentEngine *longhorn.Engine, otherEngines []*longhorn.Engine, err error) {
	for _, e := range es {
		if e.Spec.NodeID == v.Status.CurrentNodeID &&
			e.Spec.DesireState == types.InstanceStateRunning &&
			e.Status.CurrentState == types.InstanceStateRunning {
			currentEngine = e
		} else {
			otherEngines = append(otherEngines, e)
		}
	}
	if currentEngine == nil {
		return nil, nil, fmt.Errorf("volume %v is not attached", v.Name)
	}
	return
}

func (vc *VolumeController) getCurrentEngineAndCleanupOthers(v *longhorn.Volume, es map[string]*longhorn.Engine) (current *longhorn.Engine, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to clean up the extra engines for %v", v.Name)
	}()
	current, extras, err := vc.getCurrentEngineAndExtras(v, es)
	if err != nil {
		return nil, err
	}

	for _, extra := range extras {
		if extra.DeletionTimestamp == nil {
			if err := vc.deleteEngine(extra, es); err != nil {
				return nil, err
			}
		}
	}
	return current, nil
}

func (vc *VolumeController) createAndStartMatchingReplicas(v *longhorn.Volume,
	rs, pathToOldRs, pathToNewRs map[string]*longhorn.Replica,
	fixupFunc func(r *longhorn.Replica, obj string), obj string) error {
	for path, r := range pathToOldRs {
		if pathToNewRs[path] != nil {
			continue
		}
		clone := vc.duplicateReplica(r, v)
		clone.Spec.DesireState = types.InstanceStateRunning
		clone.Spec.Active = false
		fixupFunc(clone, obj)
		newReplica, err := vc.ds.CreateReplica(clone)
		if err != nil {
			return errors.Wrapf(err, "cannot create matching replica %v for volume %v", clone.Name, v.Name)
		}
		pathToNewRs[path] = newReplica
		rs[newReplica.Name] = newReplica
	}
	return nil
}

func (vc *VolumeController) switchActiveReplicas(rs map[string]*longhorn.Replica,
	activeCondFunc func(r *longhorn.Replica, obj string) bool, obj string) error {

	// Deletion of an active replica will trigger the cleanup process to
	// delete the volume data on the disk.
	// Set `active` at last to prevent data loss
	for _, r := range rs {
		r.Spec.Active = activeCondFunc(r, obj)
	}
	return nil
}

func (vc *VolumeController) processMigration(v *longhorn.Volume, es map[string]*longhorn.Engine, rs map[string]*longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to process migration for %v", v.Name)
	}()

	if !v.Spec.Migratable || v.Spec.AccessMode != types.AccessModeReadWriteMany {
		return nil
	}

	// only process if volume is attached
	if v.Spec.NodeID == "" || len(es) == 0 {
		return nil
	}

	// cannot process migrate when upgrading
	if vc.isVolumeUpgrading(v) {
		return nil
	}

	// the only time there should be more then 1 engines is when we are migrating or upgrading
	// if there are more then 1 and we no longer have a migration id set we can cleanup the extra engine
	if !vc.isVolumeMigrating(v) {
		if len(es) < 2 {
			return nil
		}

		// in the case of a confirmation we need to switch the v.Status.CurrentNodeID to v.Spec.NodeID
		// so that currentEngine becomes the migration engine
		if v.Status.CurrentNodeID != v.Spec.NodeID {
			vc.logger.Infof("volume migration complete switching current node id from %v to %v", v.Status.CurrentNodeID, v.Spec.NodeID)
			v.Status.CurrentNodeID = v.Spec.NodeID
		}

		// current engine is based on the v.Status.CurrentNodeID
		// so in the case of a rollback all we have to do is cleanup the migration engine
		currentEngine, err := vc.getCurrentEngineAndCleanupOthers(v, es)
		if err != nil {
			return err
		}

		// cleanupCorruptedOrStaleReplicas() will take care of old replicas
		if err := vc.switchActiveReplicas(rs, func(r *longhorn.Replica, engineName string) bool {
			return r.Spec.EngineName == engineName
		}, currentEngine.Name); err != nil {
			return err
		}

		return nil
	}

	// volume is migrating
	currentEngine, extras, err := vc.getCurrentEngineAndExtras(v, es)
	if err != nil {
		return err
	}

	var activeEngines int
	var migrationEngine *longhorn.Engine
	for _, extra := range extras {
		if extra.DeletionTimestamp != nil {
			continue
		}

		// a valid migration engine is either a newly created engine that hasn't been assigned a node yet
		// or an existing engine that is running on the migration node, any additional extra engines would be unexpected.
		activeEngines++
		isValidMigrationEngine := extra.Spec.NodeID == "" || extra.Spec.NodeID == v.Spec.MigrationNodeID
		if isValidMigrationEngine {
			migrationEngine = extra
		}
	}

	// verify that we are in a valid state for migration
	// we only consider active engines as candidates for the migration engine
	unexpectedEngineCount := activeEngines > 1
	invalidMigrationEngine := activeEngines > 0 && migrationEngine == nil
	if unexpectedEngineCount || invalidMigrationEngine {
		_, _ = vc.getCurrentEngineAndCleanupOthers(v, es)
		return fmt.Errorf("unexpected state for migration, current engine count %v has invalid migration engine %v",
			len(es), invalidMigrationEngine)
	}

	if migrationEngine == nil {
		migrationEngine, err = vc.createEngine(v)
		if err != nil {
			return err
		}
		es[migrationEngine.Name] = migrationEngine
	}

	currentAvailableReplicas := map[string]*longhorn.Replica{}
	migrationReplicas := map[string]*longhorn.Replica{}
	unknownReplicas := map[string]*longhorn.Replica{}
	for _, r := range rs {
		isUnavailable, err := vc.IsReplicaUnavailable(r)
		if err != nil {
			return err
		}
		if isUnavailable {
			continue
		}
		dataPath := types.GetReplicaDataPath(r.Spec.DiskPath, r.Spec.DataDirectoryName)
		if r.Spec.EngineName == currentEngine.Name {
			if currentEngine.Status.ReplicaModeMap[r.Name] == types.ReplicaModeWO {
				logrus.Debugf("Cannot start migration since the current replica %v is mode WriteOnly, which means the rebuilding is in progress", r.Name)
				return nil
			}
			if currentEngine.Status.ReplicaModeMap[r.Name] != types.ReplicaModeRW {
				return fmt.Errorf("unexpected mode %v for the current replica %v, cannot continue migration", currentEngine.Status.ReplicaModeMap[r.Name], r.Name)
			}
			currentAvailableReplicas[dataPath] = r
		} else if r.Spec.EngineName == migrationEngine.Name {
			migrationReplicas[dataPath] = r
		} else {
			vc.logger.Warnf("During migration found unknown replica with engine %v", r.Spec.EngineName)
			unknownReplicas[dataPath] = r
		}
	}

	if err := vc.createAndStartMatchingReplicas(v, rs, currentAvailableReplicas, migrationReplicas, func(r *longhorn.Replica, engineName string) {
		r.Spec.EngineName = engineName
	}, migrationEngine.Name); err != nil {
		return err
	}

	if migrationEngine.Spec.DesireState != types.InstanceStateRunning {
		replicaAddressMap := map[string]string{}
		for _, r := range migrationReplicas {
			// wait for all potentially available replicas become running
			if r.Status.CurrentState != types.InstanceStateRunning {
				return nil
			}
			if r.Status.IP == "" {
				vc.logger.Errorf("BUG: replica %v is running but IP is empty", r.Name)
				continue
			}
			if r.Status.Port == 0 {
				vc.logger.Errorf("BUG: replica %v is running but Port is empty", r.Name)
				continue
			}
			replicaAddressMap[r.Name] = imutil.GetURL(r.Status.IP, r.Status.Port)
		}
		if len(replicaAddressMap) == 0 {
			return fmt.Errorf("volume %v: no new replica during migration", v.Name)
		}
		if migrationEngine.Spec.NodeID != "" && migrationEngine.Spec.NodeID != v.Spec.MigrationNodeID {
			return fmt.Errorf("volume %v: engine is on node %v vs volume migration on %v",
				v.Name, migrationEngine.Spec.NodeID, v.Spec.MigrationNodeID)
		}
		migrationEngine.Spec.NodeID = v.Spec.MigrationNodeID
		migrationEngine.Spec.ReplicaAddressMap = replicaAddressMap
		migrationEngine.Spec.DesireState = types.InstanceStateRunning
		es[migrationEngine.Name] = migrationEngine
	}

	if migrationEngine.Status.CurrentState != types.InstanceStateRunning {
		return nil
	}

	vc.logger.Infof("volume migration engine on node %v is ready", v.Spec.MigrationNodeID)
	return nil
}

func (vc *VolumeController) IsReplicaUnavailable(r *longhorn.Replica) (bool, error) {
	log := vc.logger.WithFields(logrus.Fields{
		"replica":       r.Name,
		"replicaNodeID": r.Spec.NodeID,
	})

	if r.Spec.FailedAt != "" || r.Spec.NodeID == "" || r.Spec.DiskID == "" {
		return true, nil
	}

	isDownOrDeleted, err := vc.ds.IsNodeDownOrDeleted(r.Spec.NodeID)
	if err != nil {
		log.WithError(err).Errorf("Unable to check if node %v is still running for failed replica", r.Spec.NodeID)
		return true, err
	}
	if isDownOrDeleted {
		return true, nil
	}

	node, err := vc.ds.GetNode(r.Spec.NodeID)
	if err != nil {
		log.WithError(err).Errorf("Unable to get node %v for failed replica", r.Spec.NodeID)
		return true, err
	}
	for _, diskStatus := range node.Status.DiskStatus {
		if diskStatus.DiskUUID != r.Spec.DiskID {
			continue
		}
		if types.GetCondition(diskStatus.Conditions, types.DiskConditionTypeReady).Status != types.ConditionStatusTrue {
			return true, nil
		}
	}

	return false, nil
}

// isResponsibleFor picks a running node that has the default engine image deployed.
// We need the default engine image deployed on the node to perform operation like backup operations.
// Prefer picking the node v.Spec.NodeID if it meet the above requirement.
func (vc *VolumeController) isResponsibleFor(v *longhorn.Volume, defaultEngineImage string) (bool, error) {
	var err error
	defer func() {
		err = errors.Wrap(err, "error while checking isResponsibleFor")
	}()

	readyNodesWithDefaultEI, err := vc.ds.ListReadyNodesWithEngineImage(defaultEngineImage)
	if err != nil {
		return false, err
	}

	isResponsible := isControllerResponsibleFor(vc.controllerID, vc.ds, v.Name, v.Spec.NodeID, v.Status.OwnerID)

	// No node in the system has the default engine image,
	// Fall back to the default logic where we pick a running node to be the owner
	if len(readyNodesWithDefaultEI) == 0 {
		return isResponsible, nil
	}

	preferredOwnerEngineAvailable, err := vc.ds.CheckEngineImageReadiness(defaultEngineImage, v.Spec.NodeID)
	if err != nil {
		return false, err
	}
	currentOwnerEngineAvailable, err := vc.ds.CheckEngineImageReadiness(defaultEngineImage, v.Status.OwnerID)
	if err != nil {
		return false, err
	}
	currentNodeEngineAvailable, err := vc.ds.CheckEngineImageReadiness(defaultEngineImage, vc.controllerID)
	if err != nil {
		return false, err
	}

	isPreferredOwner := currentNodeEngineAvailable && isResponsible
	continueToBeOwner := currentNodeEngineAvailable && !preferredOwnerEngineAvailable && vc.controllerID == v.Status.OwnerID
	requiresNewOwner := currentNodeEngineAvailable && !preferredOwnerEngineAvailable && !currentOwnerEngineAvailable

	return isPreferredOwner || continueToBeOwner || requiresNewOwner, nil
}

func (vc *VolumeController) deleteReplica(r *longhorn.Replica, rs map[string]*longhorn.Replica) error {
	// Must call Update before removal to keep the fields up to date
	if _, err := vc.ds.UpdateReplica(r); err != nil {
		return err
	}
	if err := vc.ds.DeleteReplica(r.Name); err != nil {
		return err
	}
	delete(rs, r.Name)
	return nil
}

func (vc *VolumeController) deleteEngine(e *longhorn.Engine, es map[string]*longhorn.Engine) error {
	// Must call Update before removal to keep the fields up to date
	if _, err := vc.ds.UpdateEngine(e); err != nil {
		return err
	}
	if err := vc.ds.DeleteEngine(e.Name); err != nil {
		return err
	}
	delete(es, e.Name)
	return nil
}

// enqueueVolumesForShareManager enqueues all volumes that are currently claimed by this share manager
func (vc *VolumeController) enqueueVolumesForShareManager(obj interface{}) {
	sm, isShareManager := obj.(*longhorn.ShareManager)
	if !isShareManager {
		deletedState, ok := obj.(*cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to requeue the claimed volumes
		sm, ok = deletedState.Obj.(*longhorn.ShareManager)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained non ShareManager object: %#v", deletedState.Obj))
			return
		}
	}

	// we can queue the key directly since a share manager only manages a single volume from it's own namespace
	// and there is no need for us to retrieve the whole object, since we already know the volume name
	key := sm.Namespace + "/" + sm.Name
	vc.queue.Add(key)
}

// ReconcileShareManagerState is responsible for syncing the state of shared volumes with their share manager
func (vc *VolumeController) ReconcileShareManagerState(volume *longhorn.Volume) error {
	log := getLoggerForVolume(vc.logger, volume)
	sm, err := vc.ds.GetShareManager(volume.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		log.WithError(err).Error("Failed to get share manager for volume")
		return err
	}

	if volume.Spec.AccessMode != types.AccessModeReadWriteMany || volume.Spec.Migratable {
		if sm != nil {
			log.Info("Removing share manager for non shared volume")
			if err := vc.ds.DeleteShareManager(volume.Name); err != nil && !datastore.ErrorIsNotFound(err) {
				return err
			}
		}
		return nil
	}

	image, err := vc.ds.GetSettingValueExisted(types.SettingNameDefaultShareManagerImage)
	if err != nil {
		return err
	}

	// no ShareManager create a new one
	if sm == nil {
		sm, err = vc.createShareManagerForVolume(volume, image)
		if err != nil {
			log.WithError(err).Errorf("Failed to create share manager %v", volume.Name)
			return err
		}
	}

	if image != "" && sm.Spec.Image != image {
		sm.Spec.Image = image
		sm.ObjectMeta.Labels = types.GetShareManagerLabels(volume.Name, image)
		if sm, err = vc.ds.UpdateShareManager(sm); err != nil {
			return err
		}

		log.Infof("Updated image for share manager from %v to %v", sm.Spec.Image, image)
	}

	// kill the workload pods, when the share manager goes into error state
	// easiest approach is to set the RemountRequestedAt variable,
	// since that is already responsible for killing the workload pods
	if sm.Status.State == types.ShareManagerStateError || sm.Status.State == types.ShareManagerStateUnknown {
		volume.Status.RemountRequestedAt = vc.nowHandler()
		msg := fmt.Sprintf("Volume %v requested remount at %v", volume.Name, volume.Status.RemountRequestedAt)
		vc.eventRecorder.Eventf(volume, v1.EventTypeNormal, EventReasonRemount, msg)
	}

	// sync the share state and endpoint
	volume.Status.ShareState = sm.Status.State
	volume.Status.ShareEndpoint = sm.Status.Endpoint
	return nil
}

func (vc *VolumeController) createShareManagerForVolume(volume *longhorn.Volume, image string) (*longhorn.ShareManager, error) {
	sm := &longhorn.ShareManager{
		ObjectMeta: metav1.ObjectMeta{
			Name:            volume.Name,
			Namespace:       vc.namespace,
			Labels:          types.GetShareManagerLabels(volume.Name, image),
			OwnerReferences: datastore.GetOwnerReferencesForVolume(volume),
		},
		Spec: types.ShareManagerSpec{
			Image: image,
		},
	}

	return vc.ds.CreateShareManager(sm)
}
