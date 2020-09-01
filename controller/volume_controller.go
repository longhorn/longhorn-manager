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
	CronJobBackoffLimit = 3
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

	vStoreSynced cache.InformerSynced
	eStoreSynced cache.InformerSynced
	rStoreSynced cache.InformerSynced

	scheduler *scheduler.ReplicaScheduler

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

		vStoreSynced: volumeInformer.Informer().HasSynced,
		eStoreSynced: engineInformer.Informer().HasSynced,
		rStoreSynced: replicaInformer.Informer().HasSynced,

		nowHandler: util.Now,
	}

	vc.scheduler = scheduler.NewReplicaScheduler(ds)

	volumeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			v := obj.(*longhorn.Volume)
			vc.enqueueVolume(v)
		},
		UpdateFunc: func(old, cur interface{}) {
			curV := cur.(*longhorn.Volume)
			vc.enqueueVolume(curV)
		},
		DeleteFunc: func(obj interface{}) {
			v := obj.(*longhorn.Volume)
			vc.enqueueVolume(v)
		},
	})
	engineInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			vc.enqueueControlleeChange(obj)
		},
		UpdateFunc: func(old, cur interface{}) {
			vc.enqueueControlleeChange(cur)
		},
		DeleteFunc: func(obj interface{}) {
			vc.enqueueControlleeChange(obj)
		},
	})
	replicaInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			vc.enqueueControlleeChange(obj)
		},
		UpdateFunc: func(old, cur interface{}) {
			vc.enqueueControlleeChange(cur)
		},
		DeleteFunc: func(obj interface{}) {
			vc.enqueueControlleeChange(obj)
		},
	})
	return vc
}

func (vc *VolumeController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer vc.queue.ShutDown()

	vc.logger.Infof("Start Longhorn volume controller")
	defer vc.logger.Infof("Shutting down Longhorn volume controller")

	if !controller.WaitForCacheSync("longhorn engines", stopCh, vc.vStoreSynced, vc.eStoreSynced, vc.rStoreSynced) {
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
	vc.logger.Warnf("Dropping Longhorn volume %v out of the queue: %v", key, err)
	vc.queue.Forget(key)
}

func getLoggerForVolume(logger logrus.FieldLogger, v *longhorn.Volume) logrus.FieldLogger {
	return logger.WithFields(
		logrus.Fields{
			"volume":   v.Name,
			"frontend": v.Spec.Frontend,
			"state":    v.Status.State,
			"owner":    v.Status.OwnerID,
		},
	)
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
			vc.logger.WithField("volume", name).Info("Longhorn volume has been deleted")
			return nil
		}
		return err
	}

	log := getLoggerForVolume(vc.logger, volume)

	if volume.Status.OwnerID != vc.controllerID {
		if !vc.isResponsibleFor(volume) {
			// Not mines
			return nil
		}
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
			if err := vc.ds.DeletePersisentVolume(kubeStatus.PVName); err != nil {
				if !datastore.ErrorIsNotFound(err) {
					return err
				}
			}
		}

		if kubeStatus.PVCName != "" && kubeStatus.LastPVCRefAt == "" {
			if err := vc.ds.DeletePersisentVolumeClaim(kubeStatus.Namespace, kubeStatus.PVCName); err != nil {
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

	if err := vc.ReconcileVolumeState(volume, engines, replicas); err != nil {
		return err
	}

	if err := vc.cleanupReplicas(volume, engine, replicas); err != nil {
		return err
	}

	return nil
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

// ReconcileEngineReplicaState will get the current main engine e.Status.ReplicaModeMap and e.Status.RestoreStatus, then update
// v and rs accordingly.
func (vc *VolumeController) ReconcileEngineReplicaState(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to reconcile engine/replica state for %v", v.Name)
	}()

	log := getLoggerForVolume(vc.logger, v)

	if e == nil {
		return nil
	}

	if e.Status.CurrentState == types.InstanceStateUnknown {
		if v.Status.Robustness != types.VolumeRobustnessUnknown {
			v.Status.Robustness = types.VolumeRobustnessUnknown
			vc.eventRecorder.Eventf(v, v1.EventTypeWarning, EventReasonUnknown, "volume %v is unknown", v.Name)
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

	restoreStatusMap := map[string]*types.RestoreStatus{}
	replicaList := []*longhorn.Replica{}
	for _, r := range rs {
		replicaList = append(replicaList, r)
	}
	for addr, status := range e.Status.RestoreStatus {
		rName := datastore.ReplicaAddressToReplicaName(addr, replicaList)
		if _, exists := rs[rName]; exists {
			restoreStatusMap[rName] = status
		} else {
			log.Warnf("Found unknown replica in the restore status map, address %v, restore status: %+v", addr, status)
		}
	}

	// 1. remove ERR replicas
	// 2. count RW replicas
	healthyCount := 0
	for rName, mode := range e.Status.ReplicaModeMap {
		r := rs[rName]
		restoreStatus := restoreStatusMap[rName]
		if mode == types.ReplicaModeERR ||
			(restoreStatus != nil && restoreStatus.Error != "") {
			if r != nil {
				if restoreStatus != nil && restoreStatus.Error != "" {
					vc.eventRecorder.Eventf(v, v1.EventTypeWarning, EventReasonFailedRestore, "replica %v failed the restore: %s", r.Name, restoreStatus.Error)
				}
				e.Spec.LogRequested = true
				r.Spec.LogRequested = true
				r.Spec.FailedAt = vc.nowHandler()
				r.Spec.DesireState = types.InstanceStateStopped
				rs[rName] = r
			}
		} else if mode == types.ReplicaModeRW {
			if r != nil {
				// record once replica became healthy, so if it
				// failed in the future, we can tell it apart
				// from replica failed during rebuilding
				if r.Spec.HealthyAt == "" {
					r.Spec.HealthyAt = vc.nowHandler()
					rs[rName] = r
				}
				healthyCount++
			}
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
		if v.Status.State == types.VolumeStateAttached && !isDataLocalityDisabled(v) && !hasLocalReplicaOnSameNodeAsEngine(e, rs) {
			if err := vc.replenishReplicas(v, e, rs, e.Spec.NodeID); err != nil {
				return err
			}
		}

	} else { // healthyCount < v.Spec.NumberOfReplicas
		v.Status.Robustness = types.VolumeRobustnessDegraded
		if oldRobustness != types.VolumeRobustnessDegraded {
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

func (vc *VolumeController) getHealthyReplicaCount(rs map[string]*longhorn.Replica) int {
	var healthyCount int

	for _, r := range rs {
		if r.Spec.FailedAt == "" && r.Spec.HealthyAt != "" {
			healthyCount++
		}
	}

	return healthyCount
}

func (vc *VolumeController) hasReplicaEvictionRequested(rs map[string]*longhorn.Replica) bool {
	for _, r := range rs {
		if r.Status.EvictionRequested == true {
			return true
		}
	}

	return false
}

func (vc *VolumeController) cleanupReplicas(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) error {
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
	healthyCount := vc.getHealthyReplicaCount(rs)
	cleanupLeftoverReplicas := !vc.isVolumeUpgrading(v)
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

		// 1. failed before ever became healthy (RW), mostly failed during rebuilding
		// 2. failed too long ago, became stale and unnecessary to keep
		// around, unless we don't any healthy replicas
		if r.Spec.HealthyAt == "" || (healthyCount != 0 && staled) {
			log.WithField("replica", r.Name).Info("Cleaning up corrupted, staled replica")
			if err := vc.deleteReplica(r, rs); err != nil {
				return errors.Wrapf(err, "cannot cleanup staled replica %v", r.Name)
			}
		}
	}

	return nil
}

func (vc *VolumeController) cleanupFailedToScheduledReplicas(v *longhorn.Volume, rs map[string]*longhorn.Replica) (err error) {
	healthyCount := vc.getHealthyReplicaCount(rs)
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
	healthyCount := vc.getHealthyReplicaCount(rs)

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
	healthyCount = vc.getHealthyReplicaCount(rs)

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
	} else { // v.Status.CurrentNodeID != ""
		if v.Spec.NodeID != "" {
			if v.Spec.NodeID != v.Status.CurrentNodeID {
				return fmt.Errorf("volume %v has already attached to node %v, but asked to attach to node %v", v.Name, v.Status.CurrentNodeID, v.Spec.NodeID)
			}
		} else { // v.Spec.NodeID == ""
			// users can manually remount the volume and they won't be blocked by this field if remount fails
			v.Status.RemountRequired = false
		}
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

	if len(rs) == 0 {
		// first time creation
		if err := vc.replenishReplicas(v, e, rs, ""); err != nil {
			return err
		}
	}

	if e.Spec.LogRequested && e.Status.LogFetched {
		e.Spec.LogRequested = false
	}
	for _, r := range rs {
		if r.Spec.LogRequested && r.Status.LogFetched {
			r.Spec.LogRequested = false
		}
		rs[r.Name] = r
	}

	allScheduled := true
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
			allScheduled = false
		} else {
			rs[r.Name] = scheduledReplica
		}
	}
	if allScheduled {
		v.Status.Conditions = types.SetCondition(v.Status.Conditions,
			types.VolumeConditionTypeScheduled, types.ConditionStatusTrue, "", "")
	}

	if err := vc.reconcileVolumeSize(v, e, rs, allScheduled); err != nil {
		return err
	}

	if err := vc.checkForAutoAttachment(v, e, rs, allScheduled); err != nil {
		return err
	}
	if err := vc.checkForAutoDetachment(v, e, rs, allScheduled); err != nil {
		return err
	}

	// The frontend should be disabled for auto attached volumes.
	// The exception is that the frontend should be enabled for the block device expansion during the offline expansion.
	if v.Spec.NodeID == "" && v.Status.CurrentNodeID != "" {
		v.Status.FrontendDisabled = true
	} else {
		v.Status.FrontendDisabled = v.Spec.DisableFrontend
	}

	allFaulted := true
	for _, r := range rs {
		if r.Spec.FailedAt == "" {
			allFaulted = false
		}
	}
	if allFaulted {
		v.Status.Robustness = types.VolumeRobustnessFaulted
		v.Status.CurrentNodeID = ""

		autoSalvage, err := vc.ds.GetSettingAsBool(types.SettingNameAutoSalvage)
		if err != nil {
			return err
		}
		// make sure the volume is detached before automatically salvage
		if autoSalvage && v.Status.State == types.VolumeStateDetached && !v.Status.IsStandby && !v.Status.RestoreRequired {
			lastFailedAt := time.Time{}
			failedUsableReplicas := map[string]*longhorn.Replica{}
			dataExists := false
			for _, r := range rs {
				if r.Spec.HealthyAt == "" {
					continue
				}
				dataExists = true
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
				salvaged := false
				for _, r := range failedUsableReplicas {
					if util.TimestampWithinLimit(lastFailedAt, r.Spec.FailedAt, AutoSalvageTimeLimit) {
						r.Spec.FailedAt = ""
						log.WithField("replica", r.Name).Warn("Automatically salvaged volume replica")
						msg := fmt.Sprintf("Replica %v of volume %v has been automatically salvaged", r.Name, v.Name)
						vc.eventRecorder.Event(v, v1.EventTypeWarning, EventReasonAutoSalvaged, msg)
						salvaged = true
					}
				}
				if salvaged {
					// remount the reattached volume later if possible
					// For the auto-salvaged volume, `v.Status.CurrentNodeID` is empty but `v.Spec.NodeID` shouldn't be empty.
					v.Status.PendingNodeID = v.Spec.NodeID
					v.Status.RemountRequired = true
					v.Status.Robustness = types.VolumeRobustnessUnknown
				}
			}
		}

	} else { // !allFaulted
		// Don't need to touch other status since it should converge naturally
		if v.Status.Robustness == types.VolumeRobustnessFaulted {
			v.Status.Robustness = types.VolumeRobustnessUnknown
		}

		// reattach volume if detached unexpected and there are still healthy replicas
		if e.Status.CurrentState == types.InstanceStateError && v.Status.CurrentNodeID != "" {
			v.Status.PendingNodeID = v.Status.CurrentNodeID
			// remount the reattached volumes later if necessary
			v.Status.RemountRequired = true
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
			v.Status.CurrentNodeID = ""
		}
	}

	oldState := v.Status.State
	if v.Status.CurrentNodeID == "" {
		// the final state will be determined at the end of the clause
		if newVolume {
			v.Status.State = types.VolumeStateCreating
		} else {
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
		currentImage, err := vc.getEngineImage(v.Status.CurrentImage)
		if err != nil {
			log.WithError(err).Errorf("cannot access current image %v, skip auto attach", v.Status.CurrentImage)
		} else {
			if currentImage.Status.State != types.EngineImageStateReady {
				log.Warnf("current image %v is not ready, skip auto attach", v.Status.CurrentImage)
			} else if v.Status.PendingNodeID != "" {
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
			nodeDown, err := vc.ds.IsNodeDownOrDeleted(r.Spec.NodeID)
			if err != nil {
				return errors.Wrapf(err, "cannot find node %v", r.Spec.NodeID)
			}
			if nodeDown {
				if r.Spec.FailedAt == "" {
					r.Spec.FailedAt = vc.nowHandler()
				}
				r.Spec.DesireState = types.InstanceStateStopped
			} else if r.Spec.FailedAt == "" && r.Spec.EngineImage == v.Status.CurrentImage {
				r.Spec.DesireState = types.InstanceStateRunning
			}
			rs[r.Name] = r
		}
		replicaAddressMap := map[string]string{}
		for _, r := range rs {
			// Ignore unscheduled replicas
			if r.Spec.NodeID == "" {
				continue
			}
			if r.Spec.FailedAt != "" {
				continue
			}
			if r.Spec.EngineImage != v.Status.CurrentImage {
				continue
			}
			if r.Spec.EngineName != e.Name {
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
			return fmt.Errorf("no healthy replica for starting")
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
					// Here is the exception that the fronend is enabled but the volume is auto attached.
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

		if v.Status.RemountRequired {
			if err := util.RemountVolume(v.Name); err != nil {
				msg := fmt.Sprintf("cannot proceed to remount %v on %v: %v", v.Name, v.Spec.NodeID, err)
				vc.eventRecorder.Eventf(v, v1.EventTypeWarning, EventReasonRemount, msg)
			} else {
				v.Status.RemountRequired = false
				log.Infof("Volume remounted on node %v", v.Status.CurrentNodeID)
				msg := fmt.Sprintf("Volume %v has been remounted on node %v", v.Name, v.Status.CurrentNodeID)
				vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonRemount, msg)
			}
		}

		v.Status.State = types.VolumeStateAttached
		if oldState != v.Status.State {
			vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonAttached, "volume %v has been attached to %v", v.Name, v.Status.CurrentNodeID)
		}
	}
	return nil
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
	if vc.isVolumeUpgrading(v) {
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

	if hardNodeAffinity != "" && getRebuildingReplicaCount(e) == 0 {
		r, err := vc.createReplica(v, e, rs, hardNodeAffinity)
		if err != nil {
			return err
		}
		rs[r.Name] = r
	}

	replenishCount := vc.getReplenishReplicasCount(v, rs)
	if len(rs) != 0 && replenishCount > 0 {
		// For rebuild case, schedule and rebuild one replica at a time
		replenishCount = 1
		currentRebuilding := getRebuildingReplicaCount(e)
		if currentRebuilding != 0 {
			return nil
		}
	}
	for i := 0; i < replenishCount; i++ {
		r, err := vc.createReplica(v, e, rs, "")
		if err != nil {
			return err
		}
		rs[r.Name] = r
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

	if v.Status.State == types.VolumeStateDetached {
		if e.Spec.EngineImage != v.Spec.EngineImage {
			e.Spec.EngineImage = v.Spec.EngineImage
		}
		for _, r := range rs {
			if r.Spec.EngineImage != v.Spec.EngineImage {
				r.Spec.EngineImage = v.Spec.EngineImage
				rs[r.Name] = r
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

	oldImage, err := vc.getEngineImage(v.Status.CurrentImage)
	if err != nil {
		log.WithError(err).Warnf("Cannot get engine image %v for live upgrade", v.Status.CurrentImage)
		return nil
	}
	if oldImage.Status.State != types.EngineImageStateReady {
		log.Warnf("Requested for volume engine live upgrade from %v, but the image wasn't ready", oldImage.Spec.Image)
		return nil
	}
	newImage, err := vc.getEngineImage(v.Spec.EngineImage)
	if err != nil {
		log.WithError(err).Warnf("Cannot get engine image %v for live upgrade", v.Spec.EngineImage)
		return nil
	}
	if newImage.Status.State != types.EngineImageStateReady {
		log.Warnf("Requested for volume engine live upgrade from %v, but the image wasn't ready", newImage.Spec.Image)
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
	dataPathToOldReplica := map[string]*longhorn.Replica{}
	dataPathToNewReplica := map[string]*longhorn.Replica{}
	for _, r := range rs {
		if r.Spec.EngineImage == v.Status.CurrentImage {
			dataPathToOldReplica[r.Spec.DataPath] = r
		} else if r.Spec.EngineImage == v.Spec.EngineImage {
			dataPathToNewReplica[r.Spec.DataPath] = r
		} else {
			log.Warnf("Found unknown replica with image %v for live upgrade", r.Spec.EngineImage)
			unknownReplicas[r.Name] = r
		}
	}

	if err := vc.createAndStartMatchingReplicas(v, rs, dataPathToOldReplica, dataPathToNewReplica, func(r *longhorn.Replica, engineImage string) {
		r.Spec.EngineImage = engineImage
	}, v.Spec.EngineImage); err != nil {
		return err
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
		e.Spec.UpgradedReplicaAddressMap = replicaAddressMap
		e.Spec.EngineImage = v.Spec.EngineImage
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

func (vc *VolumeController) reconcileVolumeSize(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica, allScheduled bool) error {
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
		log.Infof("Start volume expand from size %v to size %v", e.Spec.VolumeSize, v.Spec.Size)
		v.Status.ExpansionRequired = true
	}

	e.Spec.VolumeSize = v.Spec.Size
	for _, r := range rs {
		r.Spec.VolumeSize = v.Spec.Size
	}

	return nil
}

func (vc *VolumeController) checkForAutoAttachment(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica, allScheduled bool) error {
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
	if !allScheduled {
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

func (vc *VolumeController) checkForAutoDetachment(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica, allScheduled bool) error {
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
	//	   3.2.2) or it's state `Degraded` with schedule failure.
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
	if e.Spec.RequestedBackupRestore != "" && e.Spec.RequestedBackupRestore == e.Status.LastRestoredBackup &&
		!v.Spec.Standby &&
		((cliAPIVersion >= engineapi.CLIVersionFour && !isPurging && (v.Status.Robustness == types.VolumeRobustnessHealthy || (v.Status.Robustness == types.VolumeRobustnessDegraded && !allScheduled))) ||
			(cliAPIVersion < engineapi.CLIVersionFour && (v.Status.Robustness == types.VolumeRobustnessHealthy || v.Status.Robustness == types.VolumeRobustnessDegraded))) {
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

func (vc *VolumeController) createReplica(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica, hardNodeAffinity string) (*longhorn.Replica, error) {
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
			EngineName:       e.Name,
			Active:           true,
			BaseImage:        v.Spec.BaseImage,
			HardNodeAffinity: hardNodeAffinity,
		},
	}

	return vc.ds.CreateReplica(replica)
}

func (vc *VolumeController) createReplicaManifest(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) *longhorn.Replica {
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
			EngineName: e.Name,
			Active:     true,
			BaseImage:  v.Spec.BaseImage,
		},
	}

	return replica
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

func (vc *VolumeController) enqueueVolume(v *longhorn.Volume) {
	key, err := controller.KeyFunc(v)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", v, err))
		return
	}

	vc.queue.AddRateLimited(key)
}

func (vc *VolumeController) enqueueControlleeChange(obj interface{}) {
	metaObj, err := meta.Accessor(obj)
	if err != nil {
		vc.logger.WithError(err).Warnf("BUG: %v cannot be convert to metav1.Object", obj)
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
	// Not ours
	if volume.Status.OwnerID != vc.controllerID {
		return
	}
	vc.enqueueVolume(volume)
}

func (vc *VolumeController) createCronJob(v *longhorn.Volume, job *types.RecurringJob, suspend bool) (*batchv1beta1.CronJob, error) {
	backoffLimit := int32(CronJobBackoffLimit)
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
	// for mounting inside container
	privilege := true
	cronJob := &batchv1beta1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:            types.GetCronJobNameForVolumeAndJob(v.Name, job.Name),
			Namespace:       vc.namespace,
			OwnerReferences: datastore.GetOwnerReferencesForVolume(v),
		},
		Spec: batchv1beta1.CronJobSpec{
			Schedule:          job.Cron,
			ConcurrencyPolicy: batchv1beta1.ForbidConcurrent,
			Suspend:           &suspend,
			JobTemplate: batchv1beta1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					BackoffLimit: &backoffLimit,
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name: types.GetCronJobNameForVolumeAndJob(v.Name, job.Name),
						},
						Spec: v1.PodSpec{
							NodeName: v.Status.CurrentNodeID,
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

	suspended := false
	if v.Status.State != types.VolumeStateAttached {
		suspended = true
	}

	// the cronjobs are RO in the map, but not the map itself
	appliedCronJobROs, err := vc.ds.ListVolumeCronJobROs(v.Name)
	if err != nil {
		return err
	}

	currentCronJobs := make(map[string]*batchv1beta1.CronJob)
	for _, job := range v.Spec.RecurringJobs {
		cronJob, err := vc.createCronJob(v, &job, suspended)
		if err != nil {
			return err
		}
		currentCronJobs[cronJob.Name] = cronJob
	}

	for name, cronJob := range currentCronJobs {
		if appliedCronJobROs[name] == nil {
			_, err := vc.ds.CreateVolumeCronJob(v.Name, cronJob)
			if err != nil {
				return err
			}
		} else if !reflect.DeepEqual(appliedCronJobROs[name].Spec, cronJob) {
			_, err := vc.ds.UpdateVolumeCronJob(v.Name, cronJob)
			if err != nil {
				return err
			}
		}
	}
	for name, job := range appliedCronJobROs {
		if currentCronJobs[name] == nil {
			if err := vc.ds.DeleteCronJob(name); err != nil {
				return err
			}
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

func (vc *VolumeController) isVolumeUpgrading(v *longhorn.Volume) bool {
	return v.Status.CurrentImage != v.Spec.EngineImage
}

func (vc *VolumeController) getEngineImage(image string) (*longhorn.EngineImage, error) {
	name := types.GetEngineImageChecksumName(image)
	img, err := vc.ds.GetEngineImage(name)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get engine image %v", image)
	}
	return img, nil
}

func (vc *VolumeController) getCurrentEngineAndExtra(v *longhorn.Volume, es map[string]*longhorn.Engine) (currentEngine, otherEngine *longhorn.Engine, err error) {
	if len(es) > 2 {
		return nil, nil, fmt.Errorf("more than two engines exists")
	}
	for _, e := range es {
		if e.Spec.NodeID == v.Status.CurrentNodeID &&
			e.Spec.DesireState == types.InstanceStateRunning &&
			e.Status.CurrentState == types.InstanceStateRunning {
			currentEngine = e
		} else {
			otherEngine = e
		}
	}
	if currentEngine == nil {
		return nil, nil, fmt.Errorf("volume %v is not attached", v.Name)
	}
	return
}

func (vc *VolumeController) getCurrentEngineAndCleanupOthers(v *longhorn.Volume, es map[string]*longhorn.Engine) (current *longhorn.Engine, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to clean up the extra engine for %v", v.Name)
	}()
	current, extra, err := vc.getCurrentEngineAndExtra(v, es)
	if err != nil {
		return nil, err
	}
	if extra != nil && extra.DeletionTimestamp == nil {
		if err := vc.deleteEngine(extra, es); err != nil {
			return nil, err
		}
	}
	return current, nil
}

func (vc *VolumeController) createAndStartMatchingReplicas(v *longhorn.Volume,
	rs, pathToOldRs, pathToNewRs map[string]*longhorn.Replica,
	fixupFunc func(r *longhorn.Replica, obj string), obj string) error {
	log := getLoggerForVolume(vc.logger, v)

	if len(pathToNewRs) == v.Spec.NumberOfReplicas {
		return nil
	}

	if len(pathToOldRs) != v.Spec.NumberOfReplicas {
		log.Debug("Volume healthy replica counts doesn't match")
		return nil
	}

	for path, r := range pathToOldRs {
		if pathToNewRs[path] != nil {
			continue
		}
		nr := vc.duplicateReplica(r, v)
		nr.Spec.DesireState = types.InstanceStateRunning
		nr.Spec.Active = false
		fixupFunc(nr, obj)
		nr, err := vc.ds.CreateReplica(nr)
		if err != nil {
			return errors.Wrapf(err, "cannot create matching replica %v for volume %v", nr.Name, v.Name)
		}
		pathToNewRs[path] = nr
		rs[nr.Name] = nr
	}
	return nil
}

func (vc *VolumeController) switchActiveReplicas(rs map[string]*longhorn.Replica,
	activeCondFunc func(r *longhorn.Replica, obj string) bool, obj string) error {

	// Deletion of an active replica will trigger the cleanup process to
	// delete the volume data on the disk.
	// Set `active` at last to prevent data loss
	for _, r := range rs {
		if activeCondFunc(r, obj) != r.Spec.Active {
			r.Spec.Active = activeCondFunc(r, obj)
			rs[r.Name] = r
		}
	}
	return nil
}

func (vc *VolumeController) isResponsibleFor(v *longhorn.Volume) bool {
	return isControllerResponsibleFor(vc.controllerID, vc.ds, v.Name, v.Spec.NodeID, v.Status.OwnerID)
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
