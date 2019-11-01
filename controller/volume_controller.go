package controller

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"encoding/json"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	batchv1 "k8s.io/api/batch/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	"k8s.io/api/core/v1"
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
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/backupstore"
	imutil "github.com/longhorn/longhorn-engine/pkg/instance-manager/util"

	"github.com/longhorn/longhorn-manager/datastore"
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
)

const (
	LabelRecurringJob = "RecurringJob"

	CronJobBackoffLimit = 3
)

type VolumeController struct {
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

	queue workqueue.RateLimitingInterface

	scheduler *scheduler.ReplicaScheduler

	// for unit test
	nowHandler func() string
}

func NewVolumeController(
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

		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "longhorn-volume"),

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

	logrus.Infof("Start Longhorn volume controller")
	defer logrus.Infof("Shutting down Longhorn volume controller")

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
		logrus.Warnf("Error syncing Longhorn volume %v: %v", key, err)
		vc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logrus.Warnf("Dropping Longhorn volume %v out of the queue: %v", key, err)
	vc.queue.Forget(key)
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
			logrus.Infof("Longhorn volume %v has been deleted", key)
			return nil
		}
		return err
	}

	takeOver := false

	ownerDown := false
	if volume.Status.OwnerID != "" {
		ownerDown, err = vc.ds.IsNodeDownOrDeleted(volume.Status.OwnerID)
		if err != nil {
			logrus.Warnf("Found error while checking if volume %v owner is down or deleted: %v", volume.Name, err)
		}
	}

	if vc.controllerID == volume.Spec.NodeID {
		takeOver = true
	} else if volume.Status.OwnerID == "" {
		if volume.Spec.NodeID == "" {
			takeOver = true
		}
	} else { // volume.Status.OwnerID != ""
		if ownerDown {
			takeOver = true
		}
	}

	if volume.Status.OwnerID != vc.controllerID {
		if !takeOver {
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
		logrus.Debugf("Volume controller %v picked up %v", vc.controllerID, volume.Name)
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
	defer func() {
		// we're going to update volume assume things changes
		if err == nil && !reflect.DeepEqual(existingVolume.Status, volume.Status) {
			_, err = vc.ds.UpdateVolumeStatus(volume)
		}
		// requeue if it's conflict
		if apierrors.IsConflict(errors.Cause(err)) {
			logrus.Debugf("Requeue %v due to conflict", key)
			vc.enqueueVolume(volume)
			err = nil
		}
	}()

	engine := vc.getNodeAttachedEngine(volume.Status.CurrentNodeID, engines)
	if engine == nil {
		if len(engines) == 1 {
			for _, e := range engines {
				engine = e
				break
			}
		} else if len(engines) > 1 {
			return fmt.Errorf("BUG: multiple engines when volume is detached")
		}
	}

	if len(engines) <= 1 {
		if err := vc.ReconcileEngineReplicaState(volume, engine, replicas); err != nil {
			return err
		}
	}

	if err := vc.ReconcileVolumeState(volume, engine, replicas); err != nil {
		return err
	}

	if len(engines) <= 1 {
		if err := vc.updateRecurringJobs(volume); err != nil {
			return err
		}

		if err := vc.updateEngineForStandbyVolume(volume, engine); err != nil {
			return err
		}

		if err := vc.upgradeEngineForVolume(volume, engine, replicas); err != nil {
			return err
		}
	}

	if err := vc.cleanupCorruptedOrStaleReplicas(volume, replicas); err != nil {
		return err
	}

	return nil
}

func (vc *VolumeController) getNodeAttachedEngine(node string, es map[string]*longhorn.Engine) *longhorn.Engine {
	if node == "" {
		return nil
	}
	for _, e := range es {
		if e.Spec.NodeID == node {
			return e
		}
	}
	return nil
}

// ReconcileEngineReplicaState will get the current main engine e.Status.ReplicaModeMap, then update
// v and rs accordingly.
func (vc *VolumeController) ReconcileEngineReplicaState(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to reconcile engine/replica state for %v", v.Name)
	}()

	if e == nil {
		return nil
	}

	if e.Status.CurrentState != types.InstanceStateRunning {
		// Corner case: when vc found healthyCount is 0, vc failed to update volume to faulted state
		// and volume may be detached (engine stopped state).
		// Then the volume needs to be reconciled to the faulted state here.
		availableCount := 0
		for _, r := range rs {
			if r.Spec.FailedAt == "" {
				availableCount++
			}
		}
		if availableCount == 0 {
			oldRobustness := v.Status.Robustness
			v.Status.Robustness = types.VolumeRobustnessFaulted
			if oldRobustness != types.VolumeRobustnessFaulted {
				vc.eventRecorder.Eventf(v, v1.EventTypeWarning, EventReasonFaulted, "volume %v became faulted", v.Name)
			}
			// detach the volume
			v.Status.CurrentNodeID = ""
		}
		return nil
	}

	// wait for monitoring to start
	if e.Status.ReplicaModeMap == nil {
		return nil
	}

	// 1. remove ERR replicas
	// 2. count RW replicas
	healthyCount := 0
	for rName, mode := range e.Status.ReplicaModeMap {
		r := rs[rName]
		if mode == types.ReplicaModeERR {
			if r != nil {
				e.Spec.LogRequested = true
				r.Spec.LogRequested = true
				r.Spec.FailedAt = vc.nowHandler()
				r.Spec.DesireState = types.InstanceStateStopped
				r, err = vc.ds.UpdateReplica(r)
				if err != nil {
					return err
				}
				rs[rName] = r
			}
		} else if mode == types.ReplicaModeRW {
			if r != nil {
				// record once replica became healthy, so if it
				// failed in the future, we can tell it apart
				// from replica failed during rebuilding
				if r.Spec.HealthyAt == "" {
					r.Spec.HealthyAt = vc.nowHandler()
					r, err = vc.ds.UpdateReplica(r)
					if err != nil {
						return err
					}
					rs[rName] = r
				}
				healthyCount++
			}
		}
	}

	oldRobustness := v.Status.Robustness
	if healthyCount == 0 { // no healthy replica exists, going to faulted
		v.Status.Robustness = types.VolumeRobustnessFaulted
		if oldRobustness != types.VolumeRobustnessFaulted {
			vc.eventRecorder.Eventf(v, v1.EventTypeWarning, EventReasonFaulted, "volume %v became faulted", v.Name)
		}
	} else if healthyCount >= v.Spec.NumberOfReplicas {
		v.Status.Robustness = types.VolumeRobustnessHealthy
		if oldRobustness == types.VolumeRobustnessDegraded {
			vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonHealthy, "volume %v became healthy", v.Name)
		}
	} else { // healthyCount < v.Spec.NumberOfReplicas
		v.Status.Robustness = types.VolumeRobustnessDegraded
		if oldRobustness != types.VolumeRobustnessDegraded {
			vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonDegraded, "volume %v became degraded", v.Name)
		}
		// start rebuilding if necessary
		if err = vc.replenishReplicas(v, e, rs); err != nil {
			return err
		}
		// replicas will be started by ReconcileVolumeState() later
	}
	return nil
}

func (vc *VolumeController) cleanupCorruptedOrStaleReplicas(v *longhorn.Volume, rs map[string]*longhorn.Replica) error {
	hasHealthyReplicas := false
	for _, r := range rs {
		if r.Spec.FailedAt == "" && r.Spec.HealthyAt != "" {
			hasHealthyReplicas = true
			break
		}
	}
	cleanupLeftoverReplicas := !vc.isVolumeUpgrading(v)

	for _, r := range rs {
		if cleanupLeftoverReplicas {
			if !r.Spec.Active {
				// Leftover by live upgrade. Successful or not, there are replicas left to clean up
				if err := vc.ds.DeleteReplica(r.Name); err != nil {
					return err
				}
				delete(rs, r.Name)
				continue
			} else if r.Spec.EngineImage != v.Spec.EngineImage {
				// r.Spec.Active shouldn't be set for the leftover replicas, something must wrong
				logrus.Errorf("BUG: replica %v engine image %v is different from volume %v engine image %v, "+
					"but replica spec.Active has been set",
					r.Name, r.Spec.EngineImage, v.Name, v.Spec.EngineImage)
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
		if r.Spec.HealthyAt == "" || (hasHealthyReplicas && staled) {

			logrus.Infof("Cleaning up corrupted or staled replica %v", r.Name)
			if err := vc.ds.DeleteReplica(r.Name); err != nil {
				return errors.Wrap(err, "cannot cleanup stale replicas")
			}
			delete(rs, r.Name)
		}
	}
	return nil
}

// ReconcileVolumeState handles the attaching and detaching of volume
func (vc *VolumeController) ReconcileVolumeState(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to reconcile volume state for %v", v.Name)
	}()

	newVolume := false
	if v.Status.CurrentImage == "" {
		v.Status.CurrentImage = v.Spec.EngineImage
	}

	if v.Spec.FromBackup != "" && !v.Status.RestoreInitiated {
		kubeStatus := &types.KubernetesStatus{}

		backupTarget, err := manager.GenerateBackupTarget(vc.ds)
		if err != nil {
			return err
		}
		backup, err := backupTarget.GetBackup(v.Spec.FromBackup)
		if err != nil {
			return fmt.Errorf("cannot get backup %v: %v", v.Spec.FromBackup, err)
		}

		// If KubernetesStatus is set on Backup, restore it.
		if statusJSON, ok := backup.Labels[types.KubernetesStatusLabel]; ok {
			if err := json.Unmarshal([]byte(statusJSON), kubeStatus); err != nil {
				logrus.Warningf("Ignore KubernetesStatus json for volume %v: backup %v: %v",
					v.Name, backup.Name, err)
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

		// set LastBackup for standby volumes only
		if v.Spec.Standby {
			v.Status.LastBackup = backup.Name
		}

		v.Status.KubernetesStatus = *kubeStatus
		v.Status.InitialRestorationRequired = true

		v.Status.RestoreInitiated = true
	}

	if v.Status.CurrentNodeID == "" {
		// If there is a pending attach operation, we need to stay detached until PendingNodeID was cleared
		if v.Status.PendingNodeID == "" && v.Status.Robustness != types.VolumeRobustnessFaulted {
			v.Status.CurrentNodeID = v.Spec.NodeID
		}
	} else { // v.Status.CurrentNodeID != ""
		if v.Spec.NodeID != "" {
			if v.Spec.NodeID != v.Status.CurrentNodeID {
				return fmt.Errorf("volume %v has already attached to node %v, but asked to attach to node %v", v.Name, v.Status.CurrentNodeID, v.Spec.NodeID)
			}
		} else { // v.Spec.NodeID == ""
			if !(v.Status.InitialRestorationRequired || v.Spec.Standby) {
				v.Status.CurrentNodeID = ""
			}
		}
	}

	if e == nil {
		// first time creation
		//TODO this creation won't be recorded in the engines, since
		//it's nil
		e, err = vc.createEngine(v)
		if err != nil {
			return err
		}
		newVolume = true
	}

	// TODO: Will remove this reference kind correcting after all Longhorn components having used the new kinds
	if len(e.OwnerReferences) < 1 || e.OwnerReferences[0].Kind != types.LonghornKindVolume {
		e.OwnerReferences = datastore.GetOwnerReferencesForVolume(v)
		e, err = vc.ds.UpdateEngine(e)
		if err != nil {
			return err
		}
	}

	if len(rs) == 0 {
		// first time creation
		if err = vc.replenishReplicas(v, e, rs); err != nil {
			return err
		}
	}

	v.Status.FrontendDisabled = v.Spec.DisableFrontend

	// InitialRestorationRequired means the volume is newly created restored volume and
	// it needs to be attached automatically so that its engine will be launched then
	// restore data from backup.
	if v.Status.InitialRestorationRequired {
		// for automatically attached volume, we should disable its frontend
		v.Status.FrontendDisabled = true
		if v.Spec.NodeID == "" {
			usableNode, err := vc.ds.GetRandomReadyNode()
			if err != nil {
				return err
			}
			v.Status.CurrentNodeID = usableNode.Name
		}
	}

	if e.Status.CurrentState == types.InstanceStateError && v.Status.CurrentNodeID != "" {
		if e.Spec.NodeID != "" && e.Spec.NodeID != v.Status.CurrentNodeID {
			return fmt.Errorf("BUG: engine %v nodeID %v doesn't match volume %v nodeID %v",
				e.Name, e.Spec.NodeID, v.Name, v.Status.CurrentNodeID)
		}
		node, err := vc.ds.GetKubernetesNode(v.Status.CurrentNodeID)
		if err != nil {
			return err
		}
		// If it's due to reboot, we're going to reattach the volume later
		// e.Status.NodeBootID would only reset when the instance stopped by request
		if e.Status.NodeBootID != "" && e.Status.NodeBootID != node.Status.NodeInfo.BootID {
			v.Status.PendingNodeID = v.Status.CurrentNodeID
			msg := fmt.Sprintf("Detect the reboot of volume %v attached node %v, reattach the volume", v.Name, v.Status.CurrentNodeID)
			logrus.Errorf(msg)
			vc.eventRecorder.Event(v, v1.EventTypeWarning, EventReasonRebooted, msg)
		} else {
			// Engine dead unexpected, force detaching the volume
			msg := fmt.Sprintf("Engine of volume %v dead unexpectedly, detach the volume", v.Name)
			logrus.Errorf(msg)
			vc.eventRecorder.Eventf(v, v1.EventTypeWarning, EventReasonFaulted, msg)
			e.Spec.LogRequested = true
			for _, r := range rs {
				if r.Status.CurrentState == types.InstanceStateRunning {
					r.Spec.LogRequested = true
					r, err = vc.ds.UpdateReplica(r)
					if err != nil {
						return err
					}
					rs[r.Name] = r
				}
			}
		}
		v.Status.CurrentNodeID = ""
	}

	allScheduled := true
	for _, r := range rs {
		// TODO: Will remove this reference kind correcting after all Longhorn components having used the new kinds
		if len(r.OwnerReferences) < 1 || r.OwnerReferences[0].Kind != types.LonghornKindVolume {
			r.OwnerReferences = datastore.GetOwnerReferencesForVolume(v)
			r, err = vc.ds.UpdateReplica(r)
			if err != nil {
				return err
			}
		}

		// check whether the replica need to be scheduled
		if r.Spec.NodeID != "" {
			continue
		}
		scheduledReplica, err := vc.scheduler.ScheduleReplica(r, rs, v)
		if err != nil {
			return err
		}
		if scheduledReplica == nil {
			logrus.Errorf("unable to schedule replica %v of volume %v", r.Name, v.Name)
			condition := types.GetVolumeConditionFromStatus(v.Status, types.VolumeConditionTypeScheduled)
			if condition.Status != types.ConditionStatusFalse {
				condition.Status = types.ConditionStatusFalse
				condition.LastTransitionTime = util.Now()
			}
			//condition.LastProbeTime = util.Now()
			condition.Reason = types.VolumeConditionReasonReplicaSchedulingFailure
			v.Status.Conditions[types.VolumeConditionTypeScheduled] = condition
			allScheduled = false
			// no need to continue, since we won't able to schedule
			// more replicas if we failed this one
			break
		} else {
			scheduledReplica, err = vc.ds.UpdateReplica(scheduledReplica)
			if err != nil {
				return err
			}
			rs[r.Name] = scheduledReplica
		}
	}
	if allScheduled {
		condition := types.GetVolumeConditionFromStatus(v.Status, types.VolumeConditionTypeScheduled)
		if condition.Status != types.ConditionStatusTrue {
			condition.Status = types.ConditionStatusTrue
			condition.Reason = ""
			condition.Message = ""
			condition.LastTransitionTime = util.Now()
		}
		//condition.LastProbeTime = util.Now()
		v.Status.Conditions[types.VolumeConditionTypeScheduled] = condition
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
	} else { // !allFaulted
		// Don't need to touch other status since it should converge naturally
		if v.Status.Robustness == types.VolumeRobustnessFaulted {
			v.Status.Robustness = types.VolumeRobustnessUnknown
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
		if v.Status.Robustness != types.VolumeRobustnessFaulted {
			v.Status.Robustness = types.VolumeRobustnessUnknown
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
					r, err = vc.ds.UpdateReplica(r)
					if err != nil {
						return err
					}
					rs[r.Name] = r
				}
			}
		}
		if e.Spec.DesireState != types.InstanceStateStopped || e.Spec.NodeID != "" {
			if v.Status.Robustness == types.VolumeRobustnessFaulted {
				e.Spec.LogRequested = true
			}
			e.Spec.NodeID = ""
			e.Spec.DesireState = types.InstanceStateStopped
			// Need to unset these fields when detaching volume. It's for:
			//   1. regular restored volume which has completed restoration and is being detaching automatically.
			//   2. standby volume which has been activated and is being detaching.
			e.Spec.RequestedBackupRestore = ""
			e.Spec.BackupVolume = ""
			e, err = vc.ds.UpdateEngine(e)
			return err
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
				r, err := vc.ds.UpdateReplica(r)
				if err != nil {
					return err
				}
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
		// Automatic reattach the volume if PendingNodeID was set, it's for reboot
		if v.Status.PendingNodeID != "" {
			v.Status.CurrentNodeID = v.Status.PendingNodeID
			v.Status.PendingNodeID = ""
		}

	} else {
		// wait for offline engine upgrade to finish
		if v.Status.State == types.VolumeStateDetached && v.Status.CurrentImage != v.Spec.EngineImage {
			logrus.Debugf("Wait for offline upgrade of volume %v to finish", v.Name)
			return nil
		}
		// if engine was running, then we are attached already
		// (but we may still need to start rebuilding replicas)
		if e.Status.CurrentState != types.InstanceStateRunning {
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

		replicaUpdated := false
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
				r.Spec.FailedAt = vc.nowHandler()
				r.Spec.DesireState = types.InstanceStateStopped
				r, err = vc.ds.UpdateReplica(r)
				if err != nil {
					return err
				}
				rs[r.Name] = r
			}
			if r.Spec.FailedAt == "" &&
				r.Spec.DesireState != types.InstanceStateRunning &&
				r.Spec.EngineImage == v.Status.CurrentImage {

				r.Spec.DesireState = types.InstanceStateRunning
				replicaUpdated = true
				r, err := vc.ds.UpdateReplica(r)
				if err != nil {
					return err
				}
				rs[r.Name] = r
			}
		}
		// wait for instances to start
		if replicaUpdated {
			return nil
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
				logrus.Errorf("BUG: replica %v is running but IP is empty", r.Name)
				continue
			}
			if r.Status.Port == 0 {
				// Do not skip this replica if its engine image is CLIAPIVersion 1.
				if !isCLIAPIVersionOne {
					logrus.Errorf("BUG: replica %v is running but Port is empty", r.Name)
					continue
				}
			}
			replicaAddressMap[r.Name] = imutil.GetURL(r.Status.IP, r.Status.Port)
		}
		if len(replicaAddressMap) == 0 {
			return fmt.Errorf("no healthy replica for starting")
		}

		engineUpdated := false
		if e.Spec.DesireState != types.InstanceStateRunning {
			if e.Spec.NodeID != "" && e.Spec.NodeID != v.Status.CurrentNodeID {
				return fmt.Errorf("engine is on node %v vs volume on %v, must detach first",
					e.Spec.NodeID, v.Status.CurrentNodeID)
			}
			e.Spec.NodeID = v.Status.CurrentNodeID
			e.Spec.ReplicaAddressMap = replicaAddressMap
			e.Spec.DesireState = types.InstanceStateRunning
			e.Spec.DisableFrontend = v.Status.FrontendDisabled
			engineUpdated = true
		}
		if !vc.isVolumeUpgrading(v) {
			if !reflect.DeepEqual(e.Spec.ReplicaAddressMap, replicaAddressMap) {
				e.Spec.ReplicaAddressMap = replicaAddressMap
				engineUpdated = true
			}
		}
		if engineUpdated {
			e, err = vc.ds.UpdateEngine(e)
			if err != nil {
				return err
			}
		}
		// wait for engine to be up
		if e.Status.CurrentState != types.InstanceStateRunning {
			return nil
		}

		v.Status.State = types.VolumeStateAttached
		if oldState != v.Status.State {
			vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonAttached, "volume %v has been attached to %v", v.Name, v.Status.CurrentNodeID)
		}

		// If the newly created restored volume is state attached,
		// it means the volume is restoring or has restored data
		// from backup. Then it may need to be detached automatically.
		if v.Status.InitialRestorationRequired == true && v.Status.State == types.VolumeStateAttached {
			// The initial full restoration is complete.
			if e.Status.LastRestoredBackup != "" {
				v.Status.InitialRestorationRequired = false
				// For disaster recovery (standby) volume, it will incrementally
				// restore backups later. Hence it cannot be detached automatically.
				if !v.Spec.Standby {
					if e.Spec.RequestedBackupRestore == e.Status.LastRestoredBackup {
						v.Status.CurrentNodeID = ""
					}
				}
			}
		}
	}
	return nil
}

// replenishReplicas will keep replicas count to v.Spec.NumberOfReplicas
// It will count all the potentially usable replicas, since some replicas maybe
// blank or in rebuilding state
func (vc *VolumeController) replenishReplicas(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) error {
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

	replenishCount := vc.getReplenishReplicasCount(v, rs)
	for i := 0; i < replenishCount; i++ {
		r, err := vc.createReplica(v, e, rs)
		if err != nil {
			return err
		}
		rs[r.Name] = r
	}
	return nil
}

func (vc *VolumeController) getReplenishReplicasCount(v *longhorn.Volume, rs map[string]*longhorn.Replica) int {
	usableCount := 0
	for _, r := range rs {
		if r.Spec.FailedAt == "" {
			usableCount++
		}
	}

	return v.Spec.NumberOfReplicas - usableCount
}

func (vc *VolumeController) hasEngineStatusSynced(e *longhorn.Engine, rs map[string]*longhorn.Replica) bool {
	healthyCount := 0
	for _, r := range rs {
		if r.Spec.FailedAt == "" {
			healthyCount++
		}
	}

	if len(e.Spec.ReplicaAddressMap) != healthyCount {
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
			e, err = vc.ds.UpdateEngine(e)
			if err != nil {
				return err
			}
		}
		return nil
	}

	if v.Status.State == types.VolumeStateDetached {
		if e.Spec.EngineImage != v.Spec.EngineImage {
			e.Spec.EngineImage = v.Spec.EngineImage
			e, err = vc.ds.UpdateEngine(e)
			if err != nil {
				return err
			}
		}
		for _, r := range rs {
			if r.Spec.EngineImage != v.Spec.EngineImage {
				r.Spec.EngineImage = v.Spec.EngineImage
				r, err = vc.ds.UpdateReplica(r)
				if err != nil {
					return err
				}
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

	oldImage, err := vc.getEngineImage(v.Status.CurrentImage)
	if err != nil {
		logrus.Warnf("live upgrade: cannot get engine image %v: %v", v.Status.CurrentImage, err)
		return nil
	}
	if oldImage.Status.State != types.EngineImageStateReady {
		logrus.Warnf("live upgrade: volume %v engine upgrade from %v requests, but the image wasn't ready", v.Name, oldImage.Spec.Image)
		return nil
	}
	newImage, err := vc.getEngineImage(v.Spec.EngineImage)
	if err != nil {
		logrus.Warnf("live upgrade: cannot get engine image %v: %v", v.Spec.EngineImage, err)
		return nil
	}
	if newImage.Status.State != types.EngineImageStateReady {
		logrus.Warnf("live upgrade: volume %v engine upgrade from %v requests, but the image wasn't ready", v.Name, newImage.Spec.Image)
		return nil
	}

	if oldImage.Status.GitCommit == newImage.Status.GitCommit {
		logrus.Infof("live upgrade: Engine image %v and %v are identical, delay upgrade until detach for %v", oldImage.Spec.Image, newImage.Spec.Image, v.Name)
		return nil
	}

	if oldImage.Status.ControllerAPIVersion > newImage.Status.ControllerAPIVersion ||
		oldImage.Status.ControllerAPIVersion < newImage.Status.ControllerAPIMinVersion {
		logrus.Warnf("live upgrade: unable to live upgrade from %v to %v: the old controller version %v "+
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
			logrus.Warnf("live upgrade: found unknown replica with image %v of volume %v",
				r.Spec.EngineImage, v.Name)
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
				logrus.Errorf("BUG: replica %v is running but IP is empty", r.Name)
				continue
			}
			if r.Status.Port == 0 {
				logrus.Errorf("BUG: replica %v is running but IP is empty", r.Name)
				continue
			}
			replicaAddressMap[r.Name] = imutil.GetURL(r.Status.IP, r.Status.Port)
		}
		e.Spec.UpgradedReplicaAddressMap = replicaAddressMap
		e.Spec.EngineImage = v.Spec.EngineImage
		e, err = vc.ds.UpdateEngine(e)
		if err != nil {
			return err
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
	e, err = vc.ds.UpdateEngine(e)
	if err != nil {
		return err
	}
	// cleanupCorruptedOrStaleReplicas() will take care of old replicas
	logrus.Infof("Engine %v of volume %s has been upgraded from %v to %v", e.Name, v.Name, v.Status.CurrentImage, v.Spec.EngineImage)
	v.Status.CurrentImage = v.Spec.EngineImage

	return nil
}

func (vc *VolumeController) updateEngineForStandbyVolume(v *longhorn.Volume, e *longhorn.Engine) (err error) {
	if e == nil {
		return nil
	}

	if !v.Spec.Standby {
		return nil
	}

	if v.Status.LastBackup != "" && v.Status.LastBackup != e.Spec.RequestedBackupRestore {
		e.Spec.RequestedBackupRestore = v.Status.LastBackup
		e, err = vc.ds.UpdateEngine(e)
		if err != nil {
			return err
		}
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

		logrus.Debugf("Created engine %v for restored volume %v, BackupVolume is %v, RequestedBackupRestore is %v",
			engine.Name, v.Name, engine.Spec.BackupVolume, engine.Spec.RequestedBackupRestore)
	}

	return vc.ds.CreateEngine(engine)
}

// createReplica returns (nil, nil) for unschedulable replica
func (vc *VolumeController) createReplica(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) (*longhorn.Replica, error) {
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

	return vc.ds.CreateReplica(replica)
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
		logrus.Warnf("BUG: %v cannot be convert to metav1.Object: %v", obj, err)
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

func (vc *VolumeController) createCronJob(v *longhorn.Volume, job *types.RecurringJob, suspend bool, backupTarget string, credentialSecret string) (*batchv1beta1.CronJob, error) {
	backoffLimit := int32(CronJobBackoffLimit)
	cmd := []string{
		"longhorn-manager", "-d",
		"snapshot", v.Name,
		"--snapshot-name", job.Name,
		"--labels", LabelRecurringJob + "=" + job.Name,
		"--retain", strconv.Itoa(job.Retain),
	}
	for key, val := range job.Labels {
		cmd = append(cmd, "--labels", key+"="+val)
	}
	if job.Task == types.RecurringJobTypeBackup {
		cmd = append(cmd, "--backuptarget", backupTarget)
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
	if job.Task == types.RecurringJobTypeBackup {
		if credentialSecret != "" {
			credentials, err := vc.ds.GetCredentialFromSecret(credentialSecret)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot get credential secret %v", credentialSecret)
			}
			hasEndpoint := (credentials[types.AWSEndPoint] != "")

			util.ConfigEnvWithCredential(backupTarget, credentialSecret, hasEndpoint, &cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0])
		}
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

	settingBackupTarget, err := vc.ds.GetSetting(types.SettingNameBackupTarget)
	if err != nil {
		return err
	}
	backupTarget := settingBackupTarget.Value

	settingBackupCredentialSecret, err := vc.ds.GetSetting(types.SettingNameBackupTargetCredentialSecret)
	if err != nil {
		return err
	}
	backupCredentialSecret := settingBackupCredentialSecret.Value

	// the cronjobs are RO in the map, but not the map itself
	appliedCronJobROs, err := vc.ds.ListVolumeCronJobROs(v.Name)
	if err != nil {
		return err
	}

	currentCronJobs := make(map[string]*batchv1beta1.CronJob)
	for _, job := range v.Spec.RecurringJobs {
		if backupTarget == "" && job.Task == types.RecurringJobTypeBackup {
			return fmt.Errorf("cannot backup with empty backup target")
		}

		cronJob, err := vc.createCronJob(v, &job, suspended, backupTarget, backupCredentialSecret)
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
		if err := vc.ds.DeleteEngine(extra.Name); err != nil {
			return nil, err
		}
		delete(es, extra.Name)
	}
	return current, nil
}

func (vc *VolumeController) createAndStartMatchingReplicas(v *longhorn.Volume,
	rs, pathToOldRs, pathToNewRs map[string]*longhorn.Replica,
	fixupFunc func(r *longhorn.Replica, obj string), obj string) error {

	if len(pathToNewRs) == v.Spec.NumberOfReplicas {
		return nil
	}

	if len(pathToOldRs) != v.Spec.NumberOfReplicas {
		logrus.Debugf("volume %v: createAndStartMatchingReplicas: healthy replica counts doesn't match", v.Name)
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
			return errors.Wrapf(err, "volume %v: cannot create matching replica %v", v.Name, nr.Name)
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
		if !activeCondFunc(r, obj) && r.Spec.Active != false {
			r.Spec.Active = false
			r, err := vc.ds.UpdateReplica(r)
			if err != nil {
				return err
			}
			rs[r.Name] = r
		}
	}
	for _, r := range rs {
		if activeCondFunc(r, obj) && r.Spec.Active != true {
			r.Spec.Active = true
			r, err := vc.ds.UpdateReplica(r)
			if err != nil {
				return err
			}
			rs[r.Name] = r
		}
	}
	return nil
}
