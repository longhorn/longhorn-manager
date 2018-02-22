package controller

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/robfig/cron"

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

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhinformers "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1alpha1"
)

var (
	ownerKindVolume = longhorn.SchemeGroupVersion.WithKind("Volume").String()
)

const (
	engineSuffix  = "-controller"
	replicaSuffix = "-replica"
)

type VolumeController struct {
	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string
	EngineImage  string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	vStoreSynced cache.InformerSynced
	eStoreSynced cache.InformerSynced
	rStoreSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface

	recurringJobMutex *sync.Mutex
	recurringJobMap   map[string]*VolumeRecurringJob
}

type VolumeRecurringJob struct {
	recurringJobs []types.RecurringJob
	cron          *cron.Cron
}

func NewVolumeController(
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	volumeInformer lhinformers.VolumeInformer,
	engineInformer lhinformers.ControllerInformer,
	replicaInformer lhinformers.ReplicaInformer,
	kubeClient clientset.Interface,
	namespace string, controllerID string, engineImage string) *VolumeController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	vc := &VolumeController{
		ds:           ds,
		namespace:    namespace,
		controllerID: controllerID,
		EngineImage:  engineImage,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-volume-controller"}),

		vStoreSynced: volumeInformer.Informer().HasSynced,
		eStoreSynced: engineInformer.Informer().HasSynced,
		rStoreSynced: replicaInformer.Informer().HasSynced,

		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "longhorn-volume"),

		recurringJobMutex: &sync.Mutex{},
		recurringJobMap:   make(map[string]*VolumeRecurringJob),
	}

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
		return err
	}
	if volume == nil {
		logrus.Infof("Longhorn volume %v has been deleted", key)
		return nil
	}

	// Not ours
	if volume.Spec.OwnerID != vc.controllerID {
		return nil
	}

	engine, err := vc.ds.GetVolumeEngine(volume.Name)
	if err != nil {
		return err
	}
	replicas, err := vc.ds.GetVolumeReplicas(volume.Name)
	if err != nil {
		return err
	}

	if volume.DeletionTimestamp != nil {
		if volume.Status.State != types.VolumeStateDeleting {
			volume.Status.State = types.VolumeStateDeleting
			volume, err = vc.ds.UpdateVolume(volume)
			if err != nil {
				return err
			}
		}
		vc.stopRecurringJobs(volume)

		if engine != nil && engine.DeletionTimestamp == nil {
			if err := vc.ds.DeleteEngine(engine.Name); err != nil {
				return err
			}
		}
		// now engine has been deleted or in the process

		for _, r := range replicas {
			if r.DeletionTimestamp == nil {
				if err := vc.ds.DeleteReplica(r.Name); err != nil {
					return err
				}
			}
		}
		// now replicas has been deleted or in the process

		return vc.ds.RemoveFinalizerForVolume(volume.Name)
	}

	defer func() {
		// we're going to update volume assume things changes
		if err == nil {
			_, err = vc.ds.UpdateVolume(volume)
		}
		// requeue if it's conflict
		if apierrors.IsConflict(errors.Cause(err)) {
			logrus.Debugf("Requeue %v due to conflict", key)
			vc.enqueueVolume(volume)
			err = nil
		}
	}()

	vc.RefreshVolumeState(volume, engine, replicas)

	if err := vc.updateRecurringJobs(volume, engine); err != nil {
		return err
	}

	if err := vc.ReconcileEngineReplicaState(volume, engine, replicas); err != nil {
		return err
	}

	if err := vc.ReconcileVolumeState(volume, engine, replicas); err != nil {
		return err
	}

	return nil
}

func (vc *VolumeController) RefreshVolumeState(v *longhorn.Volume, e *longhorn.Controller, rs map[string]*longhorn.Replica) {
	// engine wasn't created or has been deleted
	if e == nil {
		return
	}

	healthyCount := 0
	for _, r := range rs {
		if r.Spec.FailedAt == "" {
			healthyCount++
		}
	}
	if e.Status.CurrentState == types.InstanceStateRunning {
		// Don't worry about ">" now. Deal with it later
		if healthyCount >= v.Spec.NumberOfReplicas {
			v.Status.State = types.VolumeStateHealthy
		} else {
			v.Status.State = types.VolumeStateDegraded
		}
		v.Status.Endpoint = e.Status.Endpoint
	} else {
		// controller has been created by this point, so it won't be
		// in `Created` state
		if healthyCount != 0 {
			v.Status.State = types.VolumeStateDetached
		} else {
			v.Status.State = types.VolumeStateFault
		}
		v.Status.Endpoint = ""
	}
	return
}

func (vc *VolumeController) ReconcileEngineReplicaState(v *longhorn.Volume, e *longhorn.Controller, rs map[string]*longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to reconcile engine/replica state for %v", v.Name)
	}()

	if e == nil {
		return nil
	}

	if err := vc.cleanupStaleReplicas(v, rs); err != nil {
		return err
	}

	if e.Status.CurrentState != types.InstanceStateRunning {
		return nil
	}

	// remove err replicas
	for rName, mode := range e.Status.ReplicaModeMap {
		if mode == types.ReplicaModeERR {
			r := rs[rName]
			if r != nil {
				r.Spec.FailedAt = util.Now()
				r, err = vc.ds.UpdateReplica(r)
				if err != nil {
					return err
				}
				rs[rName] = r
			}
			// set replica.FailedAt first because we will lost track of it
			// if we removed the error replica from engine first
			delete(e.Spec.ReplicaAddressMap, rName)
		}
	}
	// start rebuilding if necessary
	if err = vc.replenishReplicas(v, rs); err != nil {
		return err
	}
	for _, r := range rs {
		if r.Spec.FailedAt == "" && r.Spec.DesireState == types.InstanceStateStopped {
			r.Spec.DesireState = types.InstanceStateRunning
			r, err := vc.ds.UpdateReplica(r)
			if err != nil {
				return err
			}
			rs[r.Name] = r
			continue
		}
		if r.Status.CurrentState == types.InstanceStateRunning && e.Spec.ReplicaAddressMap[r.Name] == "" {
			e.Spec.ReplicaAddressMap[r.Name] = r.Status.IP
		}
	}
	e, err = vc.ds.UpdateEngine(e)
	if err != nil {
		return err
	}
	return nil
}

func (vc *VolumeController) cleanupStaleReplicas(v *longhorn.Volume, rs map[string]*longhorn.Replica) error {
	for _, r := range rs {
		if r.Spec.FailedAt != "" {
			if util.TimestampAfterTimeout(r.Spec.FailedAt, v.Spec.StaleReplicaTimeout*60) {
				logrus.Infof("Cleaning up stale replica %v", r.Name)
				if err := vc.ds.DeleteReplica(r.Name); err != nil {
					return errors.Wrap(err, "cannot cleanup stale replicas")
				}
				delete(rs, r.Name)
			}
		}
	}
	return nil
}

func (vc *VolumeController) ReconcileVolumeState(v *longhorn.Volume, e *longhorn.Controller, rs map[string]*longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to reconcile volume state for %v", v.Name)
	}()

	// Don't reconcile from Fault state
	// User will need to salvage a replica to continue
	if v.Status.State == types.VolumeStateFault {
		return nil
	}

	if e == nil {
		// first time creation
		e, err = vc.createEngine(v)
		if err != nil {
			return err
		}
	}

	if len(rs) == 0 {
		// first time creation
		if err = vc.replenishReplicas(v, rs); err != nil {
			return err
		}
	}

	if v.Spec.NodeID == "" {
		// stop rebuilding
		for rName, mode := range e.Status.ReplicaModeMap {
			if mode == types.ReplicaModeWO {
				r := rs[rName]
				if r == nil {
					logrus.Errorf("BUG: Engine has %v as WO, but cannot find the replica", rName)
					continue
				}
				if r.Spec.FailedAt != "" {
					r.Spec.FailedAt = util.Now()
					r, err = vc.ds.UpdateReplica(r)
					if err != nil {
						return err
					}
				}
				rs[rName] = r
			}
		}
		if e.Spec.DesireState != types.InstanceStateStopped {
			e.Spec.DesireState = types.InstanceStateStopped
			e, err = vc.ds.UpdateEngine(e)
			return err
		}
		// must make sure engine stopped first before stopping replicas
		// otherwise we may corrupt the data
		if e.Status.CurrentState != types.InstanceStateStopped {
			logrus.Infof("Waiting for engine %v to stop", e.Name)
			return nil
		}

		for _, r := range rs {
			if r.Spec.DesireState != types.InstanceStateStopped {
				r.Spec.DesireState = types.InstanceStateStopped
				r, err := vc.ds.UpdateReplica(r)
				if err != nil {
					return err
				}
				rs[r.Name] = r
			}
		}
	} else {
		// volume hasn't been started before
		// we will start the engine with all healthy replicas
		replicaUpdated := false
		for _, r := range rs {
			if r.Spec.FailedAt == "" && r.Spec.DesireState != types.InstanceStateRunning {
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
			// wait for all healthy replicas become running
			if r.Spec.FailedAt == "" && r.Status.CurrentState != types.InstanceStateRunning {
				return nil
			}
			replicaAddressMap[r.Name] = r.Status.IP
		}

		if e.Status.CurrentState == types.InstanceStateRunning {
			if e.Spec.NodeID != v.Spec.NodeID {
				return fmt.Errorf("Inconsistent node ID: engine %v vs volume %v", e.Spec.NodeID, v.Spec.NodeID)
			}
		}
		e.Spec.NodeID = v.Spec.NodeID
		e.Spec.ReplicaAddressMap = replicaAddressMap
		e.Spec.DesireState = types.InstanceStateRunning
		e, err = vc.ds.UpdateEngine(e)
		return err
	}
	return nil
}

func (vc *VolumeController) replenishReplicas(v *longhorn.Volume, rs map[string]*longhorn.Replica) error {
	healthyCount := 0
	for _, r := range rs {
		if r.Spec.FailedAt == "" {
			healthyCount++
		}
	}

	for i := 0; i < v.Spec.NumberOfReplicas-healthyCount; i++ {
		r, err := vc.createReplica(v)
		if err != nil {
			return err
		}
		rs[r.Name] = r
	}
	return nil
}

func (vc *VolumeController) getEngineNameForVolume(v *longhorn.Volume) string {
	return v.Name + engineSuffix
}

func (vc *VolumeController) generateReplicaNameForVolume(v *longhorn.Volume) string {
	return v.Name + replicaSuffix + "-" + util.RandomID()
}

func (vc *VolumeController) createEngine(v *longhorn.Volume) (*longhorn.Controller, error) {
	engine := &longhorn.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name: vc.getEngineNameForVolume(v),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: longhorn.SchemeGroupVersion.String(),
					Kind:       ownerKindVolume,
					UID:        v.UID,
					Name:       v.Name,
				},
			},
		},
		Spec: types.EngineSpec{
			InstanceSpec: types.InstanceSpec{
				VolumeName:  v.Name,
				EngineImage: vc.EngineImage,
				DesireState: types.InstanceStateStopped,
				OwnerID:     vc.controllerID,
			},
		},
	}
	return vc.ds.CreateEngine(engine)
}

func (vc *VolumeController) createReplica(v *longhorn.Volume) (*longhorn.Replica, error) {
	replica := &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name: vc.generateReplicaNameForVolume(v),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: longhorn.SchemeGroupVersion.String(),
					Kind:       ownerKindVolume,
					UID:        v.UID,
					Name:       v.Name,
				},
			},
		},
		Spec: types.ReplicaSpec{
			InstanceSpec: types.InstanceSpec{
				VolumeName:  v.Name,
				EngineImage: vc.EngineImage,
				DesireState: types.InstanceStateStopped,
				OwnerID:     vc.controllerID,
			},
			VolumeSize: v.Spec.Size,
		},
	}
	if v.Spec.FromBackup != "" {
		backupID, err := util.GetBackupID(v.Spec.FromBackup)
		if err != nil {
			return nil, err
		}
		replica.Spec.RestoreFrom = v.Spec.FromBackup
		replica.Spec.RestoreName = backupID
	}
	return vc.ds.CreateReplica(replica)
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
		if ref.Kind != ownerKindVolume {
			continue
		}
		namespace := metaObj.GetNamespace()
		vc.ResolveRefAndEnqueue(namespace, &ref)
		return
	}
}

func (vc *VolumeController) ResolveRefAndEnqueue(namespace string, ref *metav1.OwnerReference) {
	if ref.Kind != ownerKindVolume {
		return
	}
	volume, err := vc.ds.GetVolume(ref.Name)
	if err != nil || volume == nil {
		return
	}
	if volume.UID != ref.UID {
		// The controller we found with this Name is not the same one that the
		// OwnerRef points to.
		return
	}
	// Not ours
	if volume.Spec.OwnerID != vc.controllerID {
		return
	}
	vc.enqueueVolume(volume)
}

func (vc *VolumeController) cleanupRecurringJobsHoldingLock(v *longhorn.Volume) {
	current := vc.recurringJobMap[v.Name]
	if current != nil && current.recurringJobs != nil {
		current.cron.Stop()
		delete(vc.recurringJobMap, v.Name)
	}
}

func (vc *VolumeController) stopRecurringJobs(v *longhorn.Volume) {
	vc.recurringJobMutex.Lock()
	defer vc.recurringJobMutex.Unlock()

	vc.cleanupRecurringJobsHoldingLock(v)
}

func (vc *VolumeController) updateRecurringJobs(v *longhorn.Volume, e *longhorn.Controller) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to update recurring jobs for %v", v.Name)
	}()

	if e == nil {
		return nil
	}

	vc.recurringJobMutex.Lock()
	defer vc.recurringJobMutex.Unlock()

	jobs := v.Spec.RecurringJobs
	if len(jobs) == 0 {
		vc.cleanupRecurringJobsHoldingLock(v)
		return nil
	}

	if e.Spec.DesireState == types.InstanceStateStopped || e.Status.CurrentState != types.InstanceStateRunning {
		vc.cleanupRecurringJobsHoldingLock(v)
		return nil
	}

	current := vc.recurringJobMap[v.Name]
	if current.recurringJobs == nil || !reflect.DeepEqual(v.Spec.RecurringJobs, current.recurringJobs) {
		engines := engineapi.EngineCollection{}
		engineClient, err := engines.NewEngineClient(&engineapi.EngineClientRequest{
			VolumeName:    v.Name,
			ControllerURL: engineapi.GetControllerDefaultURL(e.Status.IP),
		})
		if err != nil {
			return err
		}

		setting, err := vc.ds.GetSetting()
		if err != nil {
			return err
		}
		backupTarget := setting.BackupTarget

		newCron := cron.New()
		for _, job := range jobs {
			cronJob := &CronJob{
				job,
				engineClient,
				backupTarget,
			}
			if backupTarget == "" && job.Type == types.RecurringJobTypeBackup {
				return fmt.Errorf("cannot backup with empty backup target")
			}
			if err := newCron.AddJob(job.Cron, cronJob); err != nil {
				return err
			}
		}
		if current != nil {
			current.cron.Stop()
		}

		vc.recurringJobMap[v.Name] = &VolumeRecurringJob{
			recurringJobs: jobs,
			cron:          newCron,
		}
		newCron.Start()
	}
	return nil
}
