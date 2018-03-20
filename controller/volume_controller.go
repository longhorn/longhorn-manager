package controller

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

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

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhinformers "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1alpha1"
)

var (
	ownerKindVolume = longhorn.SchemeGroupVersion.WithKind("Volume").String()
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
	EngineImage    string
	ManagerImage   string
	ServiceAccount string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	vStoreSynced cache.InformerSynced
	eStoreSynced cache.InformerSynced
	rStoreSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface

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
	engineImage, managerImage string) *VolumeController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	vc := &VolumeController{
		ds:             ds,
		namespace:      namespace,
		controllerID:   controllerID,
		EngineImage:    engineImage,
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
			vc.eventRecorder.Eventf(volume, v1.EventTypeNormal, EventReasonDelete, "Deleting volume %v", volume.Name)
		}
		for _, job := range volume.Spec.RecurringJobs {
			if err := vc.ds.DeleteCronJob(types.GetCronJobNameForVolumeAndJob(volume.Name, job.Name)); err != nil {
				return err
			}
		}

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

		return vc.ds.RemoveFinalizerForVolume(volume)
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

	if err := vc.ReconcileEngineReplicaState(volume, engine, replicas); err != nil {
		return err
	}

	if err := vc.ReconcileVolumeState(volume, engine, replicas); err != nil {
		return err
	}

	if err := vc.updateRecurringJobs(volume); err != nil {
		return err
	}

	return nil
}

func (vc *VolumeController) ReconcileEngineReplicaState(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to reconcile engine/replica state for %v", v.Name)
	}()

	if e == nil {
		return nil
	}

	if err := vc.cleanupCorruptedOrStaleReplicas(v, rs); err != nil {
		return err
	}

	if e.Status.CurrentState != types.InstanceStateRunning {
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
				r.Spec.FailedAt = vc.nowHandler()
				r, err = vc.ds.UpdateReplica(r)
				if err != nil {
					return err
				}
				rs[rName] = r
			}
			// set replica.FailedAt first because we will lost track of it
			// if we removed the error replica from engine first
			delete(e.Spec.ReplicaAddressMap, rName)
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
		// detach the volume
		v.Spec.NodeID = ""
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
		if err = vc.replenishReplicas(v, rs); err != nil {
			return err
		}
		// replicas will be started by ReconcileVolumeState() later
	}
	e, err = vc.ds.UpdateEngine(e)
	if err != nil {
		return err
	}
	return nil
}

func (vc *VolumeController) cleanupCorruptedOrStaleReplicas(v *longhorn.Volume, rs map[string]*longhorn.Replica) error {
	for _, r := range rs {
		if r.Spec.FailedAt != "" {
			// 1. failed before ever became healthy (RW), mostly failed during rebuilding
			// 2. failed too long ago, became stale and unnecessary to keep around
			if r.Spec.HealthyAt == "" || util.TimestampAfterTimeout(r.Spec.FailedAt, v.Spec.StaleReplicaTimeout*60) {
				logrus.Infof("Cleaning up corrupted or staled replica %v", r.Name)
				if err := vc.ds.DeleteReplica(r.Name); err != nil {
					return errors.Wrap(err, "cannot cleanup stale replicas")
				}
				delete(rs, r.Name)
			}
		}
	}
	return nil
}

// ReconcileVolumeState handles the attaching and detaching of volume
func (vc *VolumeController) ReconcileVolumeState(v *longhorn.Volume, e *longhorn.Engine, rs map[string]*longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to reconcile volume state for %v", v.Name)
	}()

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

	oldState := v.Status.State
	if v.Spec.NodeID == "" {
		// the final state will be determined at the end of the clause
		v.Status.State = types.VolumeStateDetaching
		v.Status.Endpoint = ""

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
					r, err = vc.ds.UpdateReplica(r)
					if err != nil {
						return err
					}
					rs[r.Name] = r
				}
			}
		}
		if e.Spec.DesireState != types.InstanceStateStopped || e.Spec.NodeID != "" {
			e.Spec.NodeID = ""
			e.Spec.DesireState = types.InstanceStateStopped
			e, err = vc.ds.UpdateEngine(e)
			return err
		}
		// must make sure engine stopped first before stopping replicas
		// otherwise we may corrupt the data
		if e.Status.CurrentState != types.InstanceStateStopped {
			logrus.Infof("Waiting for engine %v state change to stopped", e.Name)
			return nil
		}

		allReplicasStopped := true
		for _, r := range rs {
			if r.Spec.DesireState != types.InstanceStateStopped {
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
	} else {
		// if engine was running, then we are attached already
		// (but we may still need to start rebuilding replicas)
		if e.Status.CurrentState != types.InstanceStateRunning {
			// the final state will be determined at the end of the clause
			v.Status.State = types.VolumeStateAttaching
		}

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

		if e.Spec.DesireState != types.InstanceStateRunning ||
			!reflect.DeepEqual(e.Spec.ReplicaAddressMap, replicaAddressMap) {
			if e.Spec.NodeID != "" && e.Spec.NodeID != v.Spec.NodeID {
				return fmt.Errorf("engine is on node %v vs volume on %v, must detach first",
					e.Spec.NodeID, v.Spec.NodeID)
			}
			e.Spec.NodeID = v.Spec.NodeID
			e.Spec.ReplicaAddressMap = replicaAddressMap
			e.Spec.DesireState = types.InstanceStateRunning
			e, err = vc.ds.UpdateEngine(e)
			if err != nil {
				return err
			}
		}
		// wait for engine to be up
		if e.Status.CurrentState != types.InstanceStateRunning {
			return nil
		}

		v.Status.Endpoint = e.Status.Endpoint
		v.Status.State = types.VolumeStateAttached
		if oldState != v.Status.State {
			vc.eventRecorder.Eventf(v, v1.EventTypeNormal, EventReasonAttached, "volume %v has been attached to %v", v.Name, v.Spec.NodeID)
		}
	}
	return nil
}

// replenishReplicas will keep replicas count to v.Spec.NumberOfReplicas
// It will count all the potentially usable replicas, since some replicas maybe
// blank or in rebuilding state
func (vc *VolumeController) replenishReplicas(v *longhorn.Volume, rs map[string]*longhorn.Replica) error {
	usableCount := 0
	for _, r := range rs {
		if r.Spec.FailedAt == "" {
			usableCount++
		}
	}

	for i := 0; i < v.Spec.NumberOfReplicas-usableCount; i++ {
		r, err := vc.createReplica(v)
		if err != nil {
			return err
		}
		rs[r.Name] = r
	}
	return nil
}

func (vc *VolumeController) createEngine(v *longhorn.Volume) (*longhorn.Engine, error) {
	engine := &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{
			Name: types.GetEngineNameForVolume(v.Name),
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
			ReplicaAddressMap: map[string]string{},
		},
	}
	return vc.ds.CreateEngine(engine)
}

func (vc *VolumeController) createReplica(v *longhorn.Volume) (*longhorn.Replica, error) {
	replica := &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name: types.GenerateReplicaNameForVolume(v.Name),
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

func (vc *VolumeController) createCronJob(v *longhorn.Volume, job *types.RecurringJob, suspend bool, backupTarget string) *batchv1beta1.CronJob {
	backoffLimit := int32(CronJobBackoffLimit)
	cmd := []string{
		"longhorn-manager", "-d",
		"snapshot", v.Name,
		"--snapshot-name", job.Name,
		"--labels", LabelRecurringJob + "=" + job.Name,
		"--retain", strconv.Itoa(job.Retain),
	}
	if job.Type == types.RecurringJobTypeBackup {
		cmd = append(cmd, "--backuptarget", backupTarget)
	}
	// for mounting inside container
	privilege := true
	cronJob := &batchv1beta1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      types.GetCronJobNameForVolumeAndJob(v.Name, job.Name),
			Namespace: vc.namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: longhorn.SchemeGroupVersion.String(),
					Kind:       ownerKindVolume,
					UID:        v.UID,
					Name:       v.Name,
				},
			},
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
							NodeName: v.Spec.NodeID,
							InitContainers: []v1.Container{
								{
									Name:    "longhorn-engine-binary",
									Image:   vc.EngineImage,
									Command: []string{"sh", "-c", "cp /usr/local/bin/* /data/"},
									VolumeMounts: []v1.VolumeMount{
										{
											Name:      "execbin",
											MountPath: "/data",
										},
									},
								},
							},
							Containers: []v1.Container{
								{
									Name:    types.GetCronJobNameForVolumeAndJob(v.Name, job.Name),
									Image:   vc.ManagerImage,
									Command: cmd,
									SecurityContext: &v1.SecurityContext{
										Privileged: &privilege,
									},
									VolumeMounts: []v1.VolumeMount{
										{
											Name:      "execbin",
											MountPath: "/usr/local/bin",
										},
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
								},
							},
							Volumes: []v1.Volume{
								{
									Name: "execbin",
									VolumeSource: v1.VolumeSource{
										EmptyDir: &v1.EmptyDirVolumeSource{},
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
	return cronJob
}

func (vc *VolumeController) updateRecurringJobs(v *longhorn.Volume) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to update recurring jobs for %v", v.Name)
	}()

	suspended := false
	if v.Status.State != types.VolumeStateAttached {
		suspended = true
	}

	backupTarget := ""
	setting, err := vc.ds.GetSetting()
	if err != nil {
		return err
	}
	if setting != nil {
		backupTarget = setting.BackupTarget
	}

	// the cronjobs are RO in the map, but not the map itself
	appliedCronJobROs, err := vc.ds.ListVolumeCronJobROs(v.Name)
	if err != nil {
		return err
	}
	currentCronJobs := make(map[string]*batchv1beta1.CronJob)
	for _, job := range v.Spec.RecurringJobs {
		if backupTarget == "" && job.Type == types.RecurringJobTypeBackup {
			return fmt.Errorf("cannot backup with empty backup target")
		}

		cronJob := vc.createCronJob(v, &job, suspended, backupTarget)
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
	for name := range appliedCronJobROs {
		if currentCronJobs[name] == nil {
			if err := vc.ds.DeleteCronJob(name); err != nil {
				return err
			}
		}
	}

	return nil
}
