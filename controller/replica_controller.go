package controller

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	batchinformers "k8s.io/client-go/informers/batch/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
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
	// maxRetries is the number of times a deployment will be retried before it is dropped out of the queue.
	// With the current rate-limiter in use (5ms*2^(maxRetries-1)) the following numbers represent the times
	// a deployment is going to be requeued:
	//
	// 5ms, 10ms, 20ms
	maxRetries = 3

	ownerKindReplica = longhorn.SchemeGroupVersion.WithKind("Replica").String()
)

const (
	// longhornDirectory is the directory going to be bind mounted on the
	// host to provide storage space to replica data
	longhornDirectory = "/var/lib/rancher/longhorn/"

	// longhornReplicaKey is the key to identify which volume the replica
	// belongs to, for scheduling purpose
	longhornReplicaKey = "longhorn-volume-replica"

	// Empty replica will response fast
	replicaReadinessProbeInitialDelay = 3
	// Otherwise we will need to wait for a restore
	replicaReadinessProbePeriodSeconds = 10
	// assuming the restore will be done at least this per second
	replicaReadinessProbeMinimalRestoreRate = 10 * 1024 * 1024
	// if replica won't start restoring, this will be the default
	replicaReadinessProbeFailureThresholdDefault = 3
)

type ReplicaController struct {
	// which namespace controller is running with
	namespace string
	// use as the OwnerID of replica
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	rStoreSynced cache.InformerSynced
	pStoreSynced cache.InformerSynced
	jStoreSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface

	instanceHandler *InstanceHandler
}

func NewReplicaController(
	ds *datastore.DataStore,
	replicaInformer lhinformers.ReplicaInformer,
	podInformer coreinformers.PodInformer,
	jobInformer batchinformers.JobInformer,
	kubeClient clientset.Interface,
	namespace string, controllerID string) *ReplicaController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	rc := &ReplicaController{
		namespace:    namespace,
		controllerID: controllerID,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "longhorn-replica-controller"}),

		ds: ds,

		rStoreSynced: replicaInformer.Informer().HasSynced,
		pStoreSynced: podInformer.Informer().HasSynced,
		jStoreSynced: jobInformer.Informer().HasSynced,

		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "longhorn-replica"),
	}
	rc.instanceHandler = NewInstanceHandler(podInformer, kubeClient, namespace, rc)

	replicaInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			r := obj.(*longhorn.Replica)
			rc.enqueueReplica(r)
		},
		UpdateFunc: func(old, cur interface{}) {
			curR := cur.(*longhorn.Replica)
			rc.enqueueReplica(curR)
		},
		DeleteFunc: func(obj interface{}) {
			r := obj.(*longhorn.Replica)
			rc.enqueueReplica(r)
		},
	})

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			rc.enqueueControlleeChange(obj)
		},
		UpdateFunc: func(old, cur interface{}) {
			rc.enqueueControlleeChange(cur)
		},
		DeleteFunc: func(obj interface{}) {
			rc.enqueueControlleeChange(obj)
		},
	})

	jobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			rc.enqueueControlleeChange(obj)
		},
		UpdateFunc: func(old, cur interface{}) {
			rc.enqueueControlleeChange(cur)
		},
		DeleteFunc: func(obj interface{}) {
			rc.enqueueControlleeChange(obj)
		},
	})
	return rc
}

func (rc *ReplicaController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer rc.queue.ShutDown()

	logrus.Infof("Start Longhorn replica controller")
	defer logrus.Infof("Shutting down Longhorn replica controller")

	if !controller.WaitForCacheSync("longhorn replicas", stopCh, rc.rStoreSynced, rc.pStoreSynced, rc.jStoreSynced) {
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

	if rc.queue.NumRequeues(key) < maxRetries {
		logrus.Warnf("Error syncing Longhorn replica %v: %v", key, err)
		rc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logrus.Warnf("Dropping Longhorn replica %v out of the queue: %v", key, err)
	rc.queue.Forget(key)
}

func (rc *ReplicaController) getJobForReplica(r *longhorn.Replica) (*batchv1.Job, error) {
	return rc.kubeClient.BatchV1().Jobs(rc.namespace).Get(r.Name, metav1.GetOptions{})
}

func (rc *ReplicaController) syncReplica(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync replica for %v", key)
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
		return err
	}
	if replica == nil {
		logrus.Infof("Longhorn replica %v has been deleted", key)
		return nil
	}

	// Not ours
	if replica.Spec.OwnerID != rc.controllerID {
		return nil
	}

	if replica.DeletionTimestamp != nil {
		if err := rc.instanceHandler.Delete(replica.Name); err != nil {
			return err
		}
		// we want to make sure data was cleaned before remove the replica
		if replica.Spec.NodeID != "" {
			return rc.cleanupReplicaInstance(replica)
		}
		return rc.ds.RemoveFinalizerForReplica(replica.Name)
	}

	defer func() {
		// we're going to update replica assume things changes
		if err == nil {
			_, err = rc.ds.UpdateReplica(replica)
		}
		// requeue if it's conflict
		if apierrors.IsConflict(err) {
			logrus.Debugf("Requeue %v due to conflict", key)
			rc.enqueueReplica(replica)
			err = nil
		}
	}()

	// we will sync up with pod status before proceed
	if err := rc.instanceHandler.SyncInstanceState(replica.Name, &replica.Spec.InstanceSpec, &replica.Status.InstanceStatus); err != nil {
		return err
	}

	// we need to stop the replica when replica failed connection with controller
	if replica.Spec.FailedAt != "" {
		if replica.Spec.DesireState != types.InstanceStateStopped {
			replica.Spec.DesireState = types.InstanceStateStopped
			_, err := rc.ds.UpdateReplica(replica)
			return err
		}
	}

	return rc.instanceHandler.ReconcileInstanceState(replica.Name, replica, &replica.Spec.InstanceSpec, &replica.Status.InstanceStatus)
}

func (rc *ReplicaController) enqueueReplica(replica *longhorn.Replica) {
	key, err := controller.KeyFunc(replica)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", replica, err))
		return
	}

	rc.queue.AddRateLimited(key)
}

func (rc *ReplicaController) getReplicaVolumeDirectory(replicaName string) string {
	return longhornDirectory + "/replicas/" + replicaName
}

func (rc *ReplicaController) getReadinessProbeFailureThreshold(r *longhorn.Replica) int32 {
	if r.Spec.RestoreFrom == "" {
		// default value if
		return replicaReadinessProbeFailureThresholdDefault
	}
	size, err := util.ConvertSize(r.Spec.VolumeSize)
	if err != nil {
		logrus.Errorf("BUG: Detect invalid size in replica spec: %+v", r)
		return replicaReadinessProbeFailureThresholdDefault
	}
	// this volume needs 2e9 * 1e7 * 10 which is 200+ Petabytes to overflow
	return int32(size / replicaReadinessProbeMinimalRestoreRate / replicaReadinessProbePeriodSeconds)
}

func (rc *ReplicaController) CreatePodSpec(obj interface{}) (*v1.Pod, error) {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		return nil, fmt.Errorf("BUG: invalid object for engine pod spec creation: %v", r)
	}
	cmd := []string{
		"launch", "replica",
		"--listen", "0.0.0.0:9502",
		"--size", r.Spec.VolumeSize,
	}
	if r.Spec.RestoreFrom != "" && r.Spec.RestoreName != "" {
		cmd = append(cmd, "--restore-from", r.Spec.RestoreFrom, "--restore-name", r.Spec.RestoreName)
	}
	cmd = append(cmd, "/volume")

	privilege := true
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.Name,
			Namespace: r.Namespace,
			Labels: map[string]string{
				longhornReplicaKey: r.Spec.VolumeName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: longhorn.SchemeGroupVersion.String(),
					Kind:       ownerKindReplica,
					UID:        r.UID,
					Name:       r.Name,
				},
			},
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			Containers: []v1.Container{
				{
					Name:    r.Name,
					Image:   r.Spec.EngineImage,
					Command: cmd,
					SecurityContext: &v1.SecurityContext{
						Privileged: &privilege,
					},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "volume",
							MountPath: "/volume",
						},
					},
					ReadinessProbe: &v1.Probe{
						Handler: v1.Handler{
							HTTPGet: &v1.HTTPGetAction{
								Path: "/v1",
								Port: intstr.FromInt(9502),
							},
						},
						InitialDelaySeconds: replicaReadinessProbeInitialDelay,
						PeriodSeconds:       replicaReadinessProbePeriodSeconds,
						FailureThreshold:    rc.getReadinessProbeFailureThreshold(r),
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "volume",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: rc.getReplicaVolumeDirectory(r.Name),
						},
					},
				},
			},
		},
	}

	// We will allow kubernetes to schedule it for the first time, later we
	// will pin it down to the same host because we have data on it
	if r.Spec.NodeID != "" {
		pod.Spec.NodeName = r.Spec.NodeID
	} else {
		pod.Spec.Affinity = &v1.Affinity{
			PodAntiAffinity: &v1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
					{
						Weight: 100,
						PodAffinityTerm: v1.PodAffinityTerm{
							LabelSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									longhornReplicaKey: r.Spec.VolumeName,
								},
							},
							TopologyKey: "kubernetes.io/hostname",
						},
					},
				},
			},
		}
	}
	return pod, nil
}

func (rc *ReplicaController) createCleanupJobSpec(r *longhorn.Replica) *batchv1.Job {
	cmd := []string{"/bin/bash", "-c"}
	// There is a delay between starting pod and mount the volume, so
	// workaround it for now
	args := []string{"sleep 1 && rm -f /volume/*"}

	jobName := r.Name
	backoffLimit := int32(1)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: r.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: longhorn.SchemeGroupVersion.String(),
					Kind:       ownerKindReplica,
					UID:        r.UID,
					Name:       r.Name,
				},
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cleanup-" + r.Name,
				},
				Spec: v1.PodSpec{
					NodeName:      r.Spec.NodeID,
					RestartPolicy: v1.RestartPolicyNever,
					Containers: []v1.Container{
						{
							Name:    "cleanup-" + r.Name,
							Image:   r.Spec.EngineImage,
							Command: cmd,
							Args:    args,
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "volume",
									MountPath: "/volume",
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "volume",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: rc.getReplicaVolumeDirectory(r.Name),
								},
							},
						},
					},
				},
			},
		},
	}
	return job
}

func (rc *ReplicaController) cleanupReplicaInstance(r *longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to cleanup replica instance for %v", r.Name)
	}()
	// replica wasn't created once or has been cleaned up
	if r.Spec.NodeID == "" {
		return nil
	}
	job, err := rc.getJobForReplica(r)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if apierrors.IsNotFound(err) {
		job := rc.createCleanupJobSpec(r)

		_, err = rc.kubeClient.BatchV1().Jobs(rc.namespace).Create(job)
		if err != nil {
			return errors.Wrap(err, "failed to create cleanup job")
		}
		logrus.Infof("Replica cleanup job created for %v", r.Name)
		return nil
	}

	if job.Status.CompletionTime != nil {
		defer func() {
			err := rc.kubeClient.BatchV1().Jobs(rc.namespace).Delete(r.Name, nil)
			if err != nil {
				logrus.Warnf("Failed to delete the cleanup job for %v: %v", r.Name, err)
			}
			rc.enqueueReplica(r)
		}()

		if job.Status.Succeeded != 0 {
			logrus.Infof("Cleanup for volume %v replica %v succeed", r.Spec.VolumeName, r.Name)
			r.Spec.NodeID = ""
			if _, err := rc.ds.UpdateReplica(r); err != nil {
				return err
			}
		} else {
			logrus.Warnf("Cleanup for volume %v replica %v failed", r.Spec.VolumeName, r.Name)
		}
	}

	return nil
}

func (rc *ReplicaController) enqueueControlleeChange(obj interface{}) {
	metaObj, err := meta.Accessor(obj)
	if err != nil {
		logrus.Warnf("BUG: %v cannot be convert to metav1.Object: %v", obj, err)
		return
	}
	ownerRefs := metaObj.GetOwnerReferences()
	for _, ref := range ownerRefs {
		if ref.Kind != ownerKindReplica {
			continue
		}
		namespace := metaObj.GetNamespace()
		rc.ResolveRefAndEnqueue(namespace, &ref)
		return
	}
}

func (rc *ReplicaController) ResolveRefAndEnqueue(namespace string, ref *metav1.OwnerReference) {
	if ref.Kind != ownerKindReplica {
		return
	}
	replica, err := rc.ds.GetReplica(ref.Name)
	if err != nil || replica == nil {
		return
	}
	if replica.UID != ref.UID {
		// The controller we found with this Name is not the same one that the
		// OwnerRef points to.
		return
	}
	// Not ours
	if replica.Spec.OwnerID != rc.controllerID {
		return
	}
	rc.enqueueReplica(replica)
}
