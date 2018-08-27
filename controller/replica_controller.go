package controller

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	batchinformers "k8s.io/client-go/informers/batch/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
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
	// maxRetries is the number of times a deployment will be retried before it is dropped out of the queue.
	// With the current rate-limiter in use (5ms*2^(maxRetries-1)) the following numbers represent the times
	// a deployment is going to be requeued:
	//
	// 5ms, 10ms, 20ms
	maxRetries = 3

	ownerKindReplica = longhorn.SchemeGroupVersion.WithKind("Replica").String()
)

const (
	// longhornReplicaKey is the key to identify which volume the replica
	// belongs to, for scheduling purpose
	longhornReplicaKey = "longhorn-volume-replica"

	// Empty replica will response fast
	replicaReadinessProbeInitialDelay = 1
	// Otherwise we will need to wait for a restore
	replicaReadinessProbePeriodSeconds = 1
	// assuming the restore will be done at least this per second
	replicaReadinessProbeMinimalRestoreRate = 10 * 1024 * 1024
	// if replica won't start restoring, this will be the default
	replicaReadinessProbeFailureThresholdDefault = 10
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
	scheme *runtime.Scheme,
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
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-replica-controller"}),

		ds: ds,

		rStoreSynced: replicaInformer.Informer().HasSynced,
		pStoreSynced: podInformer.Informer().HasSynced,
		jStoreSynced: jobInformer.Informer().HasSynced,

		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "longhorn-replica"),
	}
	rc.instanceHandler = NewInstanceHandler(podInformer, kubeClient, namespace, rc, rc.eventRecorder)

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
		if datastore.ErrorIsNotFound(err) {
			logrus.Infof("Longhorn replica %v has been deleted", key)
			return nil
		}
		return err
	}

	// Not ours
	if replica.Spec.OwnerID != rc.controllerID {
		return nil
	}

	if replica.DeletionTimestamp != nil {
		if err := rc.instanceHandler.DeleteInstanceForObject(replica); err != nil {
			return err
		}
		// we want to make sure data was cleaned before remove the
		// replica if cleanup was set
		if replica.Spec.NodeID != "" && replica.Spec.Active {
			return rc.cleanupReplicaInstance(replica)
		}
		return rc.ds.RemoveFinalizerForReplica(replica)
	}

	existingReplica := replica.DeepCopy()
	defer func() {
		// we're going to update replica assume things changes
		if err == nil && !reflect.DeepEqual(existingReplica, replica) {
			_, err = rc.ds.UpdateReplica(replica)
		}
		// requeue if it's conflict
		if apierrors.IsConflict(errors.Cause(err)) {
			logrus.Debugf("Requeue %v due to conflict", key)
			rc.enqueueReplica(replica)
			err = nil
		}
	}()

	// we need to stop the replica when replica failed connection with controller
	if replica.Spec.FailedAt != "" {
		if replica.Spec.DesireState != types.InstanceStateStopped {
			replica.Spec.DesireState = types.InstanceStateStopped
			_, err := rc.ds.UpdateReplica(replica)
			return err
		}
	}

	return rc.instanceHandler.ReconcileInstanceState(replica, &replica.Spec.InstanceSpec, &replica.Status.InstanceStatus)
}

func (rc *ReplicaController) enqueueReplica(replica *longhorn.Replica) {
	key, err := controller.KeyFunc(replica)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", replica, err))
		return
	}

	rc.queue.AddRateLimited(key)
}

func (rc *ReplicaController) getReadinessProbeFailureThreshold(r *longhorn.Replica) int32 {
	if r.Spec.RestoreFrom == "" {
		// default value if
		return replicaReadinessProbeFailureThresholdDefault
	}
	// this volume needs 2e9 * 1e7 which is 2+ Exabytes to overflow
	return int32(r.Spec.VolumeSize / replicaReadinessProbeMinimalRestoreRate / replicaReadinessProbePeriodSeconds)
}

func singleQuotes(static string) string {
	return fmt.Sprintf("'%s'", static)
}

func (rc *ReplicaController) CreatePodSpec(obj interface{}) (*v1.Pod, error) {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		return nil, fmt.Errorf("BUG: invalid object for engine pod spec creation: %v", r)
	}

	cmd := []string{
		"longhorn", "replica",
		"--listen", "0.0.0.0:9502",
		"--size", strconv.FormatInt(r.Spec.VolumeSize, 10),
	}
	if r.Spec.BaseImage != "" {
		cmd = append(cmd, "--backing-file", "/share/base_image")
	}
	if r.Spec.RestoreFrom != "" && r.Spec.RestoreName != "" {
		cmd = append(cmd, "--restore-from", singleQuotes(r.Spec.RestoreFrom))
		cmd = append(cmd, "--restore-name", singleQuotes(r.Spec.RestoreName))
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
							Path: r.Spec.DataPath,
						},
					},
				},
			},
		},
	}

	// error out if NodeID and DataPath wasn't filled in scheduler
	if r.Spec.NodeID == "" || r.Spec.DataPath == "" || r.Spec.DiskID == "" {
		return nil, fmt.Errorf("BUG: nodeID or datapath or diskID wasn't set for replica %v", r.Name)
	}

	if r.Spec.BaseImage != "" {
		// Ensure base image is present before executing main containers
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, v1.Container{
			Name:            "prime-base-image",
			Image:           r.Spec.BaseImage,
			ImagePullPolicy: v1.PullAlways,
			Command:         []string{"/bin/sh", "-c", fmt.Sprintf("echo primed %s", r.Spec.BaseImage)},
		})

		// create a volume to propagate the base image bind mount
		pod.Spec.Volumes = append(pod.Spec.Volumes, v1.Volume{
			Name: "share",
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{},
			},
		})

		hostToContainer := v1.MountPropagationHostToContainer
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, v1.VolumeMount{
			Name:             "share",
			ReadOnly:         true,
			MountPath:        "/share",
			MountPropagation: &hostToContainer,
		})
		pod.Spec.Containers[0].Command = append([]string{"/bin/sh", "-c", fmt.Sprintf(
			"while true; do list=$(ls /share/base_image/* 2>&1); if [ $? -eq 0 ]; then break; fi; echo waiting; sleep 1; done; echo Directory found $list; exec %s",
			strings.Join(pod.Spec.Containers[0].Command, " "),
		)})

		bidirectional := v1.MountPropagationBidirectional
		pod.Spec.Containers = append(pod.Spec.Containers, v1.Container{
			Name: "base-image",
			Command: []string{"/bin/sh", "-c", "function cleanup() { while true; do " +
				"umount /share/base_image; if [ $? -eq 0 ]; then echo unmounted && " +
				"kill $tpid && break; fi; echo waiting && sleep 1; done }; " +
				"mkdir -p /share/base_image && mount --bind /base_image/ /share/base_image && " +
				"echo base image mounted at /share/base_image && trap cleanup TERM && " +
				"mkfifo noop && tail -f noop & tpid=$! && trap cleanup TERM && wait $tpid"},
			Image:           r.Spec.BaseImage,
			ImagePullPolicy: v1.PullNever,
			SecurityContext: &v1.SecurityContext{
				Privileged: &privilege,
			},
			VolumeMounts: []v1.VolumeMount{
				{
					Name:             "share",
					MountPath:        "/share",
					MountPropagation: &bidirectional,
				},
			},
		})
	}

	// set pod to node that replica scheduled on
	pod.Spec.NodeName = r.Spec.NodeID

	if r.Spec.RestoreName != "" && r.Spec.RestoreFrom != "" {
		secret, err := rc.ds.GetSetting(types.SettingNameBackupTargetCredentialSecret)
		if err != nil {
			return nil, err
		}
		if secret.Value != "" {
			err := util.ConfigEnvWithCredential(r.Spec.RestoreFrom, secret.Value, &pod.Spec.Containers[0])
			if err != nil {
				return nil, err
			}
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
									Path: r.Spec.DataPath,
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
			propagation := metav1.DeletePropagationBackground
			if err := rc.kubeClient.BatchV1().Jobs(rc.namespace).Delete(r.Name,
				&metav1.DeleteOptions{
					PropagationPolicy: &propagation,
				}); err != nil {
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
	if err != nil {
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
