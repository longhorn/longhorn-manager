package controller

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	batchinformers "k8s.io/client-go/informers/batch/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	batchlisters "k8s.io/client-go/listers/batch/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/rancher/longhorn-manager/types"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhclientset "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhinformers "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1alpha1"
	lhlisters "github.com/rancher/longhorn-manager/k8s/pkg/client/listers/longhorn/v1alpha1"
)

var (
	// maxRetries is the number of times a deployment will be retried before it is dropped out of the queue.
	// With the current rate-limiter in use (5ms*2^(maxRetries-1)) the following numbers represent the times
	// a deployment is going to be requeued:
	//
	// 5ms, 10ms, 20ms
	maxRetries = 3

	ownerKind = longhorn.SchemeGroupVersion.WithKind("Replica").String()
)

const (
	// longhornDirectory is the directory going to be bind mounted on the
	// host to provide storage space to replica data
	longhornDirectory = "/var/lib/rancher/longhorn/"

	// longhornReplicaKey is the key to identify which volume the replica
	// belongs to, for scheduling purpose
	longhornReplicaKey = "longhorn-volume-replica"
)

type ReplicaController struct {
	// which namespace controller is running with
	namespace string
	// use as the OwnerID of replica
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	lhClient lhclientset.Interface

	// To allow injection for testing
	syncHandler           func(rKey string) error
	enqueueReplicaHandler func(r *longhorn.Replica)
	updateReplicaHandler  func(r *longhorn.Replica) (*longhorn.Replica, error)

	rLister      lhlisters.ReplicaLister
	rStoreSynced cache.InformerSynced

	pLister      corelisters.PodLister
	pStoreSynced cache.InformerSynced

	jLister      batchlisters.JobLister
	jStoreSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface
}

type Replica struct {
	longhorn.Replica
	namespace string
}

func NewReplicaController(
	replicaInformer lhinformers.ReplicaInformer,
	podInformer coreinformers.PodInformer,
	jobInformer batchinformers.JobInformer,
	lhClient lhclientset.Interface, kubeClient clientset.Interface,
	namespace string, controllerID string) *ReplicaController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	rc := &ReplicaController{
		namespace: namespace,

		kubeClient:    kubeClient,
		lhClient:      lhClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "longhorn-replica-controller"}),

		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "longhorn-replica"),
	}

	replicaInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			r := obj.(*longhorn.Replica)
			rc.enqueueReplicaHandler(r)
		},
		UpdateFunc: func(old, cur interface{}) {
			curR := cur.(*longhorn.Replica)
			rc.enqueueReplicaHandler(curR)
		},
		DeleteFunc: func(obj interface{}) {
			r := obj.(*longhorn.Replica)
			rc.enqueueReplicaHandler(r)
		},
	})
	rc.rLister = replicaInformer.Lister()
	rc.rStoreSynced = replicaInformer.Informer().HasSynced

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
	rc.pLister = podInformer.Lister()
	rc.pStoreSynced = podInformer.Informer().HasSynced

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
	rc.jLister = jobInformer.Lister()
	rc.jStoreSynced = jobInformer.Informer().HasSynced

	rc.syncHandler = rc.syncReplica
	rc.enqueueReplicaHandler = rc.enqueueReplica
	rc.updateReplicaHandler = rc.updateReplica

	rc.controllerID = controllerID
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

	err := rc.syncHandler(key.(string))
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

func (rc *ReplicaController) getPodForReplica(r *longhorn.Replica) (*v1.Pod, error) {
	return rc.pLister.Pods(rc.namespace).Get(r.Name)
}

func (rc *ReplicaController) getJobForReplica(r *longhorn.Replica) (*batchv1.Job, error) {
	return rc.kubeClient.BatchV1().Jobs(rc.namespace).Get(r.Name, metav1.GetOptions{})
}

func (rc *ReplicaController) syncReplicaWithPod(r *longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync replica with pod for %v", r.Name)
	}()
	pod, err := rc.getPodForReplica(r)
	if err != nil {
		if apierrors.IsNotFound(err) {
			r.Status.State = types.InstanceStateStopped
			r.Status.IP = ""
		} else {
			return err
		}
	} else {
		switch pod.Status.Phase {
		case v1.PodPending:
			r.Status.State = types.InstanceStateStopped
			r.Status.IP = ""
		case v1.PodRunning:
			r.Status.State = types.InstanceStateRunning
			r.Status.IP = pod.Status.PodIP
			// pin down to this node ID from now on
			if r.Spec.NodeID == "" {
				r.Spec.NodeID = pod.Spec.NodeName
			} else if r.Spec.NodeID != pod.Spec.NodeName {
				// it shouldn't happen
				r.Status.State = types.InstanceStateError
				r.Status.IP = ""
				err := fmt.Errorf("BUG: replica %v wasn't pin down to the host %v", r.Name, r.Spec.NodeID)
				logrus.Errorf("%v", err)
				return err
			}
		default:
			logrus.Warnf("volume %v replica %v instance state is failed/unknown, pod state %v",
				r.Spec.VolumeName, r.Name, pod.Status.Phase)
			r.Status.State = types.InstanceStateError
		}
	}
	return nil
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

	replicaRO, err := rc.rLister.Replicas(rc.namespace).Get(name)
	if apierrors.IsNotFound(err) {
		logrus.Infof("Longhorn replica %v has been deleted", key)
		return nil
	}
	if err != nil {
		return err
	}

	replica := replicaRO.DeepCopy()

	// Not ours
	if replica.Spec.DesireOwnerID != rc.controllerID {
		return nil
	}

	// Previous controller hasn't yield the control yet
	//
	// TODO Currently the waiting timeout is indefinite. If the other
	// controller is down or malfunctioning, this controller should take it
	// over after a period of time
	if replica.Status.CurrentOwnerID != "" && replica.Status.CurrentOwnerID != rc.controllerID {
		return fmt.Errorf("replica-controller %v: Waiting for previous controller %v to yield the control for replica %v", rc.controllerID, replica.Status.CurrentOwnerID, replica.Name)
	}

	defer func() {
		// we're going to update replica assume things changes
		if err == nil {
			_, err = rc.updateReplicaHandler(replica)
		}
	}()

	if replica.Status.CurrentOwnerID == "" {
		replica.Status.CurrentOwnerID = rc.controllerID
	}

	// we will sync up with pod status before proceed
	if err := rc.syncReplicaWithPod(replica); err != nil {
		return err
	}

	// we need to stop the replica which failed connection with controller
	if replica.Spec.FailedAt != "" && replica.Spec.DesireState != types.InstanceStateStopped {
		replica.Spec.DesireState = types.InstanceStateStopped
		_, err := rc.updateReplicaHandler(replica)
		if err != nil {
			return err
		}
		rc.enqueueReplicaHandler(replica)
		return nil
	}

	// API server set the replica to be deleted but we haven't cleaned up
	// yet, so we keep the initializer in place and continuing with clean up
	if replica.DeletionTimestamp != nil && replica.Spec.DesireState != types.InstanceStateDeleted {
		replica.Spec.DesireState = types.InstanceStateDeleted
		_, err := rc.updateReplicaHandler(replica)
		if err != nil {
			return err
		}
		rc.enqueueReplicaHandler(replica)
		return nil
	}

	state := replica.Status.State
	desireState := replica.Spec.DesireState
	if desireState == types.InstanceStateDeleted && state == desireState {
		return rc.deleteReplica(replica)
	}
	if state == types.InstanceStateError {
		return rc.stopReplicaInstance(replica)
	}

	if state != desireState {
		switch desireState {
		case types.InstanceStateRunning:
			if state == types.InstanceStateStopped {
				if err := rc.startReplicaInstance(replica); err != nil {
					return err
				}
				break
			}
			logrus.Errorf("unable to do replica transition: current %v, desire %v", state, desireState)
		case types.InstanceStateStopped:
			if state == types.InstanceStateRunning {
				if err := rc.stopReplicaInstance(replica); err != nil {
					return err
				}
				break
			}
			logrus.Errorf("unable to do replica transition: current %v, desire %v", state, desireState)
		case types.InstanceStateDeleted:
			if state == types.InstanceStateRunning {
				if err := rc.stopReplicaInstance(replica); err != nil {
					return err
				}
				break
			}
			if state == types.InstanceStateStopped {
				if err := rc.cleanupReplicaInstance(replica); err != nil {
					return err
				}
				break
			}
			logrus.Errorf("unable to delete replica due to unknown state %v", state)
		default:
			logrus.Errorf("unknown replica transition: current %v, desire %v", state, desireState)
		}
	}
	return nil
}

func (rc *ReplicaController) enqueueReplica(replica *longhorn.Replica) {
	key, err := controller.KeyFunc(replica)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", replica, err))
		return
	}

	rc.queue.AddRateLimited(key)
}

func (rc *ReplicaController) updateReplica(r *longhorn.Replica) (replica *longhorn.Replica, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to update replica for %v", r.Name)
	}()
	return rc.lhClient.LonghornV1alpha1().Replicas(rc.namespace).Update(r)
}

func (rc *ReplicaController) deleteReplica(r *longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to delete replica for %v", r.Name)
	}()
	name := r.Name
	result, err := rc.rLister.Replicas(r.Namespace).Get(name)
	if err != nil {
		// already deleted
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "unable to get replica during replica deletion %v", name)
	}
	resultCopy := result.DeepCopy()
	// Remove the finalizer to allow deletion of the object
	// FIXME only remove myself from the finalizer
	resultCopy.Finalizers = []string{}
	result, err = rc.updateReplicaHandler(result)
	if err != nil {
		return errors.Wrapf(err, "unable to update finalizer during replica deletion %v", name)
	}
	// No pending deletion operation, so we need to do it ourselves
	if result.DeletionTimestamp == nil {
		if err := rc.lhClient.LonghornV1alpha1().Replicas(rc.namespace).Delete(name, nil); err != nil {
			return errors.Wrapf(err, "unable to delete replica %v", name)
		}
	}

	return nil
}

func (rc *ReplicaController) getReplicaVolumeDirectory(replicaName string) string {
	return longhornDirectory + "/replicas/" + replicaName
}

func (rc *ReplicaController) createPod(r *longhorn.Replica) *v1.Pod {
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
					Kind:       ownerKind,
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
	return pod
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
					Kind: ownerKind,
					UID:  r.UID,
					Name: r.Name,
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

func (rc *ReplicaController) startReplicaInstance(r *longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to start replica instance for %v", r.Name)
	}()
	pod, err := rc.getPodForReplica(r)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	// pod already started
	if pod != nil {
		return nil
	}
	podSpec := rc.createPod(r)

	logrus.Debugf("Starting replica %v for %v", r.Name, r.Spec.VolumeName)
	if _, err := rc.kubeClient.CoreV1().Pods(rc.namespace).Create(podSpec); err != nil {
		return err
	}
	return nil
}

func (rc *ReplicaController) stopReplicaInstance(r *longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to stop replica instance for %v", r.Name)
	}()
	pod, err := rc.getPodForReplica(r)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	// pod already stopped
	if pod == nil {
		return nil
	}
	// pod has been already asked to stop
	if pod.DeletionTimestamp != nil {
		return nil
	}
	logrus.Debugf("Stopping replica %v for %v", r.Name, r.Spec.VolumeName)
	if err := rc.kubeClient.CoreV1().Pods(rc.namespace).Delete(r.Name, nil); err != nil {
		return err
	}
	return nil
}

func (rc *ReplicaController) cleanupReplicaInstance(r *longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to cleanup replica instance for %v", r.Name)
	}()
	// replica wasn't created once, doesn't need clean up
	if r.Spec.NodeID == "" {
		return nil
	}
	job, err := rc.getJobForReplica(r)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if job == nil {
		job := rc.createCleanupJobSpec(r)

		_, err = rc.kubeClient.BatchV1().Jobs(rc.namespace).Create(job)
		if err != nil {
			return errors.Wrap(err, "failed to create cleanup job")
		}
		return nil
	}

	if job.Status.CompletionTime != nil {
		defer func() {
			err := rc.kubeClient.BatchV1().Jobs(rc.namespace).Delete(r.Name, nil)
			if err != nil {
				logrus.Warnf("Failed to delete the cleanup job for %v: %v", r.Name, err)
			}
			rc.enqueueReplicaHandler(r)
		}()

		if job.Status.Succeeded != 0 {
			logrus.Infof("Cleanup for volume %v replica %v succeed", r.Spec.VolumeName, r.Name)
			r.Status.State = types.InstanceStateDeleted
			if _, err := rc.updateReplicaHandler(r); err != nil {
				return err
			}
		} else {
			logrus.Warnf("Cleanup for volume %v replica %v failed", r.Spec.VolumeName, r.Name)
		}
	}

	return nil
}

func (rc *ReplicaController) enqueueControlleeChange(obj interface{}) {
	metaObj, ok := obj.(metav1.Object)
	if !ok {
		logrus.Warnf("BUG: %v cannot be convert to metav1.Object", obj)
		return
	}
	ownerRefs := metaObj.GetOwnerReferences()
	for _, ref := range ownerRefs {
		if ref.Kind != ownerKind {
			continue
		}
		namespace := ""
		if pod, ok := obj.(*v1.Pod); ok {
			namespace = pod.Namespace
		} else if job, ok := obj.(*batchv1.Job); ok {
			namespace = job.Namespace
		} else {
			// not what we recognized
			continue
		}
		replica := rc.resolveOwnerRef(namespace, &ref)
		if replica == nil {
			return
		}
		rc.enqueueReplicaHandler(replica)
		return
	}
}

func (rc *ReplicaController) resolveOwnerRef(namespace string, ref *metav1.OwnerReference) *longhorn.Replica {
	if ref.Kind != ownerKind {
		return nil
	}
	replica, err := rc.rLister.Replicas(namespace).Get(ref.Name)
	if err != nil {
		return nil
	}
	if replica.UID != ref.UID {
		// The controller we found with this Name is not the same one that the
		// OwnerRef points to.
		return nil
	}
	// Not ours
	if replica.Status.CurrentOwnerID != rc.controllerID {
		return nil
	}
	return replica
}
