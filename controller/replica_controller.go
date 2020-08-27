package controller

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	imapi "github.com/longhorn/longhorn-instance-manager/pkg/api"
	imclient "github.com/longhorn/longhorn-instance-manager/pkg/client"
	imutil "github.com/longhorn/longhorn-instance-manager/pkg/util"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
)

var (
	// maxRetries is the number of times a deployment will be retried before it is dropped out of the queue.
	// With the current rate-limiter in use (5ms*2^(maxRetries-1)) the following numbers represent the times
	// a deployment is going to be requeued:
	//
	// 5ms, 10ms, 20ms
	maxRetries = 3
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

	nStoreSynced  cache.InformerSynced
	rStoreSynced  cache.InformerSynced
	imStoreSynced cache.InformerSynced

	instanceHandler *InstanceHandler
}

func NewReplicaController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	nodeInformer lhinformers.NodeInformer,
	replicaInformer lhinformers.ReplicaInformer,
	instanceManagerInformer lhinformers.InstanceManagerInformer,
	kubeClient clientset.Interface,
	namespace string, controllerID string) *ReplicaController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	rc := &ReplicaController{
		baseController: newBaseController("longhorn-replica", logger),

		namespace:    namespace,
		controllerID: controllerID,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-replica-controller"}),

		ds: ds,

		nStoreSynced:  nodeInformer.Informer().HasSynced,
		rStoreSynced:  replicaInformer.Informer().HasSynced,
		imStoreSynced: instanceManagerInformer.Informer().HasSynced,
	}
	rc.instanceHandler = NewInstanceHandler(ds, rc, rc.eventRecorder)

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

	instanceManagerInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			im := obj.(*longhorn.InstanceManager)
			rc.enqueueInstanceManagerChange(im)
		},
		UpdateFunc: func(old, cur interface{}) {
			curIM := cur.(*longhorn.InstanceManager)
			rc.enqueueInstanceManagerChange(curIM)
		},
		DeleteFunc: func(obj interface{}) {
			im := obj.(*longhorn.InstanceManager)
			rc.enqueueInstanceManagerChange(im)
		},
	})

	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			n := obj.(*longhorn.Node)
			rc.enqueueNodeChange(n)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			cur := newObj.(*longhorn.Node)
			rc.enqueueNodeChange(cur)
		},
		DeleteFunc: func(obj interface{}) {
			n := obj.(*longhorn.Node)
			rc.enqueueNodeChange(n)
		},
	})

	return rc
}

func (rc *ReplicaController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer rc.queue.ShutDown()

	logrus.Infof("Start Longhorn replica controller")
	defer logrus.Infof("Shutting down Longhorn replica controller")

	if !controller.WaitForCacheSync("longhorn replicas", stopCh, rc.nStoreSynced, rc.rStoreSynced, rc.imStoreSynced) {
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

// From replica to check Node.Spec.EvictionRequested of the node
// this replica first, then check Node.Spec.Disks.EvictionRequested
func (rc *ReplicaController) isEvictionRequested(replica *longhorn.Replica) bool {
	// Return false if this replica has not been assigned to a node.
	if replica.Spec.NodeID == "" {
		return false
	}

	node, err := rc.ds.GetNode(replica.Spec.NodeID)
	if err != nil {
		logrus.Warnf("Failed to get node %v information err %v", replica.Spec.NodeID, err)
		return false
	}

	// Check if node has been request eviction.
	if node.Spec.EvictionRequested == true {
		return true
	}

	// Check if disk has been request eviction.
	if node.Spec.Disks[replica.Spec.DiskID].EvictionRequested == true {
		return true
	}

	return false
}

func (rc *ReplicaController) UpdateReplicaEvictionStatus(replica *longhorn.Replica) {
	// Check if eviction has been requested on this replica
	if rc.isEvictionRequested(replica) &&
		(replica.Status.EvictionRequested == false) {
		replica.Status.EvictionRequested = true
		logrus.Debugf("Replica %v has been requested eviction.",
			replica.Name)
	}

	// Check if eviction has been cancelled on this replica
	if !rc.isEvictionRequested(replica) &&
		(replica.Status.EvictionRequested == true) {
		replica.Status.EvictionRequested = false
		logrus.Debugf("Replica %v has been cancelled eviction.",
			replica.Name)
	}

	return
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

	if replica.Status.OwnerID != rc.controllerID {
		if !rc.isResponsibleFor(replica) {
			// Not ours
			return nil
		}
		replica.Status.OwnerID = rc.controllerID
		replica, err = rc.ds.UpdateReplicaStatus(replica)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		logrus.Debugf("Replica controller %v picked up %v", rc.controllerID, replica.Name)
	}

	if replica.DeletionTimestamp != nil {
		if err := rc.DeleteInstance(replica); err != nil {
			return errors.Wrapf(err, "failed to cleanup the related replica process before deleting replica %v", replica.Name)
		}

		if replica.Spec.NodeID != "" && replica.Spec.NodeID != replica.Status.OwnerID {
			logrus.Warnf("Node %v down or deleted, can't cleanup replica %v data at %v",
				replica.Spec.NodeID, replica.Name, replica.Spec.DataPath)
		} else if replica.Spec.NodeID != "" {
			if replica.Spec.Active {
				// prevent accidentally deletion
				if !strings.Contains(filepath.Base(filepath.Clean(replica.Spec.DataPath)), "-") {
					return fmt.Errorf("%v doesn't look like a replica data path", replica.Spec.DataPath)
				}
				if err := util.RemoveHostDirectoryContent(replica.Spec.DataPath); err != nil {
					return errors.Wrapf(err, "cannot cleanup after replica %v at %v", replica.Name, replica.Spec.DataPath)
				}
				logrus.Debugf("Cleanup replica %v at %v:%v completed", replica.Name, replica.Spec.NodeID, replica.Spec.DataPath)
			} else {
				logrus.Debugf("Didn't cleanup replica %v since it's not the active one for the path %v", replica.Name, replica.Spec.DataPath)
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
			logrus.Debugf("Requeue %v due to conflict: %v", key, err)
			rc.enqueueReplica(replica)
			err = nil
		}
	}()

	// Update `Replica.Status.EvictionRequested` field
	rc.UpdateReplicaEvictionStatus(replica)

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

func (rc *ReplicaController) getProcessManagerClient(instanceManagerName string) (*imclient.ProcessManagerClient, error) {
	im, err := rc.ds.GetInstanceManager(instanceManagerName)
	if err != nil {
		return nil, fmt.Errorf("cannot find Instance Manager %v", instanceManagerName)
	}
	if im.Status.CurrentState != types.InstanceManagerStateRunning || im.Status.IP == "" {
		return nil, fmt.Errorf("invalid Instance Manager %v", instanceManagerName)
	}

	return imclient.NewProcessManagerClient(imutil.GetURL(im.Status.IP, engineapi.InstanceManagerDefaultPort)), nil
}

func (rc *ReplicaController) CreateInstance(obj interface{}) (*types.InstanceProcess, error) {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		return nil, fmt.Errorf("BUG: invalid object for replica process creation: %v", obj)
	}
	if r.Spec.NodeID == "" || r.Spec.DataPath == "" || r.Spec.DiskID == "" || r.Spec.VolumeSize == 0 {
		return nil, fmt.Errorf("missing parameters for replica process creation: %v", r)
	}

	im, err := rc.ds.GetInstanceManagerByInstance(obj)
	if err != nil {
		return nil, err
	}
	c, err := engineapi.NewInstanceManagerClient(im)
	if err != nil {
		return nil, err
	}

	return c.ReplicaProcessCreate(r.Name, r.Spec.EngineImage, r.Spec.DataPath, r.Spec.VolumeSize)
}

func (rc *ReplicaController) DeleteInstance(obj interface{}) error {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		return fmt.Errorf("BUG: invalid object for replica process deletion: %v", obj)
	}

	if err := rc.deleteInstanceWithCLIAPIVersionOne(r); err != nil {
		return err
	}

	// Not assigned, safe to delete
	if r.Status.InstanceManagerName == "" {
		return nil
	}

	im, err := rc.ds.GetInstanceManager(r.Status.InstanceManagerName)
	if err != nil {
		return err
	}

	// Node down
	if im.Spec.NodeID != im.Status.OwnerID {
		isDown, err := rc.ds.IsNodeDownOrDeleted(im.Spec.NodeID)
		if err != nil {
			return err
		}
		if isDown {
			delete(im.Status.Instances, r.Name)
			if _, err := rc.ds.UpdateInstanceManagerStatus(im); err != nil {
				return err
			}
			return nil
		}
	}

	c, err := engineapi.NewInstanceManagerClient(im)
	if err != nil {
		return err
	}
	if err := c.ProcessDelete(r.Name); err != nil && !types.ErrorIsNotFound(err) {
		return err
	}

	// Directly remove the instance from the map. Best effort.
	if im.Status.APIVersion == engineapi.IncompatibleInstanceManagerAPIVersion {
		delete(im.Status.Instances, r.Name)
		if _, err := rc.ds.UpdateInstanceManagerStatus(im); err != nil {
			return err
		}
	}

	return nil
}

func (rc *ReplicaController) deleteInstanceWithCLIAPIVersionOne(r *longhorn.Replica) (err error) {
	isCLIAPIVersionOne := false
	if r.Status.CurrentImage != "" {
		isCLIAPIVersionOne, err = rc.ds.IsEngineImageCLIAPIVersionOne(r.Status.CurrentImage)
		if err != nil {
			return err
		}
	}

	if isCLIAPIVersionOne {
		pod, err := rc.kubeClient.CoreV1().Pods(rc.namespace).Get(r.Name, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return errors.Wrapf(err, "failed to get pod for old replica %v", r.Name)
		}
		if apierrors.IsNotFound(err) {
			pod = nil
		}

		logrus.Debugf("Prepared to delete old version replica %v with running pod", r.Name)
		if err := rc.deleteOldReplicaPod(pod, r); err != nil {
			return err
		}
	}
	return nil
}

func (rc *ReplicaController) deleteOldReplicaPod(pod *v1.Pod, r *longhorn.Replica) (err error) {
	// pod already stopped
	if pod == nil {
		return nil
	}

	if pod.DeletionTimestamp != nil {
		if pod.DeletionGracePeriodSeconds != nil && *pod.DeletionGracePeriodSeconds != 0 {
			// force deletion in the case of node lost
			deletionDeadline := pod.DeletionTimestamp.Add(time.Duration(*pod.DeletionGracePeriodSeconds) * time.Second)
			now := time.Now().UTC()
			if now.After(deletionDeadline) {
				logrus.Debugf("replica pod %v still exists after grace period %v passed, force deletion: now %v, deadline %v",
					pod.Name, pod.DeletionGracePeriodSeconds, now, deletionDeadline)
				gracePeriod := int64(0)
				if err := rc.kubeClient.CoreV1().Pods(rc.namespace).Delete(pod.Name, &metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod}); err != nil {
					logrus.Debugf("failed to force deleting replica pod %v: %v ", pod.Name, err)
					return nil
				}
			}
		}
		return nil
	}

	if err := rc.kubeClient.CoreV1().Pods(rc.namespace).Delete(pod.Name, nil); err != nil {
		rc.eventRecorder.Eventf(r, v1.EventTypeWarning, EventReasonFailedStopping, "Error stopping pod for old replica %v: %v", pod.Name, err)
		return nil
	}
	rc.eventRecorder.Eventf(r, v1.EventTypeNormal, EventReasonStop, "Stops pod for old replica %v", pod.Name)
	return nil
}

func (rc *ReplicaController) GetInstance(obj interface{}) (*types.InstanceProcess, error) {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		return nil, fmt.Errorf("BUG: invalid object for replica process get: %v", obj)
	}

	var (
		im  *longhorn.InstanceManager
		err error
	)
	if r.Status.InstanceManagerName == "" {
		im, err = rc.ds.GetInstanceManagerByInstance(obj)
		if err != nil {
			return nil, err
		}
	} else {
		im, err = rc.ds.GetInstanceManager(r.Status.InstanceManagerName)
		if err != nil {
			return nil, err
		}
	}
	c, err := engineapi.NewInstanceManagerClient(im)
	if err != nil {
		return nil, err
	}

	return c.ProcessGet(r.Name)
}

func (rc *ReplicaController) LogInstance(obj interface{}) (*imapi.LogStream, error) {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		return nil, fmt.Errorf("BUG: invalid object for replica process log: %v", obj)
	}

	im, err := rc.ds.GetInstanceManager(r.Status.InstanceManagerName)
	if err != nil {
		return nil, err
	}
	c, err := engineapi.NewInstanceManagerClient(im)
	if err != nil {
		return nil, err
	}

	return c.ProcessLog(r.Name)
}

func (rc *ReplicaController) enqueueInstanceManagerChange(im *longhorn.InstanceManager) {
	imType, err := datastore.CheckInstanceManagerType(im)
	if err != nil || imType != types.InstanceManagerTypeReplica {
		return
	}

	// replica's NodeID won't change, don't need to check instance manager
	rs, err := rc.ds.ListReplicasByNode(im.Spec.NodeID)
	if err != nil {
		logrus.Warnf("Failed to list replicas on node %v", im.Spec.NodeID)
	}
	for _, rList := range rs {
		for _, r := range rList {
			if r.Status.OwnerID == rc.controllerID {
				rc.enqueueReplica(r)
			}
		}
	}
	return
}

func (rc *ReplicaController) enqueueNodeChange(node *longhorn.Node) {

	var evictionDisks []string
	// If Node evition, add all the disks to evictionDisks.
	// Otherwise add request eviction disk separately.
	if node.Spec.EvictionRequested == true {
		for diskName := range node.Spec.Disks {
			evictionDisks = append(evictionDisks, diskName)
		}
	} else {
		for diskName, diskSpec := range node.Spec.Disks {
			if diskSpec.EvictionRequested == true {
				evictionDisks = append(evictionDisks, diskName)
			}
		}
	}

	// Add eviction requested replicas to the workqueue
	for _, diskName := range evictionDisks {
		for replicaName := range node.Status.DiskStatus[diskName].ScheduledReplica {
			replica, err := rc.ds.GetReplica(replicaName)
			if err != nil {
				return
			}
			rc.enqueueReplica(replica)
		}
	}

	return
}

func (rc *ReplicaController) isResponsibleFor(r *longhorn.Replica) bool {
	return isControllerResponsibleFor(rc.controllerID, rc.ds, r.Name, r.Spec.NodeID, r.Status.OwnerID)
}
