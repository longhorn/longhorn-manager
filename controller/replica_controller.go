package controller

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

	imapi "github.com/longhorn/longhorn-instance-manager/api"
	imclient "github.com/longhorn/longhorn-instance-manager/client"
	imutil "github.com/longhorn/longhorn-instance-manager/util"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1alpha1"
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
	// which namespace controller is running with
	namespace string
	// use as the OwnerID of replica
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	rStoreSynced  cache.InformerSynced
	imStoreSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface

	instanceHandler *InstanceHandler
}

func NewReplicaController(
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	replicaInformer lhinformers.ReplicaInformer,
	instanceManagerInformer lhinformers.InstanceManagerInformer,
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

		rStoreSynced:  replicaInformer.Informer().HasSynced,
		imStoreSynced: instanceManagerInformer.Informer().HasSynced,

		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "longhorn-replica"),
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
	return rc
}

func (rc *ReplicaController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer rc.queue.ShutDown()

	logrus.Infof("Start Longhorn replica controller")
	defer logrus.Infof("Shutting down Longhorn replica controller")

	if !controller.WaitForCacheSync("longhorn replicas", stopCh, rc.rStoreSynced, rc.imStoreSynced) {
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

	if replica.DeletionTimestamp != nil {
		if replica.Spec.OwnerID != "" {
			// Check if replica's managing node died
			if down, err := rc.ds.IsNodeDownOrDeleted(replica.Spec.OwnerID); err != nil {
				return err
			} else if down {
				replica.Spec.OwnerID = rc.controllerID
				_, err = rc.ds.UpdateReplica(replica)
				return err
			}
		}

		if replica.Spec.NodeID != "" {
			// Check if replica's executing node died
			if down, err := rc.ds.IsNodeDownOrDeleted(replica.Spec.NodeID); err != nil {
				return err
			} else if down {
				dataPath := replica.Spec.DataPath
				nodeID := replica.Spec.NodeID
				replica.Spec.DataPath = ""
				replica.Spec.NodeID = ""
				_, err = rc.ds.UpdateReplica(replica)
				if err == nil {
					rc.eventRecorder.Eventf(replica, v1.EventTypeWarning, EventReasonOrphaned,
						"Node %v down or deleted, can't cleanup replica %v data at %v",
						nodeID, replica.Name, dataPath)
				}
				return err
			}
		}

		if replica.Spec.NodeID == rc.controllerID {
			// Only attempt Instance deletion if it's assigned to an InstanceManager.
			if replica.Status.InstanceManagerName != "" {
				if _, err := rc.DeleteInstance(replica); err != nil {
					return err
				}
			}
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
			return rc.ds.RemoveFinalizerForReplica(replica)
		}

		if replica.Spec.NodeID == "" && replica.Spec.OwnerID == rc.controllerID {
			logrus.Debugf("Deleted replica %v without cleanup due to no node ID", replica.Name)
			return rc.ds.RemoveFinalizerForReplica(replica)
		}
	}

	// Not ours
	if replica.Spec.OwnerID != rc.controllerID {
		return nil
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

	return rc.instanceHandler.ReconcileInstanceState(replica, &replica.Spec.InstanceSpec, &replica.Status.InstanceStatus, types.InstanceManagerTypeReplica)
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

func (rc *ReplicaController) CreateInstance(obj interface{}) (*types.InstanceProcessStatus, error) {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		return nil, fmt.Errorf("BUG: invalid object for replica process creation: %v", obj)
	}
	if r.Spec.NodeID == "" || r.Spec.DataPath == "" || r.Spec.DiskID == "" || r.Spec.VolumeSize == 0 {
		return nil, fmt.Errorf("missing parameters for replica process creation: %v", r)
	}

	args := []string{
		"replica", types.GetReplicaMountedDataPath(r.Spec.DataPath),
		"--size", strconv.FormatInt(r.Spec.VolumeSize, 10),
	}

	c, err := rc.getProcessManagerClient(r.Status.InstanceManagerName)
	if err != nil {
		return nil, err
	}

	replicaProcess, err := c.ProcessCreate(
		r.Name, types.DefaultEngineBinaryPath, types.DefaultReplicaPortCount,
		args, []string{"--listen,0.0.0.0:"})
	if err != nil {
		return nil, err
	}

	return engineapi.ReplicaProcessToInstanceStatus(replicaProcess), nil
}

func (rc *ReplicaController) DeleteInstance(obj interface{}) (*types.InstanceProcessStatus, error) {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		return nil, fmt.Errorf("BUG: invalid object for replica process deletion: %v", obj)
	}

	c, err := rc.getProcessManagerClient(r.Status.InstanceManagerName)
	if err != nil {
		return nil, err
	}

	replicaProcess, err := c.ProcessDelete(r.Name)
	if err != nil {
		return nil, err
	}

	return engineapi.ReplicaProcessToInstanceStatus(replicaProcess), nil
}

func (rc *ReplicaController) GetInstance(obj interface{}) (*types.InstanceProcessStatus, error) {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		return nil, fmt.Errorf("BUG: invalid object for replica process get: %v", obj)
	}

	c, err := rc.getProcessManagerClient(r.Status.InstanceManagerName)
	if err != nil {
		return nil, err
	}

	replicaProcess, err := c.ProcessGet(r.Name)
	if err != nil {
		return nil, err
	}

	return engineapi.ReplicaProcessToInstanceStatus(replicaProcess), nil
}

func (rc *ReplicaController) LogInstance(obj interface{}) (*imapi.LogStream, error) {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		return nil, fmt.Errorf("BUG: invalid object for reploca process log: %v", obj)
	}

	c, err := rc.getProcessManagerClient(r.Status.InstanceManagerName)
	if err != nil {
		return nil, err
	}

	stream, err := c.ProcessLog(r.Name)
	if err != nil {
		return nil, err
	}

	return stream, nil
}

func (rc *ReplicaController) enqueueInstanceManagerChange(im *longhorn.InstanceManager) {
	if im.Spec.OwnerID != rc.controllerID {
		return
	}
	imType, ok := im.Labels["type"]
	if !ok || imType != string(types.InstanceManagerTypeReplica) {
		return
	}

	rs, err := rc.ds.ListReplicasByNode(im.Spec.NodeID)
	if err != nil {
		logrus.Warnf("Failed to list replicas on node %v", im.Spec.NodeID)
	}
	for _, rList := range rs {
		for _, r := range rList {
			rc.enqueueReplica(r)
		}
	}
	return
}
