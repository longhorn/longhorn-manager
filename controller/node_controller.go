package controller

import (
	"fmt"
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
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/scheduler"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1alpha1"
)

var (
	ownerKindNode = longhorn.SchemeGroupVersion.WithKind("Node").String()
)

type NodeController struct {
	// which namespace controller is running with
	namespace    string
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	nStoreSynced  cache.InformerSynced
	pStoreSynced  cache.InformerSynced
	sStoreSynced  cache.InformerSynced
	rStoreSynced  cache.InformerSynced
	knStoreSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface

	getDiskInfoHandler                 GetDiskInfoHandler
	diskPathReplicaSubdirectoryChecker DiskPathReplicaSubdirectoryChecker

	scheduler *scheduler.ReplicaScheduler
}

type GetDiskInfoHandler func(string) (*util.DiskInfo, error)
type DiskPathReplicaSubdirectoryChecker func(string) (bool, error)

func NewNodeController(
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	nodeInformer lhinformers.NodeInformer,
	settingInformer lhinformers.SettingInformer,
	podInformer coreinformers.PodInformer,
	replicaInformer lhinformers.ReplicaInformer,
	kubeNodeInformer coreinformers.NodeInformer,
	kubeClient clientset.Interface,
	namespace, controllerID string) *NodeController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	nc := &NodeController{
		namespace:    namespace,
		controllerID: controllerID,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-node-controller"}),

		ds: ds,

		nStoreSynced:  nodeInformer.Informer().HasSynced,
		pStoreSynced:  podInformer.Informer().HasSynced,
		sStoreSynced:  settingInformer.Informer().HasSynced,
		rStoreSynced:  replicaInformer.Informer().HasSynced,
		knStoreSynced: kubeNodeInformer.Informer().HasSynced,

		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "longhorn-node"),

		getDiskInfoHandler:                 util.GetDiskInfo,
		diskPathReplicaSubdirectoryChecker: util.CheckDiskPathReplicaSubdirectory,
	}

	nc.scheduler = scheduler.NewReplicaScheduler(ds)

	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			n := obj.(*longhorn.Node)
			nc.enqueueNode(n)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			cur := newObj.(*longhorn.Node)
			nc.enqueueNode(cur)
		},
		DeleteFunc: func(obj interface{}) {
			n := obj.(*longhorn.Node)
			nc.enqueueNode(n)
		},
	})

	settingInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *longhorn.Setting:
					return filterSettings(t)
				default:
					utilruntime.HandleError(fmt.Errorf("unable to handle object in %T: %T", nc, obj))
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					s := obj.(*longhorn.Setting)
					nc.enqueueSetting(s)
				},
				UpdateFunc: func(oldObj, newObj interface{}) {
					cur := newObj.(*longhorn.Setting)
					nc.enqueueSetting(cur)
				},
			},
		},
	)

	replicaInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *longhorn.Replica:
					return nc.filterReplica(t)
				default:
					utilruntime.HandleError(fmt.Errorf("unable to handle object in %T: %T", nc, obj))
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					r := obj.(*longhorn.Replica)
					nc.enqueueReplica(r)
				},
				UpdateFunc: func(oldObj, newObj interface{}) {
					cur := newObj.(*longhorn.Replica)
					nc.enqueueReplica(cur)
				},
				DeleteFunc: func(obj interface{}) {
					r := obj.(*longhorn.Replica)
					nc.enqueueReplica(r)
				},
			},
		},
	)

	podInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					return nc.filterManagerPod(t)
				default:
					utilruntime.HandleError(fmt.Errorf("unable to handle object in %T: %T", nc, obj))
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					pod := obj.(*v1.Pod)
					nc.enqueueManagerPod(pod)
				},
				UpdateFunc: func(oldObj, newObj interface{}) {
					cur := newObj.(*v1.Pod)
					nc.enqueueManagerPod(cur)
				},
				DeleteFunc: func(obj interface{}) {
					pod := obj.(*v1.Pod)
					nc.enqueueManagerPod(pod)
				},
			},
		},
	)

	kubeNodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			cur := newObj.(*v1.Node)
			nc.enqueueKubernetesNode(cur)
		},
		DeleteFunc: func(obj interface{}) {
			n := obj.(*v1.Node)
			nc.enqueueKubernetesNode(n)
		},
	})

	return nc
}

func filterSettings(s *longhorn.Setting) bool {
	// filter that only StorageMinimalAvailablePercentage will impact disk status
	if types.SettingName(s.Name) == types.SettingNameStorageMinimalAvailablePercentage {
		return true
	}
	return false
}

func (nc *NodeController) filterReplica(r *longhorn.Replica) bool {
	// only sync replica running on current node
	if r.Spec.NodeID == nc.controllerID {
		return true
	}
	return false
}

func (nc *NodeController) filterManagerPod(obj *v1.Pod) bool {
	// only filter pod that control by manager
	controlByManager := false
	podContainers := obj.Spec.Containers
	for _, con := range podContainers {
		if con.Name == "longhorn-manager" {
			controlByManager = true
			break
		}
	}

	return controlByManager
}

func (nc *NodeController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer nc.queue.ShutDown()

	logrus.Infof("Start Longhorn node controller")
	defer logrus.Infof("Shutting down Longhorn node controller")

	if !controller.WaitForCacheSync("longhorn node", stopCh,
		nc.nStoreSynced, nc.pStoreSynced, nc.sStoreSynced, nc.rStoreSynced, nc.knStoreSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(nc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (nc *NodeController) worker() {
	for nc.processNextWorkItem() {
	}
}

func (nc *NodeController) processNextWorkItem() bool {
	key, quit := nc.queue.Get()

	if quit {
		return false
	}
	defer nc.queue.Done(key)

	err := nc.syncNode(key.(string))
	nc.handleErr(err, key)

	return true
}

func (nc *NodeController) handleErr(err error, key interface{}) {
	if err == nil {
		nc.queue.Forget(key)
		return
	}

	if nc.queue.NumRequeues(key) < maxRetries {
		logrus.Warnf("Error syncing Longhorn node %v: %v", key, err)
		nc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logrus.Warnf("Dropping Longhorn node %v out of the queue: %v", key, err)
	nc.queue.Forget(key)
}

func (nc *NodeController) syncNode(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync node for %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != nc.namespace {
		// Not ours, don't do anything
		return nil
	}

	node, err := nc.ds.GetNode(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			logrus.Errorf("BUG: Longhorn node %v has been deleted", key)
			return nil
		}
		return err
	}

	if node.DeletionTimestamp != nil {
		nc.eventRecorder.Eventf(node, v1.EventTypeWarning, EventReasonDelete, "BUG: Deleting node %v", node.Name)
		return nc.ds.RemoveFinalizerForNode(node)
	}

	existingNode := node.DeepCopy()
	defer func() {
		// we're going to update volume assume things changes
		if err == nil && !reflect.DeepEqual(existingNode, node) {
			_, err = nc.ds.UpdateNode(node)
		}
		// requeue if it's conflict
		if apierrors.IsConflict(errors.Cause(err)) {
			logrus.Debugf("Requeue %v due to conflict", key)
			nc.enqueueNode(node)
			err = nil
		}
	}()

	// sync node state by manager pod
	managerPods, err := nc.ds.ListManagerPods()
	if err != nil {
		return err
	}
	nodeManagerFound := false
	for _, pod := range managerPods {
		if pod.Spec.NodeName == node.Name {
			nodeManagerFound = true
			condition := types.GetNodeConditionFromStatus(node.Status, types.NodeConditionTypeReady)
			//condition.LastProbeTime = util.Now()
			podConditions := pod.Status.Conditions
			for _, podCondition := range podConditions {
				if podCondition.Type == v1.PodReady {
					if podCondition.Status == v1.ConditionTrue && pod.Status.Phase == v1.PodRunning {
						if condition.Status != types.ConditionStatusTrue {
							condition.LastTransitionTime = util.Now()
							nc.eventRecorder.Eventf(node, v1.EventTypeNormal, types.NodeConditionTypeReady, "Node %v is ready", node.Name)
						}
						condition.Status = types.ConditionStatusTrue
						condition.Reason = ""
						condition.Message = ""
					} else {
						if condition.Status != types.ConditionStatusFalse {
							condition.LastTransitionTime = util.Now()
							nc.eventRecorder.Eventf(node, v1.EventTypeWarning, types.NodeConditionReasonManagerPodDown, "Node %v is down: the manager pod %v is not running", node.Name, pod.Name)
						}
						condition.Status = types.ConditionStatusFalse
						condition.Reason = string(types.NodeConditionReasonManagerPodDown)
						condition.Message = fmt.Sprintf("the manager pod %v is not running", pod.Name)
					}
					break
				}
			}
			node.Status.Conditions[types.NodeConditionTypeReady] = condition
			break
		}
	}

	if !nodeManagerFound {
		condition := types.GetNodeConditionFromStatus(node.Status, types.NodeConditionTypeReady)
		if condition.Status != types.ConditionStatusFalse {
			condition.LastTransitionTime = util.Now()
			nc.eventRecorder.Eventf(node, v1.EventTypeWarning, types.NodeConditionReasonManagerPodMissing, "manager pod missing: node %v has no manager pod running on it", node.Name)
		}
		condition.Status = types.ConditionStatusFalse
		condition.Reason = string(types.NodeConditionReasonManagerPodMissing)
		condition.Message = fmt.Sprintf("manager pod missing: node %v has no manager pod running on it", node.Name)
		node.Status.Conditions[types.NodeConditionTypeReady] = condition
	}

	// sync node state with kuberentes node status
	kubeNode, err := nc.ds.GetKubernetesNode(name)
	if err != nil {
		// if kubernetes node has been removed from cluster
		if apierrors.IsNotFound(err) {
			condition := types.GetNodeConditionFromStatus(node.Status, types.NodeConditionTypeReady)
			if condition.Status != types.ConditionStatusFalse {
				condition.LastTransitionTime = util.Now()
				nc.eventRecorder.Eventf(node, v1.EventTypeWarning, types.NodeConditionReasonKubernetesNodeGone, "Kubernetes node missing: node %v has been removed from the cluster and there is no manager pod running on it", node.Name)
			}
			condition.Status = types.ConditionStatusFalse
			condition.Reason = string(types.NodeConditionReasonKubernetesNodeGone)
			condition.Message = fmt.Sprintf("Kubernetes node missing: node %v has been removed from the cluster and there is no manager pod running on it", node.Name)
			node.Status.Conditions[types.NodeConditionTypeReady] = condition
		} else {
			return err
		}
	} else {
		kubeConditions := kubeNode.Status.Conditions
		condition := types.GetNodeConditionFromStatus(node.Status, types.NodeConditionTypeReady)
		for _, con := range kubeConditions {
			switch con.Type {
			case v1.NodeReady:
				if con.Status != v1.ConditionTrue {
					if condition.Status != types.ConditionStatusFalse {
						condition.LastTransitionTime = util.Now()
						nc.eventRecorder.Eventf(node, v1.EventTypeWarning, types.NodeConditionReasonKubernetesNodeNotReady, "Kubernetes node %v not ready: %v", node.Name, con.Reason)
					}
					condition.Status = types.ConditionStatusFalse
					condition.Reason = string(types.NodeConditionReasonKubernetesNodeNotReady)
					condition.Message = fmt.Sprintf("Kubernetes node %v not ready: %v", node.Name, con.Reason)
					node.Status.Conditions[types.NodeConditionTypeReady] = condition
					break
				}
			case v1.NodeOutOfDisk,
				v1.NodeDiskPressure,
				v1.NodePIDPressure,
				v1.NodeMemoryPressure,
				v1.NodeNetworkUnavailable:
				if con.Status == v1.ConditionTrue {
					if condition.Status != types.ConditionStatusFalse {
						condition.LastTransitionTime = util.Now()
						nc.eventRecorder.Eventf(node, v1.EventTypeWarning, types.NodeConditionReasonKubernetesNodePressure, "Kubernetes node %v has pressure: %v, %v", node.Name, con.Reason, con.Message)
					}
					condition.Status = types.ConditionStatusFalse
					condition.Reason = string(types.NodeConditionReasonKubernetesNodePressure)
					condition.Message = fmt.Sprintf("Kubernetes node %v has pressure: %v, %v", node.Name, con.Reason, con.Message)
					node.Status.Conditions[types.NodeConditionTypeReady] = condition
					break
				}
			default:
				if con.Status == v1.ConditionTrue {
					nc.eventRecorder.Eventf(node, v1.EventTypeWarning, types.NodeConditionReasonUnknownNodeConditionTrue, "Unknown condition true of kubernetes node %v: condition type is %v, reason is %v, message is %v", node.Name, con.Type, con.Reason, con.Message)
				}
				break
			}
		}
	}

	if nc.controllerID != node.Name {
		return nil
	}

	// sync default Disk on labeled Nodes
	if err := nc.syncDefaultDisk(node); err != nil {
		return err
	}

	// sync disks status on current node
	if err := nc.syncDiskStatus(node); err != nil {
		return err
	}
	// sync mount propagation status on current node
	for _, pod := range managerPods {
		if pod.Spec.NodeName == node.Name {
			if err := nc.syncNodeStatus(pod, node); err != nil {
				return err
			}
		}
	}

	return nil
}

func (nc *NodeController) enqueueNode(node *longhorn.Node) {
	key, err := controller.KeyFunc(node)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", node, err))
		return
	}

	nc.queue.AddRateLimited(key)
}

func (nc *NodeController) enqueueSetting(setting *longhorn.Setting) {
	nodeList, err := nc.ds.ListNodes()
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get all nodes: %v ", err))
		return
	}

	for _, node := range nodeList {
		nc.enqueueNode(node)
	}
}

func (nc *NodeController) enqueueReplica(replica *longhorn.Replica) {
	node, err := nc.ds.GetNode(replica.Spec.NodeID)
	if err != nil {
		// no replica would be scheduled to the node if the node is not
		// available. If the node was removed after being scheduled to,
		// the replica should be removed before that.
		utilruntime.HandleError(fmt.Errorf("Couldn't get node %v for replica %v: %v ",
			replica.Spec.NodeID, replica.Name, err))
		return
	}
	nc.enqueueNode(node)
}

func (nc *NodeController) enqueueManagerPod(pod *v1.Pod) {
	nodeList, err := nc.ds.ListNodes()
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get all nodes: %v ", err))
		return
	}
	for _, node := range nodeList {
		nc.enqueueNode(node)
	}
}

func (nc *NodeController) enqueueKubernetesNode(n *v1.Node) {
	node, err := nc.ds.GetNode(n.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// there is no Longhorn node created for the Kubernetes
			// node (e.g. controller/etcd node). Skip it
			return
		}
		utilruntime.HandleError(fmt.Errorf("Couldn't get node %v: %v ", n.Name, err))
		return
	}
	nc.enqueueNode(node)
}

// syncDefaultDisk handles creation of the default Disk if Create Default Disk on Labeled Nodes is enabled. This allows
// for the default Disk to be created even if the Node has been labeled after initial registration with Longhorn,
// provided that there are no existing Disks remaining on the Node.
func (nc *NodeController) syncDefaultDisk(node *longhorn.Node) error {
	requireLabel, err := nc.ds.GetSettingAsBool(types.SettingNameCreateDefaultDiskLabeledNodes)
	if err != nil {
		return err
	}
	if requireLabel && len(node.Spec.Disks) == 0 {
		kubeNode, err := nc.ds.GetKubernetesNode(node.Name)
		if err != nil {
			return err
		}
		if val, ok := kubeNode.Labels[types.NodeCreateDefaultDiskLabel]; ok {
			createDisk, err := strconv.ParseBool(val)
			if err != nil {
				logrus.Errorf("unable to parse label %v, value %v as bool: %v",
					types.NodeCreateDefaultDiskLabel, val, err)
			} else if createDisk {
				if err := nc.ds.CreateDefaultDisk(node); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (nc *NodeController) syncDiskStatus(node *longhorn.Node) error {
	diskMap := node.Spec.Disks
	diskStatusMap := map[string]types.DiskStatus{}

	// get all replicas which have been assigned to current node
	replicaDiskMap, err := nc.ds.ListReplicasByNode(node.Name)
	if err != nil {
		return err
	}

	// get settings of StorageMinimalAvailablePercentage
	minimalAvailablePercentage, err := nc.ds.GetSettingAsInt(types.SettingNameStorageMinimalAvailablePercentage)
	if err != nil {
		return err
	}

	updateDiskMap := map[string]types.DiskSpec{}
	originDiskStatus := node.Status.DiskStatus
	if originDiskStatus == nil {
		originDiskStatus = map[string]types.DiskStatus{}
	}
	for diskID, disk := range diskMap {
		diskConditions := map[types.DiskConditionType]types.Condition{}
		updateDisk := disk
		diskStatus := types.DiskStatus{}
		_, ok := originDiskStatus[diskID]
		if ok {
			diskStatus = originDiskStatus[diskID]
		}
		scheduledReplica := map[string]int64{}
		// if there's no replica assigned to this disk
		if _, ok := replicaDiskMap[diskID]; !ok {
			diskStatus.StorageScheduled = 0
			scheduledReplica = map[string]int64{}
		} else {
			// calculate storage scheduled
			replicaArray := replicaDiskMap[diskID]
			var storageScheduled int64
			for _, replica := range replicaArray {
				storageScheduled += replica.Spec.VolumeSize
				scheduledReplica[replica.Name] = replica.Spec.VolumeSize
			}
			diskStatus.StorageScheduled = storageScheduled
			delete(replicaDiskMap, diskID)
		}
		diskStatus.ScheduledReplica = scheduledReplica
		// get disk available size
		diskInfo, err := nc.getDiskInfoHandler(disk.Path)
		readyCondition := types.GetDiskConditionFromStatus(diskStatus, types.DiskConditionTypeReady)
		if err != nil {
			if readyCondition.Status != types.ConditionStatusFalse {
				readyCondition.LastTransitionTime = util.Now()
				nc.eventRecorder.Eventf(node, v1.EventTypeWarning, types.DiskConditionReasonNoDiskInfo,
					"Disk %v on node %v is not ready: Get disk information error: %v", disk.Path, node.Name, err)
			}
			readyCondition.Status = types.ConditionStatusFalse
			readyCondition.Reason = types.DiskConditionReasonNoDiskInfo
			readyCondition.Message = fmt.Sprintf("Get disk information on node %v error: %v", node.Name, err)
			// disable invalid disk
			updateDisk.AllowScheduling = false
			diskStatus.StorageMaximum = 0
			diskStatus.StorageAvailable = 0
		} else if diskInfo == nil || diskInfo.Fsid != diskID {
			// if the file system has changed
			if readyCondition.Status != types.ConditionStatusFalse {
				readyCondition.LastTransitionTime = util.Now()
				nc.eventRecorder.Eventf(node, v1.EventTypeWarning, types.DiskConditionReasonDiskFilesystemChanged,
					"Disk %v on node %v is not ready: disk has changed file system", disk.Path, node.Name)
			}
			readyCondition.Status = types.ConditionStatusFalse
			readyCondition.Reason = types.DiskConditionReasonDiskFilesystemChanged
			readyCondition.Message = fmt.Sprintf("disk %v on node %v has changed file system", disk.Path, node.Name)
			// disable invalid disk
			updateDisk.AllowScheduling = false
			diskStatus.StorageMaximum = 0
			diskStatus.StorageAvailable = 0
		} else {
			// create the directory if disk path exists but the replica subdirectory doesn't exist
			exists, err := nc.diskPathReplicaSubdirectoryChecker(disk.Path)
			if err != nil {
				return err
			}
			if !exists {
				logrus.Warnf("The replica subdirectory of disk %v on node %v doesn't exist, will create it now", disk.Path, node.Name)
				if err := util.CreateDiskPath(disk.Path); err != nil {
					return errors.Wrapf(err, "failed to create replica subdirectory for disk %v on node %v", disk.Path, node.Name)
				}
			}

			if readyCondition.Status != types.ConditionStatusTrue {
				readyCondition.LastTransitionTime = util.Now()
				nc.eventRecorder.Eventf(node, v1.EventTypeNormal, types.DiskConditionTypeReady,
					"Disk %v on node %v is ready", disk.Path, node.Name)
			}
			readyCondition.Status = types.ConditionStatusTrue
			readyCondition.Reason = ""
			readyCondition.Message = ""
			diskStatus.StorageMaximum = diskInfo.StorageMaximum
			diskStatus.StorageAvailable = diskInfo.StorageAvailable
		}
		diskConditions[types.DiskConditionTypeReady] = readyCondition

		condition := types.GetDiskConditionFromStatus(diskStatus, types.DiskConditionTypeSchedulable)
		//condition.LastProbeTime = util.Now()
		// check disk pressure
		info, err := nc.scheduler.GetDiskSchedulingInfo(disk, diskStatus)
		if err != nil {
			return err
		}
		if !nc.scheduler.IsSchedulableToDisk(0, info) {
			if condition.Status != types.ConditionStatusFalse {
				condition.LastTransitionTime = util.Now()
				nc.eventRecorder.Eventf(node, v1.EventTypeWarning, types.DiskConditionReasonDiskPressure,
					"unable to schedule any replica to disk %v on node %v", disk.Path, node.Name)
			}
			condition.Status = types.ConditionStatusFalse
			condition.Reason = string(types.DiskConditionReasonDiskPressure)
			condition.Message = fmt.Sprintf("the disk %v on the node %v has %v available, but requires reserved %v, minimal %v%s to schedule more replicas", disk.Path, node.Name, diskStatus.StorageAvailable, disk.StorageReserved, minimalAvailablePercentage, "%")
		} else {
			if condition.Status != types.ConditionStatusTrue {
				condition.LastTransitionTime = util.Now()
				nc.eventRecorder.Eventf(node, v1.EventTypeNormal, types.DiskConditionTypeSchedulable,
					"Disk %v on node %v is schedulable", disk.Path, node.Name)
			}
			condition.Status = types.ConditionStatusTrue
			condition.Reason = ""
			condition.Message = ""
		}
		diskConditions[types.DiskConditionTypeSchedulable] = condition

		diskStatus.Conditions = diskConditions
		diskStatusMap[diskID] = diskStatus
		updateDiskMap[diskID] = updateDisk
	}

	// if there's some replicas scheduled to wrong disks, write them to error log
	if len(replicaDiskMap) > 0 {
		eReplicas := []string{}
		for _, replicas := range replicaDiskMap {
			for _, replica := range replicas {
				eReplicas = append(eReplicas, replica.Name)
			}
		}
		logrus.Errorf("Warning: These replicas have been assigned to a disk no longer exist: %v", strings.Join(eReplicas, ", "))
	}

	node.Status.DiskStatus = diskStatusMap
	node.Spec.Disks = updateDiskMap

	return nil
}

func (nc *NodeController) syncNodeStatus(pod *v1.Pod, node *longhorn.Node) error {
	// sync bidirectional mount propagation for node status to check whether the node could deploy CSI driver
	condition := types.GetNodeConditionFromStatus(node.Status, types.NodeConditionTypeMountPropagation)
	for _, mount := range pod.Spec.Containers[0].VolumeMounts {
		if mount.Name == types.LonghornSystemKey {
			mountPropagationStr := ""
			if mount.MountPropagation == nil {
				mountPropagationStr = "nil"
			} else {
				mountPropagationStr = string(*mount.MountPropagation)
			}
			if mount.MountPropagation == nil || *mount.MountPropagation != v1.MountPropagationBidirectional {
				if condition.Status != types.ConditionStatusFalse {
					condition.LastTransitionTime = util.Now()
				}
				condition.Status = types.ConditionStatusFalse
				condition.Reason = types.NodeConditionReasonNoMountPropagationSupport
				condition.Message = fmt.Sprintf("The MountPropagation value %s is not detected from pod %s, node %s", mountPropagationStr, pod.Name, pod.Spec.NodeName)
			} else {
				if condition.Status != types.ConditionStatusTrue {
					condition.LastTransitionTime = util.Now()
				}
				condition.Status = types.ConditionStatusTrue
				condition.Reason = ""
				condition.Message = ""
			}
			//condition.LastProbeTime = util.Now()
			break
		}
	}
	node.Status.Conditions[types.NodeConditionTypeMountPropagation] = condition

	return nil
}
