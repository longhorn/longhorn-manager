package controller

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
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
	topologyLabelsChecker              TopologyLabelsChecker

	scheduler *scheduler.ReplicaScheduler
}

type GetDiskInfoHandler func(string) (*util.DiskInfo, error)
type DiskPathReplicaSubdirectoryChecker func(string) (bool, error)
type TopologyLabelsChecker func(kubeClient clientset.Interface, vers string) (bool, error)

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
		topologyLabelsChecker:              util.IsKubernetesVersionAtLeast,
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
					return nc.filterSettings(t)
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

func (nc *NodeController) filterSettings(s *longhorn.Setting) bool {
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
			logrus.Errorf("Longhorn node %v has been deleted", key)
			return nil
		}
		return err
	}

	if node.DeletionTimestamp != nil {
		nc.eventRecorder.Eventf(node, v1.EventTypeWarning, EventReasonDelete, "Deleting node %v", node.Name)
		return nc.ds.RemoveFinalizerForNode(node)
	}

	existingNode := node.DeepCopy()
	defer func() {
		// we're going to update volume assume things changes
		if err == nil && !reflect.DeepEqual(existingNode.Status, node.Status) {
			_, err = nc.ds.UpdateNodeStatus(node)
		}
		// requeue if it's conflict
		if apierrors.IsConflict(errors.Cause(err)) {
			logrus.Debugf("Requeue %v due to conflict: %v", key, err)
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
			podConditions := pod.Status.Conditions
			for _, podCondition := range podConditions {
				if podCondition.Type == v1.PodReady {
					if podCondition.Status == v1.ConditionTrue && pod.Status.Phase == v1.PodRunning {
						types.SetConditionAndRecord(node.Status.Conditions, types.NodeConditionTypeReady, types.ConditionStatusTrue,
							"", fmt.Sprintf("Node %v is ready", node.Name),
							nc.eventRecorder, node, v1.EventTypeNormal)
					} else {
						types.SetConditionAndRecord(node.Status.Conditions, types.NodeConditionTypeReady, types.ConditionStatusFalse,
							string(types.NodeConditionReasonManagerPodDown),
							fmt.Sprintf("Node %v is down: the manager pod %v is not running", node.Name, pod.Name),
							nc.eventRecorder, node, v1.EventTypeWarning)
					}
					break
				}
			}
			break
		}
	}

	if !nodeManagerFound {
		types.SetConditionAndRecord(node.Status.Conditions, types.NodeConditionTypeReady, types.ConditionStatusFalse,
			string(types.NodeConditionReasonManagerPodMissing),
			fmt.Sprintf("manager pod missing: node %v has no manager pod running on it", node.Name),
			nc.eventRecorder, node, v1.EventTypeWarning)
	}

	// sync node state with kuberentes node status
	kubeNode, err := nc.ds.GetKubernetesNode(name)
	if err != nil {
		// if kubernetes node has been removed from cluster
		if apierrors.IsNotFound(err) {
			types.SetConditionAndRecord(node.Status.Conditions, types.NodeConditionTypeReady, types.ConditionStatusFalse,
				string(types.NodeConditionReasonKubernetesNodeGone),
				fmt.Sprintf("Kubernetes node missing: node %v has been removed from the cluster and there is no manager pod running on it", node.Name),
				nc.eventRecorder, node, v1.EventTypeWarning)
		} else {
			return err
		}
	} else {
		kubeConditions := kubeNode.Status.Conditions
		for _, con := range kubeConditions {
			switch con.Type {
			case v1.NodeReady:
				if con.Status != v1.ConditionTrue {
					types.SetConditionAndRecord(node.Status.Conditions, types.NodeConditionTypeReady, types.ConditionStatusFalse,
						string(types.NodeConditionReasonKubernetesNodeNotReady),
						fmt.Sprintf("Kubernetes node %v not ready: %v", node.Name, con.Reason),
						nc.eventRecorder, node, v1.EventTypeWarning)
					break
				}
			case v1.NodeOutOfDisk,
				v1.NodeDiskPressure,
				v1.NodePIDPressure,
				v1.NodeMemoryPressure,
				v1.NodeNetworkUnavailable:
				if con.Status == v1.ConditionTrue {
					types.SetConditionAndRecord(node.Status.Conditions, types.NodeConditionTypeReady, types.ConditionStatusFalse,
						string(types.NodeConditionReasonKubernetesNodePressure),
						fmt.Sprintf("Kubernetes node %v has pressure: %v, %v", node.Name, con.Reason, con.Message),
						nc.eventRecorder, node, v1.EventTypeWarning)

					break
				}
			default:
				if con.Status == v1.ConditionTrue {
					nc.eventRecorder.Eventf(node, v1.EventTypeWarning, types.NodeConditionReasonUnknownNodeConditionTrue, "Unknown condition true of kubernetes node %v: condition type is %v, reason is %v, message is %v", node.Name, con.Type, con.Reason, con.Message)
				}
				break
			}
		}

		isUsingTopologyLabels, err := nc.topologyLabelsChecker(nc.kubeClient, types.KubernetesTopologyLabelsVersion)
		if err != nil {
			return err
		}
		node.Status.Region, node.Status.Zone = types.GetRegionAndZone(kubeNode.Labels, isUsingTopologyLabels)

	}

	if nc.controllerID != node.Name {
		return nil
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

	if err := nc.syncInstanceManagers(node); err != nil {
		return err
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

	originDiskStatus := node.Status.DiskStatus
	if originDiskStatus == nil {
		originDiskStatus = map[string]types.DiskStatus{}
	}
	for diskID, disk := range diskMap {
		diskStatus := types.DiskStatus{}
		_, ok := originDiskStatus[diskID]
		if ok {
			diskStatus = originDiskStatus[diskID]
		}
		if diskStatus.Conditions == nil {
			diskStatus.Conditions = map[string]types.Condition{}
		}

		// calculate storage scheduled
		scheduledReplica := map[string]int64{}
		storageScheduled := int64(0)
		for _, replica := range replicaDiskMap[diskID] {
			storageScheduled += replica.Spec.VolumeSize
			scheduledReplica[replica.Name] = replica.Spec.VolumeSize
		}
		diskStatus.StorageScheduled = storageScheduled
		diskStatus.ScheduledReplica = scheduledReplica
		diskStatus.StorageMaximum = 0
		diskStatus.StorageAvailable = 0
		delete(replicaDiskMap, diskID)

		// get disk stat
		diskInfo, err := nc.getDiskInfoHandler(disk.Path)
		if err != nil {
			types.SetConditionAndRecord(diskStatus.Conditions, types.DiskConditionTypeReady, types.ConditionStatusFalse,
				string(types.DiskConditionReasonNoDiskInfo),
				fmt.Sprintf("Disk %v on node %v is not ready: Get disk information error: %v", disk.Path, node.Name, err),
				nc.eventRecorder, node, v1.EventTypeWarning)
		} else {
			if diskInfo.Fsid != diskID {
				types.SetConditionAndRecord(diskStatus.Conditions, types.DiskConditionTypeReady, types.ConditionStatusFalse,
					string(types.DiskConditionReasonDiskFilesystemChanged),
					fmt.Sprintf("Disk %v on node %v is not ready: disk has changed file system ID to %v", disk.Path, node.Name, diskInfo.Fsid),
					nc.eventRecorder, node, v1.EventTypeWarning)
			} else {
				diskStatus.StorageMaximum = diskInfo.StorageMaximum
				diskStatus.StorageAvailable = diskInfo.StorageAvailable
				types.SetConditionAndRecord(diskStatus.Conditions, types.DiskConditionTypeReady, types.ConditionStatusTrue,
					"", fmt.Sprintf("Disk %v on node %v is ready", disk.Path, node.Name),
					nc.eventRecorder, node, v1.EventTypeNormal)
			}
		}

		// check disk pressure
		info, err := nc.scheduler.GetDiskSchedulingInfo(disk, diskStatus)
		if err != nil {
			return err
		}
		if !nc.scheduler.IsSchedulableToDisk(0, info) {
			types.SetConditionAndRecord(diskStatus.Conditions, types.DiskConditionTypeSchedulable, types.ConditionStatusFalse,
				string(types.DiskConditionReasonDiskPressure),
				fmt.Sprintf("the disk %v on the node %v has %v available, but requires reserved %v, minimal %v%s to schedule more replicas",
					disk.Path, node.Name, diskStatus.StorageAvailable, disk.StorageReserved, minimalAvailablePercentage, "%"),
				nc.eventRecorder, node, v1.EventTypeWarning)

		} else {
			types.SetConditionAndRecord(diskStatus.Conditions, types.DiskConditionTypeSchedulable, types.ConditionStatusTrue,
				"", fmt.Sprintf("Disk %v on node %v is schedulable", disk.Path, node.Name),
				nc.eventRecorder, node, v1.EventTypeNormal)
		}

		diskStatusMap[diskID] = diskStatus
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

	return nil
}

func (nc *NodeController) syncNodeStatus(pod *v1.Pod, node *longhorn.Node) error {
	// sync bidirectional mount propagation for node status to check whether the node could deploy CSI driver
	for _, mount := range pod.Spec.Containers[0].VolumeMounts {
		if mount.Name == types.LonghornSystemKey {
			mountPropagationStr := ""
			if mount.MountPropagation == nil {
				mountPropagationStr = "nil"
			} else {
				mountPropagationStr = string(*mount.MountPropagation)
			}
			if mount.MountPropagation == nil || *mount.MountPropagation != v1.MountPropagationBidirectional {
				types.SetCondition(node.Status.Conditions, types.NodeConditionTypeMountPropagation, types.ConditionStatusFalse,
					string(types.NodeConditionReasonNoMountPropagationSupport),
					fmt.Sprintf("The MountPropagation value %s is not detected from pod %s, node %s", mountPropagationStr, pod.Name, pod.Spec.NodeName))
			} else {
				types.SetCondition(node.Status.Conditions, types.NodeConditionTypeMountPropagation, types.ConditionStatusTrue, "", "")
			}
			break
		}
	}

	return nil
}

func (nc *NodeController) syncInstanceManagers(node *longhorn.Node) error {
	defaultInstanceManagerImage, err := nc.ds.GetSettingValueExisted(types.SettingNameDefaultInstanceManagerImage)
	if err != nil {
		return err
	}

	imTypes := []types.InstanceManagerType{types.InstanceManagerTypeEngine}

	// Clean up all replica managers if there is no disk on the node
	if len(node.Spec.Disks) == 0 {
		rmMap, err := nc.ds.ListInstanceManagersByNode(node.Name, types.InstanceManagerTypeReplica)
		if err != nil {
			return err
		}
		for _, rm := range rmMap {
			logrus.Debugf("Prepare to clean up the replica manager %v since there is no available disk on node %v", rm.Name, node.Name)
			if err := nc.ds.DeleteInstanceManager(rm.Name); err != nil {
				return err
			}
		}
	} else {
		imTypes = append(imTypes, types.InstanceManagerTypeReplica)
	}

	for _, imType := range imTypes {
		defaultInstanceManagerCreated := false
		imMap, err := nc.ds.ListInstanceManagersByNode(node.Name, imType)
		if err != nil {
			return err
		}
		for _, im := range imMap {
			if im.Labels[types.GetLonghornLabelKey(types.LonghornLabelNode)] != im.Spec.NodeID {
				return fmt.Errorf("BUG: Instance manager %v NodeID %v is not consistent with the label %v=%v",
					im.Name, im.Spec.NodeID, types.GetLonghornLabelKey(types.LonghornLabelNode), im.Labels[types.GetLonghornLabelKey(types.LonghornLabelNode)])
			}
			cleanupRequired := true
			if im.Spec.Image == defaultInstanceManagerImage {
				// Create default instance manager if needed.
				defaultInstanceManagerCreated = true
				cleanupRequired = false
			} else {
				// Clean up old instance managers if there is no running instance.
				if im.Status.CurrentState == types.InstanceManagerStateRunning && im.DeletionTimestamp == nil {
					for _, instance := range im.Status.Instances {
						if instance.Status.State == types.InstanceStateRunning || instance.Status.State == types.InstanceStateStarting {
							cleanupRequired = false
							break
						}
					}
				}
			}
			if cleanupRequired {
				logrus.Debugf("Prepare to clean up the redundant instance manager %v when there is no running/starting instance", im.Name)
				if err := nc.ds.DeleteInstanceManager(im.Name); err != nil {
					return err
				}
			}
		}
		if !defaultInstanceManagerCreated {
			imName, err := types.GetInstanceManagerName(imType)
			if err != nil {
				return err
			}
			logrus.Debugf("Prepare to create default instance manager %v, node: %v, default instance manager image: %v, type: %v",
				imName, node.Name, defaultInstanceManagerImage, imType)
			if _, err := nc.createInstanceManager(node, imName, defaultInstanceManagerImage, imType); err != nil {
				return err
			}
		}
	}
	return nil
}

func (nc *NodeController) createInstanceManager(node *longhorn.Node, imName, image string, imType types.InstanceManagerType) (*longhorn.InstanceManager, error) {
	instanceManager := &longhorn.InstanceManager{
		ObjectMeta: metav1.ObjectMeta{
			Labels:          types.GetInstanceManagerLabels(node.Name, image, imType),
			Name:            imName,
			OwnerReferences: datastore.GetOwnerReferencesForNode(node),
		},
		Spec: types.InstanceManagerSpec{
			Image:  image,
			NodeID: node.Name,
			Type:   imType,
		},
	}

	return nc.ds.CreateInstanceManager(instanceManager)
}
