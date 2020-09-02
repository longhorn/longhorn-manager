package controller

import (
	"fmt"

	"github.com/sirupsen/logrus"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

const (
	ManagerPodUp     = "managerPodUp"
	ManagerPodDown   = "managerPodDown"
	KubeNodeDown     = "kubeNodeDown"
	KubeNodePressure = "kubeNodePressure"
)

var MountPropagationBidirectional = v1.MountPropagationBidirectional

type NodeTestCase struct {
	nodes           map[string]*longhorn.Node
	disks           map[string]*longhorn.Disk
	pods            map[string]*v1.Pod
	replicas        []*longhorn.Replica
	kubeNodes       map[string]*v1.Node
	engineManagers  map[string]*longhorn.InstanceManager
	replicaManagers map[string]*longhorn.InstanceManager

	expectNodeStatus      map[string]types.NodeStatus
	expectEngineManagers  map[string]*longhorn.InstanceManager
	expectReplicaManagers map[string]*longhorn.InstanceManager
	expectDisks           map[string]*longhorn.Disk
}

func newTestNodeController(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset, controllerID string) *NodeController {
	volumeInformer := lhInformerFactory.Longhorn().V1beta1().Volumes()
	engineInformer := lhInformerFactory.Longhorn().V1beta1().Engines()
	replicaInformer := lhInformerFactory.Longhorn().V1beta1().Replicas()
	engineImageInformer := lhInformerFactory.Longhorn().V1beta1().EngineImages()
	nodeInformer := lhInformerFactory.Longhorn().V1beta1().Nodes()
	diskInformer := lhInformerFactory.Longhorn().V1beta1().Disks()
	settingInformer := lhInformerFactory.Longhorn().V1beta1().Settings()
	imInformer := lhInformerFactory.Longhorn().V1beta1().InstanceManagers()

	podInformer := kubeInformerFactory.Core().V1().Pods()
	kubeNodeInformer := kubeInformerFactory.Core().V1().Nodes()
	cronJobInformer := kubeInformerFactory.Batch().V1beta1().CronJobs()
	daemonSetInformer := kubeInformerFactory.Apps().V1().DaemonSets()
	deploymentInformer := kubeInformerFactory.Apps().V1().Deployments()
	persistentVolumeInformer := kubeInformerFactory.Core().V1().PersistentVolumes()
	persistentVolumeClaimInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()
	priorityClassInformer := kubeInformerFactory.Scheduling().V1().PriorityClasses()
	csiDriverInformer := kubeInformerFactory.Storage().V1beta1().CSIDrivers()
	pdbInformer := kubeInformerFactory.Policy().V1beta1().PodDisruptionBudgets()

	ds := datastore.NewDataStore(
		volumeInformer, engineInformer, replicaInformer,
		engineImageInformer, nodeInformer, diskInformer, settingInformer, imInformer,
		lhClient,
		podInformer, cronJobInformer, daemonSetInformer,
		deploymentInformer, persistentVolumeInformer,
		persistentVolumeClaimInformer, kubeNodeInformer, priorityClassInformer,
		csiDriverInformer,
		pdbInformer,
		kubeClient, TestNamespace)

	logger := logrus.StandardLogger()
	nc := NewNodeController(logger,
		ds, scheme.Scheme,
		nodeInformer, settingInformer, podInformer, kubeNodeInformer,
		kubeClient, TestNamespace, controllerID)
	fakeRecorder := record.NewFakeRecorder(100)
	nc.eventRecorder = fakeRecorder
	nc.topologyLabelsChecker = fakeTopologyLabelsChecker
	nc.getDiskConfig = fakeGetDiskConfig
	nc.generateDiskConfig = fakeGenerateDiskConfig

	nc.nStoreSynced = alwaysReady
	nc.pStoreSynced = alwaysReady

	return nc
}

func fakeTopologyLabelsChecker(kubeClient clientset.Interface, vers string) (bool, error) {
	return false, nil
}

func generateKubeNodes(testType string) map[string]*v1.Node {
	var kubeNode1, kubeNode2 *v1.Node
	switch testType {
	case KubeNodeDown:
		kubeNode1 = newKubernetesNode(TestNode1, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionTrue)
		kubeNode2 = newKubernetesNode(TestNode2, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionTrue)
	case KubeNodePressure:
		kubeNode1 = newKubernetesNode(TestNode1, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionTrue)
		kubeNode2 = newKubernetesNode(TestNode2, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionTrue)
	default:
		kubeNode1 = newKubernetesNode(TestNode1, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionTrue)
		kubeNode2 = newKubernetesNode(TestNode2, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionTrue)
	}
	return map[string]*v1.Node{
		TestNode1: kubeNode1,
		TestNode2: kubeNode2,
	}
}

func generateManagerPod(testType string) map[string]*v1.Pod {
	var daemon1, daemon2 *v1.Pod
	switch testType {
	case ManagerPodDown:
		daemon1 = newDaemonPod(v1.PodFailed, TestDaemon1, TestNamespace, TestNode1, TestIP1, nil)
		daemon2 = newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, nil)
	default:
		daemon1 = newDaemonPod(v1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional)
		daemon2 = newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional)
	}
	return map[string]*v1.Pod{
		TestDaemon1: daemon1,
		TestDaemon2: daemon2,
	}
}

func kubeObjStatusSyncTest(testType string) *NodeTestCase {
	tc := &NodeTestCase{}
	tc.kubeNodes = generateKubeNodes(testType)
	node1 := newNode(TestNode1, TestNamespace, TestDefaultDiskUUID1, true, types.ConditionStatusUnknown, "")
	node2 := newNode(TestNode2, TestNamespace, TestDefaultDiskUUID2, true, types.ConditionStatusUnknown, "")
	nodes := map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	tc.disks = map[string]*longhorn.Disk{
		TestDefaultDiskUUID1: newDisk(TestDefaultDiskUUID1, TestNamespace, TestNode1),
		TestDefaultDiskUUID2: newDisk(TestDefaultDiskUUID2, TestNamespace, TestNode2),
	}
	nodeStatus := map[string]types.NodeStatus{}
	switch testType {
	case ManagerPodUp:
		nodeStatus = map[string]types.NodeStatus{
			TestNode1: {
				Conditions: map[string]types.Condition{
					types.NodeConditionTypeSchedulable:      newNodeCondition(types.NodeConditionTypeSchedulable, types.ConditionStatusTrue, ""),
					types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
					types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusTrue, ""),
				},
			},
			TestNode2: {
				Conditions: map[string]types.Condition{
					types.NodeConditionTypeSchedulable: newNodeCondition(types.NodeConditionTypeSchedulable, types.ConditionStatusTrue, ""),
					types.NodeConditionTypeReady:       newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				},
			},
		}
	case ManagerPodDown:
		nodeStatus = map[string]types.NodeStatus{
			TestNode1: {
				Conditions: map[string]types.Condition{
					types.NodeConditionTypeSchedulable:      newNodeCondition(types.NodeConditionTypeSchedulable, types.ConditionStatusTrue, ""),
					types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusFalse, types.NodeConditionReasonManagerPodDown),
					types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusFalse, types.NodeConditionReasonNoMountPropagationSupport),
				},
			},
			TestNode2: {
				Conditions: map[string]types.Condition{
					types.NodeConditionTypeSchedulable: newNodeCondition(types.NodeConditionTypeSchedulable, types.ConditionStatusTrue, ""),
					types.NodeConditionTypeReady:       newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				},
			},
		}
	case KubeNodeDown:
		nodeStatus = map[string]types.NodeStatus{
			TestNode1: {
				Conditions: map[string]types.Condition{
					types.NodeConditionTypeSchedulable:      newNodeCondition(types.NodeConditionTypeSchedulable, types.ConditionStatusTrue, ""),
					types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusFalse, types.NodeConditionReasonKubernetesNodeNotReady),
					types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusTrue, ""),
				},
			},
			TestNode2: {
				Conditions: map[string]types.Condition{
					types.NodeConditionTypeSchedulable: newNodeCondition(types.NodeConditionTypeSchedulable, types.ConditionStatusTrue, ""),
					types.NodeConditionTypeReady:       newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				},
			},
		}
	case KubeNodePressure:
		nodeStatus = map[string]types.NodeStatus{
			TestNode1: {
				Conditions: map[string]types.Condition{
					types.NodeConditionTypeSchedulable:      newNodeCondition(types.NodeConditionTypeSchedulable, types.ConditionStatusTrue, ""),
					types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusFalse, types.NodeConditionReasonKubernetesNodePressure),
					types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusTrue, ""),
				},
			},
			TestNode2: {
				Conditions: map[string]types.Condition{
					types.NodeConditionTypeSchedulable: newNodeCondition(types.NodeConditionTypeSchedulable, types.ConditionStatusTrue, ""),
					types.NodeConditionTypeReady:       newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				},
			},
		}
	}
	tc.pods = generateManagerPod(testType)

	tc.expectNodeStatus = nodeStatus

	return tc
}

func (s *TestSuite) TestSyncNode(c *C) {
	testCases := map[string]*NodeTestCase{}
	testCases["manager pod up"] = kubeObjStatusSyncTest(ManagerPodUp)
	testCases["manager pod down"] = kubeObjStatusSyncTest(ManagerPodDown)
	testCases["kubernetes node down"] = kubeObjStatusSyncTest(KubeNodeDown)
	testCases["kubernetes node pressure"] = kubeObjStatusSyncTest(KubeNodePressure)

	tc := &NodeTestCase{}
	tc.kubeNodes = generateKubeNodes(ManagerPodUp)
	tc.pods = generateManagerPod(ManagerPodUp)
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: newNode(TestNode1, TestNamespace, TestDefaultDiskUUID1, true, types.ConditionStatusTrue, ""),
	}
	tc.disks = map[string]*longhorn.Disk{
		TestDefaultDiskUUID1: newDisk(TestDefaultDiskUUID1, TestNamespace, TestNode1),
	}
	tc.expectNodeStatus = map[string]types.NodeStatus{
		TestNode1: {
			Conditions: map[string]types.Condition{
				types.NodeConditionTypeSchedulable:      newNodeCondition(types.NodeConditionTypeSchedulable, types.ConditionStatusTrue, ""),
				types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusTrue, ""),
			},
		},
	}
	tc.expectEngineManagers = map[string]*longhorn.InstanceManager{
		TestEngineManagerName: newInstanceManager(TestEngineManagerName, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
	}
	tc.expectReplicaManagers = map[string]*longhorn.InstanceManager{
		TestReplicaManagerName: newInstanceManager(TestReplicaManagerName, types.InstanceManagerTypeReplica, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
	}
	testCases["create default instance managers after node up"] = tc

	tc = &NodeTestCase{}
	tc.kubeNodes = generateKubeNodes(ManagerPodUp)
	tc.pods = generateManagerPod(ManagerPodUp)
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: newNode(TestNode1, TestNamespace, TestDefaultDiskUUID1, true, types.ConditionStatusTrue, ""),
	}
	tc.disks = map[string]*longhorn.Disk{
		TestDefaultDiskUUID1: newDisk(TestDefaultDiskUUID1, TestNamespace, TestNode1),
	}
	tc.expectNodeStatus = map[string]types.NodeStatus{
		TestNode1: {
			Conditions: map[string]types.Condition{
				types.NodeConditionTypeSchedulable:      newNodeCondition(types.NodeConditionTypeSchedulable, types.ConditionStatusTrue, ""),
				types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusTrue, ""),
			},
		},
	}
	extraEngineManager := newInstanceManager("extra-engine-manger-name", types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
		map[string]types.InstanceProcess{
			ExistingInstance: {
				Spec: types.InstanceProcessSpec{
					Name: ExistingInstance,
				},
				Status: types.InstanceProcessStatus{
					State:     types.InstanceStateRunning,
					PortStart: TestPort1,
				},
			},
		}, false)
	extraEngineManager.Spec.Image = TestExtraInstanceManagerImage
	extraReplicaManager := newInstanceManager("extra-replica-manger-name", types.InstanceManagerTypeReplica, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false)
	extraReplicaManager.Spec.Image = TestExtraInstanceManagerImage
	tc.engineManagers = map[string]*longhorn.InstanceManager{
		TestEngineManagerName:      newInstanceManager(TestEngineManagerName, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
		"extra-engine-manger-name": extraEngineManager,
	}
	tc.replicaManagers = map[string]*longhorn.InstanceManager{
		TestReplicaManagerName:      newInstanceManager(TestReplicaManagerName, types.InstanceManagerTypeReplica, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
		"extra-replica-manger-name": extraReplicaManager,
	}
	tc.expectEngineManagers = map[string]*longhorn.InstanceManager{
		TestEngineManagerName:      newInstanceManager(TestEngineManagerName, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
		"extra-engine-manger-name": extraEngineManager,
	}
	tc.expectReplicaManagers = map[string]*longhorn.InstanceManager{
		TestReplicaManagerName: newInstanceManager(TestReplicaManagerName, types.InstanceManagerTypeReplica, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
	}
	testCases["clean up redundant instance managers only if there is no running instances"] = tc

	tc = &NodeTestCase{}
	tc.kubeNodes = generateKubeNodes(ManagerPodUp)
	tc.pods = generateManagerPod(ManagerPodUp)
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: newNode(TestNode1, TestNamespace, "", false, types.ConditionStatusTrue, ""),
	}
	tc.disks = map[string]*longhorn.Disk{}
	tc.expectNodeStatus = map[string]types.NodeStatus{
		TestNode1: {
			Conditions: map[string]types.Condition{
				types.NodeConditionTypeSchedulable:      newNodeCondition(types.NodeConditionTypeSchedulable, types.ConditionStatusTrue, ""),
				types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusTrue, ""),
			},
		},
	}
	extraReplicaManager = newInstanceManager("extra-replica-manger-name", types.InstanceManagerTypeReplica, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
		map[string]types.InstanceProcess{
			ExistingInstance: {
				Spec: types.InstanceProcessSpec{
					Name: ExistingInstance,
				},
				Status: types.InstanceProcessStatus{
					State:     types.InstanceStateRunning,
					PortStart: TestPort1,
				},
			},
		}, false)
	extraReplicaManager.Spec.Image = TestExtraInstanceManagerImage
	tc.engineManagers = map[string]*longhorn.InstanceManager{
		TestEngineManagerName: newInstanceManager(TestEngineManagerName, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
	}
	tc.replicaManagers = map[string]*longhorn.InstanceManager{
		TestReplicaManagerName:      newInstanceManager(TestReplicaManagerName, types.InstanceManagerTypeReplica, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
		"extra-replica-manger-name": extraReplicaManager,
	}
	tc.expectEngineManagers = map[string]*longhorn.InstanceManager{
		TestEngineManagerName: newInstanceManager(TestEngineManagerName, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
	}
	tc.expectReplicaManagers = map[string]*longhorn.InstanceManager{}
	testCases["clean up all replica managers if there is no disk on the node"] = tc

	tc = &NodeTestCase{}
	tc.engineManagers = map[string]*longhorn.InstanceManager{
		TestEngineManagerName: newInstanceManager(TestEngineManagerName, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
	}
	tc.kubeNodes = generateKubeNodes(ManagerPodUp)
	tc.pods = generateManagerPod(ManagerPodUp)
	node1 := newNode(TestNode1, TestNamespace, TestDefaultDiskUUID1, true, types.ConditionStatusTrue, "")
	node1.Status.DiskPathIDMap = map[string]string{}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: node1,
	}
	tc.disks = map[string]*longhorn.Disk{}
	tc.expectNodeStatus = map[string]types.NodeStatus{
		TestNode1: {
			DiskPathIDMap: map[string]string{
				TestDefaultDataPath: TestDefaultDiskUUID1,
			},
			Conditions: map[string]types.Condition{
				types.NodeConditionTypeSchedulable:      newNodeCondition(types.NodeConditionTypeSchedulable, types.ConditionStatusTrue, ""),
				types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusTrue, ""),
			},
		},
	}
	tc.expectEngineManagers = tc.engineManagers
	expectDisk := &longhorn.Disk{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestDefaultDiskUUID1,
			Namespace: TestNamespace,
		},
		Spec: types.DiskSpec{
			AllowScheduling:   true,
			EvictionRequested: false,
			StorageReserved:   0,
			Tags:              []string{},
		},
	}
	tc.expectDisks = map[string]*longhorn.Disk{
		TestDefaultDiskUUID1: expectDisk,
	}
	testCases["create a new disk"] = tc

	tc = &NodeTestCase{}
	tc.engineManagers = map[string]*longhorn.InstanceManager{
		TestEngineManagerName: newInstanceManager(TestEngineManagerName, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
	}
	tc.replicaManagers = map[string]*longhorn.InstanceManager{
		TestReplicaManagerName: newInstanceManager(TestReplicaManagerName, types.InstanceManagerTypeReplica, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
	}
	tc.kubeNodes = generateKubeNodes(ManagerPodUp)
	tc.pods = generateManagerPod(ManagerPodUp)
	node1 = newNode(TestNode1, TestNamespace, TestDefaultDiskUUID1, true, types.ConditionStatusTrue, "")
	node1.Spec.DiskPathMap = map[string]struct{}{}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: node1,
	}
	tc.disks = map[string]*longhorn.Disk{
		TestDefaultDiskUUID1: newDisk(TestDefaultDiskUUID1, TestNamespace, TestNode1),
	}
	tc.disks[TestDefaultDiskUUID1].Status.State = types.DiskStateDisconnected
	tc.expectNodeStatus = map[string]types.NodeStatus{
		TestNode1: {
			DiskPathIDMap: map[string]string{},
			Conditions: map[string]types.Condition{
				types.NodeConditionTypeSchedulable:      newNodeCondition(types.NodeConditionTypeSchedulable, types.ConditionStatusTrue, ""),
				types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusTrue, ""),
			},
		},
	}
	tc.expectEngineManagers = tc.engineManagers
	tc.expectReplicaManagers = map[string]*longhorn.InstanceManager{}
	tc.expectDisks = map[string]*longhorn.Disk{}
	testCases["delete a disk"] = tc

	tc = &NodeTestCase{}
	tc.engineManagers = map[string]*longhorn.InstanceManager{
		TestEngineManagerName: newInstanceManager(TestEngineManagerName, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
	}
	tc.kubeNodes = generateKubeNodes(ManagerPodUp)
	tc.pods = generateManagerPod(ManagerPodUp)
	node1 = newNode(TestNode1, TestNamespace, TestDefaultDiskUUID1, true, types.ConditionStatusTrue, "")
	node1.Status.DiskPathIDMap = map[string]string{}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: node1,
	}
	tc.disks = map[string]*longhorn.Disk{
		TestDefaultDiskUUID1: newDisk(TestDefaultDiskUUID1, TestNamespace, TestNode1),
	}
	tc.disks[TestDefaultDiskUUID1].Status.State = types.DiskStateDisconnected
	tc.expectNodeStatus = map[string]types.NodeStatus{
		TestNode1: {
			DiskPathIDMap: map[string]string{
				TestDefaultDataPath: TestDefaultDiskUUID1,
			},
			Conditions: map[string]types.Condition{
				types.NodeConditionTypeSchedulable:      newNodeCondition(types.NodeConditionTypeSchedulable, types.ConditionStatusTrue, ""),
				types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusTrue, ""),
			},
		},
	}
	tc.expectEngineManagers = tc.engineManagers
	tc.expectDisks = tc.disks
	testCases["reuse an existing disk"] = tc

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)
		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		nIndexer := lhInformerFactory.Longhorn().V1beta1().Nodes().Informer().GetIndexer()
		dIndexer := lhInformerFactory.Longhorn().V1beta1().Disks().Informer().GetIndexer()
		pIndexer := kubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

		rIndexer := lhInformerFactory.Longhorn().V1beta1().Replicas().Informer().GetIndexer()
		knIndexer := kubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

		sIndexer := lhInformerFactory.Longhorn().V1beta1().Settings().Informer().GetIndexer()
		imIndexer := lhInformerFactory.Longhorn().V1beta1().InstanceManagers().Informer().GetIndexer()

		imImageSetting := newDefaultInstanceManagerImageSetting()
		imImageSetting, err := lhClient.LonghornV1beta1().Settings(TestNamespace).Create(imImageSetting)
		c.Assert(err, IsNil)
		err = sIndexer.Add(imImageSetting)
		c.Assert(err, IsNil)

		// create kuberentes node
		for _, kubeNode := range tc.kubeNodes {
			n, err := kubeClient.CoreV1().Nodes().Create(kubeNode)
			c.Assert(err, IsNil)
			knIndexer.Add(n)
		}

		nc := newTestNodeController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient, TestNode1)
		// create manager pod
		for _, pod := range tc.pods {
			p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(pod)
			c.Assert(err, IsNil)
			pIndexer.Add(p)
		}
		// create node
		for _, node := range tc.nodes {
			n, err := lhClient.LonghornV1beta1().Nodes(TestNamespace).Create(node)
			c.Assert(err, IsNil)
			c.Assert(n, NotNil)
			nIndexer.Add(n)
		}
		// create disk
		for _, disk := range tc.disks {
			d, err := lhClient.LonghornV1beta1().Disks(TestNamespace).Create(disk)
			c.Assert(err, IsNil)
			c.Assert(d, NotNil)
			dIndexer.Add(d)
		}
		// create replicas
		for _, replica := range tc.replicas {
			r, err := lhClient.LonghornV1beta1().Replicas(TestNamespace).Create(replica)
			c.Assert(err, IsNil)
			c.Assert(r, NotNil)
			rIndexer.Add(r)
		}
		// create instance managers
		for _, em := range tc.engineManagers {
			em, err := lhClient.LonghornV1beta1().InstanceManagers(TestNamespace).Create(em)
			c.Assert(err, IsNil)
			err = imIndexer.Add(em)
			c.Assert(err, IsNil)
		}
		for _, rm := range tc.replicaManagers {
			rm, err := lhClient.LonghornV1beta1().InstanceManagers(TestNamespace).Create(rm)
			c.Assert(err, IsNil)
			err = imIndexer.Add(rm)
			c.Assert(err, IsNil)
		}
		// sync node status
		for nodeName, node := range tc.nodes {
			err := nc.syncNode(getKey(node, c))
			c.Assert(err, IsNil)

			n, err := lhClient.LonghornV1beta1().Nodes(TestNamespace).Get(node.Name, metav1.GetOptions{})
			c.Assert(err, IsNil)
			for ctype, condition := range n.Status.Conditions {
				condition.LastTransitionTime = ""
				condition.Message = ""
				n.Status.Conditions[ctype] = condition
			}
			c.Assert(n.Status.Conditions, DeepEquals, tc.expectNodeStatus[nodeName].Conditions)
		}

		for emName := range tc.engineManagers {
			em, err := lhClient.LonghornV1beta1().InstanceManagers(TestNamespace).Get(emName, metav1.GetOptions{})
			if expectEM, exist := tc.expectEngineManagers[emName]; !exist {
				c.Assert(datastore.ErrorIsNotFound(err), Equals, true)
			} else {
				c.Assert(em, NotNil)
				c.Assert(em.Spec, DeepEquals, expectEM.Spec)
			}
		}
		for rmName := range tc.replicaManagers {
			rm, err := lhClient.LonghornV1beta1().InstanceManagers(TestNamespace).Get(rmName, metav1.GetOptions{})
			if expectRM, exist := tc.expectReplicaManagers[rmName]; !exist {
				c.Assert(datastore.ErrorIsNotFound(err), Equals, true)
			} else {
				c.Assert(rm, NotNil)
				c.Assert(rm.Spec, DeepEquals, expectRM.Spec)
			}
		}

	}
}
