package controller

import (
	"context"
	"strings"

	"github.com/sirupsen/logrus"
	. "gopkg.in/check.v1"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	fake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	monitor "github.com/longhorn/longhorn-manager/controller/monitor"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	eventRecorderBufferSize = 100
)

var (
	MountPropagationBidirectional = corev1.MountPropagationBidirectional

	DefaultOrphanTestNode1 = newOrphan(
		longhorn.OrphanSpec{
			NodeID: TestNode1,
			Type:   longhorn.OrphanTypeReplica,
			Parameters: map[string]string{
				longhorn.OrphanDataName: monitor.TestOrphanedReplicaDirectoryName,
				longhorn.OrphanDiskName: TestDiskID1,
				longhorn.OrphanDiskUUID: TestDiskID1,
				longhorn.OrphanDiskPath: TestDefaultDataPath,
			},
		},
		longhorn.OrphanStatus{
			OwnerID: TestNode1,
		},
	)

	DefaultInstanceManagerTestNode1 = newInstanceManager(
		TestInstanceManagerName,
		longhorn.InstanceManagerStateRunning,
		TestOwnerID1, TestNode1, TestIP1,
		map[string]longhorn.InstanceProcess{},
		map[string]longhorn.InstanceProcess{},
		longhorn.DataEngineTypeV1,
		false,
	)
)

// This data type contains all necessary fixtures for the test, like the mock
// API clients
type NodeControllerSuite struct {
	kubeClient       *fake.Clientset
	lhClient         *lhfake.Clientset
	extensionsClient *apiextensionsfake.Clientset

	informerFactories *util.InformerFactories

	lhNodeIndexer            cache.Indexer
	lhReplicaIndexer         cache.Indexer
	lhSettingsIndexer        cache.Indexer
	lhInstanceManagerIndexer cache.Indexer
	lhOrphanIndexer          cache.Indexer

	podIndexer  cache.Indexer
	nodeIndexer cache.Indexer

	eventRecorder *record.FakeRecorder

	controller *NodeController
}

// This data type contains resource that exist in the cluster environment, like
// nodes and pods
type NodeControllerFixture struct {
	lhNodes            map[string]*longhorn.Node
	lhReplicas         []*longhorn.Replica
	lhSettings         map[string]*longhorn.Setting
	lhInstanceManagers map[string]*longhorn.InstanceManager
	lhOrphans          map[string]*longhorn.Orphan
	pods               map[string]*corev1.Pod
	nodes              map[string]*corev1.Node
}

// This data type contains expected results in the form of resources. Each test
// will set up fixtures, input resources and expected results. Then it will
// initialize the mock API with initTest, execute a function of the controller
// and finally execute a set of assertions, which compare the actual contents of
// the mock API to the expected results
type NodeControllerExpectation struct {
	nodeStatus       map[string]*longhorn.NodeStatus
	instanceManagers map[string]*longhorn.InstanceManager
	orphans          map[string]*longhorn.Orphan
	events           map[string]*corev1.Event
}

var _ = Suite(&NodeControllerSuite{})

// This is setting up the NodeControllerSuite datastructure as a fixture. It is
// executed once before each test
func (s *NodeControllerSuite) SetUpTest(c *C) {
	s.kubeClient = fake.NewSimpleClientset()
	s.lhClient = lhfake.NewSimpleClientset()
	s.extensionsClient = apiextensionsfake.NewSimpleClientset()

	s.informerFactories = util.NewInformerFactories(TestNamespace, s.kubeClient, s.lhClient, controller.NoResyncPeriodFunc())

	s.lhNodeIndexer = s.informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	s.lhReplicaIndexer = s.informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
	s.lhSettingsIndexer = s.informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	s.lhInstanceManagerIndexer = s.informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	s.lhOrphanIndexer = s.informerFactories.LhInformerFactory.Longhorn().V1beta2().Orphans().Informer().GetIndexer()

	s.podIndexer = s.informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	s.nodeIndexer = s.informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

	s.eventRecorder = record.NewFakeRecorder(eventRecorderBufferSize)

	var err error
	s.controller, err = newTestNodeController(s.lhClient, s.kubeClient, s.extensionsClient, s.informerFactories, s.eventRecorder, TestNode1)
	c.Assert(err, IsNil)
}

func (s *NodeControllerSuite) TestManagerPodUp(c *C) {
	var err error

	fixture := &NodeControllerFixture{
		lhNodes: map[string]*longhorn.Node{
			TestNode1: newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
			TestNode2: newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
		},
		lhSettings: map[string]*longhorn.Setting{
			string(types.SettingNameDefaultInstanceManagerImage): newDefaultInstanceManagerImageSetting(),
		},
		lhInstanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: DefaultInstanceManagerTestNode1,
		},
		lhOrphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
		pods: map[string]*corev1.Pod{
			TestDaemon1: newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional),
			TestDaemon2: newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional),
		},
		nodes: map[string]*corev1.Node{
			TestNode1: newKubernetesNode(
				TestNode1,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
			TestNode2: newKubernetesNode(
				TestNode2,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
		},
	}
	expectation := &NodeControllerExpectation{
		nodeStatus: map[string]*longhorn.NodeStatus{
			TestNode1: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeMountPropagation, longhorn.ConditionStatusTrue, ""),
				},
			},
			TestNode2: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue, ""),
				},
			},
		},
		orphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
	}

	s.initTest(c, fixture)

	for _, node := range fixture.lhNodes {
		if s.controller.controllerID == node.Name {
			err = s.controller.diskMonitor.RunOnce()
			c.Assert(err, IsNil)
		}

		err = s.controller.syncNode(getKey(node, c))
		c.Assert(err, IsNil)

		n, err := s.lhClient.LonghornV1beta2().Nodes(TestNamespace).Get(context.TODO(), node.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		s.checkNodeConditions(c, expectation, n)
	}

	s.checkOrphans(c, expectation)
}

func (s *NodeControllerSuite) TestManagerPodDown(c *C) {
	var err error

	fixture := &NodeControllerFixture{
		lhNodes: map[string]*longhorn.Node{
			TestNode1: newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
			TestNode2: newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
		},
		lhSettings: map[string]*longhorn.Setting{
			string(types.SettingNameDefaultInstanceManagerImage): newDefaultInstanceManagerImageSetting(),
		},
		lhInstanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: DefaultInstanceManagerTestNode1,
		},
		lhOrphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
		pods: map[string]*corev1.Pod{
			TestDaemon1: newDaemonPod(corev1.PodFailed, TestDaemon1, TestNamespace, TestNode1, TestIP1, nil),
			TestDaemon2: newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, nil),
		},
		nodes: map[string]*corev1.Node{
			TestNode1: newKubernetesNode(
				TestNode1,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
			TestNode2: newKubernetesNode(
				TestNode2,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
		},
	}

	expectation := &NodeControllerExpectation{
		nodeStatus: map[string]*longhorn.NodeStatus{
			TestNode1: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusFalse, longhorn.NodeConditionReasonManagerPodDown),
					newNodeCondition(longhorn.NodeConditionTypeMountPropagation, longhorn.ConditionStatusFalse, longhorn.NodeConditionReasonNoMountPropagationSupport),
				},
			},
			TestNode2: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue, ""),
				},
			},
		},
		orphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
	}

	s.initTest(c, fixture)

	for _, node := range fixture.lhNodes {
		if s.controller.controllerID == node.Name {
			err = s.controller.diskMonitor.RunOnce()
			c.Assert(err, IsNil)
		}

		err = s.controller.syncNode(getKey(node, c))
		c.Assert(err, IsNil)

		n, err := s.lhClient.LonghornV1beta2().Nodes(TestNamespace).Get(context.TODO(), node.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		s.checkNodeConditions(c, expectation, n)
	}

	s.checkOrphans(c, expectation)
}

func (s *NodeControllerSuite) TestKubeNodeDown(c *C) {
	var err error

	fixture := &NodeControllerFixture{
		lhNodes: map[string]*longhorn.Node{
			TestNode1: newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
			TestNode2: newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
		},
		lhSettings: map[string]*longhorn.Setting{
			string(types.SettingNameDefaultInstanceManagerImage): newDefaultInstanceManagerImageSetting(),
		},
		lhInstanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: DefaultInstanceManagerTestNode1,
		},
		lhOrphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
		pods: map[string]*corev1.Pod{
			TestDaemon1: newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional),
			TestDaemon2: newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional),
		},
		nodes: map[string]*corev1.Node{
			TestNode1: newKubernetesNode(
				TestNode1,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
			TestNode2: newKubernetesNode(
				TestNode2,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
		},
	}

	expectation := &NodeControllerExpectation{
		nodeStatus: map[string]*longhorn.NodeStatus{
			TestNode1: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusFalse, longhorn.NodeConditionReasonKubernetesNodeNotReady),
					newNodeCondition(longhorn.NodeConditionTypeMountPropagation, longhorn.ConditionStatusTrue, ""),
				},
			},
			TestNode2: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue, ""),
				},
			},
		},
		orphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
	}

	s.initTest(c, fixture)

	for _, node := range fixture.lhNodes {
		if s.controller.controllerID == node.Name {
			err = s.controller.diskMonitor.RunOnce()
			c.Assert(err, IsNil)
		}

		err = s.controller.syncNode(getKey(node, c))
		c.Assert(err, IsNil)

		n, err := s.lhClient.LonghornV1beta2().Nodes(TestNamespace).Get(context.TODO(), node.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		s.checkNodeConditions(c, expectation, n)
	}

	s.checkOrphans(c, expectation)
}

func (s *NodeControllerSuite) TestKubeNodePressure(c *C) {
	var err error

	fixture := &NodeControllerFixture{
		lhNodes: map[string]*longhorn.Node{
			TestNode1: newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
			TestNode2: newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
		},
		lhSettings: map[string]*longhorn.Setting{
			string(types.SettingNameDefaultInstanceManagerImage): newDefaultInstanceManagerImageSetting(),
		},
		lhInstanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: DefaultInstanceManagerTestNode1,
		},
		lhOrphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
		pods: map[string]*corev1.Pod{
			TestDaemon1: newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional),
			TestDaemon2: newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional),
		},
		nodes: map[string]*corev1.Node{
			TestNode1: newKubernetesNode(
				TestNode1,
				corev1.ConditionTrue,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
			TestNode2: newKubernetesNode(
				TestNode2,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
		},
	}

	expectation := &NodeControllerExpectation{
		nodeStatus: map[string]*longhorn.NodeStatus{
			TestNode1: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusFalse, longhorn.NodeConditionReasonKubernetesNodePressure),
					newNodeCondition(longhorn.NodeConditionTypeMountPropagation, longhorn.ConditionStatusTrue, ""),
				},
			},
			TestNode2: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue, ""),
				},
			},
		},
		orphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
	}

	s.initTest(c, fixture)

	for _, node := range fixture.lhNodes {
		if s.controller.controllerID == node.Name {
			err = s.controller.diskMonitor.RunOnce()
			c.Assert(err, IsNil)
		}

		err = s.controller.syncNode(getKey(node, c))
		c.Assert(err, IsNil)

		n, err := s.lhClient.LonghornV1beta2().Nodes(TestNamespace).Get(context.TODO(), node.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		s.checkNodeConditions(c, expectation, n)
	}

	s.checkOrphans(c, expectation)
}

func (s *NodeControllerSuite) TestUpdateDiskStatus(c *C) {
	var err error

	node1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, "")
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		TestDiskID1: {
			StorageScheduled:    0,
			StorageAvailable:    0,
			Type:                longhorn.DiskTypeFilesystem,
			FSType:              TestDiskPathFSType,
			DiskPath:            TestDefaultDataPath,
			DiskName:            TestDiskID1,
			InstanceManagerName: TestInstanceManagerName,
		},
	}
	node2 := newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusUnknown, "")
	node2.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
			Conditions: []longhorn.Condition{
				newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusUnknown, ""),
			},
			Type:                longhorn.DiskTypeFilesystem,
			FSType:              TestDiskPathFSType,
			DiskPath:            TestDefaultDataPath,
			DiskName:            TestDiskID1,
			InstanceManagerName: TestInstanceManagerName,
		},
	}

	vol := newVolume(TestVolumeName, 2)
	eng := newEngineForVolume(vol)

	fixture := &NodeControllerFixture{
		lhNodes: map[string]*longhorn.Node{
			TestNode1: node1,
			TestNode2: node2,
		},
		lhReplicas: []*longhorn.Replica{
			newReplicaForVolume(vol, eng, TestNode1, TestDiskID1),
			newReplicaForVolume(vol, eng, TestNode2, TestDiskID2),
		},
		lhSettings: map[string]*longhorn.Setting{
			string(types.SettingNameDefaultInstanceManagerImage): newDefaultInstanceManagerImageSetting(),
		},
		lhInstanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: DefaultInstanceManagerTestNode1,
		},
		lhOrphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
		pods: map[string]*corev1.Pod{
			TestDaemon1: newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional),
			TestDaemon2: newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional),
		},
		nodes: map[string]*corev1.Node{
			TestNode1: newKubernetesNode(
				TestNode1,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
			TestNode2: newKubernetesNode(
				TestNode2,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
		},
	}

	expectation := &NodeControllerExpectation{
		nodeStatus: map[string]*longhorn.NodeStatus{
			TestNode1: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeMountPropagation, longhorn.ConditionStatusTrue, ""),
				},
				DiskStatus: map[string]*longhorn.DiskStatus{
					TestDiskID1: {
						StorageScheduled: TestVolumeSize,
						Conditions: []longhorn.Condition{
							newNodeCondition(longhorn.DiskConditionTypeReady, longhorn.ConditionStatusTrue, ""),
							newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusFalse, string(longhorn.DiskConditionReasonDiskPressure)),
						},
						ScheduledReplica: map[string]int64{
							fixture.lhReplicas[0].Name: fixture.lhReplicas[0].Spec.VolumeSize,
						},
						DiskName:            TestDiskID1,
						DiskUUID:            TestDiskID1,
						Type:                longhorn.DiskTypeFilesystem,
						FSType:              TestDiskPathFSType,
						DiskPath:            TestDefaultDataPath,
						InstanceManagerName: TestInstanceManagerName,
					},
				},
			},
			TestNode2: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue, ""),
				},
				DiskStatus: map[string]*longhorn.DiskStatus{
					TestDiskID1: {
						StorageScheduled: 0,
						StorageAvailable: 0,
						Conditions: []longhorn.Condition{
							newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusUnknown, ""),
						},
						Type:                longhorn.DiskTypeFilesystem,
						FSType:              TestDiskPathFSType,
						DiskPath:            TestDefaultDataPath,
						DiskName:            TestDiskID1,
						InstanceManagerName: TestInstanceManagerName,
					},
				},
			},
		},
		orphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
	}

	s.initTest(c, fixture)

	for _, node := range fixture.lhNodes {
		if s.controller.controllerID == node.Name {
			err = s.controller.diskMonitor.RunOnce()
			c.Assert(err, IsNil)
		}

		err = s.controller.syncNode(getKey(node, c))
		c.Assert(err, IsNil)

		n, err := s.lhClient.LonghornV1beta2().Nodes(TestNamespace).Get(context.TODO(), node.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		s.checkNodeConditions(c, expectation, n)
		s.checkDiskConditions(c, expectation, n)
	}

	s.checkOrphans(c, expectation)
}

func (s *NodeControllerSuite) TestCleanDiskStatus(c *C) {
	var err error

	node1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, "")
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
			Conditions: []longhorn.Condition{
				newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
			},
			DiskName: TestDiskID1,
		},
		"unavailable-disk": {
			StorageScheduled: 0,
			StorageAvailable: 0,
			Conditions: []longhorn.Condition{
				newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
			},
		},
	}
	node2 := newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusUnknown, "")
	node2.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		TestDiskID1: {
			StorageScheduled:    0,
			StorageAvailable:    0,
			Type:                longhorn.DiskTypeFilesystem,
			FSType:              TestDiskPathFSType,
			DiskPath:            TestDefaultDataPath,
			DiskName:            TestDiskID1,
			InstanceManagerName: TestInstanceManagerName,
		},
	}

	fixture := &NodeControllerFixture{
		lhNodes: map[string]*longhorn.Node{
			TestNode1: node1,
			TestNode2: node2,
		},
		lhSettings: map[string]*longhorn.Setting{
			string(types.SettingNameDefaultInstanceManagerImage): newDefaultInstanceManagerImageSetting(),
		},
		lhInstanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: DefaultInstanceManagerTestNode1,
		},
		lhOrphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
		pods: map[string]*corev1.Pod{
			TestDaemon1: newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional),
			TestDaemon2: newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional),
		},
		nodes: map[string]*corev1.Node{
			TestNode1: newKubernetesNode(
				TestNode1,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
			TestNode2: newKubernetesNode(
				TestNode2,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
		},
	}

	expectation := &NodeControllerExpectation{
		nodeStatus: map[string]*longhorn.NodeStatus{
			TestNode1: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeMountPropagation, longhorn.ConditionStatusTrue, ""),
				},
				DiskStatus: map[string]*longhorn.DiskStatus{
					TestDiskID1: {
						StorageScheduled: 0,
						StorageAvailable: 0,
						Conditions: []longhorn.Condition{
							newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusFalse, string(longhorn.DiskConditionReasonDiskPressure)),
							newNodeCondition(longhorn.DiskConditionTypeReady, longhorn.ConditionStatusTrue, ""),
						},
						ScheduledReplica:    map[string]int64{},
						DiskName:            TestDiskID1,
						DiskUUID:            TestDiskID1,
						Type:                longhorn.DiskTypeFilesystem,
						FSType:              TestDiskPathFSType,
						DiskPath:            TestDefaultDataPath,
						InstanceManagerName: TestInstanceManagerName,
					},
				},
			},
			TestNode2: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue, ""),
				},
				DiskStatus: map[string]*longhorn.DiskStatus{
					TestDiskID1: {
						StorageScheduled:    0,
						StorageAvailable:    0,
						Type:                longhorn.DiskTypeFilesystem,
						FSType:              TestDiskPathFSType,
						DiskPath:            TestDefaultDataPath,
						DiskName:            TestDiskID1,
						InstanceManagerName: TestInstanceManagerName,
					},
				},
			},
		},
		orphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
	}

	s.initTest(c, fixture)

	for _, node := range fixture.lhNodes {
		if s.controller.controllerID == node.Name {
			err = s.controller.diskMonitor.RunOnce()
			c.Assert(err, IsNil)
		}

		err = s.controller.syncNode(getKey(node, c))
		c.Assert(err, IsNil)

		n, err := s.lhClient.LonghornV1beta2().Nodes(TestNamespace).Get(context.TODO(), node.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		s.checkNodeConditions(c, expectation, n)
		s.checkDiskConditions(c, expectation, n)
	}

	s.checkOrphans(c, expectation)
}

func (s *NodeControllerSuite) TestDisableDiskOnFilesystemChange(c *C) {
	var err error

	node1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, "")
	node1.Spec.Disks = map[string]longhorn.DiskSpec{
		TestDiskID1: {
			Type:            longhorn.DiskTypeFilesystem,
			Path:            TestDefaultDataPath,
			DiskDriver:      longhorn.DiskDriverNone,
			AllowScheduling: true,
			StorageReserved: 0,
		},
	}
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
				newNodeCondition(longhorn.DiskConditionTypeReady, longhorn.ConditionStatusTrue, ""),
			},
			DiskName:            TestDiskID1,
			DiskUUID:            "new-uuid",
			Type:                longhorn.DiskTypeFilesystem,
			FSType:              TestDiskPathFSType,
			DiskPath:            TestDefaultDataPath,
			InstanceManagerName: TestInstanceManagerName,
		},
	}

	node2 := newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusUnknown, "")
	node2.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		TestDiskID1: {
			DiskName:            TestDiskID1,
			StorageScheduled:    0,
			StorageAvailable:    0,
			Type:                longhorn.DiskTypeFilesystem,
			FSType:              TestDiskPathFSType,
			DiskPath:            TestDefaultDataPath,
			InstanceManagerName: TestInstanceManagerName,
		},
	}

	fixture := &NodeControllerFixture{
		lhNodes: map[string]*longhorn.Node{
			TestNode1: node1,
			TestNode2: node2,
		},
		lhSettings: map[string]*longhorn.Setting{
			string(types.SettingNameDefaultInstanceManagerImage): newDefaultInstanceManagerImageSetting(),
		},
		lhInstanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: DefaultInstanceManagerTestNode1,
		},
		lhOrphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
		pods: map[string]*corev1.Pod{
			TestDaemon1: newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional),
			TestDaemon2: newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional),
		},
		nodes: map[string]*corev1.Node{
			TestNode1: newKubernetesNode(
				TestNode1,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
			TestNode2: newKubernetesNode(
				TestNode2,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
		},
	}

	expectation := &NodeControllerExpectation{
		nodeStatus: map[string]*longhorn.NodeStatus{
			TestNode1: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeMountPropagation, longhorn.ConditionStatusTrue, ""),
				},
				DiskStatus: map[string]*longhorn.DiskStatus{
					TestDiskID1: {
						StorageScheduled: 0,
						StorageAvailable: 0,
						Conditions: []longhorn.Condition{
							newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusFalse, string(longhorn.DiskConditionReasonDiskNotReady)),
							newNodeCondition(longhorn.DiskConditionTypeReady, longhorn.ConditionStatusFalse, string(longhorn.DiskConditionReasonDiskFilesystemChanged)),
						},
						ScheduledReplica:    map[string]int64{},
						DiskName:            TestDiskID1,
						DiskUUID:            "new-uuid",
						Type:                longhorn.DiskTypeFilesystem,
						FSType:              TestDiskPathFSType,
						DiskPath:            TestDefaultDataPath,
						InstanceManagerName: TestInstanceManagerName,
					},
				},
			},
			TestNode2: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue, ""),
				},
				DiskStatus: map[string]*longhorn.DiskStatus{
					TestDiskID1: {
						DiskName:            TestDiskID1,
						StorageScheduled:    0,
						StorageAvailable:    0,
						Type:                longhorn.DiskTypeFilesystem,
						FSType:              TestDiskPathFSType,
						DiskPath:            TestDefaultDataPath,
						InstanceManagerName: TestInstanceManagerName,
					},
				},
			},
		},
		orphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
	}

	s.initTest(c, fixture)

	for _, node := range fixture.lhNodes {
		if s.controller.controllerID == node.Name {
			err = s.controller.diskMonitor.RunOnce()
			c.Assert(err, IsNil)
		}

		err = s.controller.syncNode(getKey(node, c))
		c.Assert(err, IsNil)

		n, err := s.lhClient.LonghornV1beta2().Nodes(TestNamespace).Get(context.TODO(), node.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		s.checkNodeConditions(c, expectation, n)
		s.checkDiskConditions(c, expectation, n)
	}

	s.checkOrphans(c, expectation)
}

func (s *NodeControllerSuite) TestCreateDefaultInstanceManager(c *C) {
	var err error

	node1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, "")
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
			Conditions: []longhorn.Condition{
				newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
			},
			Type:     longhorn.DiskTypeFilesystem,
			DiskName: TestDiskID1,
		},
	}

	fixture := &NodeControllerFixture{
		lhNodes: map[string]*longhorn.Node{
			TestNode1: node1,
		},
		lhSettings: map[string]*longhorn.Setting{
			string(types.SettingNameDefaultInstanceManagerImage): newDefaultInstanceManagerImageSetting(),
		},
		lhInstanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: DefaultInstanceManagerTestNode1,
		},
		lhOrphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
		pods: map[string]*corev1.Pod{
			TestDaemon1: newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional),
			TestDaemon2: newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional),
		},
		nodes: map[string]*corev1.Node{
			TestNode1: newKubernetesNode(
				TestNode1,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
			TestNode2: newKubernetesNode(
				TestNode2,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
		},
	}

	expectation := &NodeControllerExpectation{
		nodeStatus: map[string]*longhorn.NodeStatus{
			TestNode1: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeMountPropagation, longhorn.ConditionStatusTrue, ""),
				},
				DiskStatus: map[string]*longhorn.DiskStatus{
					TestDiskID1: {
						StorageScheduled: 0,
						StorageAvailable: 0,
						Conditions: []longhorn.Condition{
							newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusFalse, string(longhorn.DiskConditionReasonDiskPressure)),
							newNodeCondition(longhorn.DiskConditionTypeReady, longhorn.ConditionStatusTrue, ""),
						},
						DiskName:            TestDiskID1,
						ScheduledReplica:    map[string]int64{},
						DiskUUID:            TestDiskID1,
						Type:                longhorn.DiskTypeFilesystem,
						FSType:              TestDiskPathFSType,
						DiskPath:            TestDefaultDataPath,
						InstanceManagerName: TestInstanceManagerName,
					},
				},
			},
		},
		instanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				false,
			),
		},
		orphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
	}

	s.initTest(c, fixture)

	for _, node := range fixture.lhNodes {
		if s.controller.controllerID == node.Name {
			err = s.controller.diskMonitor.RunOnce()
			c.Assert(err, IsNil)
		}

		err = s.controller.syncNode(getKey(node, c))
		c.Assert(err, IsNil)

		n, err := s.lhClient.LonghornV1beta2().Nodes(TestNamespace).Get(context.TODO(), node.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		s.checkNodeConditions(c, expectation, n)
		s.checkDiskConditions(c, expectation, n)
	}

	s.checkInstanceManagers(c, expectation)
	s.checkOrphans(c, expectation)
}

func (s *NodeControllerSuite) TestCleanupRedundantInstanceManagers(c *C) {
	var err error

	node1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, "")
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
			Conditions: []longhorn.Condition{
				newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
			},
			Type:     longhorn.DiskTypeFilesystem,
			DiskName: TestDiskID1,
		},
	}

	extraInstanceManager := newInstanceManager(
		"extra-instance-manger-name", longhorn.InstanceManagerStateRunning,
		TestOwnerID1, TestNode1, TestIP1,
		map[string]longhorn.InstanceProcess{
			ExistingInstance: {
				Spec: longhorn.InstanceProcessSpec{
					Name: ExistingInstance,
				},
				Status: longhorn.InstanceProcessStatus{
					State:     longhorn.InstanceStateRunning,
					PortStart: TestPort1,
				},
			},
		},
		map[string]longhorn.InstanceProcess{},
		longhorn.DataEngineTypeV1,
		false,
	)
	extraInstanceManager.Spec.Image = TestExtraInstanceManagerImage

	fixture := &NodeControllerFixture{
		lhNodes: map[string]*longhorn.Node{
			TestNode1: node1,
		},
		lhSettings: map[string]*longhorn.Setting{
			string(types.SettingNameDefaultInstanceManagerImage): newDefaultInstanceManagerImageSetting(),
		},
		lhInstanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName:       DefaultInstanceManagerTestNode1,
			"extra-instance-manager-name": extraInstanceManager,
		},
		lhOrphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
		pods: map[string]*corev1.Pod{
			TestDaemon1: newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional),
			TestDaemon2: newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional),
		},
		nodes: map[string]*corev1.Node{
			TestNode1: newKubernetesNode(
				TestNode1,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
			TestNode2: newKubernetesNode(
				TestNode2,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
		},
	}

	expectation := &NodeControllerExpectation{
		nodeStatus: map[string]*longhorn.NodeStatus{
			TestNode1: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeMountPropagation, longhorn.ConditionStatusTrue, ""),
				},
				DiskStatus: map[string]*longhorn.DiskStatus{
					TestDiskID1: {
						StorageScheduled: 0,
						StorageAvailable: 0,
						Conditions: []longhorn.Condition{
							newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusFalse, string(longhorn.DiskConditionReasonDiskPressure)),
							newNodeCondition(longhorn.DiskConditionTypeReady, longhorn.ConditionStatusTrue, ""),
						},
						DiskName:            TestDiskID1,
						ScheduledReplica:    map[string]int64{},
						DiskUUID:            TestDiskID1,
						Type:                longhorn.DiskTypeFilesystem,
						FSType:              TestDiskPathFSType,
						DiskPath:            TestDefaultDataPath,
						InstanceManagerName: TestInstanceManagerName,
					},
				},
			},
		},
		instanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				false,
			),
			"extra-instance-manger-name": extraInstanceManager,
		},
		orphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
	}

	s.initTest(c, fixture)

	for _, node := range fixture.lhNodes {
		if s.controller.controllerID == node.Name {
			err = s.controller.diskMonitor.RunOnce()
			c.Assert(err, IsNil)
		}

		err = s.controller.syncNode(getKey(node, c))
		c.Assert(err, IsNil)

		n, err := s.lhClient.LonghornV1beta2().Nodes(TestNamespace).Get(context.TODO(), node.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		s.checkNodeConditions(c, expectation, n)
		s.checkDiskConditions(c, expectation, n)
	}

	s.checkInstanceManagers(c, expectation)
	s.checkOrphans(c, expectation)
}

func (s *NodeControllerSuite) TestCleanupAllInstanceManagers(c *C) {
	var err error

	node1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, "")
	node1.Spec.Disks = map[string]longhorn.DiskSpec{}
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{}

	fixture := &NodeControllerFixture{
		lhNodes: map[string]*longhorn.Node{
			TestNode1: node1,
		},
		lhSettings: map[string]*longhorn.Setting{
			string(types.SettingNameDefaultInstanceManagerImage): newDefaultInstanceManagerImageSetting(),
		},
		lhInstanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: DefaultInstanceManagerTestNode1,
		},
		lhOrphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
		pods: map[string]*corev1.Pod{
			TestDaemon1: newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional),
			TestDaemon2: newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional),
		},
		nodes: map[string]*corev1.Node{
			TestNode1: newKubernetesNode(
				TestNode1,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
			TestNode2: newKubernetesNode(
				TestNode2,
				corev1.ConditionTrue,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionFalse,
				corev1.ConditionTrue,
			),
		},
	}

	expectation := &NodeControllerExpectation{
		nodeStatus: map[string]*longhorn.NodeStatus{
			TestNode1: {
				Conditions: []longhorn.Condition{
					newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue, ""),
					newNodeCondition(longhorn.NodeConditionTypeMountPropagation, longhorn.ConditionStatusTrue, ""),
				},
				DiskStatus: map[string]*longhorn.DiskStatus{},
			},
		},
		instanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				false,
			),
		},
	}

	s.initTest(c, fixture)

	for _, node := range fixture.lhNodes {
		if s.controller.controllerID == node.Name {
			err = s.controller.diskMonitor.RunOnce()
			c.Assert(err, IsNil)
		}

		err = s.controller.syncNode(getKey(node, c))
		c.Assert(err, IsNil)

		n, err := s.lhClient.LonghornV1beta2().Nodes(TestNamespace).Get(context.TODO(), node.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		s.checkNodeConditions(c, expectation, n)
	}

	s.checkInstanceManagers(c, expectation)
}

func (s *NodeControllerSuite) TestEventOnNotReady(c *C) {
	var err error

	node1 := newKubernetesNode(
		TestNode1,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionTrue,
	)

	node2 := newKubernetesNode(
		TestNode2,
		corev1.ConditionTrue,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionTrue,
	)

	fixture := &NodeControllerFixture{
		lhNodes: map[string]*longhorn.Node{
			TestNode1: newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
			TestNode2: newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
		},
		lhSettings: map[string]*longhorn.Setting{
			string(types.SettingNameDefaultInstanceManagerImage): newDefaultInstanceManagerImageSetting(),
		},
		lhInstanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: DefaultInstanceManagerTestNode1,
		},
		lhOrphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
		pods: map[string]*corev1.Pod{
			TestDaemon1: newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional),
			TestDaemon2: newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional),
		},
		nodes: map[string]*corev1.Node{
			TestNode1: node1,
			TestNode2: node2,
		},
	}

	expectation := &NodeControllerExpectation{
		events: map[string]*corev1.Event{
			"node1-ready": {
				Type:    "Normal",
				Reason:  "Ready",
				Message: "Node test-node-name-1 is ready",
			},
			"node2-ready": {
				Type:    "Normal",
				Reason:  "Ready",
				Message: "Node test-node-name-2 is ready",
			},
			"node-schedulable": {
				Type:    "Normal",
				Reason:  "Schedulable",
				Message: "",
			},
			"": {
				Type:    "Warning",
				Reason:  "Schedulable",
				Message: "Disk fsid (/var/lib/longhorn) on the node test-node-name-1 has 0 available, but requires reserved 0, minimal 25% to schedule more replicas",
			},
			"node1-not-ready": {
				Type:    "Warning",
				Reason:  "Ready",
				Message: "Kubernetes node test-node-name-1 not ready",
			},
		},
	}

	s.initTest(c, fixture)

	for _, node := range fixture.lhNodes {
		if s.controller.controllerID == node.Name {
			err = s.controller.diskMonitor.RunOnce()
			c.Assert(err, IsNil)
		}

		err = s.controller.syncNode(getKey(node, c))
		c.Assert(err, IsNil)
	}

	s.checkEvents(c, expectation)
}

func (s *NodeControllerSuite) TestEventOnDiskPressure(c *C) {
	var err error

	node1 := newKubernetesNode(
		TestNode1,
		corev1.ConditionTrue,
		corev1.ConditionTrue,  // disk
		corev1.ConditionFalse, // memory
		corev1.ConditionFalse, // pid
		corev1.ConditionFalse, // network
		corev1.ConditionTrue,
	)

	node2 := newKubernetesNode(
		TestNode2,
		corev1.ConditionTrue,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionTrue,
	)

	fixture := &NodeControllerFixture{
		lhNodes: map[string]*longhorn.Node{
			TestNode1: newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
			TestNode2: newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
		},
		lhSettings: map[string]*longhorn.Setting{
			string(types.SettingNameDefaultInstanceManagerImage): newDefaultInstanceManagerImageSetting(),
		},
		lhInstanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: DefaultInstanceManagerTestNode1,
		},
		lhOrphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
		pods: map[string]*corev1.Pod{
			TestDaemon1: newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional),
			TestDaemon2: newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional),
		},
		nodes: map[string]*corev1.Node{
			TestNode1: node1,
			TestNode2: node2,
		},
	}

	expectation := &NodeControllerExpectation{
		events: map[string]*corev1.Event{
			"node1-ready": {
				Type:    "Normal",
				Reason:  "Ready",
				Message: "Node test-node-name-1 is ready",
			},
			"node2-ready": {
				Type:    "Normal",
				Reason:  "Ready",
				Message: "Node test-node-name-2 is ready",
			},
			"node-schedulable": {
				Type:    "Normal",
				Reason:  "Schedulable",
				Message: "",
			},
			"": {
				Type:    "Warning",
				Reason:  "Schedulable",
				Message: "Disk fsid (/var/lib/longhorn) on the node test-node-name-1 has 0 available, but requires reserved 0, minimal 25% to schedule more replicas",
			},
			"node1-disk-pressure": {
				Type:    "Warning",
				Reason:  "Ready",
				Message: "Kubernetes node test-node-name-1 has pressure",
			},
		},
	}

	s.initTest(c, fixture)

	for _, node := range fixture.lhNodes {
		if s.controller.controllerID == node.Name {
			err = s.controller.diskMonitor.RunOnce()
			c.Assert(err, IsNil)
		}

		err = s.controller.syncNode(getKey(node, c))
		c.Assert(err, IsNil)
	}

	s.checkEvents(c, expectation)
}

func (s *NodeControllerSuite) TestEventOnMemoryPressure(c *C) {
	var err error

	node1 := newKubernetesNode(
		TestNode1,
		corev1.ConditionTrue,
		corev1.ConditionFalse, // disk
		corev1.ConditionTrue,  // memory
		corev1.ConditionFalse, // pid
		corev1.ConditionFalse, // network
		corev1.ConditionTrue,
	)

	node2 := newKubernetesNode(
		TestNode2,
		corev1.ConditionTrue,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionTrue,
	)

	fixture := &NodeControllerFixture{
		lhNodes: map[string]*longhorn.Node{
			TestNode1: newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
			TestNode2: newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
		},
		lhSettings: map[string]*longhorn.Setting{
			string(types.SettingNameDefaultInstanceManagerImage): newDefaultInstanceManagerImageSetting(),
		},
		lhInstanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: DefaultInstanceManagerTestNode1,
		},
		lhOrphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
		pods: map[string]*corev1.Pod{
			TestDaemon1: newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional),
			TestDaemon2: newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional),
		},
		nodes: map[string]*corev1.Node{
			TestNode1: node1,
			TestNode2: node2,
		},
	}

	expectation := &NodeControllerExpectation{
		events: map[string]*corev1.Event{
			"node1-ready": {
				Type:    "Normal",
				Reason:  "Ready",
				Message: "Node test-node-name-1 is ready",
			},
			"node2-ready": {
				Type:    "Normal",
				Reason:  "Ready",
				Message: "Node test-node-name-2 is ready",
			},
			"node-schedulable": {
				Type:    "Normal",
				Reason:  "Schedulable",
				Message: "",
			},
			"": {
				Type:    "Warning",
				Reason:  "Schedulable",
				Message: "Disk fsid (/var/lib/longhorn) on the node test-node-name-1 has 0 available, but requires reserved 0, minimal 25% to schedule more replicas",
			},
			"node1-memory-pressure": {
				Type:    "Warning",
				Reason:  "Ready",
				Message: "Kubernetes node test-node-name-1 has pressure",
			},
		},
	}

	s.initTest(c, fixture)

	for _, node := range fixture.lhNodes {
		if s.controller.controllerID == node.Name {
			err = s.controller.diskMonitor.RunOnce()
			c.Assert(err, IsNil)
		}

		err = s.controller.syncNode(getKey(node, c))
		c.Assert(err, IsNil)
	}

	s.checkEvents(c, expectation)
}

func (s *NodeControllerSuite) TestEventOnPidPressure(c *C) {
	var err error

	node1 := newKubernetesNode(
		TestNode1,
		corev1.ConditionTrue,
		corev1.ConditionFalse, // disk
		corev1.ConditionFalse, // memory
		corev1.ConditionTrue,  // pid
		corev1.ConditionFalse, // network
		corev1.ConditionTrue,
	)

	node2 := newKubernetesNode(
		TestNode2,
		corev1.ConditionTrue,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionTrue,
	)

	fixture := &NodeControllerFixture{
		lhNodes: map[string]*longhorn.Node{
			TestNode1: newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
			TestNode2: newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
		},
		lhSettings: map[string]*longhorn.Setting{
			string(types.SettingNameDefaultInstanceManagerImage): newDefaultInstanceManagerImageSetting(),
		},
		lhInstanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: DefaultInstanceManagerTestNode1,
		},
		lhOrphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
		pods: map[string]*corev1.Pod{
			TestDaemon1: newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional),
			TestDaemon2: newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional),
		},
		nodes: map[string]*corev1.Node{
			TestNode1: node1,
			TestNode2: node2,
		},
	}

	expectation := &NodeControllerExpectation{
		events: map[string]*corev1.Event{
			"node1-ready": {
				Type:    "Normal",
				Reason:  "Ready",
				Message: "Node test-node-name-1 is ready",
			},
			"node2-ready": {
				Type:    "Normal",
				Reason:  "Ready",
				Message: "Node test-node-name-2 is ready",
			},
			"node-schedulable": {
				Type:    "Normal",
				Reason:  "Schedulable",
				Message: "",
			},
			"": {
				Type:    "Warning",
				Reason:  "Schedulable",
				Message: "Disk fsid (/var/lib/longhorn) on the node test-node-name-1 has 0 available, but requires reserved 0, minimal 25% to schedule more replicas",
			},
			"node1-pid-pressure": {
				Type:    "Warning",
				Reason:  "Ready",
				Message: "Kubernetes node test-node-name-1 has pressure",
			},
		},
	}

	s.initTest(c, fixture)

	for _, node := range fixture.lhNodes {
		if s.controller.controllerID == node.Name {
			err = s.controller.diskMonitor.RunOnce()
			c.Assert(err, IsNil)
		}

		err = s.controller.syncNode(getKey(node, c))
		c.Assert(err, IsNil)
	}

	s.checkEvents(c, expectation)
}

func (s *NodeControllerSuite) TestEventOnNetworkPressure(c *C) {
	var err error

	node1 := newKubernetesNode(
		TestNode1,
		corev1.ConditionTrue,
		corev1.ConditionFalse, // disk
		corev1.ConditionFalse, // memory
		corev1.ConditionFalse, // pid
		corev1.ConditionTrue,  // network
		corev1.ConditionTrue,
	)

	node2 := newKubernetesNode(
		TestNode2,
		corev1.ConditionTrue,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionTrue,
	)

	fixture := &NodeControllerFixture{
		lhNodes: map[string]*longhorn.Node{
			TestNode1: newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
			TestNode2: newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
		},
		lhSettings: map[string]*longhorn.Setting{
			string(types.SettingNameDefaultInstanceManagerImage): newDefaultInstanceManagerImageSetting(),
		},
		lhInstanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: DefaultInstanceManagerTestNode1,
		},
		lhOrphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
		pods: map[string]*corev1.Pod{
			TestDaemon1: newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional),
			TestDaemon2: newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional),
		},
		nodes: map[string]*corev1.Node{
			TestNode1: node1,
			TestNode2: node2,
		},
	}

	expectation := &NodeControllerExpectation{
		events: map[string]*corev1.Event{
			"node1-ready": {
				Type:    "Normal",
				Reason:  "Ready",
				Message: "Node test-node-name-1 is ready",
			},
			"node2-ready": {
				Type:    "Normal",
				Reason:  "Ready",
				Message: "Node test-node-name-2 is ready",
			},
			"node-schedulable": {
				Type:    "Normal",
				Reason:  "Schedulable",
				Message: "",
			},
			"": {
				Type:    "Warning",
				Reason:  "Schedulable",
				Message: "Disk fsid (/var/lib/longhorn) on the node test-node-name-1 has 0 available, but requires reserved 0, minimal 25% to schedule more replicas",
			},
			"node1-network-pressure": {
				Type:    "Warning",
				Reason:  "Ready",
				Message: "Kubernetes node test-node-name-1 has pressure",
			},
		},
	}

	s.initTest(c, fixture)

	for _, node := range fixture.lhNodes {
		if s.controller.controllerID == node.Name {
			err = s.controller.diskMonitor.RunOnce()
			c.Assert(err, IsNil)
		}

		err = s.controller.syncNode(getKey(node, c))
		c.Assert(err, IsNil)
	}

	s.checkEvents(c, expectation)
}

func (s *NodeControllerSuite) TestNoEventOnUnknownTrueNodeCondition(c *C) {
	var err error

	node1 := newKubernetesNode(
		TestNode1,
		corev1.ConditionTrue,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionTrue,
	)
	node1.Status.Conditions = append(node1.Status.Conditions, corev1.NodeCondition{
		Type:    "EtcdIsVoter",
		Status:  corev1.ConditionTrue,
		Reason:  "MemberNotLearner",
		Message: "Node is a voting member of the etcd cluster",
	})

	node2 := newKubernetesNode(
		TestNode2,
		corev1.ConditionTrue,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionFalse,
		corev1.ConditionTrue,
	)

	fixture := &NodeControllerFixture{
		lhNodes: map[string]*longhorn.Node{
			TestNode1: newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
			TestNode2: newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusUnknown, ""),
		},
		lhSettings: map[string]*longhorn.Setting{
			string(types.SettingNameDefaultInstanceManagerImage): newDefaultInstanceManagerImageSetting(),
		},
		lhInstanceManagers: map[string]*longhorn.InstanceManager{
			TestInstanceManagerName: DefaultInstanceManagerTestNode1,
		},
		lhOrphans: map[string]*longhorn.Orphan{
			DefaultOrphanTestNode1.Name: DefaultOrphanTestNode1,
		},
		pods: map[string]*corev1.Pod{
			TestDaemon1: newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional),
			TestDaemon2: newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional),
		},
		nodes: map[string]*corev1.Node{
			TestNode1: node1,
			TestNode2: node2,
		},
	}

	expectation := &NodeControllerExpectation{
		events: map[string]*corev1.Event{
			"node1-ready": {
				Type:    "Normal",
				Reason:  "Ready",
				Message: "Node test-node-name-1 is ready",
			},
			"node2-ready": {
				Type:    "Normal",
				Reason:  "Ready",
				Message: "Node test-node-name-2 is ready",
			},
			"node-schedulable": {
				Type:    "Normal",
				Reason:  "Schedulable",
				Message: "",
			},
			"": {
				Type:    "Warning",
				Reason:  "Schedulable",
				Message: "Disk fsid (/var/lib/longhorn) on the node test-node-name-1 has 0 available, but requires reserved 0, minimal 25% to schedule more replicas",
			},
		},
	}

	s.initTest(c, fixture)

	for _, node := range fixture.lhNodes {
		if s.controller.controllerID == node.Name {
			err = s.controller.diskMonitor.RunOnce()
			c.Assert(err, IsNil)
		}

		err = s.controller.syncNode(getKey(node, c))
		c.Assert(err, IsNil)
	}

	s.checkEvents(c, expectation)
}

// -- Helpers --

func (s *NodeControllerSuite) checkNodeConditions(c *C, expectation *NodeControllerExpectation, node *longhorn.Node) {
	// Check that all node status conditions match the expected node status
	// conditions - save for the last transition timestamp and the actual
	// message
	for idx, condition := range node.Status.Conditions {
		condition.LastTransitionTime = ""
		condition.Message = ""
		node.Status.Conditions[idx] = condition
	}
	c.Assert(node.Status.Conditions, DeepEquals, expectation.nodeStatus[node.Name].Conditions)
}

func (s *NodeControllerSuite) checkDiskConditions(c *C, expectation *NodeControllerExpectation, node *longhorn.Node) {
	// Check that all disk status conditions match the expected disk status
	// conditions - save for the last transition timestamp and the actual message
	for fsid, diskStatus := range node.Status.DiskStatus {
		for idx, condition := range diskStatus.Conditions {
			if condition.Status != longhorn.ConditionStatusUnknown {
				c.Assert(condition.LastTransitionTime, Not(Equals), "")
			}
			condition.LastTransitionTime = ""
			condition.Message = ""
			diskStatus.Conditions[idx] = condition
		}
		node.Status.DiskStatus[fsid] = diskStatus
	}
	c.Assert(node.Status.DiskStatus, DeepEquals, expectation.nodeStatus[node.Name].DiskStatus)
}

func (s *NodeControllerSuite) checkInstanceManagers(c *C, expectation *NodeControllerExpectation) {
	// Check that all existing instance managers are expected and all expected
	// instance managers are existing
	imList, err := s.lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, im := range imList.Items {
		_, exists := expectation.instanceManagers[im.Name]
		c.Assert(exists, Equals, true)
	}

	for _, im := range expectation.instanceManagers {
		_, err := s.lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Get(context.TODO(), im.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)
	}
}

func (s *NodeControllerSuite) checkOrphans(c *C, expectation *NodeControllerExpectation) {
	// Check that all existing orphans are expected and all expected orphans are
	// existing
	orphanList, err := s.lhClient.LonghornV1beta2().Orphans(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, orphan := range orphanList.Items {
		_, exists := expectation.orphans[orphan.Name]
		c.Assert(exists, Equals, true)
	}

	for _, expect := range expectation.orphans {
		_, err := s.lhClient.LonghornV1beta2().Orphans(TestNamespace).Get(context.TODO(), expect.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)
	}
}

func (s *NodeControllerSuite) checkEvents(c *C, expectation *NodeControllerExpectation) {
	recordedEvents := []string{}

	// extract recorded events from buffered channel into array for easier
	// handling
out:
	for i := 0; i < eventRecorderBufferSize; i++ {
		select {
		case event, ok := <-(s.eventRecorder).Events:
			if ok {
				recordedEvents = append(recordedEvents, event)
				c.Logf("recorded event: %v", event)
			} else {
				break out
			}
		default:
			break out
		}
	}

	// check that there have not been any unexpected events
	for _, event := range recordedEvents {
		match := false
		for _, expect := range expectation.events {
			if strings.Contains(event, expect.Type) && strings.Contains(event, expect.Reason) && strings.Contains(event, expect.Message) {
				match = true
				break
			}
		}
		c.Assert(match, Equals, true, Commentf("unexpected event \"%v\"", event))
	}

	// check that all expected events have been emitted
	for _, expect := range expectation.events {
		match := false
		for _, event := range recordedEvents {
			if strings.Contains(event, expect.Type) && strings.Contains(event, expect.Reason) && strings.Contains(event, expect.Message) {
				match = true
				break
			}
		}
		c.Assert(match, Equals, true, Commentf("failed to find expected event \"%v %v %v\"", expect.Type, expect.Reason, expect.Message))
	}
}

func (s *NodeControllerSuite) initTest(c *C, fixture *NodeControllerFixture) {
	c.Assert(s.kubeClient, NotNil)
	c.Assert(s.lhClient, NotNil)
	c.Assert(s.extensionsClient, NotNil)

	for _, node := range fixture.lhNodes {
		n, err := s.lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(n, NotNil)
		err = s.lhNodeIndexer.Add(n)
		c.Assert(err, IsNil)
	}

	for _, replica := range fixture.lhReplicas {
		r, err := s.lhClient.LonghornV1beta2().Replicas(TestNamespace).Create(context.TODO(), replica, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(r, NotNil)
		err = s.lhReplicaIndexer.Add(r)
		c.Assert(err, IsNil)
	}

	for _, setting := range fixture.lhSettings {
		set, err := s.lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(set, NotNil)
		err = s.lhSettingsIndexer.Add(set)
		c.Assert(err, IsNil)
	}

	for _, instanceManager := range fixture.lhInstanceManagers {
		im, err := s.lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), instanceManager, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(im, NotNil)
		err = s.lhInstanceManagerIndexer.Add(im)
		c.Assert(err, IsNil)
	}

	for _, orphan := range fixture.lhOrphans {
		o, err := s.lhClient.LonghornV1beta2().Orphans(TestNamespace).Create(context.TODO(), orphan, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(o, NotNil)
		err = s.lhOrphanIndexer.Add(o)
		c.Assert(err, IsNil)
	}

	for _, node := range fixture.nodes {
		n, err := s.kubeClient.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(n, NotNil)
		err = s.nodeIndexer.Add(n)
		c.Assert(err, IsNil)
	}

	for _, pod := range fixture.pods {
		p, err := s.kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(p, NotNil)
		err = s.podIndexer.Add(p)
		c.Assert(err, IsNil)
	}
}

func newTestNodeController(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, eventRecorder *record.FakeRecorder, controllerID string) (*NodeController, error) {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	logger := logrus.StandardLogger()
	nc, err := NewNodeController(logger, ds, scheme.Scheme, kubeClient, TestNamespace, controllerID)
	if err != nil {
		return nil, err
	}
	nc.eventRecorder = eventRecorder
	nc.topologyLabelsChecker = fakeTopologyLabelsChecker

	enqueueNodeForMonitor := func(key string) {
		nc.queue.Add(key)
	}
	mon, err := monitor.NewFakeNodeMonitor(nc.logger, nc.ds, controllerID, enqueueNodeForMonitor)
	if err != nil {
		return nil, err
	}
	nc.diskMonitor = mon

	for index := range nc.cacheSyncs {
		nc.cacheSyncs[index] = alwaysReady
	}
	return nc, nil
}

func fakeTopologyLabelsChecker(kubeClient clientset.Interface, vers string) (bool, error) {
	return false, nil
}
