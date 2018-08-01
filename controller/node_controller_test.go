package controller

import (
	"fmt"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/types"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhfake "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

type NodeTestCase struct {
	nodes    map[string]*longhorn.Node
	pods     map[string]*v1.Pod
	replicas []*longhorn.Replica

	expectNodeStatus map[string]types.NodeStatus
}

func newTestNodeController(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset, controllerID string) *NodeController {
	volumeInformer := lhInformerFactory.Longhorn().V1alpha1().Volumes()
	engineInformer := lhInformerFactory.Longhorn().V1alpha1().Engines()
	replicaInformer := lhInformerFactory.Longhorn().V1alpha1().Replicas()
	engineImageInformer := lhInformerFactory.Longhorn().V1alpha1().EngineImages()
	nodeInformer := lhInformerFactory.Longhorn().V1alpha1().Nodes()
	settingInformer := lhInformerFactory.Longhorn().V1alpha1().Settings()

	podInformer := kubeInformerFactory.Core().V1().Pods()
	cronJobInformer := kubeInformerFactory.Batch().V1beta1().CronJobs()
	daemonSetInformer := kubeInformerFactory.Apps().V1beta2().DaemonSets()

	ds := datastore.NewDataStore(
		volumeInformer, engineInformer, replicaInformer,
		engineImageInformer, nodeInformer, settingInformer,
		lhClient,
		podInformer, cronJobInformer, daemonSetInformer,
		kubeClient, TestNamespace)

	nc := NewNodeController(ds, scheme.Scheme, nodeInformer, settingInformer, podInformer, kubeClient, TestNamespace, controllerID)
	fakeRecorder := record.NewFakeRecorder(100)
	nc.eventRecorder = fakeRecorder

	nc.nStoreSynced = alwaysReady
	nc.pStoreSynced = alwaysReady

	return nc
}

func (s *TestSuite) TestSyncNode(c *C) {
	testCases := map[string]*NodeTestCase{}
	MountPropagationBidirectional := v1.MountPropagationBidirectional

	tc := &NodeTestCase{}
	daemon1 := newDaemonPod(v1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, nil)
	daemon2 := newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, nil)
	pods := map[string]*v1.Pod{
		TestDaemon1: daemon1,
		TestDaemon2: daemon2,
	}
	tc.pods = pods
	node1 := newNode(TestNode1, TestNamespace, true, "")
	node2 := newNode(TestNode2, TestNamespace, true, "")
	nodes := map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	expectNodeStatus := map[string]types.NodeStatus{
		TestNode1: {
			State:            types.NodeStateUp,
			MountPropagation: false,
		},
		TestNode2: {
			State:            types.NodeStateUp,
			MountPropagation: false,
		},
	}
	tc.expectNodeStatus = expectNodeStatus
	testCases["all nodes up"] = tc

	tc = &NodeTestCase{}
	daemon1 = newDaemonPod(v1.PodFailed, TestDaemon1, TestNamespace, TestNode1, TestIP1, nil)
	daemon2 = newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, nil)
	pods = map[string]*v1.Pod{
		TestDaemon1: daemon1,
		TestDaemon2: daemon2,
	}
	tc.pods = pods
	node1 = newNode(TestNode1, TestNamespace, true, types.NodeStateUp)
	node2 = newNode(TestNode2, TestNamespace, true, types.NodeStateUp)
	nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	expectNodeStatus = map[string]types.NodeStatus{
		TestNode1: {
			State:            types.NodeStateDown,
			MountPropagation: false,
		},
		TestNode2: {
			State:            types.NodeStateUp,
			MountPropagation: false,
		},
	}
	tc.expectNodeStatus = expectNodeStatus
	testCases["manager pod down"] = tc

	tc = &NodeTestCase{}
	daemon1 = newDaemonPod(v1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, nil)
	daemon2 = newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, nil)
	pods = map[string]*v1.Pod{
		TestDaemon1: daemon1,
		TestDaemon2: daemon2,
	}
	tc.pods = pods
	node1 = newNode(TestNode1, TestNamespace, true, types.NodeStateUp)
	node2 = newNode(TestNode2, TestNamespace, true, types.NodeStateDown)
	nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	expectNodeStatus = map[string]types.NodeStatus{
		TestNode1: {
			State:            types.NodeStateUp,
			MountPropagation: false,
		},
		TestNode2: {
			State:            types.NodeStateUp,
			MountPropagation: false,
		},
	}
	tc.expectNodeStatus = expectNodeStatus
	testCases["set node status up"] = tc

	tc = &NodeTestCase{}
	daemon1 = newDaemonPod(v1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional)
	daemon2 = newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional)
	pods = map[string]*v1.Pod{
		TestDaemon1: daemon1,
		TestDaemon2: daemon2,
	}
	tc.pods = pods
	node1 = newNode(TestNode1, TestNamespace, true, "up")
	node1.Status.DiskStatus = map[string]types.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
			State:            types.DiskStateSchedulable,
		},
	}
	node2 = newNode(TestNode2, TestNamespace, true, "up")
	node2.Status.DiskStatus = map[string]types.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
		},
	}
	nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	volume := newVolume(TestVolumeName, 2)
	engine := newEngineForVolume(volume)
	replica1 := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1)
	replica2 := newReplicaForVolume(volume, engine, TestNode2, TestDiskID1)
	replicas := []*longhorn.Replica{replica1, replica2}
	tc.replicas = replicas

	expectNodeStatus = map[string]types.NodeStatus{
		TestNode1: {
			State:            types.NodeStateUp,
			MountPropagation: true,
			DiskStatus: map[string]types.DiskStatus{
				TestDiskID1: {
					StorageScheduled: TestVolumeSize,
					State:            types.DiskStateUnschedulable,
				},
			},
		},
		TestNode2: {
			State:            types.NodeStateUp,
			MountPropagation: false,
			DiskStatus: map[string]types.DiskStatus{
				TestDiskID1: {
					StorageScheduled: 0,
					StorageAvailable: 0,
				},
			},
		},
	}
	tc.expectNodeStatus = expectNodeStatus
	testCases["only disk on node1 should be updated status"] = tc

	tc = &NodeTestCase{}
	daemon1 = newDaemonPod(v1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional)
	daemon2 = newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional)
	pods = map[string]*v1.Pod{
		TestDaemon1: daemon1,
		TestDaemon2: daemon2,
	}
	tc.pods = pods
	node1 = newNode(TestNode1, TestNamespace, true, "up")
	node1.Status.DiskStatus = map[string]types.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
			State:            types.DiskStateSchedulable,
		},
		"unavailable-disk": {
			StorageScheduled: 0,
			StorageAvailable: 0,
			State:            types.DiskStateSchedulable,
		},
	}
	node2 = newNode(TestNode2, TestNamespace, true, "up")
	node2.Status.DiskStatus = map[string]types.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
		},
	}
	nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	expectNodeStatus = map[string]types.NodeStatus{
		TestNode1: {
			State:            types.NodeStateUp,
			MountPropagation: true,
			DiskStatus: map[string]types.DiskStatus{
				TestDiskID1: {
					StorageScheduled: 0,
					StorageAvailable: 0,
					State:            types.DiskStateUnschedulable,
				},
			},
		},
		TestNode2: {
			State:            types.NodeStateUp,
			MountPropagation: false,
			DiskStatus: map[string]types.DiskStatus{
				TestDiskID1: {
					StorageScheduled: 0,
					StorageAvailable: 0,
				},
			},
		},
	}
	tc.expectNodeStatus = expectNodeStatus
	testCases["test clean disk status when disk removed from the node spec"] = tc

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)
		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		nIndexer := lhInformerFactory.Longhorn().V1alpha1().Nodes().Informer().GetIndexer()
		pIndexer := kubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

		rIndexer := lhInformerFactory.Longhorn().V1alpha1().Replicas().Informer().GetIndexer()

		nc := newTestNodeController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient, TestNode1)
		// create manager pod
		for _, pod := range tc.pods {
			p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(pod)
			c.Assert(err, IsNil)
			pIndexer.Add(p)
		}
		// create node
		for _, node := range tc.nodes {
			n, err := lhClient.Longhorn().Nodes(TestNamespace).Create(node)
			c.Assert(err, IsNil)
			c.Assert(n, NotNil)
			nIndexer.Add(n)
		}
		// create replicas
		for _, replica := range tc.replicas {
			r, err := lhClient.Longhorn().Replicas(TestNamespace).Create(replica)
			c.Assert(err, IsNil)
			c.Assert(r, NotNil)
			rIndexer.Add(r)
		}
		// sync node status
		for nodeName, node := range tc.nodes {
			err := nc.syncNode(getKey(node, c))
			c.Assert(err, IsNil)

			n, err := lhClient.LonghornV1alpha1().Nodes(TestNamespace).Get(node.Name, metav1.GetOptions{})
			c.Assert(err, IsNil)
			c.Assert(n.Status.State, Equals, tc.expectNodeStatus[nodeName].State)
			c.Assert(n.Status.MountPropagation, Equals, tc.expectNodeStatus[nodeName].MountPropagation)
			if len(tc.expectNodeStatus[nodeName].DiskStatus) > 0 {
				c.Assert(n.Status.DiskStatus, DeepEquals, tc.expectNodeStatus[nodeName].DiskStatus)
			}
		}

	}
}
