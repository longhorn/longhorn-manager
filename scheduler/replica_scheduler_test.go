package scheduler

import (
	"fmt"
	"testing"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhfake "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

const (
	TestNamespace = "default"
	TestIP1       = "1.2.3.4"
	TestIP2       = "5.6.7.8"
	TestIP3       = "9.10.11.12"
	TestNode1     = "test-node-name-1"
	TestNode2     = "test-node-name-2"
	TestNode3     = "test-node-name-3"

	TestOwnerID1    = TestNode1
	TestEngineImage = "longhorn-engine:latest"

	TestVolumeName         = "test-volume"
	TestVolumeSize         = 1073741824
	TestVolumeStaleTimeout = 60

	TestDefaultDataPath = "/var/lib/rancher/longhorn"

	TestDaemon1 = "longhorn-manager-1"
	TestDaemon2 = "longhorn-manager-2"
	TestDaemon3 = "longhorn-manager-3"

	TestDiskID1           = "diskID1"
	TestDiskID2           = "diskID2"
	TestDiskSize          = 5000000000
	TestDiskAvailableSize = 3000000000
)

func newReplicaScheduler(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset) *ReplicaScheduler {
	fmt.Printf("testing NewReplicaScheduler\n")

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

	return NewReplicaScheduler(ds)
}

func newDaemonPod(phase v1.PodPhase, name, namespace, nodeID, podIP string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app": "longhorn-manager",
			},
		},
		Spec: v1.PodSpec{
			NodeName: nodeID,
		},
		Status: v1.PodStatus{
			Phase: phase,
			PodIP: podIP,
		},
	}
}

func newNode(name, namespace string, allowScheduling bool, nodeState types.NodeState) *longhorn.Node {
	return &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: types.NodeSpec{
			AllowScheduling: allowScheduling,
		},
		Status: types.NodeStatus{
			State: nodeState,
		},
	}
}

func newDisk(path string, allowScheduling bool, storageMaximum, storageReserved int64) types.DiskSpec {
	return types.DiskSpec{
		Path:            path,
		AllowScheduling: allowScheduling,
		StorageMaximum:  storageMaximum,
		StorageReserved: storageReserved,
	}
}

func newVolume(name string, replicaCount int) *longhorn.Volume {
	return &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Finalizers: []string{
				longhorn.SchemeGroupVersion.Group,
			},
		},
		Spec: types.VolumeSpec{
			NumberOfReplicas:    replicaCount,
			Size:                TestVolumeSize,
			OwnerID:             TestOwnerID1,
			StaleReplicaTimeout: TestVolumeStaleTimeout,
			EngineImage:         TestEngineImage,
		},
	}
}

func newReplicaForVolume(v *longhorn.Volume) *longhorn.Replica {
	return &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name: v.Name + "-r-" + util.RandomID(),
			Labels: map[string]string{
				"longhornvolume": v.Name,
			},
		},
		Spec: types.ReplicaSpec{
			InstanceSpec: types.InstanceSpec{
				OwnerID:     v.Spec.OwnerID,
				VolumeName:  v.Name,
				VolumeSize:  v.Spec.Size,
				EngineImage: TestEngineImage,
				DesireState: types.InstanceStateStopped,
			},
		},
	}
}

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
}

type ReplicaSchedulerTestCase struct {
	volume   *longhorn.Volume
	replicas map[string]*longhorn.Replica
	daemons  []*v1.Pod
	nodes    map[string]*longhorn.Node

	// schedule state
	expectedNodes map[string]*longhorn.Node
	// scheduler exception
	err bool
	// couldn't schedule replica
	isNilReplica bool
}

func generateSchedulerTestCase() *ReplicaSchedulerTestCase {
	v := newVolume(TestVolumeName, 2)
	replica1 := newReplicaForVolume(v)
	replica2 := newReplicaForVolume(v)
	replicas := map[string]*longhorn.Replica{
		replica1.Name: replica1,
		replica2.Name: replica2,
	}
	return &ReplicaSchedulerTestCase{
		volume:   v,
		replicas: replicas,
	}
}

func (s *TestSuite) TestReplicaScheduler(c *C) {
	testCases := map[string]*ReplicaSchedulerTestCase{}
	// Test only node1 could schedule replica
	tc := generateSchedulerTestCase()
	daemon1 := newDaemonPod(v1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	daemon2 := newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2)
	daemon3 := newDaemonPod(v1.PodRunning, TestDaemon3, TestNamespace, TestNode3, TestIP3)
	tc.daemons = []*v1.Pod{
		daemon1,
		daemon2,
		daemon3,
	}
	node1 := newNode(TestNode1, TestNamespace, true, types.NodeStateUp)
	disk := newDisk(TestDefaultDataPath, true, TestDiskSize, 0)
	node1.Spec.Disks = map[string]types.DiskSpec{
		TestDiskID1: disk,
	}
	node1.Status.DiskStatus = map[string]types.DiskStatus{
		TestDiskID1: {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
		},
	}
	node2 := newNode(TestNode2, TestNamespace, false, types.NodeStateUp)
	disk = newDisk(TestDefaultDataPath, true, TestDiskSize, 0)
	node2.Spec.Disks = map[string]types.DiskSpec{
		TestDiskID1: disk,
	}
	node2.Status.DiskStatus = map[string]types.DiskStatus{
		TestDiskID1: {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
		},
	}
	node3 := newNode(TestNode3, TestNamespace, true, types.NodeStateDown)
	disk = newDisk(TestDefaultDataPath, true, TestDiskSize, 0)
	node3.Spec.Disks = map[string]types.DiskSpec{
		TestDiskID1: disk,
	}
	node3.Status.DiskStatus = map[string]types.DiskStatus{
		TestDiskID1: {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
		},
	}
	nodes := map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
		TestNode3: node3,
	}
	tc.nodes = nodes
	expectedNodes := map[string]*longhorn.Node{
		TestNode1: node1,
	}
	tc.expectedNodes = expectedNodes
	tc.err = false
	tc.isNilReplica = false
	testCases["nodes could not schedule"] = tc

	// Test no disks on each nodes, volume should not schedule to any node
	tc = generateSchedulerTestCase()
	daemon1 = newDaemonPod(v1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	daemon2 = newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2)
	tc.daemons = []*v1.Pod{
		daemon1,
		daemon2,
	}
	node1 = newNode(TestNode1, TestNamespace, true, types.NodeStateUp)
	node2 = newNode(TestNode2, TestNamespace, true, types.NodeStateUp)
	nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	expectedNodes = map[string]*longhorn.Node{}
	tc.expectedNodes = expectedNodes
	tc.err = false
	tc.isNilReplica = true
	testCases["there's no disk for replica"] = tc

	// Test anti affinity nodes, replica should schedule to both node1 and node2
	tc = generateSchedulerTestCase()
	daemon1 = newDaemonPod(v1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	daemon2 = newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2)
	tc.daemons = []*v1.Pod{
		daemon1,
		daemon2,
	}
	node1 = newNode(TestNode1, TestNamespace, true, types.NodeStateUp)
	disk = newDisk(TestDefaultDataPath, true, TestDiskSize, 0)
	disk2 := newDisk(TestDefaultDataPath, true, TestDiskSize, TestDiskSize)
	node1.Spec.Disks = map[string]types.DiskSpec{
		TestDiskID1: disk,
		TestDiskID2: disk2,
	}

	node1.Status.DiskStatus = map[string]types.DiskStatus{
		TestDiskID1: {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			State:            types.DiskStateSchedulable,
		},
		TestDiskID2: {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			State:            types.DiskStateSchedulable,
		},
	}
	expectNode1 := newNode(TestNode1, TestNamespace, true, types.NodeStateUp)
	expectNode1.Spec.Disks = map[string]types.DiskSpec{
		TestDiskID1: disk,
	}
	node2 = newNode(TestNode2, TestNamespace, true, types.NodeStateUp)
	disk = newDisk(TestDefaultDataPath, true, TestDiskSize, 0)
	node2.Spec.Disks = map[string]types.DiskSpec{
		TestDiskID1: disk,
	}
	node2.Status.DiskStatus = map[string]types.DiskStatus{
		TestDiskID1: {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			State:            types.DiskStateSchedulable,
		},
	}
	nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	expectedNodes = map[string]*longhorn.Node{
		TestNode1: expectNode1,
		TestNode2: node2,
	}
	tc.expectedNodes = expectedNodes
	tc.err = false
	tc.isNilReplica = false
	testCases["anti-affinity nodes"] = tc

	// Test scheduler error when replica.NodeID is not ""
	tc = generateSchedulerTestCase()
	replicas := tc.replicas
	for _, replica := range replicas {
		replica.Spec.NodeID = TestNode1
	}
	tc.err = true
	tc.isNilReplica = true
	testCases["scheduler error when replica has NodeID"] = tc

	// Test no available disks
	tc = generateSchedulerTestCase()
	daemon1 = newDaemonPod(v1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	daemon2 = newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2)
	tc.daemons = []*v1.Pod{
		daemon1,
		daemon2,
	}
	node1 = newNode(TestNode1, TestNamespace, true, types.NodeStateUp)
	node1.Status.DiskStatus = map[string]types.DiskStatus{
		TestDiskID1: {
			StorageAvailable: 0,
			StorageScheduled: 0,
		},
	}
	node2 = newNode(TestNode2, TestNamespace, true, types.NodeStateUp)
	node2.Status.DiskStatus = map[string]types.DiskStatus{
		TestDiskID1: {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
		},
	}
	nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	expectedNodes = map[string]*longhorn.Node{}
	tc.expectedNodes = expectedNodes
	tc.err = false
	tc.isNilReplica = true
	testCases["there's no available disks for scheduling"] = tc

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		vIndexer := lhInformerFactory.Longhorn().V1alpha1().Volumes().Informer().GetIndexer()
		rIndexer := lhInformerFactory.Longhorn().V1alpha1().Replicas().Informer().GetIndexer()
		nIndexer := lhInformerFactory.Longhorn().V1alpha1().Nodes().Informer().GetIndexer()
		pIndexer := kubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

		s := newReplicaScheduler(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient)
		// create daemon pod
		for _, daemon := range tc.daemons {
			p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(daemon)
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
		// create volume
		volume, err := lhClient.LonghornV1alpha1().Volumes(TestNamespace).Create(tc.volume)
		c.Assert(err, IsNil)
		c.Assert(volume, NotNil)
		vIndexer.Add(volume)
		// validate scheduler
		for _, replica := range tc.replicas {
			r, err := lhClient.LonghornV1alpha1().Replicas(TestNamespace).Create(replica)
			c.Assert(err, IsNil)
			c.Assert(r, NotNil)
			rIndexer.Add(r)

			sr, err := s.ScheduleReplica(r, tc.replicas)
			if tc.err {
				c.Assert(err, NotNil)
			} else {
				if tc.isNilReplica {
					c.Assert(sr, IsNil)
				} else {
					c.Assert(err, IsNil)
					c.Assert(sr, NotNil)
					c.Assert(sr.Spec.NodeID, Not(Equals), "")
					c.Assert(sr.Spec.DataPath, Not(Equals), "")
					c.Assert(sr.Spec.DiskID, Not(Equals), "")
					tc.replicas[sr.Name] = sr
					// check expected node
					for nname, node := range tc.expectedNodes {
						if sr.Spec.NodeID == nname {
							c.Assert(sr.Spec.DataPath, Matches, node.Spec.Disks[sr.Spec.DiskID].Path+"/.*")
							delete(tc.expectedNodes, nname)
						}
					}
				}
			}
		}
		c.Assert(len(tc.expectedNodes), Equals, 0)
	}
}
