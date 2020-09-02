package scheduler

import (
	"fmt"
	"testing"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"

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

	TestDefaultDataPath = "/var/lib/longhorn"

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

	volumeInformer := lhInformerFactory.Longhorn().V1beta1().Volumes()
	engineInformer := lhInformerFactory.Longhorn().V1beta1().Engines()
	replicaInformer := lhInformerFactory.Longhorn().V1beta1().Replicas()
	engineImageInformer := lhInformerFactory.Longhorn().V1beta1().EngineImages()
	nodeInformer := lhInformerFactory.Longhorn().V1beta1().Nodes()
	diskInformer := lhInformerFactory.Longhorn().V1beta1().Disks()
	settingInformer := lhInformerFactory.Longhorn().V1beta1().Settings()
	imInformer := lhInformerFactory.Longhorn().V1beta1().InstanceManagers()

	podInformer := kubeInformerFactory.Core().V1().Pods()
	cronJobInformer := kubeInformerFactory.Batch().V1beta1().CronJobs()
	daemonSetInformer := kubeInformerFactory.Apps().V1().DaemonSets()
	deploymentInformer := kubeInformerFactory.Apps().V1().Deployments()
	persistentVolumeInformer := kubeInformerFactory.Core().V1().PersistentVolumes()
	persistentVolumeClaimInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()
	kubeNodeInformer := kubeInformerFactory.Core().V1().Nodes()
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

func newNode(name, namespace, diskName string, allowScheduling bool, status types.ConditionStatus) *longhorn.Node {
	node := &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: types.NodeSpec{
			AllowScheduling: allowScheduling,
			DiskPathMap:     map[string]struct{}{},
		},
		Status: types.NodeStatus{
			DiskPathIDMap: map[string]string{},
			Conditions: map[string]types.Condition{
				types.NodeConditionTypeSchedulable: newCondition(types.NodeConditionTypeSchedulable, status),
				types.NodeConditionTypeReady:       newCondition(types.NodeConditionTypeReady, status),
			},
		},
	}
	if diskName != "" {
		node.Spec.DiskPathMap[TestDefaultDataPath] = struct{}{}
		node.Status.DiskPathIDMap[TestDefaultDataPath] = diskName
	}

	return node
}

func newDisk(name, nodeID string) *longhorn.Disk {
	return &longhorn.Disk{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
			Labels: map[string]string{
				types.LonghornNodeKey: nodeID,
			},
		},
		Spec: types.DiskSpec{
			AllowScheduling:   true,
			EvictionRequested: false,
			StorageReserved:   0,
		},
		Status: types.DiskStatus{
			OwnerID:          nodeID,
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: map[string]types.Condition{
				types.DiskConditionTypeReady:       newCondition(types.DiskConditionTypeReady, types.ConditionStatusTrue),
				types.DiskConditionTypeSchedulable: newCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusTrue),
			},
			NodeID: nodeID,
			Path:   TestDefaultDataPath,
			State:  types.DiskStateConnected,
		},
	}
}

func getDiskName(nodeID, diskID string) string {
	return fmt.Sprintf("%s-%s", nodeID, diskID)
}

func newCondition(conditionType string, status types.ConditionStatus) types.Condition {
	return types.Condition{
		Type:    conditionType,
		Status:  status,
		Reason:  "",
		Message: "",
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
			StaleReplicaTimeout: TestVolumeStaleTimeout,
			EngineImage:         TestEngineImage,
		},
		Status: types.VolumeStatus{
			OwnerID: TestOwnerID1,
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
				VolumeName:  v.Name,
				VolumeSize:  v.Spec.Size,
				EngineImage: TestEngineImage,
				DesireState: types.InstanceStateStopped,
			},
		},
		Status: types.ReplicaStatus{
			InstanceStatus: types.InstanceStatus{
				OwnerID: v.Status.OwnerID,
			},
		},
	}
}

func initSettings(name, value string) *longhorn.Setting {
	setting := &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Setting: types.Setting{
			Value: value,
		},
	}
	return setting
}

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
}

type ReplicaSchedulerTestCase struct {
	volume                            *longhorn.Volume
	replicas                          map[string]*longhorn.Replica
	daemons                           []*v1.Pod
	nodes                             map[string]*longhorn.Node
	disks                             map[string]*longhorn.Disk
	storageOverProvisioningPercentage string
	storageMinimalAvailablePercentage string
	replicaNodeSoftAntiAffinity       string

	// schedule state
	expectedDisks map[string]*longhorn.Disk
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
	tc.daemons = []*v1.Pod{
		newDaemonPod(v1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1),
		newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2),
		newDaemonPod(v1.PodRunning, TestDaemon3, TestNamespace, TestNode3, TestIP3),
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: newNode(TestNode1, TestNamespace, getDiskName(TestNode1, TestDiskID1), true, types.ConditionStatusTrue),
		TestNode2: newNode(TestNode2, TestNamespace, getDiskName(TestNode2, TestDiskID1), false, types.ConditionStatusTrue),
		TestNode3: newNode(TestNode3, TestNamespace, getDiskName(TestNode3, TestDiskID1), true, types.ConditionStatusFalse),
	}
	tc.disks = map[string]*longhorn.Disk{
		getDiskName(TestNode1, TestDiskID1): newDisk(getDiskName(TestNode1, TestDiskID1), TestNode1),
		getDiskName(TestNode2, TestDiskID1): newDisk(getDiskName(TestNode2, TestDiskID1), TestNode2),
		getDiskName(TestNode3, TestDiskID1): newDisk(getDiskName(TestNode3, TestDiskID1), TestNode3),
	}
	tc.expectedDisks = map[string]*longhorn.Disk{
		getDiskName(TestNode1, TestDiskID1): tc.disks[getDiskName(TestNode1, TestDiskID1)],
	}
	tc.err = false
	tc.isNilReplica = false
	// Set replica node soft anti-affinity
	tc.replicaNodeSoftAntiAffinity = "true"
	testCases["nodes could not schedule"] = tc

	// Test no disks on each nodes, volume should not schedule to any node
	tc = generateSchedulerTestCase()
	tc.daemons = []*v1.Pod{
		newDaemonPod(v1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1),
		newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2),
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: newNode(TestNode1, TestNamespace, "", true, types.ConditionStatusTrue),
		TestNode2: newNode(TestNode2, TestNamespace, "", true, types.ConditionStatusTrue),
	}
	tc.expectedDisks = map[string]*longhorn.Disk{}
	tc.err = false
	tc.isNilReplica = true
	testCases["there's no disk for replica"] = tc

	// Test anti affinity nodes, replica should schedule to both node1 and node2
	tc = generateSchedulerTestCase()
	tc.daemons = []*v1.Pod{
		newDaemonPod(v1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1),
		newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2),
	}
	disk1Node1 := newDisk(getDiskName(TestNode1, TestDiskID1), TestNode1)
	disk1Node1.Status.Path = "/path1"
	disk2Node1 := newDisk(getDiskName(TestNode1, TestDiskID2), TestNode1)
	disk2Node1.Spec.StorageReserved = TestDiskSize
	disk2Node1.Status.Path = "/path2"
	disk1Node2 := newDisk(getDiskName(TestNode2, TestDiskID1), TestNode2)
	disk1Node2.Status.Path = "/path1"
	disk2Node2 := newDisk(getDiskName(TestNode2, TestDiskID2), TestNode2)
	disk2Node2.Status.Path = "/path2"
	disk2Node2.Status.Conditions[types.DiskConditionTypeSchedulable] = newCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusFalse)
	node1 := newNode(TestNode1, TestNamespace, "", true, types.ConditionStatusTrue)
	node1.Spec.DiskPathMap = map[string]struct{}{
		disk1Node1.Status.Path: struct{}{},
		disk2Node1.Status.Path: struct{}{},
	}
	node1.Status.DiskPathIDMap = map[string]string{
		disk1Node1.Status.Path: disk1Node1.Name,
		disk2Node1.Status.Path: disk2Node1.Name,
	}
	node2 := newNode(TestNode2, TestNamespace, "", true, types.ConditionStatusTrue)
	node2.Spec.DiskPathMap = map[string]struct{}{
		disk1Node2.Status.Path: struct{}{},
		disk2Node2.Status.Path: struct{}{},
	}
	node2.Status.DiskPathIDMap = map[string]string{
		disk1Node2.Status.Path: disk1Node2.Name,
		disk2Node2.Status.Path: disk2Node2.Name,
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.disks = map[string]*longhorn.Disk{
		disk1Node1.Name: disk1Node1,
		disk2Node1.Name: disk2Node1,
		disk1Node2.Name: disk1Node2,
		disk2Node2.Name: disk2Node2,
	}
	tc.expectedDisks = map[string]*longhorn.Disk{
		disk1Node1.Name: disk1Node1,
		disk1Node2.Name: disk1Node2,
	}
	tc.err = false
	tc.isNilReplica = false
	testCases["anti-affinity nodes"] = tc

	// Test scheduler error when replica.NodeID is not ""
	tc = generateSchedulerTestCase()
	replicas := tc.replicas
	for _, replica := range replicas {
		replica.Spec.DiskID = TestDiskID1
	}
	tc.err = true
	tc.isNilReplica = true
	testCases["scheduler error when replica has DiskID"] = tc

	// Test no available disks
	tc = generateSchedulerTestCase()
	tc.daemons = []*v1.Pod{
		newDaemonPod(v1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1),
		newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2),
	}
	disk1Node1 = newDisk(getDiskName(TestNode1, TestDiskID1), TestNode1)
	disk1Node1.Status.StorageAvailable = 0
	disk1Node2 = newDisk(getDiskName(TestNode1, TestDiskID2), TestNode2)
	disk1Node2.Status.StorageAvailable = 0
	disk1Node2.Status.StorageScheduled = TestDiskAvailableSize
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: newNode(TestNode1, TestNamespace, disk1Node1.Name, true, types.ConditionStatusTrue),
		TestNode2: newNode(TestNode2, TestNamespace, disk1Node2.Name, true, types.ConditionStatusTrue),
	}
	tc.disks = map[string]*longhorn.Disk{
		disk1Node1.Name: disk1Node1,
		disk1Node2.Name: disk1Node2,
	}
	tc.expectedDisks = map[string]*longhorn.Disk{}
	tc.err = false
	tc.isNilReplica = true
	tc.storageOverProvisioningPercentage = "0"
	tc.storageMinimalAvailablePercentage = "100"
	testCases["there's no available disks for scheduling"] = tc

	// Test no available disks due to volume.Status.ActualSize
	tc = generateSchedulerTestCase()
	tc.daemons = []*v1.Pod{
		newDaemonPod(v1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1),
		newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2),
	}
	disk1Node1 = newDisk(getDiskName(TestNode1, TestDiskID1), TestNode1)
	disk1Node1.Status.Path = "/path1"
	disk2Node1 = newDisk(getDiskName(TestNode1, TestDiskID2), TestNode1)
	disk2Node1.Status.Path = "/path2"
	disk1Node2 = newDisk(getDiskName(TestNode2, TestDiskID1), TestNode2)
	disk1Node2.Status.Path = "/path1"
	disk2Node2 = newDisk(getDiskName(TestNode2, TestDiskID2), TestNode2)
	disk2Node2.Status.Path = "/path2"
	node1 = newNode(TestNode1, TestNamespace, "", true, types.ConditionStatusTrue)
	node1.Spec.DiskPathMap = map[string]struct{}{
		disk1Node1.Status.Path: struct{}{},
		disk2Node1.Status.Path: struct{}{},
	}
	node1.Status.DiskPathIDMap = map[string]string{
		disk1Node1.Status.Path: disk1Node1.Name,
		disk2Node1.Status.Path: disk2Node1.Name,
	}
	node2 = newNode(TestNode2, TestNamespace, "", true, types.ConditionStatusTrue)
	node2.Spec.DiskPathMap = map[string]struct{}{
		disk1Node2.Status.Path: struct{}{},
		disk2Node2.Status.Path: struct{}{},
	}
	node2.Status.DiskPathIDMap = map[string]string{
		disk1Node2.Status.Path: disk1Node2.Name,
		disk2Node2.Status.Path: disk2Node2.Name,
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.disks = map[string]*longhorn.Disk{
		disk1Node1.Name: disk1Node1,
		disk2Node1.Name: disk2Node1,
		disk1Node2.Name: disk1Node2,
		disk2Node2.Name: disk2Node2,
	}
	tc.volume.Status.ActualSize = TestDiskAvailableSize - TestDiskSize*0.2
	tc.expectedDisks = map[string]*longhorn.Disk{}
	tc.err = false
	tc.isNilReplica = true
	tc.storageOverProvisioningPercentage = "200"
	tc.storageMinimalAvailablePercentage = "20"
	testCases["there's no available disks for scheduling due to required storage"] = tc

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		vIndexer := lhInformerFactory.Longhorn().V1beta1().Volumes().Informer().GetIndexer()
		rIndexer := lhInformerFactory.Longhorn().V1beta1().Replicas().Informer().GetIndexer()
		nIndexer := lhInformerFactory.Longhorn().V1beta1().Nodes().Informer().GetIndexer()
		dIndexer := lhInformerFactory.Longhorn().V1beta1().Disks().Informer().GetIndexer()
		sIndexer := lhInformerFactory.Longhorn().V1beta1().Settings().Informer().GetIndexer()
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
		// create volume
		volume, err := lhClient.LonghornV1beta1().Volumes(TestNamespace).Create(tc.volume)
		c.Assert(err, IsNil)
		c.Assert(volume, NotNil)
		vIndexer.Add(volume)
		// set settings
		if tc.storageOverProvisioningPercentage != "" && tc.storageMinimalAvailablePercentage != "" {
			s := initSettings(string(types.SettingNameStorageOverProvisioningPercentage), tc.storageOverProvisioningPercentage)
			setting, err := lhClient.LonghornV1beta1().Settings(TestNamespace).Create(s)
			c.Assert(err, IsNil)
			sIndexer.Add(setting)

			s = initSettings(string(types.SettingNameStorageMinimalAvailablePercentage), tc.storageMinimalAvailablePercentage)
			setting, err = lhClient.LonghornV1beta1().Settings(TestNamespace).Create(s)
			c.Assert(err, IsNil)
			sIndexer.Add(setting)
		}
		// Set replica node soft anti-affinity setting
		if tc.replicaNodeSoftAntiAffinity != "" {
			s := initSettings(
				string(types.SettingNameReplicaSoftAntiAffinity),
				tc.replicaNodeSoftAntiAffinity)
			setting, err :=
				lhClient.LonghornV1beta1().Settings(TestNamespace).Create(s)
			c.Assert(err, IsNil)
			sIndexer.Add(setting)
		}
		// validate scheduler
		for _, replica := range tc.replicas {
			r, err := lhClient.LonghornV1beta1().Replicas(TestNamespace).Create(replica)
			c.Assert(err, IsNil)
			c.Assert(r, NotNil)
			rIndexer.Add(r)

			sr, err := s.ScheduleReplica(r, tc.replicas, volume)
			if tc.err {
				c.Assert(err, NotNil)
			} else {
				if tc.isNilReplica {
					c.Assert(sr, IsNil)
				} else {
					c.Assert(err, IsNil)
					c.Assert(sr, NotNil)
					c.Assert(sr.Spec.DiskID, Not(Equals), "")
					c.Assert(sr.Spec.NodeID, Not(Equals), "")
					c.Assert(sr.Spec.DataPath, Not(Equals), "")
					tc.replicas[sr.Name] = sr
					// check expected disk
					for diskName, disk := range tc.expectedDisks {
						if sr.Spec.DiskID == diskName {
							c.Assert(sr.Spec.DataPath, Matches, disk.Status.Path+"/.*")
							delete(tc.expectedDisks, diskName)
							fmt.Printf("Test %v: deleting disk %v", name, diskName)
						}
					}
				}
			}
		}
		c.Assert(len(tc.expectedDisks), Equals, 0)
	}
}
