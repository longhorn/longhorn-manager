package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"

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

	// TestDiskID1           = "diskID1"
	// TestDiskID2           = "diskID2"
	TestDiskSize          = 5000000000
	TestDiskAvailableSize = 3000000000

	TestZone1 = "test-zone-1"
	TestZone2 = "test-zone-2"

	TestTimeNow          = "2015-01-02T00:00:00Z"
	TestTimeOneMinuteAgo = "2015-01-01T23:59:00Z"
)

var longhornFinalizerKey = longhorn.SchemeGroupVersion.Group

func newReplicaScheduler(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories) *ReplicaScheduler {
	fmt.Printf("testing NewReplicaScheduler\n")

	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	rcs := NewReplicaScheduler(ds)
	rcs.nowHandler = getTestNow
	return rcs
}

func newDaemonPod(phase corev1.PodPhase, name, namespace, nodeID, podIP string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app": "longhorn-manager",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: nodeID,
		},
		Status: corev1.PodStatus{
			Phase: phase,
			PodIP: podIP,
		},
	}
}

func newNode(name, namespace, zone string, allowScheduling bool, status longhorn.ConditionStatus) *longhorn.Node {
	return &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: longhorn.NodeSpec{
			AllowScheduling: allowScheduling,
		},
		Status: longhorn.NodeStatus{
			Conditions: []longhorn.Condition{
				newCondition(longhorn.NodeConditionTypeSchedulable, status),
				newCondition(longhorn.NodeConditionTypeReady, status),
			},
			Zone: zone,
		},
	}
}

func newEngineImage(image string, state longhorn.EngineImageState) *longhorn.EngineImage {
	return &longhorn.EngineImage{
		ObjectMeta: metav1.ObjectMeta{
			Name:       types.GetEngineImageChecksumName(image),
			Namespace:  TestNamespace,
			UID:        uuid.NewUUID(),
			Finalizers: []string{longhornFinalizerKey},
		},
		Spec: longhorn.EngineImageSpec{
			Image: image,
		},
		Status: longhorn.EngineImageStatus{
			OwnerID: TestNode1,
			State:   state,
			EngineVersionDetails: longhorn.EngineVersionDetails{
				Version:   "latest",
				GitCommit: "latest",

				CLIAPIVersion:           4,
				CLIAPIMinVersion:        3,
				ControllerAPIVersion:    3,
				ControllerAPIMinVersion: 3,
				DataFormatVersion:       1,
				DataFormatMinVersion:    1,
			},
			Conditions: []longhorn.Condition{
				{
					Type:   longhorn.EngineImageConditionTypeReady,
					Status: longhorn.ConditionStatusTrue,
				},
			},
			NodeDeploymentMap: map[string]bool{},
		},
	}
}

func newCondition(conditionType string, status longhorn.ConditionStatus) longhorn.Condition {
	return longhorn.Condition{
		Type:    conditionType,
		Status:  status,
		Reason:  "",
		Message: "",
	}
}

func newDisk(path string, allowScheduling bool, storageReserved int64) longhorn.DiskSpec {
	return longhorn.DiskSpec{
		Type:            longhorn.DiskTypeFilesystem,
		Path:            path,
		DiskDriver:      longhorn.DiskDriverNone,
		AllowScheduling: allowScheduling,
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
		Spec: longhorn.VolumeSpec{
			NumberOfReplicas:            replicaCount,
			Size:                        TestVolumeSize,
			StaleReplicaTimeout:         TestVolumeStaleTimeout,
			Image:                       TestEngineImage,
			ReplicaSoftAntiAffinity:     longhorn.ReplicaSoftAntiAffinityDefault,
			ReplicaZoneSoftAntiAffinity: longhorn.ReplicaZoneSoftAntiAffinityDefault,
			ReplicaDiskSoftAntiAffinity: longhorn.ReplicaDiskSoftAntiAffinityDefault,
			DataEngine:                  longhorn.DataEngineTypeV1,
		},
		Status: longhorn.VolumeStatus{
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
		Spec: longhorn.ReplicaSpec{
			InstanceSpec: longhorn.InstanceSpec{
				VolumeName:  v.Name,
				VolumeSize:  v.Spec.Size,
				Image:       TestEngineImage,
				DesireState: longhorn.InstanceStateStopped,
			},
		},
		Status: longhorn.ReplicaStatus{
			InstanceStatus: longhorn.InstanceStatus{
				OwnerID: v.Status.OwnerID,
			},
		},
	}
}

func getDiskID(nodeID, index string) string {
	return fmt.Sprintf("%s-disk%s", nodeID, index)
}

func initSettings(name, value string) *longhorn.Setting {
	setting := &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Value: value,
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
	daemons                           []*corev1.Pod
	nodes                             map[string]*longhorn.Node
	engineImage                       *longhorn.EngineImage
	storageOverProvisioningPercentage string
	storageMinimalAvailablePercentage string
	replicaNodeSoftAntiAffinity       string
	replicaZoneSoftAntiAffinity       string
	replicaDiskSoftAntiAffinity       string
	ReplicaReplenishmentWaitInterval  string

	// some test cases only try to schedule a subset of a volume's replicas
	allReplicas        map[string]*longhorn.Replica
	replicasToSchedule map[string]struct{}

	// schedule state
	expectedNodes map[string]*longhorn.Node
	expectedDisks map[string]struct{}
	// scheduler exception
	err bool
	// first replica expected to fail scheduling
	//   -1 = default in constructor, don't fail to schedule
	//    0 = fail to schedule first
	//    1 = schedule first, fail to schedule second
	//    etc...
	firstNilReplica int
}

func generateSchedulerTestCase() *ReplicaSchedulerTestCase {
	v := newVolume(TestVolumeName, 2)
	replica1 := newReplicaForVolume(v)
	replica2 := newReplicaForVolume(v)
	allReplicas := map[string]*longhorn.Replica{
		replica1.Name: replica1,
		replica2.Name: replica2,
	}
	replicasToSchedule := map[string]struct{}{}
	for name := range allReplicas {
		replicasToSchedule[name] = struct{}{}
	}
	engineImage := newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed)
	return &ReplicaSchedulerTestCase{
		volume:             v,
		engineImage:        engineImage,
		allReplicas:        allReplicas,
		replicasToSchedule: replicasToSchedule,
		firstNilReplica:    -1,
	}
}

func (s *TestSuite) TestReplicaScheduler(c *C) {
	testCases := map[string]*ReplicaSchedulerTestCase{}
	// Test only node1 could schedule replica
	tc := generateSchedulerTestCase()
	daemon1 := newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	daemon2 := newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2)
	daemon3 := newDaemonPod(corev1.PodRunning, TestDaemon3, TestNamespace, TestNode3, TestIP3)
	tc.daemons = []*corev1.Pod{
		daemon1,
		daemon2,
		daemon3,
	}
	node1 := newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	disk := newDisk(TestDefaultDataPath, true, 0)
	node1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
	}
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode1, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode1, "1"),
			Type:     longhorn.DiskTypeFilesystem,
		},
	}
	tc.engineImage.Status.NodeDeploymentMap[node1.Name] = true
	node2 := newNode(TestNode2, TestNamespace, TestZone1, false, longhorn.ConditionStatusTrue)
	disk = newDisk(TestDefaultDataPath, true, 0)
	node2.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode2, "1"): disk,
	}
	node2.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode2, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			DiskUUID:         getDiskID(TestNode2, "1"),
			Type:             longhorn.DiskTypeFilesystem,
		},
	}
	tc.engineImage.Status.NodeDeploymentMap[node2.Name] = true
	node3 := newNode(TestNode3, TestNamespace, TestZone1, true, longhorn.ConditionStatusFalse)
	disk = newDisk(TestDefaultDataPath, true, 0)
	node3.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode3, "1"): disk,
	}
	node3.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode3, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			DiskUUID:         getDiskID(TestNode3, "1"),
			Type:             longhorn.DiskTypeFilesystem,
		},
	}
	tc.engineImage.Status.NodeDeploymentMap[node3.Name] = true
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
	tc.firstNilReplica = -1
	// Set replica node soft anti-affinity
	tc.replicaNodeSoftAntiAffinity = "true"
	testCases["nodes could not schedule"] = tc

	// Test no disks on each nodes, volume should not schedule to any node
	tc = generateSchedulerTestCase()
	daemon1 = newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	daemon2 = newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2)
	tc.daemons = []*corev1.Pod{
		daemon1,
		daemon2,
	}
	node1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node1.Name] = true
	node2 = newNode(TestNode2, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node2.Name] = true
	nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	expectedNodes = map[string]*longhorn.Node{}
	tc.expectedNodes = expectedNodes
	tc.err = false
	tc.firstNilReplica = 0
	testCases["there's no disk for replica"] = tc

	// Test engine image is not deployed on any node
	tc = generateSchedulerTestCase()
	daemon1 = newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	daemon2 = newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2)
	tc.daemons = []*corev1.Pod{
		daemon1,
		daemon2,
	}
	node1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	disk = newDisk(TestDefaultDataPath, true, 0)
	node1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
	}
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode1, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode1, "1"),
			Type:     longhorn.DiskTypeFilesystem,
		},
	}
	node2 = newNode(TestNode2, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	disk = newDisk(TestDefaultDataPath, true, 0)
	node2.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode2, "1"): disk,
	}
	node2.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode2, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode2, "1"),
			Type:     longhorn.DiskTypeFilesystem,
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
	tc.firstNilReplica = 0
	testCases["there's no engine image deployed on any node"] = tc

	// Test anti affinity nodes, replica should schedule to both node1 and node2
	tc = generateSchedulerTestCase()
	daemon1 = newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	daemon2 = newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2)
	tc.daemons = []*corev1.Pod{
		daemon1,
		daemon2,
	}
	node1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node1.Name] = true
	disk = newDisk(TestDefaultDataPath, true, 0)
	disk2 := newDisk(TestDefaultDataPath, true, TestDiskSize)
	node1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
		getDiskID(TestNode1, "2"): disk2,
	}

	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode1, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode1, "1"),
			Type:     longhorn.DiskTypeFilesystem,
		},
		getDiskID(TestNode1, "2"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode1, "2"),
			Type:     longhorn.DiskTypeFilesystem,
		},
	}
	expectNode1 := newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	expectNode1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
	}
	node2 = newNode(TestNode2, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node2.Name] = true
	disk = newDisk(TestDefaultDataPath, true, 0)
	disk2 = newDisk(TestDefaultDataPath, true, 0)
	node2.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode2, "1"): disk,
		getDiskID(TestNode2, "2"): disk2,
	}
	node2.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode2, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode2, "1"),
			Type:     longhorn.DiskTypeFilesystem,
		},
		getDiskID(TestNode2, "2"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusFalse),
			},
			DiskUUID: getDiskID(TestNode2, "2"),
			Type:     longhorn.DiskTypeFilesystem,
		},
	}
	expectNode2 := newNode(TestNode2, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	expectNode2.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode2, "1"): disk,
	}
	nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	expectedNodes = map[string]*longhorn.Node{
		TestNode1: expectNode1,
		TestNode2: expectNode2,
	}
	tc.expectedNodes = expectedNodes
	tc.err = false
	tc.firstNilReplica = -1
	testCases["anti-affinity nodes"] = tc

	// Test scheduler error when replica.NodeID is not ""
	tc = generateSchedulerTestCase()
	replicas := tc.allReplicas
	for _, replica := range replicas {
		replica.Spec.NodeID = TestNode1
	}
	tc.err = true
	tc.firstNilReplica = 0
	testCases["scheduler error when replica has NodeID"] = tc

	// Test no available disks
	tc = generateSchedulerTestCase()
	daemon1 = newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	daemon2 = newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2)
	tc.daemons = []*corev1.Pod{
		daemon1,
		daemon2,
	}
	node1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node1.Name] = true
	disk = newDisk(TestDefaultDataPath, true, TestDiskSize)
	node1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
	}
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode1, "1"): {
			StorageAvailable: 0,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode1, "1"),
			Type:     longhorn.DiskTypeFilesystem,
		},
	}
	node2 = newNode(TestNode2, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node1.Name] = true
	disk = newDisk(TestDefaultDataPath, true, 0)
	disk2 = newDisk(TestDefaultDataPath, true, 0)
	node2.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode2, "1"): disk,
		getDiskID(TestNode2, "2"): disk2,
	}
	node2.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode2, "1"): {
			StorageAvailable: 0,
			StorageScheduled: TestDiskAvailableSize,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode2, "1"),
			Type:     longhorn.DiskTypeFilesystem,
		},
		getDiskID(TestNode2, "2"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusFalse),
			},
			DiskUUID: getDiskID(TestNode2, "2"),
			Type:     longhorn.DiskTypeFilesystem,
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
	tc.firstNilReplica = 0
	tc.storageOverProvisioningPercentage = "0"
	tc.storageMinimalAvailablePercentage = "100"
	testCases["there's no available disks for scheduling"] = tc

	// Test no available disks due to volume.Status.ActualSize
	tc = generateSchedulerTestCase()
	daemon1 = newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	daemon2 = newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2)
	tc.daemons = []*corev1.Pod{
		daemon1,
		daemon2,
	}
	node1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node1.Name] = true
	disk = newDisk(TestDefaultDataPath, true, TestDiskSize)
	node1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
	}
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode1, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode1, "1"),
			Type:     longhorn.DiskTypeFilesystem,
		},
	}
	node2 = newNode(TestNode2, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node2.Name] = true
	disk = newDisk(TestDefaultDataPath, true, 0)
	disk2 = newDisk(TestDefaultDataPath, true, 0)
	node2.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode2, "1"): disk,
		getDiskID(TestNode2, "2"): disk2,
	}
	node2.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode2, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode2, "1"),
			Type:     longhorn.DiskTypeFilesystem,
		},
		getDiskID(TestNode2, "2"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode2, "2"),
			Type:     longhorn.DiskTypeFilesystem,
		},
	}
	nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	tc.volume.Status.ActualSize = TestDiskAvailableSize - TestDiskSize*0.2
	expectedNodes = map[string]*longhorn.Node{}
	tc.expectedNodes = expectedNodes
	tc.err = false
	tc.firstNilReplica = 0
	tc.storageOverProvisioningPercentage = "200"
	tc.storageMinimalAvailablePercentage = "20"
	testCases["there's no available disks for scheduling due to required storage"] = tc

	// Test schedule to disk with the most usable storage
	tc = generateSchedulerTestCase()
	daemon1 = newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	daemon2 = newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2)
	tc.daemons = []*corev1.Pod{
		daemon1,
		daemon2,
	}
	node1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node1.Name] = true
	disk = newDisk(TestDefaultDataPath, true, 0)
	disk2 = newDisk(TestDefaultDataPath, true, 0)
	node1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
		getDiskID(TestNode1, "2"): disk2,
	}
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode1, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode1, "1"),
			Type:     longhorn.DiskTypeFilesystem,
		},
		getDiskID(TestNode1, "2"): {
			StorageAvailable: TestDiskAvailableSize / 2,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode1, "2"),
			Type:     longhorn.DiskTypeFilesystem,
		},
	}
	expectNode1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	expectNode1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
	}
	node2 = newNode(TestNode2, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node2.Name] = true
	disk = newDisk(TestDefaultDataPath, true, 0)
	disk2 = newDisk(TestDefaultDataPath, true, 0)
	node2.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode2, "1"): disk,
		getDiskID(TestNode2, "2"): disk2,
	}
	node2.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode2, "1"): {
			StorageAvailable: TestDiskAvailableSize / 2,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode2, "1"),
			Type:     longhorn.DiskTypeFilesystem,
		},
		getDiskID(TestNode2, "2"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode2, "2"),
			Type:     longhorn.DiskTypeFilesystem,
		},
	}
	expectNode2 = newNode(TestNode2, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	expectNode2.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode2, "2"): disk2,
	}
	nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	expectedNodes = map[string]*longhorn.Node{
		TestNode1: expectNode1,
		TestNode2: expectNode2,
	}
	tc.expectedNodes = expectedNodes
	tc.err = false
	tc.firstNilReplica = -1
	testCases["schedule to disk with the most usable storage"] = tc

	// Test schedule to a second disk on the same node even if the first has more available storage
	tc = generateSchedulerTestCase()
	daemon1 = newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	tc.daemons = []*corev1.Pod{
		daemon1,
	}
	node1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node1.Name] = true
	disk = newDisk(TestDefaultDataPath, true, 0)
	disk2 = newDisk(TestDefaultDataPath, true, 0)
	node1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
		getDiskID(TestNode1, "2"): disk2,
	}
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode1, "1"): {
			StorageAvailable: TestDiskAvailableSize * 2,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode1, "1"),
			Type:     longhorn.DiskTypeFilesystem,
		},
		getDiskID(TestNode1, "2"): {
			StorageAvailable: TestDiskAvailableSize / 2,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode1, "2"),
			Type:     longhorn.DiskTypeFilesystem,
		},
	}
	expectNode1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	expectNode1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
	}
	nodes = map[string]*longhorn.Node{
		TestNode1: node1,
	}
	tc.nodes = nodes
	expectedNodes = map[string]*longhorn.Node{
		TestNode1: expectNode1,
	}
	tc.expectedNodes = expectedNodes
	tc.expectedDisks = map[string]struct{}{
		getDiskID(TestNode1, "1"): {},
		getDiskID(TestNode1, "2"): {},
	}
	tc.err = false
	tc.replicaNodeSoftAntiAffinity = "true" // Allow replicas to schedule to the same node.
	testCases["schedule to a second disk on the same node even if the first has more available storage"] = tc

	// Test fail scheduling when replicaDiskSoftAntiAffinity is false
	tc = generateSchedulerTestCase()
	daemon1 = newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	tc.daemons = []*corev1.Pod{
		daemon1,
	}
	node1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node1.Name] = true
	disk = newDisk(TestDefaultDataPath, true, 0)
	node2 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node2.Name] = true
	node1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
	}
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode1, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode1, "1"),
			Type:     longhorn.DiskTypeFilesystem,
		},
	}
	expectNode1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	expectNode1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
	}
	nodes = map[string]*longhorn.Node{
		TestNode1: node1,
	}
	tc.nodes = nodes
	expectedNodes = map[string]*longhorn.Node{
		TestNode1: expectNode1,
	}
	tc.expectedNodes = expectedNodes
	tc.expectedDisks = map[string]struct{}{
		getDiskID(TestNode1, "1"): {},
	}
	tc.err = false
	tc.firstNilReplica = 1                   // There is only one disk, so the second replica must fail to schedule.
	tc.replicaNodeSoftAntiAffinity = "true"  // Allow replicas to schedule to the same node.
	tc.replicaDiskSoftAntiAffinity = "false" // Do not allow replicas to schedule to the same disk.
	testCases["fail scheduling when replicaDiskSoftAntiAffinity is true"] = tc

	// Test fail scheduling when zoneSoftAntiAffinity is false
	tc = generateSchedulerTestCase()
	daemon1 = newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	daemon2 = newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2)
	tc.daemons = []*corev1.Pod{
		daemon1,
		daemon2,
	}
	node1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node1.Name] = true
	disk = newDisk(TestDefaultDataPath, true, 0)
	node1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
	}
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode1, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode1, "1"),
			Type:     longhorn.DiskTypeFilesystem,
		},
	}
	disk = newDisk(TestDefaultDataPath, true, 0)
	expectNode1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	expectNode1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
	}
	disk = newDisk(TestDefaultDataPath, true, 0)
	node2 = newNode(TestNode2, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node2.Name] = true
	node2.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode2, "1"): disk,
	}
	node2.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode2, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode2, "1"),
			Type:     longhorn.DiskTypeFilesystem,
		},
	}
	nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	tc.expectedNodes = nil // We don't know or care which node the first replica will schedule to.
	tc.err = false
	tc.firstNilReplica = 1                   // There is only one disk, so the second replica must fail to schedule.
	tc.replicaNodeSoftAntiAffinity = "true"  // Allow replicas to schedule to the same node.
	tc.replicaZoneSoftAntiAffinity = "false" // Do not allow replicas to schedule to the same zone.
	testCases["fail scheduling when zoneSoftAntiAffinity is false"] = tc

	// Test schedule when zoneSoftAntiAffinity is false but there is an evicting replica
	tc = generateSchedulerTestCase()
	daemon1 = newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	daemon2 = newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2)
	tc.daemons = []*corev1.Pod{
		daemon1,
		daemon2,
	}
	node1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node1.Name] = true
	disk = newDisk(TestDefaultDataPath, true, 0)
	node1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
	}
	alreadyScheduledReplica := newReplicaForVolume(tc.volume)
	alreadyScheduledReplica.Spec.NodeID = TestNode1
	alreadyScheduledReplica.Spec.EvictionRequested = true
	tc.allReplicas[alreadyScheduledReplica.Name] = alreadyScheduledReplica
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode1, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: TestVolumeSize,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID:         getDiskID(TestNode1, "1"),
			Type:             longhorn.DiskTypeFilesystem,
			ScheduledReplica: map[string]int64{alreadyScheduledReplica.Name: TestVolumeSize},
		},
	}
	expectNode1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	expectNode1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
	}
	disk = newDisk(TestDefaultDataPath, true, 0)
	expectNode1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	expectNode1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
	}
	disk = newDisk(TestDefaultDataPath, true, 0)
	node2 = newNode(TestNode2, TestNamespace, TestZone2, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node2.Name] = true
	node2.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode2, "1"): disk,
	}
	node2.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode2, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID: getDiskID(TestNode2, "1"),
			Type:     longhorn.DiskTypeFilesystem,
		},
	}
	expectNode2 = newNode(TestNode2, TestNamespace, TestZone2, true, longhorn.ConditionStatusTrue)
	expectNode2.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode2, "1"): disk,
	}
	nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	expectedNodes = map[string]*longhorn.Node{
		TestNode1: expectNode1,
		TestNode2: expectNode2,
	}
	tc.expectedNodes = expectedNodes
	tc.err = false
	tc.firstNilReplica = -1
	tc.replicaNodeSoftAntiAffinity = "true"  // Allow replicas to schedule to the same node.
	tc.replicaZoneSoftAntiAffinity = "false" // Do not allow replicas to schedule to the same zone.
	testCases["schedule when zoneSoftAntiAffinity is false but there is an evicting replica"] = tc

	// Test fail scheduling when doing so would reuse an invalid evicting node
	tc = generateSchedulerTestCase()
	daemon1 = newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	daemon2 = newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2)
	// Implement with three nodes to create the scenario posed by
	// https://github.com/longhorn/longhorn-manager/pull/2094#discussion_r1290839641.
	daemon3 = newDaemonPod(corev1.PodRunning, TestDaemon3, TestNamespace, TestNode3, TestIP3)
	tc.daemons = []*corev1.Pod{
		daemon1,
		daemon2,
		daemon3,
	}
	node1 = newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node1.Name] = true
	disk = newDisk(TestDefaultDataPath, true, 0)
	node1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
	}
	alreadyScheduledReplica = newReplicaForVolume(tc.volume)
	alreadyScheduledReplica.Spec.NodeID = TestNode1
	alreadyScheduledReplica.Spec.EvictionRequested = true
	tc.allReplicas[alreadyScheduledReplica.Name] = alreadyScheduledReplica
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode1, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: TestVolumeSize,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID:         getDiskID(TestNode1, "1"),
			Type:             longhorn.DiskTypeFilesystem,
			ScheduledReplica: map[string]int64{alreadyScheduledReplica.Name: TestVolumeSize},
		},
	}
	disk = newDisk(TestDefaultDataPath, true, 0)
	node2 = newNode(TestNode2, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node2.Name] = true
	node2.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode2, "1"): disk,
	}
	alreadyScheduledReplica = newReplicaForVolume(tc.volume)
	alreadyScheduledReplica.Spec.NodeID = TestNode2
	alreadyScheduledReplica.Spec.EvictionRequested = false
	tc.allReplicas[alreadyScheduledReplica.Name] = alreadyScheduledReplica
	node2.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode2, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID:         getDiskID(TestNode2, "1"),
			Type:             longhorn.DiskTypeFilesystem,
			ScheduledReplica: map[string]int64{alreadyScheduledReplica.Name: TestVolumeSize},
		},
	}
	disk = newDisk(TestDefaultDataPath, true, 0)
	node3 = newNode(TestNode3, TestNamespace, TestZone2, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node2.Name] = true
	node3.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode3, "1"): disk,
	}
	alreadyScheduledReplica = newReplicaForVolume(tc.volume)
	alreadyScheduledReplica.Spec.NodeID = TestNode3
	alreadyScheduledReplica.Spec.EvictionRequested = false
	tc.allReplicas[alreadyScheduledReplica.Name] = alreadyScheduledReplica
	node3.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode3, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID:         getDiskID(TestNode3, "1"),
			Type:             longhorn.DiskTypeFilesystem,
			ScheduledReplica: map[string]int64{alreadyScheduledReplica.Name: TestVolumeSize},
		},
	}
	nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
		TestNode3: node3,
	}
	tc.nodes = nodes
	tc.err = false
	tc.firstNilReplica = 0                   // We cannot schedule to an evicting node in a used zone.
	tc.replicaNodeSoftAntiAffinity = "false" // Do not allow replicas to schedule to the same node.
	tc.replicaZoneSoftAntiAffinity = "false" // Do not allow replicas to schedule to the same zone.
	testCases["fail scheduling when doing so would reuse an invalid evicting node"] = tc

	// Test potentially reusable replica before interval expires
	// We should fail to schedule a new replica to this node until the interval expires.
	tc = generateFailedReplicaTestCase(true, false)
	tc.err = false
	tc.firstNilReplica = 0
	testCases["potentially reusable replica before interval expires"] = tc

	// Test potentially reusable replica after interval expires
	// We should succeed to schedule a new replica to this node because the interval expired.
	tc = generateFailedReplicaTestCase(true, true)
	tc.err = false
	tc.firstNilReplica = -1
	testCases["potentially reusable replica after interval expires"] = tc

	// Test non-reusable replica before interval expires
	// We should succeed to schedule a new replica to this node because the existing replica is not reusable.
	tc = generateFailedReplicaTestCase(false, false)
	tc.err = false
	tc.firstNilReplica = -1
	testCases["non-reusable replica before interval expires"] = tc

	// Test non-reusable replica after interval expires
	// We should succeed to schedule a new replica to this node because the existing replica is not reusable and the
	// interval expired anyway.
	tc = generateFailedReplicaTestCase(false, true)
	tc.err = false
	tc.firstNilReplica = -1
	testCases["non-reusable replica after interval expires"] = tc

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		lhClient := lhfake.NewSimpleClientset()
		extensionsClient := apiextensionsfake.NewSimpleClientset()

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		vIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
		rIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
		nIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
		eiIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
		sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
		pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

		s := newReplicaScheduler(lhClient, kubeClient, extensionsClient, informerFactories)
		// create daemon pod
		for _, daemon := range tc.daemons {
			p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), daemon, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = pIndexer.Add(p)
			c.Assert(err, IsNil)
		}
		// create node
		for _, node := range tc.nodes {
			n, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			c.Assert(n, NotNil)
			err = nIndexer.Add(n)
			c.Assert(err, IsNil)
		}
		// Create engine image
		ei, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), tc.engineImage, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(ei, NotNil)
		err = eiIndexer.Add(ei)
		c.Assert(err, IsNil)
		// create volume
		volume, err := lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(context.TODO(), tc.volume, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(volume, NotNil)
		err = vIndexer.Add(volume)
		c.Assert(err, IsNil)
		// set settings
		setSettings(tc, lhClient, sIndexer, c)
		// validate scheduler
		numScheduled := 0
		for replicaName := range tc.replicasToSchedule {
			r, err := lhClient.LonghornV1beta2().Replicas(TestNamespace).Create(context.TODO(), tc.allReplicas[replicaName], metav1.CreateOptions{})
			c.Assert(err, IsNil)
			c.Assert(r, NotNil)
			err = rIndexer.Add(r)
			c.Assert(err, IsNil)

			sr, _, err := s.ScheduleReplica(r, tc.allReplicas, volume)
			if tc.err {
				c.Assert(err, NotNil)
			} else {
				if numScheduled == tc.firstNilReplica {
					c.Assert(sr, IsNil)
				} else {
					c.Assert(err, IsNil)
					c.Assert(sr, NotNil)
					c.Assert(sr.Spec.NodeID, Not(Equals), "")
					c.Assert(sr.Spec.DiskID, Not(Equals), "")
					c.Assert(sr.Spec.DiskPath, Not(Equals), "")
					c.Assert(sr.Spec.DataDirectoryName, Not(Equals), "")
					tc.allReplicas[sr.Name] = sr
					// check expected node
					for name, node := range tc.expectedNodes {
						if sr.Spec.NodeID == name {
							c.Assert(sr.Spec.DiskPath, Equals, node.Spec.Disks[sr.Spec.DiskID].Path)
							delete(tc.expectedNodes, name)
						}
					}
					// check expected disk
					for diskUUID := range tc.expectedDisks {
						if sr.Spec.DiskID == diskUUID {
							delete(tc.expectedDisks, diskUUID)
						}
					}
					numScheduled++
				}
			}
		}
		c.Assert(len(tc.expectedNodes), Equals, 0)
		c.Assert(len(tc.expectedDisks), Equals, 0)
	}
}

// generateFailedReplicaTestCase helps generate test cases in which a node contains a failed replica and the scheduler
// must decide whether to allow additional replicas to schedule to it.
func generateFailedReplicaTestCase(
	replicaReusable, waitIntervalExpired bool) (tc *ReplicaSchedulerTestCase) {
	tc = generateSchedulerTestCase()
	daemon1 := newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	tc.daemons = []*corev1.Pod{
		daemon1,
	}
	node1 := newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node1.Name] = true
	disk := newDisk(TestDefaultDataPath, true, 0)
	node1.Spec.Disks = map[string]longhorn.DiskSpec{
		getDiskID(TestNode1, "1"): disk,
	}

	// We are specifically interested in situations in which a replica is ONLY schedulable to a node because its
	// existing replica is failed.
	tc.replicaNodeSoftAntiAffinity = "false"

	// A failed replica is already scheduled.
	var alreadyScheduledReplica *longhorn.Replica
	for _, replica := range tc.allReplicas {
		alreadyScheduledReplica = replica
		break
	}
	delete(tc.replicasToSchedule, alreadyScheduledReplica.Name)
	alreadyScheduledReplica.Spec.NodeID = TestNode1
	alreadyScheduledReplica.Spec.DiskID = getDiskID(TestNode1, "1")
	alreadyScheduledReplica.Spec.FailedAt = TestTimeNow
	tc.volume.Status.Robustness = longhorn.VolumeRobustnessDegraded
	tc.volume.Status.LastDegradedAt = TestTimeOneMinuteAgo

	if replicaReusable {
		alreadyScheduledReplica.Spec.RebuildRetryCount = 0
	} else {
		alreadyScheduledReplica.Spec.RebuildRetryCount = 5
	}

	if waitIntervalExpired {
		tc.ReplicaReplenishmentWaitInterval = "30"
	} else {
		tc.ReplicaReplenishmentWaitInterval = "90"
	}

	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		getDiskID(TestNode1, "1"): {
			StorageAvailable: TestDiskAvailableSize,
			StorageScheduled: TestVolumeSize,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID:         getDiskID(TestNode1, "1"),
			Type:             longhorn.DiskTypeFilesystem,
			ScheduledReplica: map[string]int64{alreadyScheduledReplica.Name: TestVolumeSize},
		},
	}
	nodes := map[string]*longhorn.Node{
		TestNode1: node1,
	}
	tc.nodes = nodes
	return
}

func setSettings(tc *ReplicaSchedulerTestCase, lhClient *lhfake.Clientset, sIndexer cache.Indexer, c *C) {
	// Set storage over-provisioning percentage settings
	if tc.storageOverProvisioningPercentage != "" && tc.storageMinimalAvailablePercentage != "" {
		s := initSettings(string(types.SettingNameStorageOverProvisioningPercentage), tc.storageOverProvisioningPercentage)
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)

		s = initSettings(string(types.SettingNameStorageMinimalAvailablePercentage), tc.storageMinimalAvailablePercentage)
		setting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}
	// Set replica node soft anti-affinity setting
	if tc.replicaNodeSoftAntiAffinity != "" {
		s := initSettings(
			string(types.SettingNameReplicaSoftAntiAffinity),
			tc.replicaNodeSoftAntiAffinity)
		setting, err :=
			lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}
	// Set replica zone soft anti-affinity setting
	if tc.replicaZoneSoftAntiAffinity != "" {
		s := initSettings(
			string(types.SettingNameReplicaZoneSoftAntiAffinity),
			tc.replicaZoneSoftAntiAffinity)
		setting, err :=
			lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}
	// Set replica disk soft anti-affinity setting
	if tc.replicaDiskSoftAntiAffinity != "" {
		s := initSettings(
			string(types.SettingNameReplicaDiskSoftAntiAffinity),
			tc.replicaDiskSoftAntiAffinity)
		setting, err :=
			lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}
	// Set replica replenishment wait interval setting
	if tc.ReplicaReplenishmentWaitInterval != "" {
		s := initSettings(
			string(types.SettingNameReplicaReplenishmentWaitInterval),
			tc.ReplicaReplenishmentWaitInterval)
		setting, err :=
			lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}
}

func (s *TestSuite) TestFilterDisksWithMatchingReplicas(c *C) {
	type testCase struct {
		inputDiskUUIDs       []string
		inputReplicas        map[string]*longhorn.Replica
		diskSoftAntiAffinity bool

		expectDiskUUIDs []string
	}
	tests := map[string]testCase{}

	tc := testCase{}
	diskUUID1 := getDiskID(TestNode1, "1")
	diskUUID2 := getDiskID(TestNode2, "2")
	tc.inputDiskUUIDs = []string{diskUUID1, diskUUID2}
	v := newVolume(TestVolumeName, 3)
	replica1 := newReplicaForVolume(v)
	replica1.Spec.DiskID = diskUUID1
	replica2 := newReplicaForVolume(v)
	replica2.Spec.DiskID = diskUUID2
	tc.inputReplicas = map[string]*longhorn.Replica{
		replica1.Name: replica1,
		replica2.Name: replica2,
	}
	tc.diskSoftAntiAffinity = false
	tc.expectDiskUUIDs = []string{} // No disks can be scheduled.
	tests["diskSoftAntiAffinity = false and no empty disks"] = tc

	tc = testCase{}
	diskUUID1 = getDiskID(TestNode1, "1")
	diskUUID2 = getDiskID(TestNode2, "2")
	tc.inputDiskUUIDs = []string{diskUUID1, diskUUID2}
	v = newVolume(TestVolumeName, 3)
	replica1 = newReplicaForVolume(v)
	replica1.Spec.DiskID = diskUUID1
	replica2 = newReplicaForVolume(v)
	replica2.Spec.DiskID = diskUUID2
	tc.inputReplicas = map[string]*longhorn.Replica{
		replica1.Name: replica1,
		replica2.Name: replica2,
	}
	tc.diskSoftAntiAffinity = true
	tc.expectDiskUUIDs = append(tc.expectDiskUUIDs, tc.inputDiskUUIDs...) // Both disks are equally viable.
	tests["diskSoftAntiAffinity = true and no empty disks"] = tc

	tc = testCase{}
	diskUUID1 = getDiskID(TestNode1, "1")
	diskUUID2 = getDiskID(TestNode2, "2")
	diskUUID3 := getDiskID(TestNode2, "3")
	diskUUID4 := getDiskID(TestNode2, "4")
	diskUUID5 := getDiskID(TestNode2, "5")
	tc.inputDiskUUIDs = []string{diskUUID1, diskUUID2, diskUUID3, diskUUID4, diskUUID5}
	v = newVolume(TestVolumeName, 3)
	replica1 = newReplicaForVolume(v)
	replica1.Spec.DiskID = diskUUID1
	replica2 = newReplicaForVolume(v)
	replica2.Spec.DiskID = diskUUID2
	replica3 := newReplicaForVolume(v)
	replica3.Spec.DiskID = diskUUID3
	replica4 := newReplicaForVolume(v)
	replica4.Spec.DiskID = diskUUID4
	tc.inputReplicas = map[string]*longhorn.Replica{
		replica1.Name: replica1,
		replica2.Name: replica2,
		replica3.Name: replica3,
		replica4.Name: replica4,
	}
	tc.diskSoftAntiAffinity = true
	tc.expectDiskUUIDs = []string{diskUUID5} // Only disk5 has no matching replica.
	tests["only schedule to disk without matching replica"] = tc

	for name, tc := range tests {
		fmt.Printf("testing %v\n", name)
		inputDisks := map[string]*Disk{}
		for _, UUID := range tc.inputDiskUUIDs {
			inputDisks[UUID] = &Disk{}
		}
		outputDiskUUIDs := filterDisksWithMatchingReplicas(inputDisks, tc.inputReplicas, tc.diskSoftAntiAffinity)
		c.Assert(len(outputDiskUUIDs), Equals, len(tc.expectDiskUUIDs))
		for _, UUID := range tc.expectDiskUUIDs {
			_, ok := outputDiskUUIDs[UUID]
			c.Assert(ok, Equals, true)
		}
	}
}

// TestGetCurrentNodesAndZones can easily be extended with additional test cases. However, it was originally written to
// verify the behavior of getCurrentNodesAndZones when replicas with different values of
// replica.Status.EvictionRequested were considered in different orders.
func (s *TestSuite) TestGetCurrentNodesAndZones(c *C) {
	const (
		TestReplica1 = "test-replica-1"
		TestReplica2 = "test-replica-2"
	)

	generateNodeInZone := func(nodeName, zoneName string) *longhorn.Node {
		return &longhorn.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: nodeName,
			},
			Status: longhorn.NodeStatus{
				Zone: zoneName,
			},
		}
	}

	generateNodeMap := func(nodes ...*longhorn.Node) map[string]*longhorn.Node {
		nodeMap := map[string]*longhorn.Node{}
		for _, node := range nodes {
			nodeMap[node.Name] = node
		}
		return nodeMap
	}

	generateReplica := func(replicaName, nodeName string, evictionRequested bool) *longhorn.Replica {
		return &longhorn.Replica{
			ObjectMeta: metav1.ObjectMeta{
				Name: replicaName,
			},
			Spec: longhorn.ReplicaSpec{
				InstanceSpec: longhorn.InstanceSpec{
					NodeID: nodeName,
				},
				EvictionRequested: evictionRequested,
			},
		}
	}

	generateReplicaMap := func(replicas ...*longhorn.Replica) map[string]*longhorn.Replica {
		replicaMap := map[string]*longhorn.Replica{}
		for _, replica := range replicas {
			replicaMap[replica.Name] = replica
		}
		return replicaMap
	}

	verifyNodeNames := func(usedNodeNames []string, usedNodes map[string]*longhorn.Node) {
		c.Assert(len(usedNodes), Equals, len(usedNodeNames))
		for _, usedNodeName := range usedNodeNames {
			_, ok := usedNodes[usedNodeName]
			c.Assert(ok, Equals, true)
		}
	}

	verifyZoneNames := func(usedZoneNames []string, usedZones map[string]bool) {
		c.Assert(len(usedZones), Equals, len(usedZoneNames))
		for _, usedZoneName := range usedZoneNames {
			used, ok := usedZones[usedZoneName]
			c.Assert(ok, Equals, true)
			c.Assert(used, Equals, true)
		}
	}

	verifyOnlyEvictingNodeNames := func(onlyEvictingNodeNames []string, onlyEvictingNodes map[string]bool) {
		for k, v := range onlyEvictingNodes {
			if v == false {
				delete(onlyEvictingNodes, k) // These are false. We want to compare those that are true.
			}
		}
		c.Assert(len(onlyEvictingNodes), Equals, len(onlyEvictingNodeNames))
		for _, onlyEvictingNodeName := range onlyEvictingNodeNames {
			used, ok := onlyEvictingNodes[onlyEvictingNodeName]
			c.Assert(ok, Equals, true)
			c.Assert(used, Equals, true)
			delete(onlyEvictingNodes, onlyEvictingNodeName)
		}
	}

	verifyOnlyEvictingZoneNames := func(onlyEvictingZoneNames []string, onlyEvictingZones map[string]bool) {
		for k, v := range onlyEvictingZones {
			if v == false {
				delete(onlyEvictingZones, k) // These are false. We want to compare those that are true.
			}
		}
		c.Assert(len(onlyEvictingZones), Equals, len(onlyEvictingZoneNames))
		for _, onlyEvictingZoneName := range onlyEvictingZoneNames {
			used, ok := onlyEvictingZones[onlyEvictingZoneName]
			c.Assert(ok, Equals, true)
			c.Assert(used, Equals, true)
			delete(onlyEvictingZones, onlyEvictingZoneName)
		}
	}

	testCases := map[string]struct {
		replicas                    map[string]*longhorn.Replica
		nodeInfo                    map[string]*longhorn.Node
		expectUsedNodeNames         []string
		expectUsedZoneNames         []string
		expectOnlyEvictingNodeNames []string
		expectOnlyEvictingZoneNames []string
	}{
		"one evicting, then one not evicting": {
			// A previous attempt at refactoring the scheduler gave different results depending on the loop order of
			// replicas. We can't guarantee the loop order in this test case, but generally, it sees the evicting
			// replica first.
			nodeInfo: generateNodeMap(generateNodeInZone(TestNode1, TestZone1)),
			replicas: generateReplicaMap(generateReplica(TestReplica1, TestNode1, true),
				generateReplica(TestReplica2, TestNode1, false)),
			expectUsedNodeNames:         []string{TestNode1},
			expectUsedZoneNames:         []string{TestZone1},
			expectOnlyEvictingNodeNames: []string{},
			expectOnlyEvictingZoneNames: []string{},
		},
		"one not evicting, then one evicting": {
			// A previous attempt at refactoring the scheduler gave different results depending on the loop order of
			// replicas. We can't guarantee the loop order in this test case, but generally, it sees the not evicting
			// replica first.
			nodeInfo: generateNodeMap(generateNodeInZone(TestNode1, TestZone1)),
			replicas: generateReplicaMap(generateReplica(TestReplica1, TestNode1, false),
				generateReplica(TestReplica2, TestNode1, true)),
			expectUsedNodeNames:         []string{TestNode1},
			expectUsedZoneNames:         []string{TestZone1},
			expectOnlyEvictingNodeNames: []string{},
			expectOnlyEvictingZoneNames: []string{},
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)
		usedNodes, usedZones, onlyEvictingNodes, onlyEvictingZones := getCurrentNodesAndZones(tc.replicas, tc.nodeInfo,
			false, false)
		verifyNodeNames(tc.expectUsedNodeNames, usedNodes)
		verifyZoneNames(tc.expectUsedZoneNames, usedZones)
		verifyOnlyEvictingNodeNames(tc.expectOnlyEvictingNodeNames, onlyEvictingNodes)
		verifyOnlyEvictingZoneNames(tc.expectOnlyEvictingZoneNames, onlyEvictingZones)
	}
}

func getTestNow() time.Time {
	now, _ := time.Parse(time.RFC3339, TestTimeNow)
	return now
}
