package scheduler

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	. "gopkg.in/check.v1"

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
)

const (
	TestNamespace = "default"
	TestIP1       = "1.2.3.4"
	TestIP2       = "5.6.7.8"
	TestIP3       = "9.10.11.12"
	TestNode1     = "test-node-name-1"
	TestNode2     = "test-node-name-2"
	TestNode3     = "test-node-name-3"

	TestOwnerID1             = TestNode1
	TestEngineImage          = "longhorn-engine:latest"
	TestInstanceManagerImage = "longhorn-instance-manager:latest"

	TestVolumeName         = "test-volume"
	TestVolumeSize         = 1073741824
	TestVolumeStaleTimeout = 60

	TestDefaultDataPath = "/var/lib/longhorn"

	TestDaemon1 = "longhorn-manager-1"
	TestDaemon2 = "longhorn-manager-2"
	TestDaemon3 = "longhorn-manager-3"

	TestDisk1ID = "test-disk-id-1"
	TestDisk2ID = "test-disk-id-2"
	TestDisk3ID = "test-disk-id-3"
	TestDisk4ID = "test-disk-id-4"

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

func newInstanceManager(nodeName string) *longhorn.InstanceManager {
	return &longhorn.InstanceManager{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "instance-manager-" + util.RandomID(),
			Namespace: TestNamespace,
			Labels:    types.GetInstanceManagerLabels(nodeName, TestInstanceManagerImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1),
		},
		Spec: longhorn.InstanceManagerSpec{
			Image:  TestInstanceManagerImage,
			NodeID: nodeName,
			Type:   longhorn.InstanceManagerTypeAllInOne,
		},
		Status: longhorn.InstanceManagerStatus{
			CurrentState: longhorn.InstanceManagerStateRunning,
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
	replicaAutoBalance                string
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

	// Test scheduling on the right node when "best-effort" auto balancing is enabled and an incorrect node has a
	// node with less load.
	tc = generateBestEffortAutoBalanceScheduleTestCase()
	testCases["scheduling on the right node with \"best-effort\" auto balancing"] = tc

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		vIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
		rIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
		nIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
		eiIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
		imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
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
		// Create instance manager
		for _, node := range tc.nodes {
			fakeInstanceManager := newInstanceManager(node.Name)
			im, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), fakeInstanceManager, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			c.Assert(im, NotNil)
			err = imIndexer.Add(im)
			c.Assert(err, IsNil)
		}
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

			sr, errs := s.ScheduleReplica(r, tc.allReplicas, volume)
			if tc.err {
				// TODO:
				c.Assert(len(errs), Not(Equals), 0)
			} else {
				if numScheduled == tc.firstNilReplica {
					c.Assert(sr, IsNil)
				} else {
					c.Assert(len(errs), Equals, 0)
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

// Test scheduling on the right node when "best-effort" auto balancing is enabled and an incorrect node has a
// node with less load.
func generateBestEffortAutoBalanceScheduleTestCase() *ReplicaSchedulerTestCase {
	tc := &ReplicaSchedulerTestCase{
		engineImage:        newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed),
		allReplicas:        make(map[string]*longhorn.Replica),
		replicasToSchedule: map[string]struct{}{},
		firstNilReplica:    -1,
	}

	daemon1 := newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1)
	daemon2 := newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2)
	tc.daemons = []*corev1.Pod{
		daemon1,
		daemon2,
	}

	// Create 2 nodes in the same zone
	node1 := newNode(TestNode1, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	node2 := newNode(TestNode2, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
	tc.engineImage.Status.NodeDeploymentMap[node1.Name] = true
	tc.engineImage.Status.NodeDeploymentMap[node2.Name] = true

	// For the test create a volume that requires 2 replicas
	tc.volume = newVolume(TestVolumeName, 2)

	// Give each node 2 disks
	addDisks := func(node *longhorn.Node, index int64, disk longhorn.DiskSpec, storageAvailable int64, hasReplica bool) (diskID string) {
		if node.Spec.Disks == nil {
			node.Spec.Disks = make(map[string]longhorn.DiskSpec)
		}
		if node.Status.DiskStatus == nil {
			node.Status.DiskStatus = make(map[string]*longhorn.DiskStatus)
		}

		var scheduledReplica map[string]int64
		if hasReplica {
			replica := newReplicaForVolume(tc.volume)
			replica.Spec.NodeID = node.Name
			tc.allReplicas[replica.Name] = replica
			scheduledReplica = map[string]int64{replica.Name: TestVolumeSize}
		}

		id := getDiskID(node.Name, strconv.FormatInt(index, 10))
		node.Spec.Disks[id] = disk
		node.Status.DiskStatus[id] = &longhorn.DiskStatus{
			StorageAvailable: storageAvailable,
			StorageScheduled: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: []longhorn.Condition{
				newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
			},
			DiskUUID:         id,
			Type:             longhorn.DiskTypeFilesystem,
			ScheduledReplica: scheduledReplica,
		}
		return id
	}

	node1disk1 := newDisk(TestDefaultDataPath, true, 0)
	node1disk2 := newDisk(TestDefaultDataPath, true, 0)
	addDisks(node1, 1, node1disk1, TestDiskAvailableSize, true)  // Later we schedule a replica on this disk
	addDisks(node1, 2, node1disk2, TestDiskAvailableSize, false) // No replica

	node2disk1 := newDisk(TestDefaultDataPath, true, 0)
	node2disk2 := newDisk(TestDefaultDataPath, true, 0)
	addDisks(node2, 1, node2disk1, TestDiskAvailableSize/2-100, false)               // No replica
	expectedDiskID := addDisks(node2, 2, node2disk2, TestDiskAvailableSize/2, false) // No replica, scheduler should choose this since it has the most storage available from the valid options.

	tc.replicaAutoBalance = "best-effort"
	tc.replicaDiskSoftAntiAffinity = "false" // Do not allow scheduling of replicas on the same disk.
	tc.replicaNodeSoftAntiAffinity = "false" // Do not allow scheduling of replica on the same node.
	tc.replicaZoneSoftAntiAffinity = "true"  // Allow scheduling in the same zone, both nodes are in the same one. The scheduler takes a shortcut otherwise.

	tc.nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}

	// Add replica that still needs to be scheduled
	replicaToSchedule := newReplicaForVolume(tc.volume)
	tc.allReplicas[replicaToSchedule.Name] = replicaToSchedule

	// Only test scheduling for the replicaToSchedule, we don't want to schedule the replica that is already on node1disk1 again
	tc.replicasToSchedule[replicaToSchedule.Name] = struct{}{}

	// Expect replica to be scheduled on node2disk2.
	// - node1disk1 should not be possible, there is already a replica on the node and on the disk
	// - node1disk2 should not be possible, there is already a replica on the node
	// - node2disk1 is possible, but does not have the most available storage space left
	// - node2disk2 is possible, and has the most storage left of all valid options. This should be the candidate.
	tc.expectedNodes = map[string]*longhorn.Node{
		TestNode2: node2,
	}
	tc.expectedDisks = map[string]struct{}{
		expectedDiskID: {},
	}

	return tc
}

func setSettings(tc *ReplicaSchedulerTestCase, lhClient *lhfake.Clientset, sIndexer cache.Indexer, c *C) {
	// Set default-instance-manager-image setting
	s := initSettings(string(types.SettingNameDefaultInstanceManagerImage), TestInstanceManagerImage)
	setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = sIndexer.Add(setting)
	c.Assert(err, IsNil)

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
	// Set replica auto-balance setting
	if tc.replicaAutoBalance != "" {
		s := initSettings(
			string(types.SettingNameReplicaAutoBalance),
			tc.replicaAutoBalance)
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
		ignoreFailedReplicas bool

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
	tc.ignoreFailedReplicas = false
	tc.expectDiskUUIDs = []string{diskUUID5} // Only disk5 has no matching replica.
	tests["only schedule to disk without matching replica"] = tc

	for name, tc := range tests {
		fmt.Printf("testing %v\n", name)
		inputDisks := map[string]*Disk{}
		for _, UUID := range tc.inputDiskUUIDs {
			inputDisks[UUID] = &Disk{}
		}
		outputDiskUUIDs := filterDisksWithMatchingReplicas(inputDisks, tc.inputReplicas, tc.diskSoftAntiAffinity, tc.ignoreFailedReplicas)
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

func (s *TestSuite) TestIsSchedulableToDiskConsiderDiskPressure(c *C) {
	diskPressurePercentage := int64(80)
	diskUUID := "disk-1"
	minimalAvailablePercentage := int64(25)
	overProvisioningPercentage := int64(100)

	replicaScheduler := NewReplicaScheduler(nil)

	type TestCase struct {
		size            int64
		requiredStorage int64
		info            *DiskSchedulingInfo

		expectedIsSchedulable bool
	}

	testCases := map[string]TestCase{
		"schedulable under pressure": {
			size:            500,
			requiredStorage: 100,
			info: &DiskSchedulingInfo{
				StorageScheduled:           200,
				StorageReserved:            100,
				StorageMaximum:             1000,
				StorageAvailable:           600,
				MinimalAvailablePercentage: minimalAvailablePercentage,
				OverProvisioningPercentage: overProvisioningPercentage,
				DiskUUID:                   diskUUID,
			},
			expectedIsSchedulable: true, // newDiskUsagePercentage = (100 + 200 + 100) * 100 / 1000 = 40% < 80%
		},
		"not schedulable due to new disk under pressure": {
			size:            500,
			requiredStorage: 400,
			info: &DiskSchedulingInfo{
				StorageScheduled:           200,
				StorageReserved:            100,
				StorageMaximum:             1000,
				StorageAvailable:           600,
				MinimalAvailablePercentage: minimalAvailablePercentage,
				OverProvisioningPercentage: overProvisioningPercentage,
				DiskUUID:                   diskUUID,
			},
			expectedIsSchedulable: false, // newDiskUsagePercentage = (100 + 200 + 100 + 400) * 100 / 1000 = 80% >= 80%
		},
		"integer divide by zero": {
			size:            500,
			requiredStorage: 400,
			info: &DiskSchedulingInfo{
				StorageScheduled:           200,
				StorageReserved:            100,
				StorageMaximum:             0,
				StorageAvailable:           600,
				MinimalAvailablePercentage: minimalAvailablePercentage,
				OverProvisioningPercentage: overProvisioningPercentage,
				DiskUUID:                   diskUUID,
			},
			expectedIsSchedulable: false, // newDiskUsagePercentage = (100 + 200 + 100 + 400) * 100 / 1000 = 80% >= 80%
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		isSchedulable := replicaScheduler.IsSchedulableToDiskConsiderDiskPressure(diskPressurePercentage, tc.size, tc.requiredStorage, tc.info)
		c.Assert(isSchedulable, Equals, tc.expectedIsSchedulable)
	}
}

func getTestNow() time.Time {
	now, _ := time.Parse(time.RFC3339, TestTimeNow)
	return now
}

func (s *TestSuite) TestScheduleReplicaToDiskOnLocalNode(c *C) {
	rs := NewReplicaScheduler(nil)
	volume := newVolume(TestVolumeName, 2)
	replica1 := newReplicaForVolume(volume)
	replica2 := newReplicaForVolume(volume)
	replicas := map[string]*longhorn.Replica{}
	replicas[replica1.Name] = replica1
	replicas[replica2.Name] = replica2
	diskCandidates := map[string]*Disk{}
	diskCandidates["disk2"] = &Disk{NodeID: TestNode2, DiskSpec: longhorn.DiskSpec{}, DiskStatus: &longhorn.DiskStatus{}}
	diskCandidates["disk3"] = &Disk{NodeID: TestNode3, DiskSpec: longhorn.DiskSpec{}, DiskStatus: &longhorn.DiskStatus{}}

	// Case 1: Volume not attached, skip scheduling
	rs.scheduleReplicaToDiskOnLocalNode(replica1, replicas, volume, diskCandidates)
	c.Assert(replica1.Spec.NodeID, Equals, "")

	// Case 2: Volume attached but no disks available on local node
	volume.Spec.NodeID = TestNode1
	rs.scheduleReplicaToDiskOnLocalNode(replica1, replicas, volume, diskCandidates)
	c.Assert(replica1.Spec.NodeID, Equals, "")

	// Case 3: Schedule to available local disk
	diskCandidates["disk1"] = &Disk{NodeID: TestNode1, DiskSpec: longhorn.DiskSpec{}, DiskStatus: &longhorn.DiskStatus{StorageAvailable: TestVolumeSize}}
	rs.scheduleReplicaToDiskOnLocalNode(replica1, replicas, volume, diskCandidates)
	c.Assert(replica1.Spec.NodeID, Equals, TestNode1)

	// Case 4: Another replica (replica2) should not be scheduled to the local node
	// because there is already a healthy replica (replica1) on that node.
	rs.scheduleReplicaToDiskOnLocalNode(replica2, replicas, volume, diskCandidates)
	c.Assert(replica2.Spec.NodeID, Equals, "")

	// Case 5: replica1 is marked as failed. In this case, replica2 is allowed to be
	// scheduled to the local node.
	replica1.Spec.FailedAt = getTestNow().String()
	rs.scheduleReplicaToDiskOnLocalNode(replica2, replicas, volume, diskCandidates)
	c.Assert(replica2.Spec.NodeID, Equals, TestNode1)
}

func (s *TestSuite) TestGetDiskWithMostBalanceScore(c *C) {
	replicaScheduler := NewReplicaScheduler(nil)

	// ------------------------------------------------------------------------
	// Disk setup summary:
	//   Node1: Disk1 (1000 avail), Disk3 (500 avail)
	//   Node2: Disk2 (2000 avail)
	//   Node3: Disk4 (0 avail)
	// ------------------------------------------------------------------------
	disk1 := &Disk{
		DiskSpec: longhorn.DiskSpec{
			StorageReserved: 0,
		},
		DiskStatus: &longhorn.DiskStatus{
			DiskUUID:         TestDisk1ID,
			StorageAvailable: 1000,
			StorageScheduled: 0,
			StorageMaximum:   1000,
		},
		NodeID: TestNode1,
	}

	disk2 := &Disk{
		DiskSpec: longhorn.DiskSpec{
			StorageReserved: 0,
		},
		DiskStatus: &longhorn.DiskStatus{
			DiskUUID:         TestDisk2ID,
			StorageAvailable: 2000,
			StorageScheduled: 0,
			StorageMaximum:   2000,
		},
		NodeID: TestNode2,
	}

	disk3 := &Disk{
		DiskSpec: longhorn.DiskSpec{
			StorageReserved: 0,
		},
		DiskStatus: &longhorn.DiskStatus{
			DiskUUID:         TestDisk3ID,
			StorageAvailable: 500,
			StorageScheduled: 0,
			StorageMaximum:   1000,
		},
		NodeID: TestNode1,
	}

	disk4 := &Disk{
		DiskSpec: longhorn.DiskSpec{
			StorageReserved: 0,
		},
		DiskStatus: &longhorn.DiskStatus{
			DiskUUID:         TestDisk4ID,
			StorageAvailable: 0,
			StorageScheduled: 0,
			StorageMaximum:   1000,
		},
		NodeID: TestNode3,
	}

	tests := []struct {
		name           string
		candidateDisks map[string]*Disk
		replicaSize    int64
		expectDisk     *Disk
	}{
		{
			name: "Single candidate disk returns itself",
			// Scenario:
			// - Only Disk1 on Node1.
			// Expectation: Disk1 selected directly.
			candidateDisks: map[string]*Disk{
				TestDisk1ID: disk1,
			},
			replicaSize: 100,
			expectDisk:  disk1,
		},
		{
			name: "Two disks on different nodes, select one with more usable capacity",
			// Scenario:
			// - Node1: Disk1 (1000 avail)
			// - Node2: Disk2 (2000 avail)
			// Expectation: Disk2 chosen due to higher usable capacity.
			candidateDisks: map[string]*Disk{
				TestDisk1ID: disk1,
				TestDisk2ID: disk2,
			},
			replicaSize: 100,
			expectDisk:  disk2,
		},
		{
			name: "Disks on same node, select disk with higher available space",
			// Scenario:
			// - Node1: Disk1 (1000 avail), Disk3 (500 avail)
			// Expectation: Disk1 chosen.
			candidateDisks: map[string]*Disk{
				TestDisk1ID: disk1,
				TestDisk3ID: disk3,
			},
			replicaSize: 100,
			expectDisk:  disk1,
		},
		{
			name: "Disk with zero available capacity still returns a valid disk",
			// Scenario:
			// - Node3: Disk4 (0 avail)
			// Expectation: still return Disk4 (no error).
			candidateDisks: map[string]*Disk{
				TestDisk4ID: disk4,
			},
			replicaSize: 100,
			expectDisk:  disk4,
		},
	}

	for _, tt := range tests {
		c.Logf("Running scenario: %s", tt.name)
		result := replicaScheduler.getDiskWithMostBalanceScore(tt.candidateDisks, tt.replicaSize)
		c.Assert(result, NotNil)
		c.Assert(result, Equals, tt.expectDisk)
	}
}

func (s *TestSuite) TestSelectBestNode(c *C) {
	tests := []struct {
		name        string
		nodeUsable  map[string]int64
		nodeTotal   map[string]int64
		replicaSize int64
		expectNode  string
		expectErr   bool
	}{
		{
			name: "No nodes available",
			// Scenario:
			// - Empty cluster map.
			// Expectation: error since no candidates exist.
			nodeUsable:  map[string]int64{},
			nodeTotal:   map[string]int64{},
			replicaSize: 10,
			expectNode:  "",
			expectErr:   true,
		},
		{
			name: "All nodes have insufficient capacity",
			// Scenario:
			// - Node1 usable: 5, Node2 usable: 8, replica requires 10.
			// Expectation: no eligible node, expect error.
			nodeUsable: map[string]int64{
				TestNode1: 5,
				TestNode2: 8,
			},
			nodeTotal: map[string]int64{
				TestNode1: 10,
				TestNode2: 10,
			},
			replicaSize: 10,
			expectNode:  "",
			expectErr:   true,
		},
		{
			name: "Select node with lowest imbalance after placement",
			// Scenario:
			// - Node1 usable: 100, Node2 usable: 200
			// Expectation: selects node with balanced post-placement ratio.
			nodeUsable: map[string]int64{
				TestNode1: 100,
				TestNode2: 200,
			},
			nodeTotal: map[string]int64{
				TestNode1: 100,
				TestNode2: 200,
			},
			replicaSize: 50,
			expectNode:  "non-empty",
			expectErr:   false,
		},
	}

	for _, tt := range tests {
		c.Logf("Running scenario: %s", tt.name)
		node, err := selectBestNode(tt.nodeUsable, tt.nodeTotal, tt.replicaSize)
		if tt.expectErr {
			c.Assert(err, NotNil)
			c.Assert(node, Equals, tt.expectNode)
		} else {
			c.Assert(err, IsNil)
			c.Assert(node, Not(Equals), "")
		}
	}
}

func (s *TestSuite) TestSelectBestDisk(c *C) {
	// ------------------------------------------------------------------------
	// Disk setup: single node (Node1) hosting two disks
	// ------------------------------------------------------------------------
	disk1 := &Disk{
		DiskStatus: &longhorn.DiskStatus{
			DiskUUID: TestDisk1ID,
		},
		NodeID: TestNode1,
	}
	disk2 := &Disk{
		DiskStatus: &longhorn.DiskStatus{
			DiskUUID: TestDisk2ID,
		},
		NodeID: TestNode1,
	}

	tests := []struct {
		name           string
		candidateDisks map[string]*Disk
		diskUsable     map[string]int64
		diskTotal      map[string]int64
		nodeID         string
		replicaSize    int64
		expectDisk     *Disk
		expectErr      bool
	}{
		{
			name: "No candidate disks",
			// Scenario:
			// - Empty disk map.
			// Expectation: error since no disks can host replica.
			candidateDisks: map[string]*Disk{},
			diskUsable:     map[string]int64{},
			diskTotal:      map[string]int64{},
			nodeID:         TestNode1,
			replicaSize:    10,
			expectDisk:     nil,
			expectErr:      true,
		},
		{
			name: "All candidate disks have insufficient space",
			// Scenario:
			// - Disk1 usable: 5, replica requires 10.
			// Expectation: error due to insufficient capacity.
			candidateDisks: map[string]*Disk{
				TestDisk1ID: disk1,
			},
			diskUsable: map[string]int64{
				TestDisk1ID: 5,
			},
			diskTotal: map[string]int64{
				TestDisk1ID: 10,
			},
			nodeID:      TestNode1,
			replicaSize: 10,
			expectDisk:  nil,
			expectErr:   true,
		},
		{
			name: "Select disk with highest available capacity within node",
			// Scenario:
			// - Disk1 usable: 100, Disk2 usable: 200
			// Expectation: Disk2 selected.
			candidateDisks: map[string]*Disk{
				TestDisk1ID: disk1,
				TestDisk2ID: disk2,
			},
			diskUsable: map[string]int64{
				TestDisk1ID: 100,
				TestDisk2ID: 200,
			},
			diskTotal: map[string]int64{
				TestDisk1ID: 100,
				TestDisk2ID: 200,
			},
			nodeID:      TestNode1,
			replicaSize: 50,
			expectDisk:  disk2,
			expectErr:   false,
		},
	}

	for _, tt := range tests {
		c.Logf("Running scenario: %s", tt.name)
		disk, err := selectBestDisk(tt.candidateDisks, tt.diskUsable, tt.diskTotal, tt.nodeID, tt.replicaSize)
		if tt.expectErr {
			c.Assert(err, NotNil)
			c.Assert(disk, IsNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(disk, NotNil)
		}
	}
}

func (s *TestSuite) TestIsDiskEligibleForVolume(c *C) {
	rcs := &ReplicaScheduler{} // No datastore calls needed; all inputs are passed in.

	baseDiskSpec := longhorn.DiskSpec{
		Type:            longhorn.DiskTypeFilesystem,
		Path:            TestDefaultDataPath,
		AllowScheduling: true,
		Tags:            []string{"primary"},
	}
	baseDiskStatus := &longhorn.DiskStatus{
		StorageAvailable: TestDiskAvailableSize,
		StorageMaximum:   TestDiskSize,
		FSType:           "ext4",
	}
	baseVolume := &longhorn.Volume{
		Spec: longhorn.VolumeSpec{
			Size:         TestVolumeSize,
			DataEngine:   longhorn.DataEngineTypeV1,
			DiskSelector: []string{"primary"},
		},
	}

	tests := []struct {
		name                      string
		diskSpec                  longhorn.DiskSpec
		diskStatus                *longhorn.DiskStatus
		volume                    *longhorn.Volume
		allowEmptyDiskSelectorVol bool
		biDiskSelector            []string
		expect                    bool
		expectReason              string
	}{
		{
			name:       "Eligible - tags match, no backing image selector",
			diskSpec:   baseDiskSpec,
			diskStatus: baseDiskStatus,
			volume:     baseVolume,
			expect:     true,
		},
		{
			name: "Ineligible - volume diskSelector mismatch",
			diskSpec: longhorn.DiskSpec{
				Type:            longhorn.DiskTypeFilesystem,
				Path:            TestDefaultDataPath,
				AllowScheduling: true,
				Tags:            []string{"secondary"},
			},
			diskStatus:   baseDiskStatus,
			volume:       baseVolume,
			expect:       false,
			expectReason: longhorn.ErrorReplicaScheduleTagsNotFulfilled,
		},
		{
			name: "Ineligible - AllowScheduling disabled",
			diskSpec: longhorn.DiskSpec{
				Type:            longhorn.DiskTypeFilesystem,
				Path:            TestDefaultDataPath,
				AllowScheduling: false,
				Tags:            []string{"primary"},
			},
			diskStatus:   baseDiskStatus,
			volume:       baseVolume,
			expect:       false,
			expectReason: longhorn.ErrorReplicaScheduleDiskUnavailable,
		},
		{
			name: "Ineligible - EvictionRequested",
			diskSpec: longhorn.DiskSpec{
				Type:              longhorn.DiskTypeFilesystem,
				Path:              TestDefaultDataPath,
				AllowScheduling:   true,
				EvictionRequested: true,
				Tags:              []string{"primary"},
			},
			diskStatus:   baseDiskStatus,
			volume:       baseVolume,
			expect:       false,
			expectReason: longhorn.ErrorReplicaScheduleDiskUnavailable,
		},
		{
			name: "Ineligible - disk type mismatch (block disk for v1 engine)",
			diskSpec: longhorn.DiskSpec{
				Type:            longhorn.DiskTypeBlock,
				Path:            TestDefaultDataPath,
				AllowScheduling: true,
				Tags:            []string{"primary"},
			},
			diskStatus:   baseDiskStatus,
			volume:       baseVolume,
			expect:       false,
			expectReason: longhorn.ErrorReplicaScheduleDiskUnavailable,
		},
		{
			name:       "Ineligible - backing image diskSelector mismatch",
			diskSpec:   baseDiskSpec,
			diskStatus: baseDiskStatus,
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Size:         TestVolumeSize,
					DataEngine:   longhorn.DataEngineTypeV1,
					DiskSelector: []string{"primary"},
					BackingImage: "test-bi",
				},
			},
			biDiskSelector: []string{"special-bi-tag"},
			expect:         false,
			expectReason:   longhorn.ErrorReplicaScheduleTagsNotFulfilled,
		},
		{
			name: "Eligible - backing image diskSelector matches disk tags",
			diskSpec: longhorn.DiskSpec{
				Type:            longhorn.DiskTypeFilesystem,
				Path:            TestDefaultDataPath,
				AllowScheduling: true,
				Tags:            []string{"primary", "special-bi-tag"},
			},
			diskStatus: baseDiskStatus,
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Size:         TestVolumeSize,
					DataEngine:   longhorn.DataEngineTypeV1,
					DiskSelector: []string{"primary"},
					BackingImage: "test-bi",
				},
			},
			biDiskSelector: []string{"special-bi-tag"},
			expect:         true,
		},
		{
			name: "Ineligible - backing image with empty diskSelector, allowEmpty=false, disk has tags",
			diskSpec: longhorn.DiskSpec{
				Type:            longhorn.DiskTypeFilesystem,
				Path:            TestDefaultDataPath,
				AllowScheduling: true,
				Tags:            []string{"primary"},
			},
			diskStatus: baseDiskStatus,
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Size:         TestVolumeSize,
					DataEngine:   longhorn.DataEngineTypeV1,
					DiskSelector: []string{"primary"},
					BackingImage: "test-bi",
				},
			},
			biDiskSelector:            []string{},
			allowEmptyDiskSelectorVol: false,
			expect:                    false,
			expectReason:              longhorn.ErrorReplicaScheduleTagsNotFulfilled,
		},
		{
			name: "Eligible - empty volume diskSelector with allowEmptyDiskSelectorVolume=true",
			diskSpec: longhorn.DiskSpec{
				Type:            longhorn.DiskTypeFilesystem,
				Path:            TestDefaultDataPath,
				AllowScheduling: true,
				Tags:            []string{"anything"},
			},
			diskStatus: baseDiskStatus,
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Size:         TestVolumeSize,
					DataEngine:   longhorn.DataEngineTypeV1,
					DiskSelector: []string{},
				},
			},
			allowEmptyDiskSelectorVol: true,
			expect:                    true,
		},
		{
			name: "Ineligible - empty volume diskSelector with allowEmptyDiskSelectorVolume=false and disk has tags",
			diskSpec: longhorn.DiskSpec{
				Type:            longhorn.DiskTypeFilesystem,
				Path:            TestDefaultDataPath,
				AllowScheduling: true,
				Tags:            []string{"anything"},
			},
			diskStatus: baseDiskStatus,
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Size:         TestVolumeSize,
					DataEngine:   longhorn.DataEngineTypeV1,
					DiskSelector: []string{},
				},
			},
			allowEmptyDiskSelectorVol: false,
			expect:                    false,
			expectReason:              longhorn.ErrorReplicaScheduleTagsNotFulfilled,
		},
	}

	for _, tt := range tests {
		c.Logf("Running scenario: %s", tt.name)
		result, reason, _ := rcs.IsDiskEligibleForVolume(tt.diskSpec, tt.diskStatus, tt.volume, tt.allowEmptyDiskSelectorVol, tt.biDiskSelector)
		c.Assert(result, Equals, tt.expect, Commentf("scenario: %s", tt.name))
		if !tt.expect {
			c.Assert(reason, Equals, tt.expectReason, Commentf("scenario: %s reason", tt.name))
		}
	}
}

// TestLinkedCloneScheduler covers the linked-clone scheduling path introduced
// in the refactoring of buildLinkedCloneSrcNodeDiskMap and FindDiskCandidates.
func (s *TestSuite) TestLinkedCloneScheduler(c *C) {

	// ------------------------------------------------------------------
	// setupLinkedCloneEnv builds the full scheduler environment used by
	// Tests 1–5.  schedulableNodes are created as proper Longhorn nodes
	// with disks.  For each name in srcReplicaNodes, a healthy source
	// replica is registered in the indexer (no node object is required
	// for src nodes — only replica objects are read during map building).
	// ------------------------------------------------------------------
	type linkedCloneEnv struct {
		rcs         *ReplicaScheduler
		srcReplicas []*longhorn.Replica // one per srcReplicaNode, in input order
		cloneVolume *longhorn.Volume
	}

	setupLinkedCloneEnv := func(schedulableNodes, srcReplicaNodes []string) linkedCloneEnv {
		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		rIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
		nIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
		eiIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
		imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
		sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
		pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

		rcs := newReplicaScheduler(lhClient, kubeClient, extensionsClient, informerFactories)

		// Default instance-manager-image setting.
		defaultIMSetting := initSettings(string(types.SettingNameDefaultInstanceManagerImage), TestInstanceManagerImage)
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), defaultIMSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)

		// Engine image shared by all schedulable nodes.
		ei := newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed)

		daemonIPs := []string{TestIP1, TestIP2, TestIP3}
		daemonNames := []string{TestDaemon1, TestDaemon2, TestDaemon3}
		for i, nodeName := range schedulableNodes {
			daemon := newDaemonPod(corev1.PodRunning, daemonNames[i%3], TestNamespace, nodeName, daemonIPs[i%3])
			p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), daemon, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = pIndexer.Add(p)
			c.Assert(err, IsNil)

			diskID := getDiskID(nodeName, "1")
			node := newNode(nodeName, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
			node.Spec.Disks = map[string]longhorn.DiskSpec{
				diskID: newDisk(TestDefaultDataPath, true, 0),
			}
			node.Status.DiskStatus = map[string]*longhorn.DiskStatus{
				diskID: {
					StorageAvailable: TestDiskAvailableSize,
					StorageScheduled: 0,
					StorageMaximum:   TestDiskSize,
					Conditions: []longhorn.Condition{
						newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
					},
					DiskUUID: diskID,
					Type:     longhorn.DiskTypeFilesystem,
				},
			}
			ei.Status.NodeDeploymentMap[nodeName] = true

			n, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = nIndexer.Add(n)
			c.Assert(err, IsNil)

			im := newInstanceManager(nodeName)
			imObj, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = imIndexer.Add(imObj)
			c.Assert(err, IsNil)
		}

		eiObj, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), ei, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = eiIndexer.Add(eiObj)
		c.Assert(err, IsNil)

		// Source volume: one healthy replica per srcReplicaNode.
		srcVolName := "src-vol-linked-clone"
		srcVol := newVolume(srcVolName, len(srcReplicaNodes))
		srcReplicas := make([]*longhorn.Replica, 0, len(srcReplicaNodes))
		for _, nodeName := range srcReplicaNodes {
			r := newReplicaForVolume(srcVol)
			r.Spec.NodeID = nodeName
			r.Spec.DiskID = getDiskID(nodeName, "1")
			r.Spec.HealthyAt = TestTimeNow
			r.Spec.FailedAt = ""
			created, err := lhClient.LonghornV1beta2().Replicas(TestNamespace).Create(context.TODO(), r, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = rIndexer.Add(created)
			c.Assert(err, IsNil)
			srcReplicas = append(srcReplicas, created)
		}

		// Clone volume that references the source volume.
		cloneVol := newVolume("clone-vol-linked-clone", 1)
		cloneVol.Spec.CloneMode = longhorn.CloneModeLinkedClone
		cloneVol.Spec.DataSource = types.NewVolumeDataSourceTypeVolume(srcVolName)

		return linkedCloneEnv{
			rcs:         rcs,
			srcReplicas: srcReplicas,
			cloneVolume: cloneVol,
		}
	}

	// ------------------------------------------------------------------
	// Test 1: FindDiskCandidates — linked-clone, successful scheduling
	//   All three schedulable nodes each hold a healthy source replica.
	//   Scheduling must succeed and every returned disk must belong to
	//   one of the source replica nodes.
	// ------------------------------------------------------------------
	fmt.Println("testing FindDiskCandidates linked-clone - successful scheduling")
	{
		env := setupLinkedCloneEnv(
			[]string{TestNode1, TestNode2, TestNode3},
			[]string{TestNode1, TestNode2, TestNode3},
		)

		cloneReplica3 := newReplicaForVolume(env.cloneVolume)
		allReplicas3 := map[string]*longhorn.Replica{cloneReplica3.Name: cloneReplica3}

		diskCandidates3, errs3 := env.rcs.FindDiskCandidates(cloneReplica3, allReplicas3, env.cloneVolume)
		c.Assert(len(errs3), Equals, 0)
		c.Assert(len(diskCandidates3), Not(Equals), 0)

		validSrcDisks3 := map[string]bool{
			getDiskID(TestNode1, "1"): true,
			getDiskID(TestNode2, "1"): true,
			getDiskID(TestNode3, "1"): true,
		}
		for diskUUID := range diskCandidates3 {
			c.Assert(validSrcDisks3[diskUUID], Equals, true,
				Commentf("unexpected disk candidate %q not in source replica disks", diskUUID))
		}
	}

	// ------------------------------------------------------------------
	// Test 2: FindDiskCandidates — linked-clone, no schedulable src nodes
	//   Source replicas live on nodes outside the schedulable set.  After
	//   filtering the intersection is empty, so scheduling must fail with
	//   ErrorReplicaScheduleLinkedCloneNotSatisfied.
	// ------------------------------------------------------------------
	fmt.Println("testing FindDiskCandidates linked-clone - no schedulable src nodes")
	{
		env := setupLinkedCloneEnv(
			[]string{TestNode1, TestNode2, TestNode3},
			[]string{"extra-node-4", "extra-node-5"},
		)

		cloneReplica4 := newReplicaForVolume(env.cloneVolume)
		allReplicas4 := map[string]*longhorn.Replica{cloneReplica4.Name: cloneReplica4}

		diskCandidates4, errs4 := env.rcs.FindDiskCandidates(cloneReplica4, allReplicas4, env.cloneVolume)
		c.Assert(len(diskCandidates4), Equals, 0)
		_, hasLinkedCloneErr := errs4[longhorn.ErrorReplicaScheduleLinkedCloneNotSatisfied]
		c.Assert(hasLinkedCloneErr, Equals, true)
	}

	// ------------------------------------------------------------------
	// Test 3: FindDiskCandidates — linked-clone, hard constraint
	//   LinkedCloneSrcReplicaName is set to the replica on TestNode2.
	//   Even though all three nodes are schedulable, only the disk on
	//   TestNode2 may be returned as a candidate.
	// ------------------------------------------------------------------
	fmt.Println("testing FindDiskCandidates linked-clone - hard constraint (known src replica)")
	{
		env := setupLinkedCloneEnv(
			[]string{TestNode1, TestNode2, TestNode3},
			[]string{TestNode1, TestNode2, TestNode3},
		)

		cloneReplica5 := newReplicaForVolume(env.cloneVolume)
		// srcReplicas[1] is the replica on TestNode2 (order matches input slice).
		cloneReplica5.Spec.LinkedCloneSrcReplicaName = env.srcReplicas[1].Name

		allReplicas5 := map[string]*longhorn.Replica{cloneReplica5.Name: cloneReplica5}

		diskCandidates5, errs5 := env.rcs.FindDiskCandidates(cloneReplica5, allReplicas5, env.cloneVolume)
		c.Assert(len(errs5), Equals, 0)
		c.Assert(len(diskCandidates5), Equals, 1)

		expectedDisk5 := getDiskID(TestNode2, "1")
		_, hasDisk5 := diskCandidates5[expectedDisk5]
		c.Assert(hasDisk5, Equals, true,
			Commentf("expected disk %q to be the sole candidate, got %v", expectedDisk5, diskCandidates5))
	}

	// ------------------------------------------------------------------
	// Test 4: FindDiskCandidates — soft anti-affinity fallback
	//   Only TestNode1 is schedulable. Source replicas exist on all three
	//   nodes, but TestNode2 and TestNode3 are not schedulable (no node
	//   object / instance manager). An existing clone replica already
	//   occupies TestNode1. With node soft anti-affinity explicitly
	//   enabled on the volume, scheduling must fall back to TestNode1
	//   rather than fail.
	// ------------------------------------------------------------------
	fmt.Println("testing FindDiskCandidates linked-clone - soft anti-affinity fallback")
	{
		// Only TestNode1 is schedulable; src replicas exist on all three.
		env := setupLinkedCloneEnv(
			[]string{TestNode1},
			[]string{TestNode1, TestNode2, TestNode3},
		)

		// Enable node soft anti-affinity on the volume so the scheduler
		// is allowed to reuse a node that already hosts a clone replica.
		env.cloneVolume.Spec.ReplicaSoftAntiAffinity = longhorn.ReplicaSoftAntiAffinityEnabled

		// Existing clone replica already placed on TestNode1.
		existingClone := newReplicaForVolume(env.cloneVolume)
		existingClone.Spec.NodeID = TestNode1
		existingClone.Spec.DiskID = getDiskID(TestNode1, "1")
		existingClone.Spec.HealthyAt = TestTimeNow
		existingClone.Spec.LinkedCloneSrcReplicaName = env.srcReplicas[0].Name

		// New clone replica to schedule — no src replica assigned yet.
		newClone := newReplicaForVolume(env.cloneVolume)

		allReplicas6 := map[string]*longhorn.Replica{
			existingClone.Name: existingClone,
			newClone.Name:      newClone,
		}

		// With soft anti-affinity enabled, scheduling must succeed by
		// falling back to TestNode1 even though it already hosts a clone.
		diskCandidates6, errs6 := env.rcs.FindDiskCandidates(newClone, allReplicas6, env.cloneVolume)
		c.Assert(len(errs6), Equals, 0,
			Commentf("expected no errors but got: %v", errs6))
		c.Assert(len(diskCandidates6), Not(Equals), 0,
			Commentf("expected fallback scheduling on TestNode1 to succeed"))

		expectedDisk6 := getDiskID(TestNode1, "1")
		_, hasDisk6 := diskCandidates6[expectedDisk6]
		c.Assert(hasDisk6, Equals, true,
			Commentf("expected fallback disk %q, got %v", expectedDisk6, diskCandidates6))
	}

	// ------------------------------------------------------------------
	// Test 5: FindDiskCandidates — linked-clone, strict anti-affinity blocks reuse
	//   Only TestNode1 is schedulable and source replicas exist on all three
	//   nodes. An existing clone replica already occupies TestNode1. With
	//   node soft anti-affinity disabled (the global default), the scheduler
	//   must not reuse TestNode1 and must return no candidates and no error,
	//   because the anti-affinity gate silently skips used nodes rather than
	//   raising a scheduling error.
	// ------------------------------------------------------------------
	fmt.Println("testing FindDiskCandidates linked-clone - strict anti-affinity blocks reuse")
	{
		env := setupLinkedCloneEnv(
			[]string{TestNode1},
			[]string{TestNode1, TestNode2, TestNode3},
		)

		// ReplicaSoftAntiAffinity is left at its global default ("false"),
		// so the volume spec override is intentionally not set here.

		existingClone7 := newReplicaForVolume(env.cloneVolume)
		existingClone7.Spec.NodeID = TestNode1
		existingClone7.Spec.DiskID = getDiskID(TestNode1, "1")
		existingClone7.Spec.HealthyAt = TestTimeNow
		existingClone7.Spec.LinkedCloneSrcReplicaName = env.srcReplicas[0].Name

		newClone7 := newReplicaForVolume(env.cloneVolume)

		allReplicas7 := map[string]*longhorn.Replica{
			existingClone7.Name: existingClone7,
			newClone7.Name:      newClone7,
		}

		diskCandidates7, errs7 := env.rcs.FindDiskCandidates(newClone7, allReplicas7, env.cloneVolume)
		c.Assert(len(diskCandidates7), Equals, 0,
			Commentf("expected no candidates when strict anti-affinity blocks the only available node"))
		c.Assert(len(errs7), Equals, 0,
			Commentf("expected no scheduling error — strict anti-affinity silently skips used nodes"))
	}
}
