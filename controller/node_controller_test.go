package controller

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	monitor "github.com/longhorn/longhorn-manager/controller/monitor"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"

	. "gopkg.in/check.v1"
)

const (
	ManagerPodUp     = "managerPodUp"
	ManagerPodDown   = "managerPodDown"
	KubeNodeDown     = "kubeNodeDown"
	KubeNodePressure = "kubeNodePressure"
)

var (
	MountPropagationBidirectional = v1.MountPropagationBidirectional
)

type NodeTestCase struct {
	nodes            map[string]*longhorn.Node
	pods             map[string]*v1.Pod
	replicas         []*longhorn.Replica
	kubeNodes        map[string]*v1.Node
	instanceManagers map[string]*longhorn.InstanceManager

	expectNodeStatus       map[string]longhorn.NodeStatus
	expectInstanceManagers map[string]*longhorn.InstanceManager
	expectOrphans          []*longhorn.Orphan
}

func newTestNodeController(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset, controllerID string) *NodeController {
	ds := datastore.NewDataStore(lhInformerFactory, lhClient, kubeInformerFactory, kubeClient, extensionsClient, TestNamespace)

	logger := logrus.StandardLogger()
	nc := NewNodeController(logger, ds, scheme.Scheme, kubeClient, TestNamespace, controllerID)
	fakeRecorder := record.NewFakeRecorder(100)
	nc.eventRecorder = fakeRecorder
	nc.topologyLabelsChecker = fakeTopologyLabelsChecker

	enqueueNodeForMonitor := func(key string) {
		nc.queue.Add(key)
	}
	mon, err := monitor.NewFakeNodeMonitor(nc.logger, nc.ds, controllerID, enqueueNodeForMonitor)
	if err != nil {
		return nil
	}
	nc.diskMonitor = mon

	for index := range nc.cacheSyncs {
		nc.cacheSyncs[index] = alwaysReady
	}
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
	node1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusUnknown, "")
	node2 := newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusUnknown, "")
	nodes := map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	nodeStatus := map[string]longhorn.NodeStatus{}
	switch testType {
	case ManagerPodUp:
		nodeStatus = map[string]longhorn.NodeStatus{
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
		}
	case ManagerPodDown:
		nodeStatus = map[string]longhorn.NodeStatus{
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
		}
	case KubeNodeDown:
		nodeStatus = map[string]longhorn.NodeStatus{
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
		}
	case KubeNodePressure:
		nodeStatus = map[string]longhorn.NodeStatus{
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
		}
	}
	tc.pods = generateManagerPod(testType)

	tc.expectNodeStatus = nodeStatus

	tc.expectOrphans = []*longhorn.Orphan{
		{
			Spec: longhorn.OrphanSpec{
				NodeID: TestNode1,
				Type:   longhorn.OrphanTypeReplica,
				Parameters: map[string]string{
					longhorn.OrphanDataName: monitor.TestOrphanedReplicaDirectoryName,
					longhorn.OrphanDiskName: TestDiskID1,
					longhorn.OrphanDiskUUID: TestDiskID1,
					longhorn.OrphanDiskPath: TestDefaultDataPath,
				},
			},
			Status: longhorn.OrphanStatus{
				OwnerID: TestNode1,
			},
		},
	}

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
	node1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
			Type:             longhorn.DiskTypeFilesystem,
		},
	}
	node2 := newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node2.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
			Conditions: []longhorn.Condition{
				newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusUnknown, ""),
			},
			Type: longhorn.DiskTypeFilesystem,
		},
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	volume := newVolume(TestVolumeName, 2)
	engine := newEngineForVolume(volume)
	replica1 := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1)
	replica2 := newReplicaForVolume(volume, engine, TestNode2, TestDiskID2)
	replicas := []*longhorn.Replica{replica1, replica2}
	tc.replicas = replicas

	tc.expectNodeStatus = map[string]longhorn.NodeStatus{
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
						replica1.Name: replica1.Spec.VolumeSize,
					},
					DiskUUID: TestDiskID1,
					Type:     longhorn.DiskTypeFilesystem,
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
					Type: longhorn.DiskTypeFilesystem,
				},
			},
		},
	}

	tc.expectOrphans = []*longhorn.Orphan{
		{
			Spec: longhorn.OrphanSpec{
				NodeID: TestNode1,
				Type:   longhorn.OrphanTypeReplica,
				Parameters: map[string]string{
					longhorn.OrphanDataName: monitor.TestOrphanedReplicaDirectoryName,
					longhorn.OrphanDiskName: TestDiskID1,
					longhorn.OrphanDiskUUID: TestDiskID1,
					longhorn.OrphanDiskPath: TestDefaultDataPath,
				},
			},
			Status: longhorn.OrphanStatus{
				OwnerID: TestNode1,
			},
		},
	}
	testCases["only disk on node1 should be updated status"] = tc

	tc = &NodeTestCase{}
	tc.kubeNodes = generateKubeNodes(ManagerPodUp)
	tc.pods = generateManagerPod(ManagerPodUp)
	node1 = newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
			Conditions: []longhorn.Condition{
				newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
			},
		},
		"unavailable-disk": {
			StorageScheduled: 0,
			StorageAvailable: 0,
			Conditions: []longhorn.Condition{
				newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
			},
		},
	}
	node2 = newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node2.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
			Type:             longhorn.DiskTypeFilesystem,
		},
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.expectNodeStatus = map[string]longhorn.NodeStatus{
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
					ScheduledReplica: map[string]int64{},
					DiskUUID:         TestDiskID1,
					Type:             longhorn.DiskTypeFilesystem,
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
					Type:             longhorn.DiskTypeFilesystem,
				},
			},
		},
	}

	tc.expectOrphans = []*longhorn.Orphan{
		{
			Spec: longhorn.OrphanSpec{
				NodeID: TestNode1,
				Type:   longhorn.OrphanTypeReplica,
				Parameters: map[string]string{
					longhorn.OrphanDataName: monitor.TestOrphanedReplicaDirectoryName,
					longhorn.OrphanDiskName: TestDiskID1,
					longhorn.OrphanDiskUUID: TestDiskID1,
					longhorn.OrphanDiskPath: TestDefaultDataPath,
				},
			},
			Status: longhorn.OrphanStatus{
				OwnerID: TestNode1,
			},
		},
	}
	testCases["clean disk status when disk removed from the node spec"] = tc

	tc = &NodeTestCase{}
	tc.kubeNodes = generateKubeNodes(ManagerPodUp)
	tc.pods = generateManagerPod(ManagerPodUp)
	node1 = newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node1.Spec.Disks = map[string]longhorn.DiskSpec{
		TestDiskID1: {
			Type:            longhorn.DiskTypeFilesystem,
			Path:            TestDefaultDataPath,
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
			DiskUUID: "new-uuid",
			Type:     longhorn.DiskTypeFilesystem,
		},
	}
	node2 = newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node2.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
			Type:             longhorn.DiskTypeFilesystem,
		},
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.expectNodeStatus = map[string]longhorn.NodeStatus{
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
					ScheduledReplica: map[string]int64{},
					DiskUUID:         "new-uuid",
					Type:             longhorn.DiskTypeFilesystem,
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
					Type:             longhorn.DiskTypeFilesystem,
				},
			},
		},
	}
	tc.expectOrphans = []*longhorn.Orphan{
		{
			Spec: longhorn.OrphanSpec{
				NodeID: TestNode1,
				Type:   longhorn.OrphanTypeReplica,
				Parameters: map[string]string{
					longhorn.OrphanDataName: monitor.TestOrphanedReplicaDirectoryName,
					longhorn.OrphanDiskName: TestDiskID1,
					longhorn.OrphanDiskUUID: TestDiskID1,
					longhorn.OrphanDiskPath: TestDefaultDataPath,
				},
			},
			Status: longhorn.OrphanStatus{
				OwnerID: TestNode1,
			},
		},
	}
	testCases["test disable disk when file system changed"] = tc

	tc = &NodeTestCase{}
	tc.kubeNodes = generateKubeNodes(ManagerPodUp)
	tc.pods = generateManagerPod(ManagerPodUp)
	node1 = newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
			Conditions: []longhorn.Condition{
				newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
			},
			Type: longhorn.DiskTypeFilesystem,
		},
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: node1,
	}

	tc.expectNodeStatus = map[string]longhorn.NodeStatus{
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
					ScheduledReplica: map[string]int64{},
					DiskUUID:         TestDiskID1,
					Type:             longhorn.DiskTypeFilesystem,
				},
			},
		},
	}

	tc.expectInstanceManagers = map[string]*longhorn.InstanceManager{
		TestInstanceManagerName: newInstanceManager(
			TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
			TestOwnerID1, TestNode1, TestIP1,
			map[string]longhorn.InstanceProcess{},
			map[string]longhorn.InstanceProcess{},
			false,
		),
	}

	tc.expectOrphans = []*longhorn.Orphan{
		{
			Spec: longhorn.OrphanSpec{
				NodeID: TestNode1,
				Type:   longhorn.OrphanTypeReplica,
				Parameters: map[string]string{
					longhorn.OrphanDataName: monitor.TestOrphanedReplicaDirectoryName,
					longhorn.OrphanDiskName: TestDiskID1,
					longhorn.OrphanDiskUUID: TestDiskID1,
					longhorn.OrphanDiskPath: TestDefaultDataPath,
				},
			},
			Status: longhorn.OrphanStatus{
				OwnerID: TestNode1,
			},
		},
	}
	testCases["create default instance managers after node up"] = tc

	tc = &NodeTestCase{}
	tc.kubeNodes = generateKubeNodes(ManagerPodUp)
	tc.pods = generateManagerPod(ManagerPodUp)
	node1 = newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
			Conditions: []longhorn.Condition{
				newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
			},
			Type: longhorn.DiskTypeFilesystem,
		},
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: node1,
	}
	tc.expectNodeStatus = map[string]longhorn.NodeStatus{
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
					ScheduledReplica: map[string]int64{},
					DiskUUID:         TestDiskID1,
					Type:             longhorn.DiskTypeFilesystem,
				},
			},
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
		false,
	)
	extraInstanceManager.Spec.Image = TestExtraInstanceManagerImage

	tc.instanceManagers = map[string]*longhorn.InstanceManager{
		TestInstanceManagerName: newInstanceManager(
			TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
			TestOwnerID1, TestNode1, TestIP1,
			map[string]longhorn.InstanceProcess{},
			map[string]longhorn.InstanceProcess{},
			false,
		),
		"extra-instance-manger-name": extraInstanceManager,
	}

	tc.expectInstanceManagers = map[string]*longhorn.InstanceManager{
		TestInstanceManagerName: newInstanceManager(
			TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
			TestOwnerID1, TestNode1, TestIP1,
			map[string]longhorn.InstanceProcess{},
			map[string]longhorn.InstanceProcess{},
			false,
		),
		"extra-instance-manger-name": extraInstanceManager,
	}

	tc.expectOrphans = []*longhorn.Orphan{
		{
			Spec: longhorn.OrphanSpec{
				NodeID: TestNode1,
				Type:   longhorn.OrphanTypeReplica,
				Parameters: map[string]string{
					longhorn.OrphanDataName: monitor.TestOrphanedReplicaDirectoryName,
					longhorn.OrphanDiskName: TestDiskID1,
					longhorn.OrphanDiskUUID: TestDiskID1,
					longhorn.OrphanDiskPath: TestDefaultDataPath,
				},
			},
			Status: longhorn.OrphanStatus{
				OwnerID: TestNode1,
			},
		},
	}
	testCases["clean up redundant instance managers only if there is no running instances"] = tc

	tc = &NodeTestCase{}
	tc.kubeNodes = generateKubeNodes(ManagerPodUp)
	tc.pods = generateManagerPod(ManagerPodUp)
	node1 = newNode(TestNode1, TestNamespace, false, longhorn.ConditionStatusTrue, "")
	node1.Spec.Disks = map[string]longhorn.DiskSpec{}
	node1.Status.DiskStatus = map[string]*longhorn.DiskStatus{}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: node1,
	}
	tc.expectNodeStatus = map[string]longhorn.NodeStatus{
		TestNode1: {
			Conditions: []longhorn.Condition{
				newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
				newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue, ""),
				newNodeCondition(longhorn.NodeConditionTypeMountPropagation, longhorn.ConditionStatusTrue, ""),
			},
			DiskStatus: map[string]*longhorn.DiskStatus{},
		},
	}
	extraInstanceManager = newInstanceManager(
		"extra-instance-manger-name", longhorn.InstanceManagerStateRunning,
		TestOwnerID1, TestNode1, TestIP1,
		map[string]longhorn.InstanceProcess{},
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
		}, false)
	extraInstanceManager.Spec.Image = TestExtraInstanceManagerImage

	tc.instanceManagers = map[string]*longhorn.InstanceManager{
		TestInstanceManagerName: newInstanceManager(
			TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
			TestOwnerID1, TestNode1, TestIP1,
			map[string]longhorn.InstanceProcess{},
			map[string]longhorn.InstanceProcess{},
			false,
		),
	}

	tc.expectInstanceManagers = map[string]*longhorn.InstanceManager{
		TestInstanceManagerName: newInstanceManager(
			TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
			TestOwnerID1, TestNode1, TestIP1,
			map[string]longhorn.InstanceProcess{},
			map[string]longhorn.InstanceProcess{},
			false,
		),
	}
	tc.expectOrphans = []*longhorn.Orphan{}
	testCases["clean up all instance managers if there is no disk on the node"] = tc

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)
		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		nIndexer := lhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
		pIndexer := kubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

		rIndexer := lhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
		knIndexer := kubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

		sIndexer := lhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
		imIndexer := lhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()

		imImageSetting := newDefaultInstanceManagerImageSetting()
		imImageSetting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), imImageSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(imImageSetting)
		c.Assert(err, IsNil)

		// create kubernetes node
		for _, kubeNode := range tc.kubeNodes {
			n, err := kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = knIndexer.Add(n)
			c.Assert(err, IsNil)

		}

		extensionsClient := apiextensionsfake.NewSimpleClientset()

		nc := newTestNodeController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient, extensionsClient, TestNode1)
		c.Assert(err, IsNil)

		// create manager pod
		for _, pod := range tc.pods {
			p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
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
		// create replicas
		for _, replica := range tc.replicas {
			r, err := lhClient.LonghornV1beta2().Replicas(TestNamespace).Create(context.TODO(), replica, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			c.Assert(r, NotNil)
			err = rIndexer.Add(r)
			c.Assert(err, IsNil)
		}
		// create instance managers
		for _, instanceManager := range tc.instanceManagers {
			em, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), instanceManager, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = imIndexer.Add(em)
			c.Assert(err, IsNil)
		}
		// sync node status
		for nodeName, node := range tc.nodes {
			if nc.controllerID == node.Name {
				err = nc.diskMonitor.RunOnce()
				c.Assert(err, IsNil)
			}

			err = nc.syncNode(getKey(node, c))
			c.Assert(err, IsNil)

			n, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Get(context.TODO(), node.Name, metav1.GetOptions{})
			c.Assert(err, IsNil)
			for ctype, condition := range n.Status.Conditions {
				condition.LastTransitionTime = ""
				condition.Message = ""
				n.Status.Conditions[ctype] = condition
			}
			c.Assert(n.Status.Conditions, DeepEquals, tc.expectNodeStatus[nodeName].Conditions)
			if len(tc.expectNodeStatus[nodeName].DiskStatus) > 0 {
				diskConditions := n.Status.DiskStatus
				for fsid, diskStatus := range diskConditions {
					for ctype, condition := range diskStatus.Conditions {
						if condition.Status != longhorn.ConditionStatusUnknown {
							c.Assert(condition.LastTransitionTime, Not(Equals), "")
						}
						condition.LastTransitionTime = ""
						condition.Message = ""
						diskStatus.Conditions[ctype] = condition
					}
					n.Status.DiskStatus[fsid] = diskStatus
				}
				c.Assert(n.Status.DiskStatus, DeepEquals, tc.expectNodeStatus[nodeName].DiskStatus)
			}
		}

		for name := range tc.instanceManagers {
			instanceManager, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Get(context.TODO(), name, metav1.GetOptions{})
			c.Assert(err, IsNil)
			if expectInstanceManager, exist := tc.expectInstanceManagers[name]; !exist {
				c.Assert(datastore.ErrorIsNotFound(err), Equals, true)
			} else {
				c.Assert(instanceManager.Spec, DeepEquals, expectInstanceManager.Spec)
			}
		}

		if orphanList, err := lhClient.LonghornV1beta2().Orphans(TestNamespace).List(context.TODO(), metav1.ListOptions{}); err == nil {
			c.Assert(len(orphanList.Items), Equals, len(tc.expectOrphans))
		} else {
			c.Assert(len(tc.expectOrphans), Equals, 0)
		}

		for _, orphan := range tc.expectOrphans {
			orphanName := types.GetOrphanChecksumNameForOrphanedDirectory(orphan.Spec.NodeID,
				orphan.Spec.Parameters[longhorn.OrphanDiskName],
				orphan.Spec.Parameters[longhorn.OrphanDiskPath],
				orphan.Spec.Parameters[longhorn.OrphanDiskUUID],
				orphan.Spec.Parameters[longhorn.OrphanDataName])
			_, err := lhClient.LonghornV1beta2().Orphans(TestNamespace).Get(context.TODO(), orphanName, metav1.GetOptions{})
			c.Assert(err, IsNil)
		}
	}
}
