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

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
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
	pods            map[string]*v1.Pod
	replicas        []*longhorn.Replica
	kubeNodes       map[string]*v1.Node
	engineManagers  map[string]*longhorn.InstanceManager
	replicaManagers map[string]*longhorn.InstanceManager

	expectNodeStatus      map[string]longhorn.NodeStatus
	expectEngineManagers  map[string]*longhorn.InstanceManager
	expectReplicaManagers map[string]*longhorn.InstanceManager
}

func newTestNodeController(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset, controllerID string) *NodeController {
	ds := datastore.NewDataStore(lhInformerFactory, lhClient, kubeInformerFactory, kubeClient, TestNamespace)

	logger := logrus.StandardLogger()
	nc := NewNodeController(logger, ds, scheme.Scheme, kubeClient, TestNamespace, controllerID)
	fakeRecorder := record.NewFakeRecorder(100)
	nc.eventRecorder = fakeRecorder
	nc.getDiskInfoHandler = fakeGetDiskInfo
	nc.topologyLabelsChecker = fakeTopologyLabelsChecker
	nc.getDiskConfig = fakeGetDiskConfig
	nc.generateDiskConfig = fakeGenerateDiskConfig
	for index := range nc.cacheSyncs {
		nc.cacheSyncs[index] = alwaysReady
	}
	return nc
}

func fakeGetDiskInfo(directory string) (*util.DiskInfo, error) {
	return &util.DiskInfo{
		Fsid:       "fsid",
		Path:       directory,
		Type:       "ext4",
		FreeBlock:  0,
		TotalBlock: 0,
		BlockSize:  0,

		StorageMaximum:   0,
		StorageAvailable: 0,
	}, nil
}

func fakeTopologyLabelsChecker(kubeClient clientset.Interface, vers string) (bool, error) {
	return false, nil
}

func fakeGetDiskConfig(path string) (*util.DiskConfig, error) {
	return &util.DiskConfig{
		DiskUUID: TestDiskID1,
	}, nil
}

func fakeGenerateDiskConfig(path string) (*util.DiskConfig, error) {
	return &util.DiskConfig{
		DiskUUID: TestDiskID1,
	}, nil
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
				},
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
				},
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
		},
	}
	node2 = newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node2.Status.DiskStatus = map[string]*longhorn.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
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
				},
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
				},
			},
		},
	}
	tc.expectEngineManagers = map[string]*longhorn.InstanceManager{
		TestEngineManagerName: newInstanceManager(TestEngineManagerName, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, false),
	}
	tc.expectReplicaManagers = map[string]*longhorn.InstanceManager{
		TestReplicaManagerName: newInstanceManager(TestReplicaManagerName, longhorn.InstanceManagerTypeReplica, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, false),
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
				},
			},
		},
	}
	extraEngineManager := newInstanceManager("extra-engine-manger-name", longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
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
	extraEngineManager.Spec.Image = TestExtraInstanceManagerImage
	extraReplicaManager := newInstanceManager("extra-replica-manger-name", longhorn.InstanceManagerTypeReplica, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, false)
	extraReplicaManager.Spec.Image = TestExtraInstanceManagerImage
	tc.engineManagers = map[string]*longhorn.InstanceManager{
		TestEngineManagerName:      newInstanceManager(TestEngineManagerName, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, false),
		"extra-engine-manger-name": extraEngineManager,
	}
	tc.replicaManagers = map[string]*longhorn.InstanceManager{
		TestReplicaManagerName:      newInstanceManager(TestReplicaManagerName, longhorn.InstanceManagerTypeReplica, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, false),
		"extra-replica-manger-name": extraReplicaManager,
	}
	tc.expectEngineManagers = map[string]*longhorn.InstanceManager{
		TestEngineManagerName:      newInstanceManager(TestEngineManagerName, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, false),
		"extra-engine-manger-name": extraEngineManager,
	}
	tc.expectReplicaManagers = map[string]*longhorn.InstanceManager{
		TestReplicaManagerName: newInstanceManager(TestReplicaManagerName, longhorn.InstanceManagerTypeReplica, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, false),
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
	extraReplicaManager = newInstanceManager("extra-replica-manger-name", longhorn.InstanceManagerTypeReplica, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
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
	extraReplicaManager.Spec.Image = TestExtraInstanceManagerImage
	tc.engineManagers = map[string]*longhorn.InstanceManager{
		TestEngineManagerName: newInstanceManager(TestEngineManagerName, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, false),
	}
	tc.replicaManagers = map[string]*longhorn.InstanceManager{
		TestReplicaManagerName:      newInstanceManager(TestReplicaManagerName, longhorn.InstanceManagerTypeReplica, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, false),
		"extra-replica-manger-name": extraReplicaManager,
	}
	tc.expectEngineManagers = map[string]*longhorn.InstanceManager{
		TestEngineManagerName: newInstanceManager(TestEngineManagerName, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, false),
	}
	tc.expectReplicaManagers = map[string]*longhorn.InstanceManager{}
	testCases["clean up all replica managers if there is no disk on the node"] = tc

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

		// create kuberentes node
		for _, kubeNode := range tc.kubeNodes {
			n, err := kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = knIndexer.Add(n)
			c.Assert(err, IsNil)

		}

		nc := newTestNodeController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient, TestNode1)
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
		for _, em := range tc.engineManagers {
			em, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), em, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = imIndexer.Add(em)
			c.Assert(err, IsNil)
		}
		for _, rm := range tc.replicaManagers {
			rm, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), rm, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = imIndexer.Add(rm)
			c.Assert(err, IsNil)
		}
		// sync node status
		for nodeName, node := range tc.nodes {
			err := nc.syncNode(getKey(node, c))
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

		for emName := range tc.engineManagers {
			em, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Get(context.TODO(), emName, metav1.GetOptions{})
			if expectEM, exist := tc.expectEngineManagers[emName]; !exist {
				c.Assert(datastore.ErrorIsNotFound(err), Equals, true)
			} else {
				c.Assert(em.Spec, DeepEquals, expectEM.Spec)
			}
		}
		for rmName := range tc.replicaManagers {
			rm, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Get(context.TODO(), rmName, metav1.GetOptions{})
			if expectRM, exist := tc.expectReplicaManagers[rmName]; !exist {
				c.Assert(datastore.ErrorIsNotFound(err), Equals, true)
			} else {
				c.Assert(rm.Spec, DeepEquals, expectRM.Spec)
			}
		}

	}
}
