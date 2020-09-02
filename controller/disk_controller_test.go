package controller

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"path/filepath"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

type DiskTestCase struct {
	disks    map[string]*longhorn.Disk
	nodes    map[string]*longhorn.Node
	replicas []*longhorn.Replica

	expectDiskStatus map[string]types.DiskStatus
	expectReplicas   map[string]*longhorn.Replica
}

func newTestDiskController(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset, controllerID string) *DiskController {
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
	dc := NewDiskController(logger,
		ds, scheme.Scheme,
		diskInformer, nodeInformer, replicaInformer, settingInformer,
		kubeClient, TestNamespace, controllerID)
	fakeRecorder := record.NewFakeRecorder(100)
	dc.eventRecorder = fakeRecorder
	dc.getDiskInfoHandler = fakeGetDiskInfo
	dc.getDiskConfig = fakeGetDiskConfig

	dc.dStoreSynced = alwaysReady
	dc.nStoreSynced = alwaysReady
	dc.rStoreSynced = alwaysReady
	dc.sStoreSynced = alwaysReady

	dc.nowHandler = getTestNow

	return dc
}

func fakeGetDiskInfo(directory string) (*util.DiskInfo, error) {
	if directory == TestInvalidDataPath {
		return nil, fmt.Errorf("invalid path")
	}
	return &util.DiskInfo{
		Fsid:       TestDefaultDiskFSID,
		Path:       directory,
		Type:       "ext4",
		FreeBlock:  0,
		TotalBlock: 0,
		BlockSize:  0,

		StorageMaximum:   TestDiskSize,
		StorageAvailable: TestDiskAvailableSize,
	}, nil
}

func fakeGetDiskConfig(path string) (*util.DiskConfig, error) {
	return &util.DiskConfig{
		DiskUUID: TestDefaultDiskUUID1,
	}, nil
}

func fakeGenerateDiskConfig(path string) (*util.DiskConfig, error) {
	return &util.DiskConfig{
		DiskUUID: TestDefaultDiskUUID1,
	}, nil
}

func (s *TestSuite) TestSyncDisk(c *C) {
	testCases := map[string]*DiskTestCase{}

	tc := &DiskTestCase{}
	tc.disks = map[string]*longhorn.Disk{
		TestDefaultDiskUUID1: newDisk(TestDefaultDiskUUID1, TestNamespace, TestNode1),
		TestDefaultDiskUUID2: newDisk(TestDefaultDiskUUID2, TestNamespace, TestNode2),
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: newNode(TestNode1, TestNamespace, TestDefaultDiskUUID1, true, types.ConditionStatusTrue, ""),
		TestNode2: newNode(TestNode2, TestNamespace, "", true, types.ConditionStatusTrue, ""),
	}
	volume := newVolume(TestVolumeName, 2)
	engine := newEngineForVolume(volume)
	replica1 := newReplicaForVolume(volume, engine, TestNode1, TestDefaultDiskUUID1)
	replica2 := newReplicaForVolume(volume, engine, TestNode2, TestDefaultDiskUUID2)
	tc.replicas = []*longhorn.Replica{replica1, replica2}
	tc.expectDiskStatus = map[string]types.DiskStatus{
		TestDefaultDiskUUID1: types.DiskStatus{
			OwnerID:          TestNode1,
			StorageScheduled: TestVolumeSize,
			StorageAvailable: TestDiskAvailableSize,
			StorageMaximum:   TestDiskSize,
			Conditions: map[string]types.Condition{
				types.DiskConditionTypeReady:       newNodeCondition(types.DiskConditionTypeReady, types.ConditionStatusTrue, ""),
				types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusTrue, ""),
			},
			ScheduledReplica: map[string]int64{
				replica1.Name: replica1.Spec.VolumeSize,
			},
			NodeID: TestNode1,
			FSID:   TestDefaultDiskFSID,
			Path:   TestDefaultDataPath,
			State:  types.DiskStateConnected,
		},
		TestDefaultDiskUUID2: tc.disks[TestDefaultDiskUUID2].Status,
	}
	tc.expectReplicas = map[string]*longhorn.Replica{
		replica1.Name: replica1,
		replica2.Name: replica2,
	}
	testCases["only disk on node1 should be updated status"] = tc

	tc = &DiskTestCase{}
	tc.disks = map[string]*longhorn.Disk{
		TestDefaultDiskUUID1: newDisk(TestDefaultDiskUUID1, TestNamespace, ""),
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode2: newNode(TestNode2, TestNamespace, TestDefaultDiskUUID1, true, types.ConditionStatusTrue, ""),
	}
	tc.expectDiskStatus = map[string]types.DiskStatus{
		TestDefaultDiskUUID1: types.DiskStatus{
			OwnerID:          TestNode1,
			StorageScheduled: 0,
			StorageAvailable: 0,
			StorageMaximum:   0,
			Conditions: map[string]types.Condition{
				types.DiskConditionTypeReady:       newNodeCondition(types.DiskConditionTypeReady, types.ConditionStatusFalse, types.DiskConditionReasonNodeUnknown),
				types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusFalse, types.DiskConditionReasonDiskNotReady),
			},
			ScheduledReplica: map[string]int64{},
			NodeID:           TestNode2,
			FSID:             "",
			Path:             TestDefaultDataPath,
			State:            types.DiskStateDisconnected,
		},
	}
	testCases["disk won't be ready if it is picked up by its preferred node"] = tc

	tc = &DiskTestCase{}
	disk1 := newDisk(TestDefaultDiskUUID1, TestNamespace, "")
	disk1.Status.OwnerID = TestNode2
	disk1.Status.NodeID = TestNode1
	tc.disks = map[string]*longhorn.Disk{
		TestDefaultDiskUUID1: disk1,
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: newNode(TestNode1, TestNamespace, TestDefaultDiskUUID1, true, types.ConditionStatusTrue, ""),
	}
	tc.expectDiskStatus = map[string]types.DiskStatus{
		TestDefaultDiskUUID1: types.DiskStatus{
			OwnerID:          TestNode1,
			StorageScheduled: 0,
			StorageAvailable: TestDiskAvailableSize,
			StorageMaximum:   TestDiskSize,
			Conditions: map[string]types.Condition{
				types.DiskConditionTypeReady:       newNodeCondition(types.DiskConditionTypeReady, types.ConditionStatusTrue, ""),
				types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusTrue, ""),
			},
			ScheduledReplica: map[string]int64{},
			NodeID:           TestNode1,
			FSID:             TestDefaultDiskFSID,
			Path:             TestDefaultDataPath,
			State:            types.DiskStateConnected,
		},
	}
	testCases["disk will be ready once it is handled by its preferred node"] = tc

	tc = &DiskTestCase{}
	replica1 = newReplicaForVolume(volume, engine, TestNode1, TestDefaultDiskUUID1)
	replica2 = newReplicaForVolume(volume, engine, TestNode1, TestDefaultDiskUUID1)
	tc.replicas = []*longhorn.Replica{replica1, replica2}
	tc.disks = map[string]*longhorn.Disk{
		TestDefaultDiskUUID1: newDisk(TestDefaultDiskUUID1, TestNamespace, TestNode1),
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: newNode(TestNode1, TestNamespace, TestDefaultDiskUUID1, true, types.ConditionStatusFalse, types.NodeConditionReasonKubernetesNodeGone),
	}
	expectedReplica1 := replica1.DeepCopy()
	expectedReplica1.Spec.FailedAt = getTestNow()
	expectedReplica2 := replica1.DeepCopy()
	expectedReplica2.Spec.FailedAt = getTestNow()
	tc.expectReplicas = map[string]*longhorn.Replica{
		expectedReplica1.Name: expectedReplica1,
		expectedReplica2.Name: expectedReplica2,
	}
	tc.expectDiskStatus = map[string]types.DiskStatus{
		TestDefaultDiskUUID1: types.DiskStatus{
			OwnerID:          TestNode1,
			StorageScheduled: 0,
			StorageAvailable: 0,
			StorageMaximum:   0,
			ScheduledReplica: map[string]int64{},
			NodeID:           TestNode1,
			FSID:             "",
			Path:             TestDefaultDataPath,
			State:            types.DiskStateDisconnected,
			Conditions: map[string]types.Condition{
				types.DiskConditionTypeReady:       newNodeCondition(types.DiskConditionTypeReady, types.ConditionStatusFalse, types.DiskConditionReasonNodeUnknown),
				types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusFalse, types.DiskConditionReasonDiskNotReady),
			},
		},
	}
	testCases["disk becomes disconnected when the node is down"] = tc

	tc = &DiskTestCase{}
	disk2 := newDisk(TestDefaultDiskUUID2, TestNamespace, TestNode1)
	disk2.Status.Path = "/new-path"
	disk2.Status.NodeID = TestNode1
	disk2.Status.FSID = ""
	disk2.Status.Conditions = map[string]types.Condition{}
	disk2.Status.State = ""
	tc.disks = map[string]*longhorn.Disk{
		TestDefaultDiskUUID1: newDisk(TestDefaultDiskUUID1, TestNamespace, TestNode1),
		TestDefaultDiskUUID2: disk2,
	}
	node1 := newNode(TestNode1, TestNamespace, "", true, types.ConditionStatusTrue, "")
	node1.Spec.DiskPathMap = map[string]struct{}{
		TestDefaultDataPath: struct{}{},
		"/new-path":         struct{}{},
	}
	node1.Status.DiskPathIDMap = map[string]string{
		TestDefaultDataPath: TestDefaultDiskUUID1,
		"/new-path":         TestDefaultDiskUUID2,
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: node1,
	}
	tc.expectDiskStatus = map[string]types.DiskStatus{
		TestDefaultDiskUUID1: newDisk(TestDefaultDiskUUID1, TestNamespace, TestNode1).Status,
		TestDefaultDiskUUID2: {
			OwnerID:          TestNode1,
			StorageScheduled: 0,
			StorageAvailable: 0,
			StorageMaximum:   0,
			ScheduledReplica: map[string]int64{},
			Conditions: map[string]types.Condition{
				types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusFalse, string(types.DiskConditionReasonDiskNotReady)),
				types.DiskConditionTypeReady:       newNodeCondition(types.DiskConditionTypeReady, types.ConditionStatusFalse, string(types.DiskConditionReasonDiskFilesystemChanged)),
			},
			NodeID: TestNode1,
			FSID:   "",
			Path:   "/new-path",
			State:  types.DiskStateDisconnected,
		},
	}
	testCases["test disable disk when there is duplicate fsid"] = tc

	tc = &DiskTestCase{}
	replica1 = newReplicaForVolume(volume, engine, TestNode2, TestDefaultDiskUUID1)
	replica2 = newReplicaForVolume(volume, engine, TestNode2, TestDefaultDiskUUID1)
	tc.replicas = []*longhorn.Replica{replica1, replica2}
	disk1 = newDisk(TestDefaultDiskUUID1, TestNamespace, "")
	disk1.Status = types.DiskStatus{
		OwnerID:          TestNode1,
		StorageScheduled: 0,
		StorageAvailable: 0,
		StorageMaximum:   0,
		Conditions: map[string]types.Condition{
			types.DiskConditionTypeReady:       newNodeCondition(types.DiskConditionTypeReady, types.ConditionStatusFalse, ""),
			types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusFalse, ""),
		},
		ScheduledReplica: map[string]int64{},
		NodeID:           TestNode2,
		FSID:             "",
		Path:             TestDefaultDataPath,
		State:            types.DiskStateDisconnected,
	}
	tc.disks = map[string]*longhorn.Disk{
		TestDefaultDiskUUID1: disk1,
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: newNode(TestNode1, TestNamespace, TestDefaultDiskUUID1, true, types.ConditionStatusTrue, ""),
		TestNode2: newNode(TestNode2, TestNamespace, TestDefaultDiskUUID1, true, types.ConditionStatusFalse, types.NodeConditionReasonKubernetesNodeGone),
	}
	tc.nodes[TestNode1].Spec.DiskPathMap = map[string]struct{}{
		TestDefaultDataPath + "/extra": struct{}{},
	}
	tc.nodes[TestNode1].Status.DiskPathIDMap = map[string]string{
		TestDefaultDataPath + "/extra": TestDefaultDiskUUID1,
	}
	expectedReplica1 = replica1.DeepCopy()
	expectedReplica1.Spec.NodeID = TestNode1
	expectedReplica1.Spec.DataPath = filepath.Join(TestDefaultDataPath, "/replicas/extra", expectedReplica1.Name)
	expectedReplica2 = replica2.DeepCopy()
	expectedReplica2.Spec.NodeID = TestNode1
	expectedReplica2.Spec.DataPath = filepath.Join(TestDefaultDataPath, "/replicas/extra", expectedReplica2.Name)
	tc.expectReplicas = map[string]*longhorn.Replica{
		expectedReplica1.Name: expectedReplica1,
		expectedReplica2.Name: expectedReplica2,
	}
	tc.expectDiskStatus = map[string]types.DiskStatus{
		TestDefaultDiskUUID1: types.DiskStatus{
			OwnerID:          TestNode1,
			StorageScheduled: expectedReplica1.Spec.VolumeSize + expectedReplica2.Spec.VolumeSize,
			StorageAvailable: TestDiskAvailableSize,
			StorageMaximum:   TestDiskSize,
			Conditions: map[string]types.Condition{
				types.DiskConditionTypeReady:       newNodeCondition(types.DiskConditionTypeReady, types.ConditionStatusTrue, ""),
				types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusTrue, ""),
			},
			ScheduledReplica: map[string]int64{
				expectedReplica1.Name: expectedReplica1.Spec.VolumeSize,
				expectedReplica2.Name: expectedReplica2.Spec.VolumeSize,
			},
			NodeID: TestNode1,
			FSID:   TestDefaultDiskFSID,
			Path:   TestDefaultDataPath + "/extra",
			State:  types.DiskStateConnected,
		},
	}
	testCases["the running node tries to reuse a disk on a down node"] = tc

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)
		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		dIndexer := lhInformerFactory.Longhorn().V1beta1().Disks().Informer().GetIndexer()
		nIndexer := lhInformerFactory.Longhorn().V1beta1().Nodes().Informer().GetIndexer()
		rIndexer := lhInformerFactory.Longhorn().V1beta1().Replicas().Informer().GetIndexer()
		sIndexer := lhInformerFactory.Longhorn().V1beta1().Settings().Informer().GetIndexer()

		imImageSetting := newDefaultInstanceManagerImageSetting()
		imImageSetting, err := lhClient.LonghornV1beta1().Settings(TestNamespace).Create(imImageSetting)
		c.Assert(err, IsNil)
		err = sIndexer.Add(imImageSetting)
		c.Assert(err, IsNil)

		dc := newTestDiskController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient, TestNode1)
		// create disk
		for _, disk := range tc.disks {
			d, err := lhClient.LonghornV1beta1().Disks(TestNamespace).Create(disk)
			c.Assert(err, IsNil)
			c.Assert(d, NotNil)
			dIndexer.Add(d)
		}
		// create node
		for _, node := range tc.nodes {
			n, err := lhClient.LonghornV1beta1().Nodes(TestNamespace).Create(node)
			c.Assert(err, IsNil)
			c.Assert(n, NotNil)
			nIndexer.Add(n)
		}
		// create replicas
		for _, replica := range tc.replicas {
			r, err := lhClient.LonghornV1beta1().Replicas(TestNamespace).Create(replica)
			c.Assert(err, IsNil)
			c.Assert(r, NotNil)
			rIndexer.Add(r)
		}

		// sync disk status
		for diskName, disk := range tc.disks {
			err := dc.syncDisk(getKey(disk, c))
			c.Assert(err, IsNil)

			d, err := lhClient.LonghornV1beta1().Disks(TestNamespace).Get(disk.Name, metav1.GetOptions{})
			c.Assert(err, IsNil)

			for ctype, condition := range d.Status.Conditions {
				condition.LastTransitionTime = ""
				condition.Message = ""
				d.Status.Conditions[ctype] = condition
			}
			c.Assert(d.Status, DeepEquals, tc.expectDiskStatus[diskName])
		}
		if tc.expectReplicas != nil {
			for replicaName, expectReplica := range tc.expectReplicas {
				r, err := lhClient.LonghornV1beta1().Replicas(TestNamespace).Get(replicaName, metav1.GetOptions{})
				c.Assert(err, IsNil)
				c.Assert(r.Spec.NodeID, Equals, expectReplica.Spec.NodeID)
			}
		}
	}
}
