package controller

import (
	"fmt"
	"path/filepath"
	"time"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

func getVolumeLabelSelector(volumeName string) string {
	return "longhornvolume=" + volumeName
}

func initSettings(ds *datastore.DataStore) {
	settingDefaultEngineImage := &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(types.SettingNameDefaultEngineImage),
		},
		Setting: types.Setting{
			Value: TestEngineImage,
		},
	}
	ds.CreateSetting(settingDefaultEngineImage)
}

func newTestVolumeController(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset,
	controllerID string) *VolumeController {

	volumeInformer := lhInformerFactory.Longhorn().V1alpha1().Volumes()
	engineInformer := lhInformerFactory.Longhorn().V1alpha1().Engines()
	replicaInformer := lhInformerFactory.Longhorn().V1alpha1().Replicas()
	engineImageInformer := lhInformerFactory.Longhorn().V1alpha1().EngineImages()
	nodeInformer := lhInformerFactory.Longhorn().V1alpha1().Nodes()
	settingInformer := lhInformerFactory.Longhorn().V1alpha1().Settings()

	podInformer := kubeInformerFactory.Core().V1().Pods()
	cronJobInformer := kubeInformerFactory.Batch().V1beta1().CronJobs()
	daemonSetInformer := kubeInformerFactory.Apps().V1beta2().DaemonSets()
	persistentVolumeInformer := kubeInformerFactory.Core().V1().PersistentVolumes()
	persistentVolumeClaimInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()

	ds := datastore.NewDataStore(
		volumeInformer, engineInformer, replicaInformer,
		engineImageInformer, nodeInformer, settingInformer,
		lhClient,
		podInformer, cronJobInformer, daemonSetInformer,
		persistentVolumeInformer, persistentVolumeClaimInformer,
		kubeClient, TestNamespace)
	initSettings(ds)

	vc := NewVolumeController(ds, scheme.Scheme, volumeInformer, engineInformer, replicaInformer, kubeClient, TestNamespace, controllerID, TestServiceAccount, TestManagerImage)

	fakeRecorder := record.NewFakeRecorder(100)
	vc.eventRecorder = fakeRecorder

	vc.vStoreSynced = alwaysReady
	vc.rStoreSynced = alwaysReady
	vc.eStoreSynced = alwaysReady
	vc.nowHandler = getTestNow

	return vc
}

type VolumeTestCase struct {
	volume   *longhorn.Volume
	engines  map[string]*longhorn.Engine
	replicas map[string]*longhorn.Replica
	nodes    []*longhorn.Node

	expectVolume   *longhorn.Volume
	expectEngines  map[string]*longhorn.Engine
	expectReplicas map[string]*longhorn.Replica
}

func (s *TestSuite) TestVolumeLifeCycle(c *C) {
	var tc *VolumeTestCase
	testCases := map[string]*VolumeTestCase{}

	// We have to skip lister check for unit tests
	// Because the changes written through the API won't be reflected in the listers
	datastore.SkipListerCheck = true

	// normal volume creation
	tc = generateVolumeTestCaseTemplate()
	// default replica and engine objects will be copied by copyCurrentToExpect
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateCreating
	tc.expectVolume.Status.Robustness = types.VolumeRobustnessUnknown
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.EngineImage
	tc.engines = nil
	tc.replicas = nil
	testCases["volume create"] = tc

	// unable to create volume because no node to schedule
	tc = generateVolumeTestCaseTemplate()
	for i := range tc.nodes {
		tc.nodes[i].Spec.AllowScheduling = false
	}
	tc.copyCurrentToExpect()
	// replicas and engine object would still be created
	tc.replicas = nil
	tc.engines = nil
	for _, r := range tc.expectReplicas {
		r.Spec.NodeID = ""
		r.Spec.DiskID = ""
		r.Spec.DataPath = ""
	}
	tc.expectVolume.Status.State = types.VolumeStateCreating
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.EngineImage
	tc.expectVolume.Status.Robustness = types.VolumeRobustnessUnknown
	tc.expectVolume.Status.Conditions = map[types.VolumeConditionType]types.Condition{
		types.VolumeConditionTypeScheduled: {
			Type:   string(types.VolumeConditionTypeScheduled),
			Status: types.ConditionStatusFalse,
			Reason: types.VolumeConditionReasonReplicaSchedulingFailure,
		},
	}
	testCases["volume create - replica scheduling failure"] = tc

	// after creation, volume in detached state
	tc = generateVolumeTestCaseTemplate()
	for _, e := range tc.engines {
		e.Status.CurrentState = types.InstanceStateStopped
	}
	for _, r := range tc.replicas {
		r.Status.CurrentState = types.InstanceStateStopped
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateDetached
	tc.expectVolume.Status.Robustness = types.VolumeRobustnessUnknown
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.EngineImage
	testCases["volume detached"] = tc

	// volume attaching, start replicas
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateAttaching
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.EngineImage
	// replicas will be started first
	// engine will be started only after all the replicas are running
	for _, r := range tc.expectReplicas {
		r.Spec.DesireState = types.InstanceStateRunning
	}
	testCases["volume attaching - start replicas"] = tc

	// volume attaching, start engine
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	for _, r := range tc.replicas {
		r.Spec.DesireState = types.InstanceStateRunning
		r.Status.CurrentState = types.InstanceStateRunning
		r.Status.IP = randomIP()
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateAttaching
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.EngineImage
	for _, e := range tc.expectEngines {
		e.Spec.NodeID = tc.volume.Spec.NodeID
		e.Spec.DesireState = types.InstanceStateRunning
	}
	for name, r := range tc.expectReplicas {
		//TODO update to r.Spec.AssociatedEngine
		for _, e := range tc.expectEngines {
			e.Spec.ReplicaAddressMap[name] = r.Status.IP
		}
	}
	testCases["volume attaching - start controller"] = tc

	// volume attached
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1

	for _, e := range tc.engines {
		e.Spec.NodeID = tc.volume.Spec.NodeID
		e.Spec.DesireState = types.InstanceStateRunning
		e.Status.CurrentState = types.InstanceStateRunning
		e.Status.IP = randomIP()
		e.Status.Endpoint = "/dev/" + tc.volume.Name
		e.Status.ReplicaModeMap = map[string]types.ReplicaMode{}
	}
	for name, r := range tc.replicas {
		r.Spec.DesireState = types.InstanceStateRunning
		r.Status.CurrentState = types.InstanceStateRunning
		r.Status.IP = randomIP()
		//TODO update to r.Spec.AssociatedEngine
		for _, e := range tc.engines {
			e.Spec.ReplicaAddressMap[name] = r.Status.IP
			e.Status.ReplicaModeMap[name] = types.ReplicaModeRW
		}
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateAttached
	tc.expectVolume.Status.Robustness = types.VolumeRobustnessHealthy
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.EngineImage
	for _, r := range tc.expectReplicas {
		r.Spec.HealthyAt = getTestNow()
	}
	testCases["volume attached"] = tc

	// after creation, restored volume should automatically be in attaching state
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.FromBackup = "random"
	tc.volume.Spec.Standby = false
	tc.volume.Spec.InitialRestorationRequired = true
	for _, r := range tc.replicas {
		r.Spec.HealthyAt = ""
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateAttaching
	tc.expectVolume.Spec.InitialRestorationRequired = true
	tc.expectVolume.Spec.NodeID = TestNode1
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.EngineImage
	for _, r := range tc.expectReplicas {
		r.Spec.DesireState = types.InstanceStateRunning
	}
	testCases["restored volume is automatically attaching after creation"] = tc

	// Newly restored volume changed from attaching to attached
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.volume.Spec.FromBackup = "random"
	tc.volume.Spec.Standby = false
	tc.volume.Status.State = types.VolumeStateAttaching
	tc.volume.Spec.InitialRestorationRequired = true
	for _, e := range tc.engines {
		e.Spec.NodeID = tc.volume.Spec.NodeID
		e.Spec.DesireState = types.InstanceStateRunning
		e.Status.CurrentState = types.InstanceStateRunning
		e.Status.IP = randomIP()
		e.Status.Endpoint = "/dev/" + tc.volume.Name
		e.Status.ReplicaModeMap = map[string]types.ReplicaMode{}
	}
	for name, r := range tc.replicas {
		r.Spec.DesireState = types.InstanceStateRunning
		r.Status.CurrentState = types.InstanceStateRunning
		r.Status.IP = randomIP()
		for _, e := range tc.engines {
			e.Spec.ReplicaAddressMap[name] = r.Status.IP
			e.Status.ReplicaModeMap[name] = types.ReplicaModeRW
		}
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateAttached
	tc.expectVolume.Status.Robustness = types.VolumeRobustnessHealthy
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.EngineImage
	for _, r := range tc.expectReplicas {
		r.Spec.HealthyAt = getTestNow()
	}
	tc.expectVolume.Spec.NodeID = ""
	tc.expectVolume.Spec.InitialRestorationRequired = false
	testCases["newly restored volume attaching to attached"] = tc

	// Newly restored volume is automatically detaching after restoration completed
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.FromBackup = "random"
	tc.volume.Spec.Standby = false
	tc.volume.Spec.InitialRestorationRequired = true
	tc.volume.Status.State = types.VolumeStateAttached
	for _, r := range tc.replicas {
		r.Spec.HealthyAt = getTestNow()
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateDetaching
	tc.expectVolume.Status.Robustness = types.VolumeRobustnessUnknown
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.EngineImage
	for _, r := range tc.expectReplicas {
		r.Spec.HealthyAt = getTestNow()
	}
	testCases["newly restored volume automatically detaching"] = tc

	// after creation, standby volume should automatically be in attaching state
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.FromBackup = "random"
	tc.volume.Spec.Standby = true
	tc.volume.Spec.InitialRestorationRequired = true
	for _, r := range tc.replicas {
		r.Spec.HealthyAt = ""
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateAttaching
	tc.expectVolume.Spec.InitialRestorationRequired = true
	tc.expectVolume.Spec.NodeID = TestNode1
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.EngineImage
	for _, r := range tc.expectReplicas {
		r.Spec.DesireState = types.InstanceStateRunning
	}
	testCases["new standby volume automatically attaching"] = tc

	// New standby volume changed from attaching to attached, and it's not automatically detached
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.volume.Spec.FromBackup = "random"
	tc.volume.Spec.Standby = true
	tc.volume.Status.State = types.VolumeStateAttaching
	tc.volume.Spec.InitialRestorationRequired = true
	for _, e := range tc.engines {
		e.Spec.NodeID = tc.volume.Spec.NodeID
		e.Spec.DesireState = types.InstanceStateRunning
		e.Status.CurrentState = types.InstanceStateRunning
		e.Status.IP = randomIP()
		e.Status.Endpoint = "/dev/" + tc.volume.Name
		e.Status.ReplicaModeMap = map[string]types.ReplicaMode{}
	}
	for name, r := range tc.replicas {
		r.Spec.DesireState = types.InstanceStateRunning
		r.Status.CurrentState = types.InstanceStateRunning
		r.Status.IP = randomIP()
		for _, e := range tc.engines {
			e.Spec.ReplicaAddressMap[name] = r.Status.IP
			e.Status.ReplicaModeMap[name] = types.ReplicaModeRW
		}
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateAttached
	tc.expectVolume.Status.Robustness = types.VolumeRobustnessHealthy
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.EngineImage
	for _, r := range tc.expectReplicas {
		r.Spec.HealthyAt = getTestNow()
	}
	tc.expectVolume.Spec.NodeID = tc.volume.Spec.NodeID
	tc.expectVolume.Spec.InitialRestorationRequired = false
	testCases["standby volume is not automatically detached"] = tc

	// volume detaching - stop engine
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = ""
	for _, e := range tc.engines {
		e.Spec.NodeID = TestNode1
		e.Spec.DesireState = types.InstanceStateRunning
		e.Status.CurrentState = types.InstanceStateRunning
		e.Status.IP = randomIP()
		e.Status.Endpoint = "/dev/" + tc.volume.Name
		e.Status.ReplicaModeMap = map[string]types.ReplicaMode{}
	}
	for name, r := range tc.replicas {
		r.Spec.DesireState = types.InstanceStateRunning
		r.Spec.HealthyAt = getTestNow()
		r.Status.CurrentState = types.InstanceStateRunning
		r.Status.IP = randomIP()
		//TODO update to r.Spec.AssociatedEngine
		for _, e := range tc.engines {
			e.Spec.ReplicaAddressMap[name] = r.Status.IP
			e.Status.ReplicaModeMap[name] = types.ReplicaModeRW
		}
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateDetaching
	tc.expectVolume.Status.Robustness = types.VolumeRobustnessUnknown
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.EngineImage
	for _, e := range tc.expectEngines {
		e.Spec.NodeID = ""
		e.Spec.DesireState = types.InstanceStateStopped
	}
	testCases["volume detaching - stop engine"] = tc

	// volume detaching - stop replicas
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = ""
	for _, e := range tc.engines {
		e.Spec.NodeID = ""
		e.Status.CurrentState = types.InstanceStateStopped
	}
	for name, r := range tc.replicas {
		r.Spec.DesireState = types.InstanceStateRunning
		r.Spec.HealthyAt = getTestNow()
		r.Status.CurrentState = types.InstanceStateRunning
		r.Status.IP = randomIP()
		//TODO update to r.Spec.AssociatedEngine
		for _, e := range tc.engines {
			e.Spec.ReplicaAddressMap[name] = r.Status.IP
		}
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateDetaching
	tc.expectVolume.Status.Robustness = types.VolumeRobustnessUnknown
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.EngineImage
	for _, r := range tc.expectReplicas {
		r.Spec.DesireState = types.InstanceStateStopped
	}
	testCases["volume detaching - stop replicas"] = tc

	// volume deleting
	tc = generateVolumeTestCaseTemplate()
	now := metav1.NewTime(time.Now())
	tc.volume.SetDeletionTimestamp(&now)
	tc.volume.Status.Conditions = map[types.VolumeConditionType]types.Condition{}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateDeleting
	tc.expectEngines = nil
	tc.expectReplicas = nil
	testCases["volume deleting"] = tc

	// volume attaching, start replicas, one node down
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.nodes[1] = newNode(TestNode2, TestNamespace, false, types.ConditionStatusFalse, string(types.NodeConditionReasonKubernetesNodeGone))
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateAttaching
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.EngineImage
	expectRs := map[string]*longhorn.Replica{}
	for _, r := range tc.expectReplicas {
		if r.Spec.NodeID != TestNode2 {
			r.Spec.DesireState = types.InstanceStateRunning
			expectRs[r.Name] = r
		}
	}
	tc.expectReplicas = expectRs
	testCases["volume attaching - start replicas - node failed"] = tc
	s.runTestCases(c, testCases)

	// volume attaching, start replicas, manager restart
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.nodes[1] = newNode(TestNode2, TestNamespace, false, types.ConditionStatusFalse, string(types.NodeConditionReasonManagerPodDown))
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateAttaching
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.EngineImage
	expectRs = map[string]*longhorn.Replica{}
	for _, r := range tc.expectReplicas {
		r.Spec.DesireState = types.InstanceStateRunning
		expectRs[r.Name] = r
	}
	tc.expectReplicas = expectRs
	testCases["volume attaching - start replicas - manager down"] = tc
	s.runTestCases(c, testCases)
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
			Frontend:            types.VolumeFrontendBlockDev,
			NumberOfReplicas:    replicaCount,
			Size:                TestVolumeSize,
			OwnerID:             TestOwnerID1,
			StaleReplicaTimeout: TestVolumeStaleTimeout,
			EngineImage:         TestEngineImage,
		},
		Status: types.VolumeStatus{
			Conditions: map[types.VolumeConditionType]types.Condition{
				types.VolumeConditionTypeScheduled: {
					Type:   string(types.VolumeConditionTypeScheduled),
					Status: types.ConditionStatusTrue,
				},
			},
		},
	}
}

func newEngineForVolume(v *longhorn.Volume) *longhorn.Engine {
	return &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{
			Name: v.Name + "-e-" + util.RandomID(),
			Labels: map[string]string{
				"longhornvolume": v.Name,
			},
		},
		Spec: types.EngineSpec{
			InstanceSpec: types.InstanceSpec{
				OwnerID:     v.Spec.OwnerID,
				VolumeName:  v.Name,
				VolumeSize:  v.Spec.Size,
				EngineImage: TestEngineImage,
				DesireState: types.InstanceStateStopped,
			},
			Frontend:                  types.VolumeFrontendBlockDev,
			ReplicaAddressMap:         map[string]string{},
			UpgradedReplicaAddressMap: map[string]string{},
		},
	}
}

func newReplicaForVolume(v *longhorn.Volume, e *longhorn.Engine, nodeID, diskID string) *longhorn.Replica {
	replicaName := v.Name + "-r-" + util.RandomID()
	return &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name: replicaName,
			Labels: map[string]string{
				"longhornvolume":      v.Name,
				types.LonghornNodeKey: nodeID,
			},
		},
		Spec: types.ReplicaSpec{
			InstanceSpec: types.InstanceSpec{
				OwnerID:     v.Spec.OwnerID,
				NodeID:      nodeID,
				VolumeName:  v.Name,
				VolumeSize:  v.Spec.Size,
				EngineImage: TestEngineImage,
				DesireState: types.InstanceStateStopped,
			},
			EngineName: e.Name,
			DiskID:     diskID,
			DataPath:   filepath.Join(TestDefaultDataPath, "/replicas", replicaName),
			Active:     true,
		},
	}
}

func newDaemonPod(phase v1.PodPhase, name, namespace, nodeID, podIP string, mountpropagation *v1.MountPropagationMode) *v1.Pod {
	podStatus := v1.ConditionFalse
	if phase == v1.PodRunning {
		podStatus = v1.ConditionTrue
	}
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
			Containers: []v1.Container{
				{
					Name:  "test-container",
					Image: TestEngineImage,
					VolumeMounts: []v1.VolumeMount{
						{
							Name:             "longhorn",
							MountPath:        TestDefaultDataPath,
							MountPropagation: mountpropagation,
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: podStatus,
				},
			},
			Phase: phase,
			PodIP: podIP,
		},
	}
}

func newNode(name, namespace string, allowScheduling bool, status types.ConditionStatus, reason string) *longhorn.Node {
	return &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: types.NodeSpec{
			AllowScheduling: allowScheduling,
			Disks: map[string]types.DiskSpec{
				TestDiskID1: {
					Path:            TestDefaultDataPath,
					AllowScheduling: true,
					StorageReserved: 0,
				},
			},
		},
		Status: types.NodeStatus{
			Conditions: map[types.NodeConditionType]types.Condition{
				types.NodeConditionTypeReady: newNodeCondition(types.NodeConditionTypeReady, status, reason),
			},
		},
	}
}

func newNodeCondition(conditionType string, status types.ConditionStatus, reason string) types.Condition {
	return types.Condition{
		Type:    conditionType,
		Status:  status,
		Reason:  reason,
		Message: "",
	}
}

func newKubernetesNode(name string, readyStatus, diskPressureStatus, memoryStatus, outOfDiskStatus, pidStatus, networkStatus, kubeletStatus v1.ConditionStatus) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{
				{
					Type:   v1.NodeReady,
					Status: readyStatus,
				},
				{
					Type:   v1.NodeDiskPressure,
					Status: diskPressureStatus,
				},
				{
					Type:   v1.NodeMemoryPressure,
					Status: memoryStatus,
				},
				{
					Type:   v1.NodeOutOfDisk,
					Status: outOfDiskStatus,
				},
				{
					Type:   v1.NodePIDPressure,
					Status: pidStatus,
				},
				{
					Type:   v1.NodeNetworkUnavailable,
					Status: networkStatus,
				},
			},
		},
	}
}

func generateVolumeTestCaseTemplate() *VolumeTestCase {
	volume := newVolume(TestVolumeName, 2)
	engine := newEngineForVolume(volume)
	replica1 := newReplicaForVolume(volume, engine, TestNode1, "fsid")
	replica2 := newReplicaForVolume(volume, engine, TestNode2, "fsid")
	node1 := newNode(TestNode1, TestNamespace, true, types.ConditionStatusTrue, "")
	node2 := newNode(TestNode2, TestNamespace, false, types.ConditionStatusTrue, "")

	return &VolumeTestCase{
		volume: volume,
		engines: map[string]*longhorn.Engine{
			engine.Name: engine,
		},
		replicas: map[string]*longhorn.Replica{
			replica1.Name: replica1,
			replica2.Name: replica2,
		},
		nodes: []*longhorn.Node{
			node1,
			node2,
		},

		expectVolume:   nil,
		expectEngines:  map[string]*longhorn.Engine{},
		expectReplicas: map[string]*longhorn.Replica{},
	}
}

func (tc *VolumeTestCase) copyCurrentToExpect() {
	tc.expectVolume = tc.volume.DeepCopy()
	for n, e := range tc.engines {
		tc.expectEngines[n] = e.DeepCopy()
	}
	for n, r := range tc.replicas {
		tc.expectReplicas[n] = r.DeepCopy()
	}
}

func (s *TestSuite) runTestCases(c *C, testCases map[string]*VolumeTestCase) {
	for name, tc := range testCases {
		var err error
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())
		vIndexer := lhInformerFactory.Longhorn().V1alpha1().Volumes().Informer().GetIndexer()
		eIndexer := lhInformerFactory.Longhorn().V1alpha1().Engines().Informer().GetIndexer()
		rIndexer := lhInformerFactory.Longhorn().V1alpha1().Replicas().Informer().GetIndexer()
		nIndexer := lhInformerFactory.Longhorn().V1alpha1().Nodes().Informer().GetIndexer()

		pIndexer := kubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
		knIndexer := kubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

		vc := newTestVolumeController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient, TestOwnerID1)

		// Need to create daemon pod for node
		daemon1 := newDaemonPod(v1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, nil)
		p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(daemon1)
		c.Assert(err, IsNil)
		pIndexer.Add(p)
		daemon2 := newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, nil)
		p, err = kubeClient.CoreV1().Pods(TestNamespace).Create(daemon2)
		c.Assert(err, IsNil)
		pIndexer.Add(p)

		// need to create default node
		for _, node := range tc.nodes {
			node.Status.DiskStatus = map[string]types.DiskStatus{
				TestDiskID1: {
					StorageAvailable: TestDiskAvailableSize,
					StorageScheduled: 0,
					StorageMaximum:   TestDiskSize,
					Conditions: map[types.DiskConditionType]types.Condition{
						types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusTrue, ""),
					},
				},
			}
			n, err := lhClient.Longhorn().Nodes(TestNamespace).Create(node)
			c.Assert(err, IsNil)
			c.Assert(n, NotNil)
			nIndexer.Add(n)

			knodeCondition := v1.ConditionTrue
			if node.Status.Conditions[types.NodeConditionTypeReady].Status != types.ConditionStatusTrue {
				knodeCondition = v1.ConditionFalse
			}
			knode := newKubernetesNode(node.Name, knodeCondition, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionTrue)
			kn, err := kubeClient.CoreV1().Nodes().Create(knode)
			c.Assert(err, IsNil)
			knIndexer.Add(kn)
		}

		// Need to put it into both fakeclientset and Indexer
		v, err := lhClient.LonghornV1alpha1().Volumes(TestNamespace).Create(tc.volume)
		c.Assert(err, IsNil)
		err = vIndexer.Add(v)
		c.Assert(err, IsNil)

		if tc.engines != nil {
			for _, e := range tc.engines {
				e, err := lhClient.LonghornV1alpha1().Engines(TestNamespace).Create(e)
				c.Assert(err, IsNil)
				err = eIndexer.Add(e)
				c.Assert(err, IsNil)
			}
		}

		if tc.replicas != nil {
			for _, r := range tc.replicas {
				r, err = lhClient.LonghornV1alpha1().Replicas(TestNamespace).Create(r)
				c.Assert(err, IsNil)
				err = rIndexer.Add(r)
				c.Assert(err, IsNil)
			}
		}

		err = vc.syncVolume(getKey(v, c))
		c.Assert(err, IsNil)

		retV, err := lhClient.LonghornV1alpha1().Volumes(TestNamespace).Get(v.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)
		c.Assert(retV.Spec, DeepEquals, tc.expectVolume.Spec)
		// mask timestamps
		for ctype, condition := range retV.Status.Conditions {
			/*
				if ctype == types.VolumeConditionTypeScheduled {
					c.Assert(condition.LastProbeTime, Not(Equals), "")
				}
			*/
			condition.LastProbeTime = ""
			condition.LastTransitionTime = ""
			retV.Status.Conditions[ctype] = condition
		}
		c.Assert(retV.Status, DeepEquals, tc.expectVolume.Status)

		retEs, err := lhClient.LonghornV1alpha1().Engines(TestNamespace).List(metav1.ListOptions{LabelSelector: getVolumeLabelSelector(v.Name)})
		c.Assert(err, IsNil)
		c.Assert(retEs.Items, HasLen, len(tc.expectEngines))
		for _, retE := range retEs.Items {
			if tc.engines == nil {
				// test creation, name would be different
				var expectE *longhorn.Engine
				for _, expectE = range tc.expectEngines {
					break
				}
				c.Assert(retE.Status, DeepEquals, expectE.Status)
			} else {
				c.Assert(retE.Spec, DeepEquals, tc.expectEngines[retE.Name].Spec)
				c.Assert(retE.Status, DeepEquals, tc.expectEngines[retE.Name].Status)
			}
		}

		retRs, err := lhClient.LonghornV1alpha1().Replicas(TestNamespace).List(metav1.ListOptions{LabelSelector: getVolumeLabelSelector(v.Name)})
		c.Assert(err, IsNil)
		c.Assert(retRs.Items, HasLen, len(tc.expectReplicas))
		for _, retR := range retRs.Items {
			if tc.replicas == nil {
				// test creation, name would be different
				var expectR *longhorn.Replica
				for _, expectR = range tc.expectReplicas {
					break
				}
				if expectR.Spec.NodeID != "" {
					// validate DataPath and NodeID of replica have been set in scheduler
					c.Assert(retR.Spec.DataPath, Not(Equals), "")
					c.Assert(retR.Spec.NodeID, Not(Equals), "")
					c.Assert(retR.Spec.DiskID, Not(Equals), "")
					c.Assert(retR.Spec.NodeID, Equals, TestNode1)
				} else {
					// not schedulable
					c.Assert(retR.Spec.DataPath, Equals, "")
					c.Assert(retR.Spec.NodeID, Equals, "")
					c.Assert(retR.Spec.DiskID, Equals, "")
				}
				c.Assert(retR.Status, DeepEquals, expectR.Status)
			} else {
				c.Assert(retR.Spec, DeepEquals, tc.expectReplicas[retR.Name].Spec)
				c.Assert(retR.Status, DeepEquals, tc.expectReplicas[retR.Name].Status)
			}
		}
	}
}

func getTestNow() string {
	return TestTimeNow
}
