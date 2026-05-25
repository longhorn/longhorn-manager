package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"k8s.io/apimachinery/pkg/runtime"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

// ---------------------------------------------------------------------------
// Constants used across IMU tests
// ---------------------------------------------------------------------------

const (
	TestIMUName       = "test-imu"
	TestIMUName2      = "test-imu-2"
	TestSourceIMName  = "instance-manager-source"
	TestTargetIMName  = "instance-manager-target"
	TestTempIMName    = "instance-manager-temp"
	TestTempNode2     = "test-node-name-3"
	TestSourceImage   = TestInstanceManagerImage      // stale image
	TestTargetImage   = TestExtraInstanceManagerImage // new image
	TestSourceNode    = TestNode1
	TestTempNode      = TestNode2
	TestEngineNameIMU = "test-volume-engine-imu"
	TestVolumeName2   = "test-volume-imu"
	TestReplicaName2  = "test-replica-imu"
	TestReplicaName3  = "test-replica-imu-2"
)

// ---------------------------------------------------------------------------
// Controller constructor for tests
// ---------------------------------------------------------------------------

func newTestIMUController(
	lhClient *lhfake.Clientset,
	kubeClient *fake.Clientset,
	extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories,
	controllerID string,
) (*InstanceManagerUpgradeController, error) {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
	logger := logrus.StandardLogger()

	imuc, err := NewInstanceManagerUpgradeController(logger, ds, scheme.Scheme, kubeClient, TestNamespace, controllerID)
	if err != nil {
		return nil, err
	}

	fakeRecorder := record.NewFakeRecorder(100)
	imuc.eventRecorder = fakeRecorder
	for i := range imuc.cacheSyncs {
		imuc.cacheSyncs[i] = alwaysReady
	}
	return imuc, nil
}

// ---------------------------------------------------------------------------
// Object builders
// ---------------------------------------------------------------------------

func newIMU(name, nodeID, targetImage string, state longhorn.InstanceManagerUpgradeState) *longhorn.InstanceManagerUpgrade {
	imu := &longhorn.InstanceManagerUpgrade{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  TestNamespace,
			Finalizers: []string{longhornFinalizerKey},
		},
		Spec: longhorn.InstanceManagerUpgradeSpec{
			NodeID:      nodeID,
			TargetImage: targetImage,
		},
		Status: longhorn.InstanceManagerUpgradeStatus{
			OwnerID: nodeID,
			State:   state,
		},
	}
	return imu
}

func newTestVolumeForIMU(name, nodeID, currentEngineNodeID string) *longhorn.Volume {
	return &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
		},
		Spec: longhorn.VolumeSpec{
			NodeID:       nodeID,
			EngineNodeID: currentEngineNodeID,
			DataEngine:   longhorn.DataEngineTypeV2,
		},
		Status: longhorn.VolumeStatus{
			CurrentNodeID:       nodeID,
			CurrentEngineNodeID: currentEngineNodeID,
		},
	}
}

func newTestEngineForIMU(name, nodeID, volumeName string, state longhorn.InstanceState) *longhorn.Engine {
	return &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
			Labels: map[string]string{
				types.LonghornNodeKey:     nodeID,
				types.LonghornLabelVolume: volumeName,
			},
		},
		Spec: longhorn.EngineSpec{
			InstanceSpec: longhorn.InstanceSpec{
				NodeID:     nodeID,
				VolumeName: volumeName,
				DataEngine: longhorn.DataEngineTypeV2,
			},
		},
		Status: longhorn.EngineStatus{
			InstanceStatus: longhorn.InstanceStatus{
				CurrentState: state,
			},
		},
	}
}

func newTestReplicaForIMU(name, nodeID, volumeName string, healthy bool) *longhorn.Replica {
	failedAt := ""
	healthyAt := "2024-01-01T00:00:00Z"
	currentState := longhorn.InstanceStateRunning
	if !healthy {
		failedAt = "2024-01-01T00:00:00Z"
		healthyAt = ""
		currentState = longhorn.InstanceStateStopped
	}
	return &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
			Labels: map[string]string{
				types.LonghornNodeKey:     nodeID,
				types.LonghornLabelVolume: volumeName,
			},
		},
		Spec: longhorn.ReplicaSpec{
			InstanceSpec: longhorn.InstanceSpec{
				NodeID:     nodeID,
				VolumeName: volumeName,
			},
			FailedAt:  failedAt,
			HealthyAt: healthyAt,
		},
		Status: longhorn.ReplicaStatus{
			InstanceStatus: longhorn.InstanceStatus{
				CurrentState: currentState,
			},
		},
	}
}

func newTestIMForNode(name, nodeID, image string, imType longhorn.InstanceManagerType, dataEngine longhorn.DataEngineType, state longhorn.InstanceManagerState) *longhorn.InstanceManager {
	return &longhorn.InstanceManager{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
			Labels:    types.GetInstanceManagerLabels(nodeID, image, imType, dataEngine),
		},
		Spec: longhorn.InstanceManagerSpec{
			NodeID:     nodeID,
			Image:      image,
			Type:       imType,
			DataEngine: dataEngine,
		},
		Status: longhorn.InstanceManagerStatus{
			OwnerID:      nodeID,
			CurrentState: state,
		},
	}
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

type InstanceManagerUpgradeControllerTestCase struct {
	// Initial state
	imu             *longhorn.InstanceManagerUpgrade
	sourceIM        *longhorn.InstanceManager // IM on source node (nil = not found)
	tempIM          *longhorn.InstanceManager // IM on temp node (nil = not found)
	settings        []*longhorn.Setting
	snapshots       []*longhorn.Snapshot
	volumes         []*longhorn.Volume // Volumes for IMU tests
	engines         []*longhorn.Engine
	replicas        []*longhorn.Replica
	otherIMUs       []*longhorn.InstanceManagerUpgrade // other IMUs in the cluster
	updateVolumeErr error

	// Expected outcome
	expectedState               longhorn.InstanceManagerUpgradeState
	expectedVolumeEngineNodeIDs map[string]string // volumeName -> expected Spec.EngineNodeID after reconcile
	expectedSnapshotNames       map[string]string // volumeName -> expected snapshot name stored in relocation status
	expectedSnapshotCount       int
	expectedStartedAt           bool
	expectedAbort               bool
	expectedErrorMsg            string
	expectedSyncError           string
}

func (s *TestSuite) TestSyncInstanceManagerUpgrade(c *C) {
	var err error

	testCases := map[string]InstanceManagerUpgradeControllerTestCase{

		// -----------------------------------------------------------------
		// Pending state cases
		// -----------------------------------------------------------------

		"pending: missing spec → Failed": {
			imu: &longhorn.InstanceManagerUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Name:       TestIMUName,
					Namespace:  TestNamespace,
					Finalizers: []string{longhornFinalizerKey},
				},
				Spec: longhorn.InstanceManagerUpgradeSpec{
					// NodeID, TargetImage all empty
				},
				Status: longhorn.InstanceManagerUpgradeStatus{
					OwnerID: TestSourceNode,
					State:   longhorn.InstanceManagerUpgradeStatePending,
				},
			},
			expectedState: longhorn.InstanceManagerUpgradeStateFailed,
		},

		"pending: source IM not running → stay Pending": {
			imu:               newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStatePending),
			sourceIM:          newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateError),
			expectedState:     longhorn.InstanceManagerUpgradeStatePending,
			expectedStartedAt: true,
		},

		"pending: abort requested → Failed": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStatePending)
				imu.Status.AbortRequested = true
				imu.Status.AbortReason = "timeout"
				return imu
			}(),
			expectedState:    longhorn.InstanceManagerUpgradeStateFailed,
			expectedErrorMsg: "upgrade aborted: timeout",
		},

		"pending: source IM already running target image → Completed": {
			imu:           newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStatePending),
			sourceIM:      newTestIMForNode(TestSourceIMName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			expectedState: longhorn.InstanceManagerUpgradeStateCompleted,
		},

		"pending: source IM not found, new IM with target image running → Completed": {
			imu: newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStatePending),
			// sourceIM is nil (not found), but a new IM with target image is running
			tempIM:        newTestIMForNode(TestTargetIMName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			expectedState: longhorn.InstanceManagerUpgradeStateCompleted,
		},

		"pending: source IM not found, no target IM yet → WaitingForSourceIM": {
			imu:           newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStatePending),
			expectedState: longhorn.InstanceManagerUpgradeStateWaitingForSourceIM,
		},

		"pending: no engines on source node → WaitingForSourceIM": {
			imu:      newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStatePending),
			sourceIM: newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			// no engines added
			expectedState: longhorn.InstanceManagerUpgradeStateWaitingForSourceIM,
		},

		"pending: engine not running → stay Pending": {
			imu:      newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStatePending),
			sourceIM: newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			volumes: []*longhorn.Volume{
				newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode),
			},
			engines: []*longhorn.Engine{
				newTestEngineForIMU(TestEngineNameIMU, TestSourceNode, TestVolumeName2, longhorn.InstanceStateError),
			},
			replicas: []*longhorn.Replica{
				newTestReplicaForIMU(TestReplicaName2, TestTempNode, TestVolumeName2, true),
			},
			tempIM:        newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			expectedState: longhorn.InstanceManagerUpgradeStatePending,
		},

		"pending: no replica on other node (single replica) → Failed": {
			imu:      newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStatePending),
			sourceIM: newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			volumes: []*longhorn.Volume{
				newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode),
			},
			engines: []*longhorn.Engine{
				newTestEngineForIMU(TestEngineNameIMU, TestSourceNode, TestVolumeName2, longhorn.InstanceStateRunning),
			},
			replicas: []*longhorn.Replica{
				// Only replica is on the source node — no temp node available
				newTestReplicaForIMU(TestReplicaName2, TestSourceNode, TestVolumeName2, true),
			},
			expectedState:    longhorn.InstanceManagerUpgradeStateFailed,
			expectedErrorMsg: "upgrade requirement unsupported: cannot find temporary node for engine test-volume-engine-imu (volume test-volume-imu): upgrade requirement unsupported: no replica exists on nodes other than source node test-node-name-1 for volume test-volume-imu; single-replica or co-located volumes are not supported for live upgrade",
		},

		"pending: healthy replica on other node but not running → stay Pending": {
			imu:      newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStatePending),
			sourceIM: newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			volumes: []*longhorn.Volume{
				newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode),
			},
			engines: []*longhorn.Engine{
				newTestEngineForIMU(TestEngineNameIMU, TestSourceNode, TestVolumeName2, longhorn.InstanceStateRunning),
			},
			replicas: []*longhorn.Replica{
				func() *longhorn.Replica {
					r := newTestReplicaForIMU(TestReplicaName2, TestTempNode, TestVolumeName2, true)
					r.Status.CurrentState = longhorn.InstanceStateStopped
					return r
				}(),
			},
			tempIM:            newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			expectedState:     longhorn.InstanceManagerUpgradeStatePending,
			expectedStartedAt: true,
		},

		"pending: engine running, temp node available → RelocatingEngines": {
			imu:      newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStatePending),
			sourceIM: newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			volumes: []*longhorn.Volume{
				newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode),
			},
			engines: []*longhorn.Engine{
				newTestEngineForIMU(TestEngineNameIMU, TestSourceNode, TestVolumeName2, longhorn.InstanceStateRunning),
			},
			replicas: []*longhorn.Replica{
				newTestReplicaForIMU(TestReplicaName2, TestTempNode, TestVolumeName2, true),
			},
			tempIM:        newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			expectedState: longhorn.InstanceManagerUpgradeStateRelocatingEngines,
		},

		// -----------------------------------------------------------------
		// RelocatingEngines state cases
		// -----------------------------------------------------------------

		"relocating: engine not yet directed to temp node → volume.Spec.EngineNodeID updated": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRelocatingEngines)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {
						OriginalNodeID:  TestSourceNode,
						TemporaryNodeID: TestTempNode,
					},
				}
				return imu
			}(),
			sourceIM: newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			tempIM:   newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			volumes: []*longhorn.Volume{
				newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode),
			},
			engines: []*longhorn.Engine{
				// Engine still on source node — not yet directed
				newTestEngineForIMU(TestEngineNameIMU, TestSourceNode, TestVolumeName2, longhorn.InstanceStateRunning),
			},
			expectedState: longhorn.InstanceManagerUpgradeStateRelocatingEngines,
			expectedVolumeEngineNodeIDs: map[string]string{
				TestVolumeName2: TestTempNode,
			},
		},

		"relocating: pre-upgrade snapshot checksum pending → wait before relocating": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRelocatingEngines)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {
						OriginalNodeID:  TestSourceNode,
						TemporaryNodeID: TestTempNode,
						SnapshotName:    "pre-upgrade-snapshot-pending",
					},
				}
				return imu
			}(),
			settings: []*longhorn.Setting{
				newSetting(string(types.SettingNameFastReplicaRebuildEnabled), "{\"v1\":\"true\",\"v2\":\"true\"}"),
				newSetting(string(types.SettingNameTakeSnapshotBeforeV2DataEngineUpgrade), "true"),
			},
			snapshots: []*longhorn.Snapshot{
				func() *longhorn.Snapshot {
					snapshot := newSnapshot("pre-upgrade-snapshot-pending")
					snapshot.Spec.Volume = TestVolumeName2
					snapshot.Status.Parent = "volume-head"
					snapshot.Status.UserCreated = true
					snapshot.Status.ReadyToUse = true
					return snapshot
				}(),
			},
			sourceIM: newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			tempIM:   newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			volumes: []*longhorn.Volume{
				func() *longhorn.Volume {
					v := newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode)
					v.Spec.SnapshotDataIntegrity = longhorn.SnapshotDataIntegrityEnabled
					return v
				}(),
			},
			engines: []*longhorn.Engine{
				newTestEngineForIMU(TestEngineNameIMU, TestSourceNode, TestVolumeName2, longhorn.InstanceStateRunning),
			},
			expectedState: longhorn.InstanceManagerUpgradeStateRelocatingEngines,
			expectedVolumeEngineNodeIDs: map[string]string{
				TestVolumeName2: TestSourceNode,
			},
		},

		"relocating: pre-upgrade snapshot checksum ready → relocation proceeds": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRelocatingEngines)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {
						OriginalNodeID:  TestSourceNode,
						TemporaryNodeID: TestTempNode,
						SnapshotName:    "pre-upgrade-snapshot-ready",
					},
				}
				return imu
			}(),
			settings: []*longhorn.Setting{
				newSetting(string(types.SettingNameFastReplicaRebuildEnabled), "{\"v1\":\"true\",\"v2\":\"true\"}"),
				newSetting(string(types.SettingNameTakeSnapshotBeforeV2DataEngineUpgrade), "true"),
			},
			snapshots: []*longhorn.Snapshot{
				func() *longhorn.Snapshot {
					snapshot := newSnapshot("pre-upgrade-snapshot-ready")
					snapshot.Spec.Volume = TestVolumeName2
					snapshot.Status.Parent = "volume-head"
					snapshot.Status.UserCreated = true
					snapshot.Status.ReadyToUse = true
					snapshot.Status.Checksum = "abc123"
					return snapshot
				}(),
			},
			sourceIM: newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			tempIM:   newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			volumes: []*longhorn.Volume{
				func() *longhorn.Volume {
					v := newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode)
					v.Spec.SnapshotDataIntegrity = longhorn.SnapshotDataIntegrityEnabled
					return v
				}(),
			},
			engines: []*longhorn.Engine{
				newTestEngineForIMU(TestEngineNameIMU, TestSourceNode, TestVolumeName2, longhorn.InstanceStateRunning),
			},
			expectedState: longhorn.InstanceManagerUpgradeStateRelocatingEngines,
			expectedVolumeEngineNodeIDs: map[string]string{
				TestVolumeName2: TestTempNode,
			},
		},

		"relocating: reuse deterministic pre-upgrade snapshot after deferred status write was missed": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRelocatingEngines)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {
						OriginalNodeID:  TestSourceNode,
						TemporaryNodeID: TestTempNode,
					},
				}
				return imu
			}(),
			settings: []*longhorn.Setting{
				newSetting(string(types.SettingNameFastReplicaRebuildEnabled), "{\"v1\":\"true\",\"v2\":\"true\"}"),
				newSetting(string(types.SettingNameTakeSnapshotBeforeV2DataEngineUpgrade), "true"),
			},
			snapshots: []*longhorn.Snapshot{
				func() *longhorn.Snapshot {
					snapshot := newSnapshot(getPreUpgradeSnapshotName(TestIMUName, TestVolumeName2))
					snapshot.Spec.Volume = TestVolumeName2
					snapshot.Status.Parent = "volume-head"
					snapshot.Status.UserCreated = true
					snapshot.Status.ReadyToUse = true
					snapshot.Status.Checksum = "abc123"
					return snapshot
				}(),
			},
			sourceIM: newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			tempIM:   newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			volumes: []*longhorn.Volume{
				func() *longhorn.Volume {
					v := newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode)
					v.Spec.SnapshotDataIntegrity = longhorn.SnapshotDataIntegrityEnabled
					return v
				}(),
			},
			engines: []*longhorn.Engine{
				newTestEngineForIMU(TestEngineNameIMU, TestSourceNode, TestVolumeName2, longhorn.InstanceStateRunning),
			},
			expectedState: longhorn.InstanceManagerUpgradeStateRelocatingEngines,
			expectedVolumeEngineNodeIDs: map[string]string{
				TestVolumeName2: TestTempNode,
			},
			expectedSnapshotNames: map[string]string{
				TestVolumeName2: getPreUpgradeSnapshotName(TestIMUName, TestVolumeName2),
			},
			expectedSnapshotCount: 1,
		},

		"relocating: engine directed and Running on temp node → WaitingForSourceIM": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRelocatingEngines)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {
						OriginalNodeID:  TestSourceNode,
						TemporaryNodeID: TestTempNode,
					},
				}
				return imu
			}(),
			sourceIM: newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			tempIM:   newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			volumes: []*longhorn.Volume{
				newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestTempNode),
			},
			engines: []*longhorn.Engine{
				// Engine already on temp node and Running
				newTestEngineForIMU(TestEngineNameIMU, TestTempNode, TestVolumeName2, longhorn.InstanceStateRunning),
			},
			expectedState: longhorn.InstanceManagerUpgradeStateWaitingForSourceIM,
		},

		"relocating: engine on temp node, not Running, temp IM healthy → stay RelocatingEngines": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRelocatingEngines)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {
						OriginalNodeID:  TestSourceNode,
						TemporaryNodeID: TestTempNode,
					},
				}
				return imu
			}(),
			tempIM: newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			volumes: []*longhorn.Volume{
				func() *longhorn.Volume {
					v := newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode)
					v.Spec.EngineNodeID = TestTempNode
					return v
				}(),
			},
			engines: []*longhorn.Engine{
				// Engine directed to temp node but still Starting
				newTestEngineForIMU(TestEngineNameIMU, TestTempNode, TestVolumeName2, longhorn.InstanceStateStarting),
			},
			expectedState: longhorn.InstanceManagerUpgradeStateRelocatingEngines,
			expectedVolumeEngineNodeIDs: map[string]string{
				TestVolumeName2: TestTempNode, // unchanged
			},
		},

		"relocating: engine on temp node, not Running, temp IM down → replan to new temp node": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRelocatingEngines)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {
						OriginalNodeID:  TestSourceNode,
						TemporaryNodeID: TestTempNode,
					},
				}
				return imu
			}(),
			// temp node IM is down — no Running IM on TestTempNode
			// but TestNode2 (alias) has a different IM... actually we need a third node for replan.
			// Let's simulate: temp IM is Error, source IM is Running, replica on temp node is healthy
			// GetRunningInstanceManagerByNodeRO(TestTempNode) will return NotFound since IM is not Running
			tempIM: newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateError),
			volumes: []*longhorn.Volume{
				func() *longhorn.Volume {
					v := newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode)
					v.Spec.EngineNodeID = TestTempNode
					return v
				}(),
			},
			engines: []*longhorn.Engine{
				newTestEngineForIMU(TestEngineNameIMU, TestTempNode, TestVolumeName2, longhorn.InstanceStateError),
			},
			replicas: []*longhorn.Replica{
				newTestReplicaForIMU(TestReplicaName2, TestTempNode, TestVolumeName2, true),
			},
			expectedState:    longhorn.InstanceManagerUpgradeStateRestoringEngines,
			expectedAbort:    true,
			expectedErrorMsg: "upgrade aborted: no-temporary-node",
			expectedVolumeEngineNodeIDs: map[string]string{
				TestVolumeName2: TestSourceNode,
			},
		},

		// -----------------------------------------------------------------
		// WaitingForSourceIM state cases
		// -----------------------------------------------------------------

		"waiting: no target IM yet → stay WaitingForSourceIM": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			volumes: []*longhorn.Volume{
				newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestTempNode),
			},
			tempIM: newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage,
				longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			// sourceIM is nil (old IM is gone), no new IM with target image
			expectedState: longhorn.InstanceManagerUpgradeStateWaitingForSourceIM,
		},

		"waiting: temp IM down after relocation → replan from temp A to temp B without source IM": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			volumes: []*longhorn.Volume{
				newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestTempNode),
			},
			tempIM: newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage,
				longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateError),
			sourceIM: newTestIMForNode(TestTargetIMName, TestTempNode2, TestSourceImage,
				longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			replicas: []*longhorn.Replica{
				newTestReplicaForIMU(TestReplicaName2, TestTempNode, TestVolumeName2, true),
				newTestReplicaForIMU(TestReplicaName3, TestTempNode2, TestVolumeName2, true),
			},
			expectedState: longhorn.InstanceManagerUpgradeStateWaitingForSourceIM,
			expectedVolumeEngineNodeIDs: map[string]string{
				TestVolumeName2: TestTempNode2,
			},
		},

		"waiting: temp IM down without replacement and revert update fails → return error": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			volumes: []*longhorn.Volume{
				newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestTempNode),
			},
			tempIM: newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage,
				longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateError),
			replicas: []*longhorn.Replica{
				newTestReplicaForIMU(TestReplicaName2, TestSourceNode, TestVolumeName2, true),
			},
			updateVolumeErr:   fmt.Errorf("update volume failed"),
			expectedState:     longhorn.InstanceManagerUpgradeStateWaitingForSourceIM,
			expectedSyncError: "failed to revert volume test-volume-imu to original node test-node-name-1",
		},

		"waiting: target IM now Running, has engines → RestoringEngines": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			volumes: []*longhorn.Volume{
				newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestTempNode),
			},
			// New IM with target image is Running on source node
			sourceIM:      newTestIMForNode(TestTargetIMName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			expectedState: longhorn.InstanceManagerUpgradeStateRestoringEngines,
		},

		"waiting: target IM Running, no engines in plan → Completed": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
				imu.Status.StartedAt = util.Now()
				// Empty engine plan (no-engine-to-relocate path)
				imu.Status.Engines = map[string]longhorn.EngineRelocation{}
				return imu
			}(),
			sourceIM:      newTestIMForNode(TestTargetIMName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			expectedState: longhorn.InstanceManagerUpgradeStateCompleted,
		},

		// -----------------------------------------------------------------
		// RestoringEngines state cases
		// -----------------------------------------------------------------

		"restoring: engine directed back and Running on original node → WaitingForHealthyVolumes": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRestoringEngines)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			volumes: []*longhorn.Volume{
				func() *longhorn.Volume {
					v := newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestTempNode)
					v.Spec.EngineNodeID = TestTempNode
					return v
				}(),
			},
			engines: []*longhorn.Engine{
				// Engine still on temp node
				newTestEngineForIMU(TestEngineNameIMU, TestTempNode, TestVolumeName2, longhorn.InstanceStateRunning),
			},
			expectedState: longhorn.InstanceManagerUpgradeStateRestoringEngines,
			expectedVolumeEngineNodeIDs: map[string]string{
				TestVolumeName2: TestSourceNode,
			},
		},

		"restoring: engine back on original node → WaitingForHealthyVolumes": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRestoringEngines)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			volumes: []*longhorn.Volume{
				newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode),
			},
			engines: []*longhorn.Engine{
				// Engine back on source node and Running
				newTestEngineForIMU(TestEngineNameIMU, TestSourceNode, TestVolumeName2, longhorn.InstanceStateRunning),
			},
			expectedState: longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes,
		},

		"restoring: engine directed back, source IM down → wait (stay RestoringEngines)": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRestoringEngines)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			volumes: []*longhorn.Volume{
				func() *longhorn.Volume {
					v := newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestTempNode)
					v.Spec.EngineNodeID = TestSourceNode
					return v
				}(),
			},
			engines: []*longhorn.Engine{
				// Engine directed to original node but not Running (source IM/node is down)
				newTestEngineForIMU(TestEngineNameIMU, TestSourceNode, TestVolumeName2, longhorn.InstanceStateError),
			},
			expectedState: longhorn.InstanceManagerUpgradeStateRestoringEngines,
		},

		// -----------------------------------------------------------------
		// WaitingForHealthyVolumes state cases
		// -----------------------------------------------------------------

		"waiting-healthy: volume not healthy → stay WaitingForHealthyVolumes": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			volumes: []*longhorn.Volume{
				func() *longhorn.Volume {
					v := newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode)
					v.Status.Robustness = longhorn.VolumeRobustnessDegraded
					return v
				}(),
			},
			expectedState: longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes,
		},

		"waiting-healthy: volume healthy → Completed": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			volumes: []*longhorn.Volume{
				func() *longhorn.Volume {
					v := newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode)
					v.Status.Robustness = longhorn.VolumeRobustnessHealthy
					return v
				}(),
			},
			expectedState: longhorn.InstanceManagerUpgradeStateCompleted,
		},

		"waiting-healthy: timeout elapsed → stay WaitingForHealthyVolumes": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes)
				imu.Status.StartedAt = time.Now().Add(-(60*time.Minute + time.Minute)).UTC().Format(time.RFC3339)
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			volumes: []*longhorn.Volume{
				func() *longhorn.Volume {
					v := newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode)
					v.Status.Robustness = longhorn.VolumeRobustnessDegraded
					return v
				}(),
			},
			expectedState: longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes,
		},

		// -----------------------------------------------------------------
		// Abort cases
		// -----------------------------------------------------------------

		"relocating: abort requested → RestoringEngines": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRelocatingEngines)
				imu.Status.StartedAt = util.Now()
				imu.Status.AbortRequested = true
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			volumes: []*longhorn.Volume{
				newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode),
			},
			expectedState: longhorn.InstanceManagerUpgradeStateRestoringEngines,
		},

		"relocating: timed out → abort requested and transition to RestoringEngines": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRelocatingEngines)
				imu.Status.StartedAt = time.Now().Add(-(60*time.Minute + time.Minute)).UTC().Format(time.RFC3339)
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			expectedState: longhorn.InstanceManagerUpgradeStateRestoringEngines,
			expectedAbort: true,
		},

		"relocating: temp IM lost and no new temp node → abort and restore": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRelocatingEngines)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			volumes: []*longhorn.Volume{
				func() *longhorn.Volume {
					v := newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode)
					v.Spec.EngineNodeID = TestTempNode
					return v
				}(),
			},
			replicas: []*longhorn.Replica{
				newTestReplicaForIMU(TestReplicaName2, TestSourceNode, TestVolumeName2, true),
			},
			tempIM: newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage,
				longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateError),
			expectedState:    longhorn.InstanceManagerUpgradeStateRestoringEngines,
			expectedAbort:    true,
			expectedErrorMsg: "upgrade aborted: no-temporary-node",
			expectedVolumeEngineNodeIDs: map[string]string{
				TestVolumeName2: TestSourceNode,
			},
		},

		"waiting-for-source-im: abort requested → RestoringEngines": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
				imu.Status.StartedAt = util.Now()
				imu.Status.AbortRequested = true
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			volumes: []*longhorn.Volume{
				newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestTempNode),
			},
			expectedState: longhorn.InstanceManagerUpgradeStateRestoringEngines,
		},

		"waiting-for-source-im: temp IM lost and no new temp node → abort and restore": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
				imu.Status.StartedAt = util.Now()
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			volumes: []*longhorn.Volume{
				func() *longhorn.Volume {
					v := newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestTempNode)
					v.Status.CurrentEngineNodeID = TestTempNode
					return v
				}(),
			},
			replicas: []*longhorn.Replica{
				newTestReplicaForIMU(TestReplicaName2, TestSourceNode, TestVolumeName2, true),
			},
			tempIM: newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage,
				longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateError),
			expectedState:    longhorn.InstanceManagerUpgradeStateRestoringEngines,
			expectedAbort:    true,
			expectedErrorMsg: "upgrade aborted: no-temporary-node",
			expectedVolumeEngineNodeIDs: map[string]string{
				TestVolumeName2: TestSourceNode,
			},
		},

		"restoring: timed out after abort → Failed": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRestoringEngines)
				imu.Status.AbortRequested = true
				imu.Status.AbortReason = "timeout"
				imu.Status.StartedAt = time.Now().Add(-(60*time.Minute + time.Minute)).UTC().Format(time.RFC3339)
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			expectedState:    longhorn.InstanceManagerUpgradeStateFailed,
			expectedErrorMsg: "upgrade aborted: timeout",
		},

		"restoring: abort + all engines restored → Failed": {
			imu: func() *longhorn.InstanceManagerUpgrade {
				imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRestoringEngines)
				imu.Status.StartedAt = util.Now()
				imu.Status.AbortRequested = true
				imu.Status.Engines = map[string]longhorn.EngineRelocation{
					TestVolumeName2: {OriginalNodeID: TestSourceNode, TemporaryNodeID: TestTempNode},
				}
				return imu
			}(),
			volumes: []*longhorn.Volume{
				// Engine already back on original node
				newTestVolumeForIMU(TestVolumeName2, TestSourceNode, TestSourceNode),
			},
			engines: []*longhorn.Engine{
				newTestEngineForIMU(TestEngineNameIMU, TestSourceNode, TestVolumeName2, longhorn.InstanceStateRunning),
			},
			expectedState: longhorn.InstanceManagerUpgradeStateFailed,
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		lhClient := lhfake.NewSimpleClientset() //nolint:staticcheck // generated fake field manager does not support these CRDs in this test harness
		extensionsClient := apiextensionsfake.NewSimpleClientset()

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()
		imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
		engineIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Engines().Informer().GetIndexer()
		replicaIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
		snapshotIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Snapshots().Informer().GetIndexer()
		vIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
		sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()

		imuc, controllerErr := newTestIMUController(lhClient, kubeClient, extensionsClient, informerFactories, tc.imu.Status.OwnerID)
		c.Assert(controllerErr, IsNil)
		if tc.updateVolumeErr != nil {
			lhClient.PrependReactor("update", "volumes", func(action k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, tc.updateVolumeErr
			})
		}

		// Settings
		imImageSetting := newDefaultInstanceManagerImageSetting()
		err = sIndexer.Add(imImageSetting)
		c.Assert(err, IsNil)

		upgradeTimeoutSetting := newV2InstanceManagerUpgradeTimeoutSetting()
		err = sIndexer.Add(upgradeTimeoutSetting)
		c.Assert(err, IsNil)

		for _, setting := range tc.settings {
			err = sIndexer.Add(setting)
			c.Assert(err, IsNil)
		}

		// IMU under test
		imu, err := lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), tc.imu, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = imuIndexer.Add(imu)
		c.Assert(err, IsNil)

		// Other IMUs
		for _, other := range tc.otherIMUs {
			other, err = lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), other, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = imuIndexer.Add(other)
			c.Assert(err, IsNil)
		}

		// Source IM (on source node, old image)
		if tc.sourceIM != nil {
			im, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), tc.sourceIM, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = imIndexer.Add(im)
			c.Assert(err, IsNil)
		}

		// Temp IM (on temp node; also reused as "target IM on source node" for waiting tests)
		if tc.tempIM != nil {
			im, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), tc.tempIM, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = imIndexer.Add(im)
			c.Assert(err, IsNil)
		}

		// Volumes
		for _, volume := range tc.volumes {
			v, err := lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(context.TODO(), volume, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = vIndexer.Add(v)
			c.Assert(err, IsNil)
		}

		for _, snapshot := range tc.snapshots {
			snapshot, err = lhClient.LonghornV1beta2().Snapshots(TestNamespace).Create(context.TODO(), snapshot, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = snapshotIndexer.Add(snapshot)
			c.Assert(err, IsNil)
		}

		// Engines
		for _, engine := range tc.engines {
			e, err := lhClient.LonghornV1beta2().Engines(TestNamespace).Create(context.TODO(), engine, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = engineIndexer.Add(e)
			c.Assert(err, IsNil)
		}

		// Replicas
		for _, replica := range tc.replicas {
			r, err := lhClient.LonghornV1beta2().Replicas(TestNamespace).Create(context.TODO(), replica, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = replicaIndexer.Add(r)
			c.Assert(err, IsNil)
		}

		// Run reconcile
		err = imuc.syncInstanceManagerUpgrade(getKey(tc.imu, c))
		if tc.expectedSyncError != "" {
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Matches, ".*"+tc.expectedSyncError+".*")
			continue
		}
		c.Assert(err, IsNil)

		// Fetch updated IMU from fake client
		updatedIMU, err := lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Get(context.TODO(), tc.imu.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)
		c.Assert(updatedIMU.Status.State, Equals, tc.expectedState,
			Commentf("test case %q: expected state %v, got %v", name, tc.expectedState, updatedIMU.Status.State))
		if tc.expectedAbort {
			c.Assert(updatedIMU.Status.AbortRequested, Equals, true,
				Commentf("test case %q: expected abort=true", name))
		}
		if tc.expectedErrorMsg != "" {
			c.Assert(updatedIMU.Status.ErrorMsg, Equals, tc.expectedErrorMsg,
				Commentf("test case %q: expected error %q, got %q", name, tc.expectedErrorMsg, updatedIMU.Status.ErrorMsg))
		}
		if tc.expectedStartedAt {
			c.Assert(updatedIMU.Status.StartedAt, Not(Equals), "",
				Commentf("test case %q: expected StartedAt to be set", name))
		}
		for volumeName, expectedSnapshotName := range tc.expectedSnapshotNames {
			reloc, exists := updatedIMU.Status.Engines[volumeName]
			c.Assert(exists, Equals, true,
				Commentf("test case %q: expected relocation plan for volume %q", name, volumeName))
			c.Assert(reloc.SnapshotName, Equals, expectedSnapshotName,
				Commentf("test case %q: volume %q: expected snapshot name %q, got %q", name, volumeName, expectedSnapshotName, reloc.SnapshotName))
		}
		if tc.expectedSnapshotCount != 0 {
			snapshotList, err := lhClient.LonghornV1beta2().Snapshots(TestNamespace).List(context.TODO(), metav1.ListOptions{})
			c.Assert(err, IsNil)
			c.Assert(len(snapshotList.Items), Equals, tc.expectedSnapshotCount,
				Commentf("test case %q: expected %d snapshots, got %d", name, tc.expectedSnapshotCount, len(snapshotList.Items)))
		}

		// Check volume Spec.EngineNodeID updates
		for volumeName, expectedNodeID := range tc.expectedVolumeEngineNodeIDs {
			updatedVolume, err := lhClient.LonghornV1beta2().Volumes(TestNamespace).Get(context.TODO(), volumeName, metav1.GetOptions{})
			c.Assert(err, IsNil)
			c.Assert(updatedVolume.Spec.EngineNodeID, Equals, expectedNodeID,
				Commentf("test case %q: volume %q: expected EngineNodeID %v, got %v", name, volumeName, expectedNodeID, updatedVolume.Spec.EngineNodeID))
		}
	}
}

func (s *TestSuite) TestEnqueueInstanceManagerChangeForPendingIMU(c *C) {
	datastore.SkipListerCheck = true
	defer func() { datastore.SkipListerCheck = false }()

	kubeClient := fake.NewSimpleClientset()
	lhClient := lhfake.NewSimpleClientset() //nolint:staticcheck // generated fake field manager does not support these CRDs in this test harness
	extensionsClient := apiextensionsfake.NewSimpleClientset()
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imuc, err := newTestIMUController(lhClient, kubeClient, extensionsClient, informerFactories, TestSourceNode)
	c.Assert(err, IsNil)

	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()

	pendingIMU := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStatePending)
	createdIMU, err := lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), pendingIMU, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(createdIMU)
	c.Assert(err, IsNil)

	// IM on a different node becomes available; Pending IMU should be re-enqueued
	// so temporary-node preconditions are re-evaluated promptly.
	changedIM := newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning)
	imuc.enqueueInstanceManagerChange(changedIM)

	c.Assert(imuc.queue.Len(), Equals, 1)
	item, shutdown := imuc.queue.Get()
	c.Assert(shutdown, Equals, false)
	c.Assert(item, Equals, TestNamespace+"/"+TestIMUName)
	imuc.queue.Done(item)
}

func (s *TestSuite) TestEnqueueInstanceManagerChangeForTemporaryNode(c *C) {
	datastore.SkipListerCheck = true
	defer func() { datastore.SkipListerCheck = false }()

	kubeClient := fake.NewSimpleClientset()
	lhClient := lhfake.NewSimpleClientset() //nolint:staticcheck // generated fake field manager does not support these CRDs in this test harness
	extensionsClient := apiextensionsfake.NewSimpleClientset()
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imuc, err := newTestIMUController(lhClient, kubeClient, extensionsClient, informerFactories, TestSourceNode)
	c.Assert(err, IsNil)

	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()

	relocatingIMU := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRelocatingEngines)
	relocatingIMU.Status.Engines = map[string]longhorn.EngineRelocation{
		TestVolumeName2: {
			OriginalNodeID:  TestSourceNode,
			TemporaryNodeID: TestTempNode,
		},
	}
	createdIMU, err := lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), relocatingIMU, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(createdIMU)
	c.Assert(err, IsNil)

	// IM on the planned temporary node changes; the IMU should be re-enqueued so
	// relocation/readiness can be re-evaluated.
	changedIM := newTestIMForNode(TestTempIMName, TestTempNode, TestSourceImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning)
	imuc.enqueueInstanceManagerChange(changedIM)

	c.Assert(imuc.queue.Len(), Equals, 1)
	item, shutdown := imuc.queue.Get()
	c.Assert(shutdown, Equals, false)
	c.Assert(item, Equals, TestNamespace+"/"+TestIMUName)
	imuc.queue.Done(item)
}

// ---------------------------------------------------------------------------
// IMUC (InstanceManagerUpgradeControl) controller tests
// ---------------------------------------------------------------------------

func newTestIMUCController(
	lhClient *lhfake.Clientset,
	kubeClient *fake.Clientset,
	extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories,
	controllerID string,
) (*InstanceManagerUpgradeControlController, error) {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
	logger := logrus.StandardLogger()

	c, err := NewInstanceManagerUpgradeControlController(logger, ds, scheme.Scheme, kubeClient, TestNamespace, controllerID)
	if err != nil {
		return nil, err
	}

	fakeRecorder := record.NewFakeRecorder(100)
	c.eventRecorder = fakeRecorder
	for i := range c.cacheSyncs {
		c.cacheSyncs[i] = alwaysReady
	}
	return c, nil
}

func newIMUC(targetImage string) *longhorn.InstanceManagerUpgradeControl {
	return &longhorn.InstanceManagerUpgradeControl{
		ObjectMeta: metav1.ObjectMeta{
			Name:      types.InstanceManagerUpgradeControlName,
			Namespace: TestNamespace,
		},
		Spec: longhorn.InstanceManagerUpgradeControlSpec{
			TargetImage: targetImage,
		},
		Status: longhorn.InstanceManagerUpgradeControlStatus{
			OwnerID: TestNode1,
		},
	}
}

type IMUCTestCase struct {
	// Initial state
	imuc         *longhorn.InstanceManagerUpgradeControl
	sourceIMs    []*longhorn.InstanceManager        // IMs with old image (trigger upgrade)
	existingIMUs []*longhorn.InstanceManagerUpgrade // pre-existing IMU CRs

	// Expected outcomes
	expectedCurrentNode string // "" means no active node
	expectedIMUCreated  bool   // whether a new IMU CR should be created
	expectedIMUCount    int
	expectedNodeStates  map[string]longhorn.NodeUpgradeState
	expectedRetryCount  map[string]int
	expectedAbortOnIMU  string // name of IMU that should have Abort=true set
}

func (s *TestSuite) TestSyncIMUC(c *C) {
	datastore.SkipListerCheck = true
	defer func() { datastore.SkipListerCheck = false }()

	testCases := map[string]IMUCTestCase{

		"no nodes needing upgrade → idle, no IMU created": {
			imuc: newIMUC(TestTargetImage),
			// No IMs with old image — all nodes already on target image
			expectedCurrentNode: "",
			expectedIMUCreated:  false,
		},

		"one node with old IM, fresh start → IMU created, CurrentNode set": {
			imuc: newIMUC(TestTargetImage),
			sourceIMs: []*longhorn.InstanceManager{
				newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage,
					longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			},
			expectedCurrentNode: TestSourceNode,
			expectedIMUCreated:  true,
			expectedNodeStates: map[string]longhorn.NodeUpgradeState{
				TestSourceNode: longhorn.NodeUpgradeStateInProgress,
			},
		},

		"scheduled start time in the future → idle until start time": {
			imuc: func() *longhorn.InstanceManagerUpgradeControl {
				imuc := newIMUC(TestTargetImage)
				imuc.Spec.StartAt = time.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339)
				return imuc
			}(),
			sourceIMs: []*longhorn.InstanceManager{
				newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage,
					longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			},
			expectedCurrentNode: "",
			expectedIMUCreated:  false,
		},

		"scheduled start time in the future but current node already in progress → continue processing": {
			imuc: func() *longhorn.InstanceManagerUpgradeControl {
				imuc := newIMUC(TestTargetImage)
				imuc.Spec.StartAt = time.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339)
				imuc.Status.CurrentNode = TestSourceNode
				imuc.Status.Nodes = map[string]longhorn.NodeUpgradeInfo{
					TestSourceNode: {
						State:     longhorn.NodeUpgradeStateInProgress,
						IMUName:   TestIMUName,
						StartedAt: util.Now(),
					},
				}
				return imuc
			}(),
			sourceIMs: []*longhorn.InstanceManager{
				newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage,
					longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			},
			existingIMUs: []*longhorn.InstanceManagerUpgrade{
				newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM),
			},
			expectedCurrentNode: TestSourceNode,
			expectedIMUCreated:  false,
			expectedNodeStates: map[string]longhorn.NodeUpgradeState{
				TestSourceNode: longhorn.NodeUpgradeStateInProgress,
			},
		},

		"current node missing from status → repaired without starting another node in same reconcile": {
			imuc: func() *longhorn.InstanceManagerUpgradeControl {
				imuc := newIMUC(TestTargetImage)
				imuc.Status.CurrentNode = TestSourceNode
				return imuc
			}(),
			sourceIMs: []*longhorn.InstanceManager{
				newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage,
					longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			},
			expectedCurrentNode: "",
			expectedIMUCreated:  false,
			expectedNodeStates: map[string]longhorn.NodeUpgradeState{
				TestSourceNode: longhorn.NodeUpgradeStatePending,
			},
		},

		"current node IMU completed → CurrentNode cleared, node marked completed": {
			imuc: func() *longhorn.InstanceManagerUpgradeControl {
				imuc := newIMUC(TestTargetImage)
				imuc.Status.CurrentNode = TestSourceNode
				imuc.Status.Nodes = map[string]longhorn.NodeUpgradeInfo{
					TestSourceNode: {
						State:     longhorn.NodeUpgradeStateInProgress,
						IMUName:   TestIMUName,
						StartedAt: util.Now(),
					},
				}
				return imuc
			}(),
			existingIMUs: []*longhorn.InstanceManagerUpgrade{
				func() *longhorn.InstanceManagerUpgrade {
					imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateCompleted)
					return imu
				}(),
				func() *longhorn.InstanceManagerUpgrade {
					imu := newIMU("test-imu-old-failed", TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateFailed)
					imu.Status.ErrorMsg = "old failure"
					return imu
				}(),
			},
			expectedCurrentNode: "",
			expectedIMUCreated:  false,
			expectedIMUCount:    1,
			expectedNodeStates: map[string]longhorn.NodeUpgradeState{
				TestSourceNode: longhorn.NodeUpgradeStateCompleted,
			},
		},

		"current node IMU failed, retries remain → new IMU created, node stays InProgress": {
			imuc: func() *longhorn.InstanceManagerUpgradeControl {
				imuc := newIMUC(TestTargetImage)
				imuc.Status.CurrentNode = TestSourceNode
				imuc.Status.Nodes = map[string]longhorn.NodeUpgradeInfo{
					TestSourceNode: {
						State:      longhorn.NodeUpgradeStateInProgress,
						IMUName:    TestIMUName,
						StartedAt:  util.Now(),
						RetryCount: 0,
					},
				}
				return imuc
			}(),
			existingIMUs: []*longhorn.InstanceManagerUpgrade{
				newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateFailed),
			},
			sourceIMs: []*longhorn.InstanceManager{
				// Still has old image (so pickNextPendingNode finds it)
				newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage,
					longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			},
			expectedCurrentNode: TestSourceNode,
			expectedIMUCreated:  true, // new IMU created for retry
			expectedNodeStates: map[string]longhorn.NodeUpgradeState{
				TestSourceNode: longhorn.NodeUpgradeStateInProgress,
			},
		},

		"current node IMU failed, retries exhausted → node marked Failed, CurrentNode cleared": {
			imuc: func() *longhorn.InstanceManagerUpgradeControl {
				imuc := newIMUC(TestTargetImage)
				imuc.Status.CurrentNode = TestSourceNode
				imuc.Status.Nodes = map[string]longhorn.NodeUpgradeInfo{
					TestSourceNode: {
						State:      longhorn.NodeUpgradeStateInProgress,
						IMUName:    TestIMUName,
						StartedAt:  util.Now(),
						RetryCount: imucMaxNodeRetries,
					},
				}
				return imuc
			}(),
			existingIMUs: []*longhorn.InstanceManagerUpgrade{
				newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateFailed),
			},
			expectedCurrentNode: "",
			expectedIMUCreated:  false,
			expectedNodeStates: map[string]longhorn.NodeUpgradeState{
				TestSourceNode: longhorn.NodeUpgradeStateFailed,
			},
		},

		"IMU deleted externally → treat as failure, retry": {
			imuc: func() *longhorn.InstanceManagerUpgradeControl {
				imuc := newIMUC(TestTargetImage)
				imuc.Status.CurrentNode = TestSourceNode
				imuc.Status.Nodes = map[string]longhorn.NodeUpgradeInfo{
					TestSourceNode: {
						State:      longhorn.NodeUpgradeStateInProgress,
						IMUName:    TestIMUName, // IMU not created in fake client — deleted
						StartedAt:  util.Now(),
						RetryCount: 3,
					},
				}
				return imuc
			}(),
			sourceIMs: []*longhorn.InstanceManager{
				newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage,
					longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			},
			// no existingIMUs — simulates external deletion
			expectedCurrentNode: TestSourceNode,
			expectedIMUCreated:  true,
			expectedNodeStates: map[string]longhorn.NodeUpgradeState{
				TestSourceNode: longhorn.NodeUpgradeStateInProgress,
			},
			expectedRetryCount: map[string]int{
				TestSourceNode: 3,
			},
		},

		"deleting IMU is not reused for a new node attempt": {
			imuc: newIMUC(TestTargetImage),
			sourceIMs: []*longhorn.InstanceManager{
				newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage,
					longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			},
			existingIMUs: []*longhorn.InstanceManagerUpgrade{
				func() *longhorn.InstanceManagerUpgrade {
					imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStatePending)
					now := metav1.NewTime(time.Now())
					imu.DeletionTimestamp = &now
					return imu
				}(),
			},
			expectedCurrentNode: TestSourceNode,
			expectedIMUCreated:  true,
			expectedNodeStates: map[string]longhorn.NodeUpgradeState{
				TestSourceNode: longhorn.NodeUpgradeStateInProgress,
			},
		},

		"target image changed + IMU failed → reset to Pending without retry": {
			imuc: func() *longhorn.InstanceManagerUpgradeControl {
				imuc := newIMUC(TestTargetImage)
				imuc.Spec.TargetImage = "longhorn-instance-manager:new-target"
				imuc.Status.CurrentNode = TestSourceNode
				imuc.Status.Nodes = map[string]longhorn.NodeUpgradeInfo{
					TestSourceNode: {
						State:      longhorn.NodeUpgradeStateInProgress,
						IMUName:    TestIMUName,
						RetryCount: 0,
						StartedAt:  util.Now(),
					},
				}
				return imuc
			}(),
			existingIMUs: []*longhorn.InstanceManagerUpgrade{
				func() *longhorn.InstanceManagerUpgrade {
					imu := newIMU(TestIMUName, TestSourceNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateFailed)
					imu.Status.AbortRequested = true
					imu.Status.AbortReason = "target-image-changed"
					imu.Status.ErrorMsg = "upgrade aborted: target-image-changed"
					return imu
				}(),
			},
			sourceIMs: []*longhorn.InstanceManager{
				// Still has old image (so pickNextPendingNode finds it and starts new IMU)
				newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage,
					longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			},
			expectedCurrentNode: TestSourceNode, // picked again immediately after reset
			expectedIMUCreated:  true,           // new IMU created with updated target image
			expectedNodeStates: map[string]longhorn.NodeUpgradeState{
				TestSourceNode: longhorn.NodeUpgradeStateInProgress, // picked up again for new attempt
			},
		},

		"terminal node with stale IM image → retry count preserved when restarting upgrade": {
			imuc: func() *longhorn.InstanceManagerUpgradeControl {
				imuc := newIMUC(TestTargetImage)
				imuc.Status.Nodes = map[string]longhorn.NodeUpgradeInfo{
					TestSourceNode: {
						State:      longhorn.NodeUpgradeStateCompleted,
						RetryCount: 3,
					},
				}
				return imuc
			}(),
			sourceIMs: []*longhorn.InstanceManager{
				newTestIMForNode(TestSourceIMName, TestSourceNode, TestSourceImage,
					longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning),
			},
			expectedCurrentNode: TestSourceNode,
			expectedIMUCreated:  true,
			expectedNodeStates: map[string]longhorn.NodeUpgradeState{
				TestSourceNode: longhorn.NodeUpgradeStateInProgress,
			},
			expectedRetryCount: map[string]int{
				TestSourceNode: 3,
			},
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing IMUC: %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		lhClient := lhfake.NewSimpleClientset() //nolint:staticcheck // generated fake field manager does not support these CRDs in this test harness
		extensionsClient := apiextensionsfake.NewSimpleClientset()

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		imucIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgradeControls().Informer().GetIndexer()
		imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()
		imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()

		imuccController, err := newTestIMUCController(lhClient, kubeClient, extensionsClient, informerFactories, tc.imuc.Status.OwnerID)
		c.Assert(err, IsNil)

		// IMUC under test
		imucObj, err := lhClient.LonghornV1beta2().InstanceManagerUpgradeControls(TestNamespace).Create(context.TODO(), tc.imuc, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = imucIndexer.Add(imucObj)
		c.Assert(err, IsNil)

		// Pre-existing IMUs
		for _, imu := range tc.existingIMUs {
			created, err := lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), imu, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = imuIndexer.Add(created)
			c.Assert(err, IsNil)
		}

		// Instance managers (nodes with old image that need upgrading)
		for _, im := range tc.sourceIMs {
			created, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = imIndexer.Add(created)
			c.Assert(err, IsNil)
		}

		// Run reconcile
		key := TestNamespace + "/" + types.InstanceManagerUpgradeControlName
		err = imuccController.syncIMUC(key)
		c.Assert(err, IsNil)

		// Fetch updated IMUC
		updatedIMUC, err := lhClient.LonghornV1beta2().InstanceManagerUpgradeControls(TestNamespace).Get(
			context.TODO(), types.InstanceManagerUpgradeControlName, metav1.GetOptions{})
		c.Assert(err, IsNil)

		c.Assert(updatedIMUC.Status.CurrentNode, Equals, tc.expectedCurrentNode,
			Commentf("test case %q: expected CurrentNode %q, got %q", name, tc.expectedCurrentNode, updatedIMUC.Status.CurrentNode))

		// Check node states
		for nodeID, expectedState := range tc.expectedNodeStates {
			info, ok := updatedIMUC.Status.Nodes[nodeID]
			c.Assert(ok, Equals, true,
				Commentf("test case %q: node %q not found in status", name, nodeID))
			c.Assert(info.State, Equals, expectedState,
				Commentf("test case %q: node %q: expected state %v, got %v", name, nodeID, expectedState, info.State))
		}
		for nodeID, expectedRetryCount := range tc.expectedRetryCount {
			info, ok := updatedIMUC.Status.Nodes[nodeID]
			c.Assert(ok, Equals, true,
				Commentf("test case %q: node %q not found in status for retry count", name, nodeID))
			c.Assert(info.RetryCount, Equals, expectedRetryCount,
				Commentf("test case %q: node %q: expected retry count %d, got %d", name, nodeID, expectedRetryCount, info.RetryCount))
		}

		// Check whether a new IMU was created (beyond the pre-existing ones)
		allIMUs, err := lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).List(context.TODO(), metav1.ListOptions{})
		c.Assert(err, IsNil)
		newIMUCreated := len(allIMUs.Items) > len(tc.existingIMUs)
		c.Assert(newIMUCreated, Equals, tc.expectedIMUCreated,
			Commentf("test case %q: expectedIMUCreated=%v but got %v total IMUs (had %v pre-existing)",
				name, tc.expectedIMUCreated, len(allIMUs.Items), len(tc.existingIMUs)))
		if tc.expectedIMUCount != 0 {
			c.Assert(len(allIMUs.Items), Equals, tc.expectedIMUCount,
				Commentf("test case %q: expected %d IMUs remaining, got %d", name, tc.expectedIMUCount, len(allIMUs.Items)))
		}

		// Check Abort was set on the expected IMU
		if tc.expectedAbortOnIMU != "" {
			imu, err := lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Get(
				context.TODO(), tc.expectedAbortOnIMU, metav1.GetOptions{})
			c.Assert(err, IsNil)
			c.Assert(imu.Status.AbortRequested, Equals, true,
				Commentf("test case %q: expected AbortRequested=true on IMU %q", name, tc.expectedAbortOnIMU))
		}
	}
}

func (s *TestSuite) TestSyncIMUCRecoverOrphanedNodeAbortFailureReturnsError(c *C) {
	datastore.SkipListerCheck = true
	defer func() { datastore.SkipListerCheck = false }()

	kubeClient := fake.NewSimpleClientset()
	lhClient := lhfake.NewSimpleClientset() //nolint:staticcheck // generated fake field manager does not support these CRDs in this test harness
	extensionsClient := apiextensionsfake.NewSimpleClientset()
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imucIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgradeControls().Informer().GetIndexer()
	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()

	imuccController, err := newTestIMUCController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imuc := newIMUC(TestTargetImage)
	imuc.Status.Nodes = map[string]longhorn.NodeUpgradeInfo{
		TestSourceNode: {
			State:   longhorn.NodeUpgradeStatePending,
			IMUName: "",
		},
		TestTempNode: {
			State:   longhorn.NodeUpgradeStateInProgress,
			IMUName: TestIMUName,
		},
	}

	createdIMUC, err := lhClient.LonghornV1beta2().InstanceManagerUpgradeControls(TestNamespace).Create(context.TODO(), imuc, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imucIndexer.Add(createdIMUC)
	c.Assert(err, IsNil)

	orphanedIMU := newIMU(TestIMUName, TestTempNode, TestTargetImage, longhorn.InstanceManagerUpgradeStateRelocatingEngines)
	createdIMU, err := lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), orphanedIMU, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(createdIMU)
	c.Assert(err, IsNil)

	lhClient.PrependReactor("update", "instancemanagerupgrades", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "status" {
			return false, nil, nil
		}
		return true, nil, fmt.Errorf("injected abort status update failure")
	})

	err = imuccController.syncIMUC(TestNamespace + "/" + types.InstanceManagerUpgradeControlName)
	c.Assert(err, NotNil)

	updatedIMUC, getErr := lhClient.LonghornV1beta2().InstanceManagerUpgradeControls(TestNamespace).Get(
		context.TODO(), types.InstanceManagerUpgradeControlName, metav1.GetOptions{})
	c.Assert(getErr, IsNil)
	c.Assert(updatedIMUC.Status.Nodes[TestTempNode].State, Equals, longhorn.NodeUpgradeStateInProgress)
	c.Assert(updatedIMUC.Status.Nodes[TestTempNode].IMUName, Equals, TestIMUName)
}
