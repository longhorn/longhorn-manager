package controller

import (
	"context"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	imclient "github.com/longhorn/longhorn-instance-manager/pkg/client"
	imrpc "github.com/longhorn/types/pkg/generated/imrpc"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

// mockEngineClientProxy wraps EngineSimulator and overrides ReplicaRebuildVerify for test control.
type mockEngineClientProxy struct {
	*engineapi.EngineSimulator
	verifyErr    error
	verifyCalled []string // replica names passed to ReplicaRebuildVerify
}

var _ engineapi.EngineClientProxy = (*mockEngineClientProxy)(nil)

func (m *mockEngineClientProxy) Close() {}

func (m *mockEngineClientProxy) ReplicaRebuildVerify(_ *longhorn.Engine, replicaName, _ string) error {
	m.verifyCalled = append(m.verifyCalled, replicaName)
	return m.verifyErr
}

type replicaAddEngineClientProxy struct {
	*engineapi.EngineSimulator
	replicaAddErr   error
	replicaAddCalls int
}

func (m *replicaAddEngineClientProxy) Close() {}

func (m *replicaAddEngineClientProxy) ReplicaAdd(_ engineapi.DataEngineObject, _ string, _ string, _ bool, _ bool, _ *etypes.FileLocalSync, _ int64, _ int64, _ *imrpc.LinkedCloneSource) error {
	m.replicaAddCalls++
	return m.replicaAddErr
}

func TestNeedStatusUpdate(t *testing.T) {
	newMonitor := func() *EngineMonitor {
		logger := logrus.New()
		logger.Out = io.Discard
		return &EngineMonitor{
			logger: logger,
		}
	}

	newEngine := func(nominalSize, volumeHeadSize int64) *longhorn.Engine {
		return &longhorn.Engine{
			Spec: longhorn.EngineSpec{
				InstanceSpec: longhorn.InstanceSpec{
					VolumeSize: nominalSize,
				},
			},
			Status: longhorn.EngineStatus{
				Snapshots: map[string]*longhorn.SnapshotInfo{
					etypes.VolumeHeadName: {
						Size: strconv.FormatInt(volumeHeadSize, 10),
					},
				},
			},
		}
	}

	type testCase struct {
		existingEngine         *longhorn.Engine
		engine                 *longhorn.Engine
		monitor                *EngineMonitor
		expectNeedStatusUpdate bool
		expectRateLimited      bool
	}
	tests := map[string]testCase{}

	tc := testCase{
		existingEngine:         newEngine(TestVolumeSize, TestVolumeSize/2),
		engine:                 newEngine(TestVolumeSize, TestVolumeSize/2),
		monitor:                newMonitor(),
		expectNeedStatusUpdate: false,
		expectRateLimited:      false,
	}
	tests["no field changed"] = tc

	tc = testCase{
		existingEngine:         newEngine(TestVolumeSize, TestVolumeSize/2),
		engine:                 newEngine(TestVolumeSize, TestVolumeSize/2),
		monitor:                newMonitor(),
		expectNeedStatusUpdate: true,
		expectRateLimited:      false,
	}
	tc.existingEngine.Status.CurrentImage = TestEngineImageName
	tc.engine.Status.CurrentImage = "different"
	tests["arbitrary field changed"] = tc

	tc = testCase{
		existingEngine:         newEngine(1*util.GiB, 512*util.MiB),
		engine:                 newEngine(1*util.GiB, 512*util.MiB+1*util.MiB+1),
		monitor:                newMonitor(),
		expectNeedStatusUpdate: true,
		expectRateLimited:      false,
	}
	tests["size update larger than threshold, 1 GiB volume"] = tc

	tc = testCase{
		existingEngine:         newEngine(50*util.GiB, 25*util.GiB),
		engine:                 newEngine(50*util.GiB, 25*util.GiB+50*util.MiB+1),
		monitor:                newMonitor(),
		expectNeedStatusUpdate: true,
		expectRateLimited:      false,
	}
	tests["size update larger than threshold, 50 GiB volume"] = tc

	tc = testCase{
		existingEngine:         newEngine(150*util.GiB, 75*util.GiB),
		engine:                 newEngine(150*util.GiB, 75*util.GiB+100*util.MiB+1),
		monitor:                newMonitor(),
		expectNeedStatusUpdate: true,
		expectRateLimited:      false,
	}
	tests["size update larger than threshold, 150 GiB volume"] = tc

	tc = testCase{
		existingEngine:         newEngine(1*util.GiB, 512*util.MiB),
		engine:                 newEngine(1*util.GiB, 512*util.MiB+1*util.MiB-1),
		monitor:                newMonitor(),
		expectNeedStatusUpdate: true,
		expectRateLimited:      true,
	}
	tests["size update smaller than threshold, 1 GiB volume"] = tc

	tc = testCase{
		existingEngine:         newEngine(50*util.GiB, 25*util.GiB),
		engine:                 newEngine(50*util.GiB, 25*util.GiB+50*util.MiB-1),
		monitor:                newMonitor(),
		expectNeedStatusUpdate: true,
		expectRateLimited:      true,
	}
	tests["size update smaller than threshold, 50 GiB volume"] = tc

	tc = testCase{
		existingEngine:         newEngine(150*util.GiB, 75*util.GiB),
		engine:                 newEngine(150*util.GiB, 75*util.GiB+100*util.MiB-1),
		monitor:                newMonitor(),
		expectNeedStatusUpdate: true,
		expectRateLimited:      true,
	}
	tests["size update smaller than threshold, 150 GiB volume"] = tc

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert := require.New(t)
			needStatusUpdate, rateLimited := tc.monitor.needStatusUpdate(tc.existingEngine, tc.engine)
			assert.Equal(tc.expectNeedStatusUpdate, needStatusUpdate, "needStatusUpdate")
			assert.Equal(tc.expectRateLimited, rateLimited, "rateLimited")
		})
	}
}

func TestShouldAllowEngineImageUpgrade(t *testing.T) {
	shouldAllowEngineImageUpgrade := func(sourceGitCommit, targetGitCommit, targetImage string) bool {
		return sourceGitCommit != targetGitCommit || isRevisionedEngineImage(targetImage)
	}

	tests := map[string]struct {
		sourceGitCommit string
		targetGitCommit string
		targetImage     string
		expected        bool
	}{
		"same commit and regular release tag is blocked": {
			sourceGitCommit: "same-commit",
			targetGitCommit: "same-commit",
			targetImage:     "dp.apps.rancher.io/containers/longhorn-engine:1.10.2",
			expected:        false,
		},
		"same commit and revisioned release tag is allowed": {
			sourceGitCommit: "same-commit",
			targetGitCommit: "same-commit",
			targetImage:     "dp.apps.rancher.io/containers/longhorn-engine:1.10.2-4.12",
			expected:        true,
		},
		"same commit and master head tag is blocked": {
			sourceGitCommit: "same-commit",
			targetGitCommit: "same-commit",
			targetImage:     "longhornio/longhorn-engine:master-head",
			expected:        false,
		},
		"same commit and revisioned release tag with registry port is allowed": {
			sourceGitCommit: "same-commit",
			targetGitCommit: "same-commit",
			targetImage:     "registry:5000/longhorn-engine:1.10.2-1.1",
			expected:        true,
		},
		"different commit and regular release tag is allowed": {
			sourceGitCommit: "old-commit",
			targetGitCommit: "new-commit",
			targetImage:     "dp.apps.rancher.io/containers/longhorn-engine:1.10.3",
			expected:        true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, shouldAllowEngineImageUpgrade(tc.sourceGitCommit, tc.targetGitCommit, tc.targetImage))
		})
	}
}

func TestVerifyCompletedRebuild(t *testing.T) {
	newMonitor := func() *EngineMonitor {
		logger := logrus.New()
		logger.Out = io.Discard
		return &EngineMonitor{logger: logger}
	}

	const (
		replicaRW = "replica-rw"
		replicaWO = "replica-wo"
		addrRW    = "10.0.0.1:10000"
		addrWO    = "10.0.0.2:10000"
		urlRW     = "tcp://" + addrRW
		urlWO     = "tcp://" + addrWO
	)

	type testCase struct {
		rebuildStatus     map[string]*longhorn.RebuildStatus
		replicaModeMap    map[string]longhorn.ReplicaMode
		addressReplicaMap map[string]string
		verifyErr         error
		expectVerified    []string // replica names we expect ReplicaRebuildVerify to be called for
	}

	tests := map[string]testCase{
		// The core fix: replica finished rebuilding while the gRPC connection to the sync agent
		// was interrupted. reloadAndVerify was never called, so the replica is stuck in WO.
		"completed rebuild with WO replica triggers verify": {
			rebuildStatus: map[string]*longhorn.RebuildStatus{
				urlWO: {State: engineapi.ProcessStateComplete, IsRebuilding: false},
			},
			replicaModeMap:    map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			addressReplicaMap: map[string]string{addrWO: replicaWO},
			expectVerified:    []string{replicaWO},
		},
		// Rebuild still in progress — must not call verify yet.
		"in-progress rebuild is skipped": {
			rebuildStatus: map[string]*longhorn.RebuildStatus{
				urlWO: {State: engineapi.ProcessStateInProgress, IsRebuilding: true},
			},
			replicaModeMap:    map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			addressReplicaMap: map[string]string{addrWO: replicaWO},
			expectVerified:    nil,
		},
		// Replica already in RW (normal rebuild path succeeded) — must not call verify again.
		"completed rebuild with RW replica is skipped": {
			rebuildStatus: map[string]*longhorn.RebuildStatus{
				urlRW: {State: engineapi.ProcessStateComplete, IsRebuilding: false},
			},
			replicaModeMap:    map[string]longhorn.ReplicaMode{replicaRW: longhorn.ReplicaModeRW},
			addressReplicaMap: map[string]string{addrRW: replicaRW},
			expectVerified:    nil,
		},
		// Verify fails (e.g. connection still flaky) — must only warn, not return error,
		// so the next monitor poll cycle can retry instead of setting the replica to ERR.
		"verify failure is logged but not returned": {
			rebuildStatus: map[string]*longhorn.RebuildStatus{
				urlWO: {State: engineapi.ProcessStateComplete, IsRebuilding: false},
			},
			replicaModeMap:    map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			addressReplicaMap: map[string]string{addrWO: replicaWO},
			verifyErr:         errors.New("connection refused"),
			expectVerified:    []string{replicaWO},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert := require.New(t)

			proxy := &mockEngineClientProxy{
				EngineSimulator: &engineapi.EngineSimulator{},
				verifyErr:       tc.verifyErr,
			}
			engine := &longhorn.Engine{
				Status: longhorn.EngineStatus{
					ReplicaModeMap: tc.replicaModeMap,
				},
			}

			newMonitor().verifyCompletedRebuild(engine, tc.addressReplicaMap, tc.rebuildStatus, proxy)
			assert.ElementsMatch(tc.expectVerified, proxy.verifyCalled, "ReplicaRebuildVerify call mismatch")
		})
	}
}

func TestHandleRestoreError(t *testing.T) {
	const replicaAddress = "tcp://10.0.0.1:10000"
	const lockConflictMessage = "failed to restore backup data cifs://backup-target?backup=backup-1&volume=volume-1: rpc error: code = Unknown desc = error starting backup restore: error initiating incremental backup restore: failed to acquire lock backupstore/volumes/aa/bb/volume-1/locks/lock-1234.lck when performing backup create/restore, please try again later"
	const terminalMessage = "failed to restore backup data: checksum mismatch"

	tests := map[string]struct {
		message         string
		expectError     string
		expectInBackoff bool
	}{
		// A concurrent deletion/retention lock only blocks the restore temporarily. Recording it
		// would fail every restoring replica and leave the DR volume Faulted.
		"backupstore lock conflict is retried": {
			message:         lockConflictMessage,
			expectError:     "",
			expectInBackoff: true,
		},
		"terminal restore error is recorded": {
			message:         terminalMessage,
			expectError:     replicaAddress + ": " + terminalMessage,
			expectInBackoff: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert := require.New(t)

			logger := logrus.New()
			logger.Out = io.Discard
			engine := &longhorn.Engine{}
			engine.Name = "engine"
			backoff := flowcontrol.NewBackOff(time.Second, time.Minute)
			rsMap := map[string]*longhorn.RestoreStatus{replicaAddress: {}}

			err := imclient.TaskError{ReplicaErrors: []imclient.ReplicaError{
				{Address: replicaAddress, Message: tc.message},
			}}

			assert.NoError(handleRestoreError(logger, engine, rsMap, backoff, err))
			assert.Equal(tc.expectError, rsMap[replicaAddress].Error, "restore status error")
			assert.Equal(tc.expectInBackoff, backoff.IsInBackOffSinceUpdate(engine.Name, time.Now()), "restore backoff")
		})
	}
}

// snapshotListEngineClientProxy wraps EngineSimulator and overrides SnapshotList for test control.
type snapshotListEngineClientProxy struct {
	*engineapi.EngineSimulator
	snapshots map[string]*longhorn.SnapshotInfo
}

func (m *snapshotListEngineClientProxy) Close() {}

func (m *snapshotListEngineClientProxy) SnapshotList(_ engineapi.DataEngineObject) (map[string]*longhorn.SnapshotInfo, error) {
	return m.snapshots, nil
}

func TestGetReplicaReusableDataCutoff(t *testing.T) {
	tests := map[string]struct {
		lastHealthyAt string
		lastFailedAt  string
		expected      string
	}{
		"never healthy replica has no reusable data": {
			lastFailedAt: "2026-01-01T00:10:00Z",
		},
		"unparsable last healthy time is treated as no reusable data": {
			lastHealthyAt: "invalid",
			lastFailedAt:  "2026-01-01T00:10:00Z",
		},
		"failed replica uses the last failed time": {
			lastHealthyAt: "2026-01-01T00:00:00Z",
			lastFailedAt:  "2026-01-01T00:10:00Z",
			expected:      "2026-01-01T00:10:00Z",
		},
		"replica healthy again after the last failure uses the last healthy time": {
			lastHealthyAt: "2026-01-01T00:20:00Z",
			lastFailedAt:  "2026-01-01T00:10:00Z",
			expected:      "2026-01-01T00:20:00Z",
		},
		"replica without failure uses the last healthy time": {
			lastHealthyAt: "2026-01-01T00:00:00Z",
			expected:      "2026-01-01T00:00:00Z",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert := require.New(t)

			r := &longhorn.Replica{
				Spec: longhorn.ReplicaSpec{
					LastHealthyAt: tc.lastHealthyAt,
					LastFailedAt:  tc.lastFailedAt,
				},
			}
			cutoff := getReplicaReusableDataCutoff(r)
			if tc.expected == "" {
				assert.True(cutoff.IsZero())
				return
			}
			expected, err := time.Parse(time.RFC3339, tc.expected)
			assert.NoError(err)
			assert.True(expected.Equal(cutoff), "expected %v, got %v", expected, cutoff)
		})
	}
}

func TestGetReplicaRebuildStatistics(t *testing.T) {
	const (
		oldHashedSnapshot   = "snap-old-hashed"
		oldUnhashedSnapshot = "snap-old-unhashed"
		newSystemSnapshot   = "snap-new-system"
		newUserSnapshot     = "snap-new-user"
		duringRebuildUser   = "snap-during-rebuild-user"
		duringRebuildSystem = "snap-during-rebuild-system"
	)

	mustParse := func(timestamp string) time.Time {
		parsed, err := time.Parse(time.RFC3339, timestamp)
		require.NoError(t, err)
		return parsed
	}

	cutoff := mustParse("2026-01-01T00:10:00Z")
	rebuildStartedAt := mustParse("2026-01-01T00:30:00Z")

	snapshots := map[string]*longhorn.SnapshotInfo{
		etypes.VolumeHeadName: {Name: etypes.VolumeHeadName, Created: "2026-01-01T00:31:00Z"},
		oldHashedSnapshot:     {Name: oldHashedSnapshot, Created: "2026-01-01T00:01:00Z", UserCreated: true},
		oldUnhashedSnapshot:   {Name: oldUnhashedSnapshot, Created: "2026-01-01T00:10:00Z", UserCreated: true},
		newSystemSnapshot:     {Name: newSystemSnapshot, Created: "2026-01-01T00:20:00Z"},
		newUserSnapshot:       {Name: newUserSnapshot, Created: "2026-01-01T00:25:00Z", UserCreated: true},
		duringRebuildUser:     {Name: duringRebuildUser, Created: "2026-01-01T00:40:00Z", UserCreated: true},
		duringRebuildSystem:   {Name: duringRebuildSystem, Created: "2026-01-01T00:30:01Z"},
		"nil-snapshot":        nil,
	}
	snapshotCRs := map[string]*longhorn.Snapshot{
		oldHashedSnapshot:   {Status: longhorn.SnapshotStatus{Checksum: "checksum-old"}},
		oldUnhashedSnapshot: {},
		newSystemSnapshot:   {Status: longhorn.SnapshotStatus{Checksum: "checksum-new"}},
	}

	tests := map[string]struct {
		reusableDataCutoff time.Time
		fastReplicaRebuild bool
		expected           longhorn.ReplicaRebuildStatistics
	}{
		"replica without reusable data is fully rebuilt": {
			fastReplicaRebuild: true,
			expected:           longhorn.ReplicaRebuildStatistics{FullRebuildSnapshotCount: 5},
		},
		"reused replica is delta rebuilt when fast replica rebuild is disabled": {
			reusableDataCutoff: cutoff,
			expected: longhorn.ReplicaRebuildStatistics{
				FullRebuildSnapshotCount:  3,
				DeltaRebuildSnapshotCount: 2,
			},
		},
		"reused replica is fast rebuilt for the hashed snapshots when fast replica rebuild is enabled": {
			reusableDataCutoff: cutoff,
			fastReplicaRebuild: true,
			expected: longhorn.ReplicaRebuildStatistics{
				FullRebuildSnapshotCount:  3,
				DeltaRebuildSnapshotCount: 1,
				FastRebuildSnapshotCount:  1,
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			statistics := getReplicaRebuildStatistics(snapshots, snapshotCRs, tc.reusableDataCutoff, rebuildStartedAt, tc.fastReplicaRebuild)
			require.Equal(t, tc.expected, *statistics)
		})
	}
}

func TestGetReplicaRebuiltConditionReasonAndMessage(t *testing.T) {
	tests := map[string]struct {
		statistics      longhorn.ReplicaRebuildStatistics
		expectedReason  string
		expectedMessage string
	}{
		"no snapshot": {
			expectedReason:  longhorn.ReplicaConditionReasonRebuiltFull,
			expectedMessage: "Rebuilt 0 snapshot(s): 0 by full rebuild, 0 by delta rebuild, 0 by fast rebuild",
		},
		"full rebuild only": {
			statistics:      longhorn.ReplicaRebuildStatistics{FullRebuildSnapshotCount: 3},
			expectedReason:  longhorn.ReplicaConditionReasonRebuiltFull,
			expectedMessage: "Rebuilt 3 snapshot(s): 3 by full rebuild, 0 by delta rebuild, 0 by fast rebuild",
		},
		"delta rebuild": {
			statistics:      longhorn.ReplicaRebuildStatistics{FullRebuildSnapshotCount: 1, DeltaRebuildSnapshotCount: 2},
			expectedReason:  longhorn.ReplicaConditionReasonRebuiltDelta,
			expectedMessage: "Rebuilt 3 snapshot(s): 1 by full rebuild, 2 by delta rebuild, 0 by fast rebuild",
		},
		"fast rebuild": {
			statistics:      longhorn.ReplicaRebuildStatistics{FullRebuildSnapshotCount: 1, DeltaRebuildSnapshotCount: 2, FastRebuildSnapshotCount: 3},
			expectedReason:  longhorn.ReplicaConditionReasonRebuiltFast,
			expectedMessage: "Rebuilt 6 snapshot(s): 1 by full rebuild, 2 by delta rebuild, 3 by fast rebuild",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			reason, message := getReplicaRebuiltConditionReasonAndMessage(&tc.statistics)
			require.Equal(t, tc.expectedReason, reason)
			require.Equal(t, tc.expectedMessage, message)
		})
	}
}

func TestIsSnapshotSyncRebuild(t *testing.T) {
	tests := map[string]struct {
		dataEngine             longhorn.DataEngineType
		requestedBackupRestore string
		expected               bool
	}{
		"v1 volume":         {dataEngine: longhorn.DataEngineTypeV1, expected: true},
		"v1 restore volume": {dataEngine: longhorn.DataEngineTypeV1, requestedBackupRestore: "backup-1", expected: false},
		"v2 volume":         {dataEngine: longhorn.DataEngineTypeV2, expected: true},
		"v2 restore volume": {dataEngine: longhorn.DataEngineTypeV2, requestedBackupRestore: "backup-1", expected: true},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			e := &longhorn.Engine{
				Spec: longhorn.EngineSpec{
					InstanceSpec:           longhorn.InstanceSpec{DataEngine: tc.dataEngine},
					RequestedBackupRestore: tc.requestedBackupRestore,
				},
			}
			require.Equal(t, tc.expected, isSnapshotSyncRebuild(e))
		})
	}
}

func TestUpdateReplicaRebuiltStatus(t *testing.T) {
	assert := require.New(t)

	const (
		volumeName  = "test-volume"
		replicaName = "test-volume-r-00000000"
		snapshot1   = "snap-1"
		snapshot2   = "snap-2"
	)

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	replica := &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      replicaName,
			Namespace: TestNamespace,
		},
		Spec: longhorn.ReplicaSpec{
			InstanceSpec: longhorn.InstanceSpec{
				VolumeName: volumeName,
			},
		},
	}
	replica.Status.Conditions = types.SetCondition(replica.Status.Conditions,
		longhorn.ReplicaConditionTypeRebuilt, longhorn.ConditionStatusFalse, "", "")
	replica, err := lhClient.LonghornV1beta2().Replicas(TestNamespace).Create(context.TODO(), replica, metav1.CreateOptions{})
	assert.NoError(err)
	replicaIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
	assert.NoError(replicaIndexer.Add(replica))

	snapshotIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Snapshots().Informer().GetIndexer()
	snapshotCR := newSnapshot(snapshot1)
	snapshotCR.Labels = types.GetVolumeLabels(volumeName)
	snapshotCR.Status.Checksum = "checksum-1"
	assert.NoError(snapshotIndexer.Add(snapshotCR))

	logger := logrus.New()
	logger.Out = io.Discard
	ec := &EngineController{ds: ds}
	rc := &rebuildContext{
		log: logrus.NewEntry(logger),
		currentEngine: &longhorn.Engine{
			Spec: longhorn.EngineSpec{
				InstanceSpec: longhorn.InstanceSpec{
					VolumeName: volumeName,
				},
			},
		},
		cleanupProxy: &snapshotListEngineClientProxy{
			EngineSimulator: &engineapi.EngineSimulator{},
			snapshots: map[string]*longhorn.SnapshotInfo{
				etypes.VolumeHeadName: {Name: etypes.VolumeHeadName},
				snapshot1:             {Name: snapshot1, Created: "2026-01-01T00:00:00Z", UserCreated: true},
				snapshot2:             {Name: snapshot2, Created: "2026-01-01T00:20:00Z"},
			},
		},
		replicaName:        replicaName,
		fastReplicaRebuild: true,
		reusableDataCutoff: time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC),
		rebuildStartedAt:   time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC),
	}

	ec.updateReplicaRebuiltStatus(rc)

	updated, err := lhClient.LonghornV1beta2().Replicas(TestNamespace).Get(context.TODO(), replicaName, metav1.GetOptions{})
	assert.NoError(err)
	assert.NotNil(updated.Status.LastRebuildStatistics)
	assert.NotEmpty(updated.Status.LastRebuildStatistics.RebuiltAt)
	assert.Equal(1, updated.Status.LastRebuildStatistics.FullRebuildSnapshotCount)
	assert.Equal(0, updated.Status.LastRebuildStatistics.DeltaRebuildSnapshotCount)
	assert.Equal(1, updated.Status.LastRebuildStatistics.FastRebuildSnapshotCount)

	condition := types.GetCondition(updated.Status.Conditions, longhorn.ReplicaConditionTypeRebuilt)
	assert.Equal(longhorn.ConditionStatusTrue, condition.Status)
	assert.Equal(longhorn.ReplicaConditionReasonRebuiltFast, condition.Reason)
	assert.Equal("Rebuilt 2 snapshot(s): 1 by full rebuild, 0 by delta rebuild, 1 by fast rebuild", condition.Message)
}

func TestUpdateReplicaRebuiltStatusRetriesWithUncachedReplica(t *testing.T) {
	assert := require.New(t)

	const (
		volumeName  = "test-volume"
		replicaName = "test-volume-r-00000000"
		snapshot1   = "snap-1"
	)

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	staleReplica := &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name:            replicaName,
			Namespace:       TestNamespace,
			ResourceVersion: "1",
		},
		Spec: longhorn.ReplicaSpec{
			InstanceSpec: longhorn.InstanceSpec{
				VolumeName: volumeName,
			},
		},
	}
	staleReplica.Status.Conditions = types.SetCondition(staleReplica.Status.Conditions,
		longhorn.ReplicaConditionTypeRebuilt, longhorn.ConditionStatusFalse, "", "")

	currentReplica := staleReplica.DeepCopy()
	currentReplica.ResourceVersion = "2"

	replicaIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
	assert.NoError(replicaIndexer.Add(staleReplica))

	lhClient.PrependReactor("get", "replicas", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.(k8stesting.GetAction).GetName() != replicaName {
			return false, nil, nil
		}
		return true, currentReplica.DeepCopy(), nil
	})
	lhClient.PrependReactor("update", "replicas", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "status" {
			return false, nil, nil
		}

		updated := action.(k8stesting.UpdateAction).GetObject().(*longhorn.Replica)
		if updated.ResourceVersion != currentReplica.ResourceVersion {
			return true, nil, apierrors.NewConflict(longhorn.Resource("replicas"), updated.Name, errors.New("replica status changed"))
		}

		currentReplica = updated.DeepCopy()
		currentReplica.ResourceVersion = "3"
		return true, currentReplica.DeepCopy(), nil
	})

	snapshotIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Snapshots().Informer().GetIndexer()
	snapshotCR := newSnapshot(snapshot1)
	snapshotCR.Labels = types.GetVolumeLabels(volumeName)
	snapshotCR.Status.Checksum = "checksum-1"
	assert.NoError(snapshotIndexer.Add(snapshotCR))

	logger := logrus.New()
	logger.Out = io.Discard
	ec := &EngineController{ds: ds}
	rc := &rebuildContext{
		log: logrus.NewEntry(logger),
		currentEngine: &longhorn.Engine{
			Spec: longhorn.EngineSpec{
				InstanceSpec: longhorn.InstanceSpec{
					VolumeName: volumeName,
				},
			},
		},
		cleanupProxy: &snapshotListEngineClientProxy{
			EngineSimulator: &engineapi.EngineSimulator{},
			snapshots: map[string]*longhorn.SnapshotInfo{
				etypes.VolumeHeadName: {Name: etypes.VolumeHeadName},
				snapshot1:             {Name: snapshot1, Created: "2026-01-01T00:00:00Z", UserCreated: true},
			},
		},
		replicaName:        replicaName,
		fastReplicaRebuild: true,
		reusableDataCutoff: time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC),
		rebuildStartedAt:   time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC),
	}

	ec.updateReplicaRebuiltStatus(rc)

	assert.NotNil(currentReplica.Status.LastRebuildStatistics)
	assert.NotEmpty(currentReplica.Status.LastRebuildStatistics.RebuiltAt)
	assert.Equal(0, currentReplica.Status.LastRebuildStatistics.FullRebuildSnapshotCount)
	assert.Equal(0, currentReplica.Status.LastRebuildStatistics.DeltaRebuildSnapshotCount)
	assert.Equal(1, currentReplica.Status.LastRebuildStatistics.FastRebuildSnapshotCount)

	condition := types.GetCondition(currentReplica.Status.Conditions, longhorn.ReplicaConditionTypeRebuilt)
	assert.Equal(longhorn.ConditionStatusTrue, condition.Status)
	assert.Equal(longhorn.ReplicaConditionReasonRebuiltFast, condition.Reason)
	assert.Equal("Rebuilt 1 snapshot(s): 0 by full rebuild, 0 by delta rebuild, 1 by fast rebuild", condition.Message)
}

func TestRunRebuildRejectedStartRestoresRebuiltCondition(t *testing.T) {
	assert := require.New(t)

	const (
		volumeName  = "test-volume"
		engineName  = "test-engine"
		replicaName = "test-volume-r-00000000"
		replicaAddr = "10.0.0.1:10000"
	)

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	replica := &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      replicaName,
			Namespace: TestNamespace,
		},
		Spec: longhorn.ReplicaSpec{
			InstanceSpec: longhorn.InstanceSpec{
				VolumeName: volumeName,
			},
		},
	}
	replica.Status.Conditions = types.SetCondition(replica.Status.Conditions,
		longhorn.ReplicaConditionTypeRebuilt, longhorn.ConditionStatusTrue, longhorn.ReplicaConditionReasonRebuiltFull, "already rebuilt")
	replica, err := lhClient.LonghornV1beta2().Replicas(TestNamespace).Create(context.TODO(), replica, metav1.CreateOptions{})
	assert.NoError(err)

	logger := logrus.New()
	logger.Out = io.Discard
	rebuildProxy := &replicaAddEngineClientProxy{
		EngineSimulator: &engineapi.EngineSimulator{},
		replicaAddErr:   errors.New("restore is in progress"),
	}
	ec := &EngineController{
		ds:            ds,
		eventRecorder: record.NewFakeRecorder(10),
	}
	rc := &rebuildContext{
		log: logrus.NewEntry(logger),
		engine: &longhorn.Engine{
			ObjectMeta: metav1.ObjectMeta{Name: engineName, Namespace: TestNamespace},
			Spec: longhorn.EngineSpec{
				InstanceSpec: longhorn.InstanceSpec{
					DataEngine: longhorn.DataEngineTypeV2,
					VolumeName: volumeName,
				},
			},
		},
		currentEngine: &longhorn.Engine{
			ObjectMeta: metav1.ObjectMeta{Name: engineName, Namespace: TestNamespace},
			Spec: longhorn.EngineSpec{
				InstanceSpec: longhorn.InstanceSpec{
					DataEngine: longhorn.DataEngineTypeV2,
					VolumeName: volumeName,
				},
			},
		},
		currentEF: &longhorn.EngineFrontend{
			ObjectMeta: metav1.ObjectMeta{Name: "test-ef", Namespace: TestNamespace},
		},
		rebuildProxy:                rebuildProxy,
		cleanupProxy:                rebuildProxy,
		rebuildObj:                  &longhorn.EngineFrontend{},
		replica:                     replica,
		replicaName:                 replicaName,
		replicaURL:                  engineapi.GetBackendReplicaURL(replicaAddr),
		addr:                        replicaAddr,
		previousRebuiltCondition:    types.GetCondition(replica.Status.Conditions, longhorn.ReplicaConditionTypeRebuilt),
		hasPreviousRebuiltCondition: true,
	}

	ec.runRebuild(rc)

	updated, err := lhClient.LonghornV1beta2().Replicas(TestNamespace).Get(context.TODO(), replicaName, metav1.GetOptions{})
	assert.NoError(err)

	condition := types.GetCondition(updated.Status.Conditions, longhorn.ReplicaConditionTypeRebuilt)
	assert.Equal(longhorn.ConditionStatusTrue, condition.Status)
	assert.Equal(longhorn.ReplicaConditionReasonRebuiltFull, condition.Reason)
	assert.Equal("already rebuilt", condition.Message)
	assert.Nil(updated.Status.LastRebuildStatistics)
	assert.Equal(1, rebuildProxy.replicaAddCalls)
}

func TestRestoreReplicaRebuiltConditionRetriesWithUncachedReplica(t *testing.T) {
	testCases := map[string]struct {
		initialConditions         []longhorn.Condition
		previousCondition         longhorn.Condition
		hasPreviousCondition      bool
		expectedConditionsChecker func(*require.Assertions, []longhorn.Condition)
	}{
		"restore previous condition": {
			initialConditions: types.SetCondition(nil,
				longhorn.ReplicaConditionTypeRebuilt, longhorn.ConditionStatusFalse, "", ""),
			previousCondition:    longhorn.Condition{Type: longhorn.ReplicaConditionTypeRebuilt, Status: longhorn.ConditionStatusTrue, Reason: longhorn.ReplicaConditionReasonRebuiltFull, Message: "already rebuilt"},
			hasPreviousCondition: true,
			expectedConditionsChecker: func(assert *require.Assertions, conditions []longhorn.Condition) {
				condition := types.GetCondition(conditions, longhorn.ReplicaConditionTypeRebuilt)
				assert.Equal(longhorn.ConditionStatusTrue, condition.Status)
				assert.Equal(longhorn.ReplicaConditionReasonRebuiltFull, condition.Reason)
				assert.Equal("already rebuilt", condition.Message)
			},
		},
		"remove rebuilt condition": {
			initialConditions: types.SetCondition(nil,
				longhorn.ReplicaConditionTypeRebuilt, longhorn.ConditionStatusFalse, "", ""),
			expectedConditionsChecker: func(assert *require.Assertions, conditions []longhorn.Condition) {
				_, exists := getReplicaConditionWithExistence(conditions, longhorn.ReplicaConditionTypeRebuilt)
				assert.False(exists)
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			assert := require.New(t)

			const (
				volumeName  = "test-volume"
				replicaName = "test-volume-r-00000000"
			)

			kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
			lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
			extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
			informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)
			ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

			staleReplica := &longhorn.Replica{
				ObjectMeta: metav1.ObjectMeta{
					Name:            replicaName,
					Namespace:       TestNamespace,
					ResourceVersion: "1",
				},
				Spec: longhorn.ReplicaSpec{
					InstanceSpec: longhorn.InstanceSpec{
						VolumeName: volumeName,
					},
				},
			}
			staleReplica.Status.Conditions = append([]longhorn.Condition(nil), tc.initialConditions...)

			currentReplica := staleReplica.DeepCopy()
			currentReplica.ResourceVersion = "2"

			replicaIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
			assert.NoError(replicaIndexer.Add(staleReplica))

			lhClient.PrependReactor("get", "replicas", func(action k8stesting.Action) (bool, runtime.Object, error) {
				if action.(k8stesting.GetAction).GetName() != replicaName {
					return false, nil, nil
				}
				return true, currentReplica.DeepCopy(), nil
			})
			lhClient.PrependReactor("update", "replicas", func(action k8stesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != "status" {
					return false, nil, nil
				}

				updated := action.(k8stesting.UpdateAction).GetObject().(*longhorn.Replica)
				if updated.ResourceVersion != currentReplica.ResourceVersion {
					return true, nil, apierrors.NewConflict(longhorn.Resource("replicas"), updated.Name, errors.New("replica status changed"))
				}

				currentReplica = updated.DeepCopy()
				currentReplica.ResourceVersion = "3"
				return true, currentReplica.DeepCopy(), nil
			})

			ec := &EngineController{ds: ds}
			rc := &rebuildContext{
				engine: &longhorn.Engine{
					Spec: longhorn.EngineSpec{
						InstanceSpec: longhorn.InstanceSpec{
							DataEngine: longhorn.DataEngineTypeV2,
						},
					},
				},
				replica:                     staleReplica.DeepCopy(),
				replicaName:                 replicaName,
				previousRebuiltCondition:    tc.previousCondition,
				hasPreviousRebuiltCondition: tc.hasPreviousCondition,
			}

			err := ec.restoreReplicaRebuiltCondition(rc)
			assert.NoError(err)
			assert.Equal("3", rc.replica.ResourceVersion)
			tc.expectedConditionsChecker(assert, currentReplica.Status.Conditions)
		})
	}
}
