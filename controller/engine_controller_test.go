package controller

import (
	"context"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	imclient "github.com/longhorn/longhorn-instance-manager/pkg/client"

	"github.com/longhorn/longhorn-manager/constant"
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
	removeCalled []string // replica names passed to ReplicaRemove
}

var _ engineapi.EngineClientProxy = (*mockEngineClientProxy)(nil)

func (m *mockEngineClientProxy) Close() {}

func (m *mockEngineClientProxy) ReplicaRebuildVerify(_ *longhorn.Engine, replicaName, _ string) error {
	m.verifyCalled = append(m.verifyCalled, replicaName)
	return m.verifyErr
}

func (m *mockEngineClientProxy) ReplicaRemove(_ *longhorn.Engine, _, replicaName string) error {
	m.removeCalled = append(m.removeCalled, replicaName)
	return nil
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
		existingEngine:         newEngine(TestVolumeSize, TestVolumeSize/2),
		engine:                 newEngine(TestVolumeSize, TestVolumeSize/2),
		monitor:                newMonitor(),
		expectNeedStatusUpdate: true,
		expectRateLimited:      false,
	}
	tc.engine.Status.EngineRestoreError = "restore engine: submit read blob: Input/output error"
	tests["engine restore error changed"] = tc

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

func TestIsBackupRestoreFailed(t *testing.T) {
	tests := map[string]struct {
		status   longhorn.EngineStatus
		expected bool
	}{
		"no restore status": {
			status:   longhorn.EngineStatus{},
			expected: false,
		},
		"replica restoring without error": {
			status: longhorn.EngineStatus{
				RestoreStatus: map[string]*longhorn.RestoreStatus{
					"tcp://10.0.0.1:10000": {IsRestoring: true},
				},
			},
			expected: false,
		},
		"replica error while not restoring": {
			status: longhorn.EngineStatus{
				RestoreStatus: map[string]*longhorn.RestoreStatus{
					"tcp://10.0.0.1:10000": {Error: "replica restore failed"},
				},
			},
			expected: true,
		},
		"restoring replica with error is not yet failed": {
			// A replica that is still restoring is not counted as failed even if it
			// also reports an error.
			status: longhorn.EngineStatus{
				RestoreStatus: map[string]*longhorn.RestoreStatus{
					"tcp://10.0.0.1:10000": {IsRestoring: true, Error: "replica restore failed"},
				},
			},
			expected: false,
		},
		"errored replica while another is still restoring": {
			// Regardless of map iteration order, a still-restoring replica means the
			// attempt is not failed yet.
			status: longhorn.EngineStatus{
				RestoreStatus: map[string]*longhorn.RestoreStatus{
					"tcp://10.0.0.1:10000": {Error: "replica restore failed"},
					"tcp://10.0.0.2:10000": {IsRestoring: true},
					"tcp://10.0.0.3:10000": {},
				},
			},
			expected: false,
		},
		"errored replica after all replicas stopped restoring": {
			status: longhorn.EngineStatus{
				RestoreStatus: map[string]*longhorn.RestoreStatus{
					"tcp://10.0.0.1:10000": {},
					"tcp://10.0.0.2:10000": {Error: "replica restore failed"},
				},
			},
			expected: true,
		},
		"engine-level error only": {
			status: longhorn.EngineStatus{
				EngineRestoreError: "restore engine: submit read blob: Input/output error",
			},
			expected: true,
		},
		"engine-level error with stale restoring replica": {
			// An error left over from a previous attempt must not mark the
			// restore as failed while a replica is still restoring the new
			// attempt.
			status: longhorn.EngineStatus{
				EngineRestoreError: "restore engine: submit read blob: Input/output error",
				RestoreStatus: map[string]*longhorn.RestoreStatus{
					"tcp://10.0.0.1:10000": {IsRestoring: true},
				},
			},
			expected: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			require.New(t).Equal(tc.expected, isBackupRestoreFailed(&tc.status))
		})
	}
}

func TestShouldAbandonRestore(t *testing.T) {
	now := time.Now()
	exhausted := now.Add(-restoreErrorRetryBudget)
	withinBudget := now.Add(-restoreErrorRetryBudget / 2)

	tests := map[string]struct {
		isDRVolume      bool
		firstErrorAt    time.Time
		expectedAbandon bool
		expectedStart   time.Time
	}{
		"first engine-error sighting starts the timer without abandoning": {
			expectedAbandon: false,
			expectedStart:   now,
		},
		"engine error within the budget keeps retrying": {
			firstErrorAt:    withinBudget,
			expectedAbandon: false,
			expectedStart:   withinBudget,
		},
		"engine error past the budget abandons before a new attempt": {
			firstErrorAt:    exhausted,
			expectedAbandon: true,
			expectedStart:   exhausted,
		},
		"DR volume never abandons": {
			isDRVolume:      true,
			firstErrorAt:    exhausted,
			expectedAbandon: false,
			expectedStart:   exhausted,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			abandon, firstErrorAt := shouldAbandonRestore(tc.isDRVolume, tc.firstErrorAt, now)
			require.New(t).Equal(tc.expectedAbandon, abandon)
			require.New(t).Equal(tc.expectedStart, firstErrorAt)
		})
	}
}

func TestFillRestoreStatusErrors(t *testing.T) {
	const errMsg = "failed to restore backup: NoSuchKey"

	tests := map[string]struct {
		rsMap             map[string]*longhorn.RestoreStatus
		replicaAddressMap map[string]string
		expected          map[string]*longhorn.RestoreStatus
	}{
		"empty rsMap creates entries from replica addresses": {
			rsMap: map[string]*longhorn.RestoreStatus{},
			replicaAddressMap: map[string]string{
				"replica-1": "10.0.0.1:10000",
				"replica-2": "10.0.0.2:10000",
			},
			expected: map[string]*longhorn.RestoreStatus{
				"tcp://10.0.0.1:10000": {Error: errMsg},
				"tcp://10.0.0.2:10000": {Error: errMsg},
			},
		},
		"nil rsMap creates entries from replica addresses": {
			rsMap: nil,
			replicaAddressMap: map[string]string{
				"replica-1": "10.0.0.1:10000",
			},
			expected: map[string]*longhorn.RestoreStatus{
				"tcp://10.0.0.1:10000": {Error: errMsg},
			},
		},
		"existing errors are preserved, gaps are filled, restoring is cleared": {
			rsMap: map[string]*longhorn.RestoreStatus{
				"tcp://10.0.0.1:10000": {Error: "replica-specific failure"},
				"tcp://10.0.0.2:10000": {IsRestoring: true},
			},
			expected: map[string]*longhorn.RestoreStatus{
				"tcp://10.0.0.1:10000": {Error: "replica-specific failure"},
				"tcp://10.0.0.2:10000": {Error: errMsg},
			},
		},
		"partial rsMap gains entries for unreported replicas": {
			rsMap: map[string]*longhorn.RestoreStatus{
				"tcp://10.0.0.1:10000": {Error: "replica-specific failure"},
			},
			replicaAddressMap: map[string]string{
				"replica-1": "10.0.0.1:10000",
				"replica-2": "10.0.0.2:10000",
			},
			expected: map[string]*longhorn.RestoreStatus{
				"tcp://10.0.0.1:10000": {Error: "replica-specific failure"},
				"tcp://10.0.0.2:10000": {Error: errMsg},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			require.New(t).Equal(tc.expected, fillRestoreStatusErrors(tc.rsMap, tc.replicaAddressMap, errMsg))
		})
	}
}

func TestAreV2ReplicasReadyForRestore(t *testing.T) {
	tests := map[string]struct {
		dataEngine longhorn.DataEngineType
		modes      map[string]longhorn.ReplicaMode
		expected   bool
	}{
		"v2 all replicas RW": {
			dataEngine: longhorn.DataEngineTypeV2,
			modes: map[string]longhorn.ReplicaMode{
				"r1": longhorn.ReplicaModeRW,
				"r2": longhorn.ReplicaModeRW,
			},
			expected: true,
		},
		"v2 rebuilding replica": {
			dataEngine: longhorn.DataEngineTypeV2,
			modes: map[string]longhorn.ReplicaMode{
				"r1": longhorn.ReplicaModeRW,
				"r2": longhorn.ReplicaModeWO,
			},
			expected: false,
		},
		"v2 error replica": {
			dataEngine: longhorn.DataEngineTypeV2,
			modes: map[string]longhorn.ReplicaMode{
				"r1": longhorn.ReplicaModeRW,
				"r2": longhorn.ReplicaModeERR,
			},
			expected: false,
		},
		"v2 empty replica mode map": {
			// The engine itself rejects a restore when it has no replicas; this
			// check only waits for replicas that are still rebuilding.
			dataEngine: longhorn.DataEngineTypeV2,
			modes:      map[string]longhorn.ReplicaMode{},
			expected:   true,
		},
		"v1 rebuilding replica is not gated": {
			// A v1 restore volume keeps a restore-path replica WO until its first
			// completed restore. The restore must be allowed to start.
			dataEngine: longhorn.DataEngineTypeV1,
			modes: map[string]longhorn.ReplicaMode{
				"r1": longhorn.ReplicaModeRW,
				"r2": longhorn.ReplicaModeWO,
			},
			expected: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			engine := &longhorn.Engine{
				Spec:   longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{DataEngine: tc.dataEngine}},
				Status: longhorn.EngineStatus{ReplicaModeMap: tc.modes},
			}
			require.New(t).Equal(tc.expected, areV2ReplicasReadyForRestore(logrus.StandardLogger(), engine))
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

// When the operations before `rc.rebuildProxy.ReplicaAdd` take longer than the rebuild timeout, a
// second rebuild goroutine can be started for the same replica. The loser of that race gets
// "replica already exists at address ..." back from the engine, which means the replica is being
// rebuilt by the winning goroutine rather than that anything is wrong with it.
func TestIsReplicaAddressExistError(t *testing.T) {
	tests := map[string]struct {
		err      error
		expected bool
	}{
		"nil error": {
			err:      nil,
			expected: false,
		},
		"unrelated error": {
			err:      errors.New("connection refused"),
			expected: false,
		},
		// The literal message longhorn-engine builds in pkg/controller/control.go. Hard-coded on
		// purpose: the matcher is only useful while it still matches what the engine emits.
		"error message from the engine": {
			err:      errors.New("replica already exists at address tcp://10.0.0.1:10000"),
			expected: true,
		},
		"wrapped error from the proxy": {
			err: errors.Wrap(errors.New("rpc error: code = Unknown desc = replica already exists at address tcp://10.42.0.15:10000"),
				"failed to add replica tcp://10.42.0.15:10000 for volume"),
			expected: true,
		},
		// The replica is genuinely gone, so the rebuild failure has to be cleaned up as usual.
		"replica missing at address": {
			err:      errors.New("replica does not exist at address tcp://10.0.0.1:10000"),
			expected: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, isReplicaAddressExistError(tc.err))
		})
	}
}
func TestHandleRebuildFailure(t *testing.T) {
	const (
		addr       = "10.0.0.1:10000"
		replicaURL = "tcp://" + addr
	)

	type testCase struct {
		rebuildErr          error
		engineOwner         string   // Status.OwnerID of the engine when the failure is handled
		expectRemoved       []string // replica names we expect ReplicaRemove to be called for
		expectReplicaFailed bool
	}

	tests := map[string]testCase{
		// The losing rebuild goroutine must leave the replica alone. Removing it from the engine or
		// marking it failed would tear down the rebuild the winning goroutine is running.
		"replica already exists at address is left alone": {
			rebuildErr:          errors.New("proxyServer=10.0.0.1:8501 destination=10.0.0.1:10000: failed to add replica tcp://10.0.0.1:10000 for volume: rpc error: code = Unknown desc = replica already exists at address tcp://10.0.0.1:10000"),
			engineOwner:         TestNode1,
			expectRemoved:       nil,
			expectReplicaFailed: false,
		},
		// Every other rebuild failure still has to clean up the half-added replica.
		"other rebuild failure is cleaned up": {
			rebuildErr:          errors.New("connection refused"),
			engineOwner:         TestNode1,
			expectRemoved:       []string{TestReplicaName},
			expectReplicaFailed: true,
		},
		// The engine was taken over by another node while this rebuild goroutine was running. The new
		// owner starts its own rebuild for the same replica, so a failure of the stale goroutine must not
		// remove that replica from the engine or mark it failed.
		"rebuild failure after the engine changed owner is left to the new owner": {
			rebuildErr:          errors.New("connection refused"),
			engineOwner:         TestNode2,
			expectRemoved:       nil,
			expectReplicaFailed: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert := require.New(t)
			datastore.SkipListerCheck = true

			kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
			lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
			extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
			informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)

			ec := &EngineController{
				baseController: newBaseController("longhorn-engine", logrus.New()),
				controllerID:   TestNode1,
				ds:             datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories),
				eventRecorder:  record.NewFakeRecorder(100),
				backoff:        flowcontrol.NewBackOff(time.Second*10, time.Minute*5),
			}

			engine := &longhorn.Engine{
				ObjectMeta: metav1.ObjectMeta{Name: TestEngineName, Namespace: TestNamespace},
				Spec: longhorn.EngineSpec{
					InstanceSpec: longhorn.InstanceSpec{VolumeName: TestVolumeName},
				},
				Status: longhorn.EngineStatus{
					InstanceStatus: longhorn.InstanceStatus{OwnerID: tc.engineOwner},
				},
			}
			eIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Engines().Informer().GetIndexer()
			assert.NoError(eIndexer.Add(engine))
			replica := &longhorn.Replica{
				ObjectMeta: metav1.ObjectMeta{Name: TestReplicaName, Namespace: TestNamespace},
				Spec: longhorn.ReplicaSpec{
					InstanceSpec: longhorn.InstanceSpec{
						VolumeName:  TestVolumeName,
						NodeID:      TestNode1,
						DesireState: longhorn.InstanceStateRunning,
					},
				},
			}
			_, err := lhClient.LonghornV1beta2().Replicas(TestNamespace).Create(context.TODO(), replica, metav1.CreateOptions{})
			assert.NoError(err)

			logger := logrus.New()
			logger.Out = io.Discard
			proxy := &mockEngineClientProxy{EngineSimulator: &engineapi.EngineSimulator{}}

			ec.handleRebuildFailure(logrus.NewEntry(logger), engine, nil, TestReplicaName, addr, replicaURL, replica, proxy, tc.rebuildErr)

			assert.ElementsMatch(tc.expectRemoved, proxy.removeCalled, "ReplicaRemove call mismatch")

			updated, err := lhClient.LonghornV1beta2().Replicas(TestNamespace).Get(context.TODO(), TestReplicaName, metav1.GetOptions{})
			assert.NoError(err)
			if tc.expectReplicaFailed {
				assert.Equal(longhorn.InstanceStateStopped, updated.Spec.DesireState)
				assert.NotEmpty(updated.Spec.FailedAt)
			} else {
				assert.Equal(longhorn.InstanceStateRunning, updated.Spec.DesireState)
				assert.Empty(updated.Spec.FailedAt)
			}
		})
	}
}

func TestSyncSnapshotCRs(t *testing.T) {
	const snapName = "snap-1"

	type testCase struct {
		active          bool
		snapshots       map[string]*longhorn.SnapshotInfo
		expectCRDeleted bool
	}

	tests := map[string]testCase{
		// A v2 switchover target engine is inactive and has not fetched snapshots yet.
		// It must not delete the CRs, otherwise the snapshot controller removes the snapshots.
		"inactive engine keeps snapshot CRs": {
			active:          false,
			snapshots:       map[string]*longhorn.SnapshotInfo{},
			expectCRDeleted: false,
		},
		"active engine without fetched snapshots keeps snapshot CRs": {
			active:          true,
			snapshots:       nil,
			expectCRDeleted: false,
		},
		"active engine deletes CR of snapshot missing from engine": {
			active:          true,
			snapshots:       map[string]*longhorn.SnapshotInfo{etypes.VolumeHeadName: {Name: etypes.VolumeHeadName}},
			expectCRDeleted: true,
		},
		"active engine keeps CR of snapshot reported by engine": {
			active: true,
			snapshots: map[string]*longhorn.SnapshotInfo{
				snapName:              {Name: snapName},
				etypes.VolumeHeadName: {Name: etypes.VolumeHeadName},
			},
			expectCRDeleted: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert := require.New(t)
			datastore.SkipListerCheck = true

			kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
			lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
			extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
			informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)

			logger := logrus.New()
			logger.Out = io.Discard
			ec := &EngineController{
				baseController: newBaseController("longhorn-engine", logger),
				ds:             datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories),
			}

			volume := &longhorn.Volume{ObjectMeta: metav1.ObjectMeta{Name: TestVolumeName, Namespace: TestNamespace}}
			vIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
			assert.NoError(vIndexer.Add(volume))

			snapshot := &longhorn.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      snapName,
					Namespace: TestNamespace,
					Labels:    types.GetVolumeLabels(TestVolumeName),
				},
				Spec:   longhorn.SnapshotSpec{Volume: TestVolumeName},
				Status: longhorn.SnapshotStatus{CreationTime: "2026-01-01T00:00:00Z"},
			}
			snapshot, err := lhClient.LonghornV1beta2().Snapshots(TestNamespace).Create(context.TODO(), snapshot, metav1.CreateOptions{})
			assert.NoError(err)
			sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Snapshots().Informer().GetIndexer()
			assert.NoError(sIndexer.Add(snapshot))

			engine := &longhorn.Engine{
				ObjectMeta: metav1.ObjectMeta{Name: TestEngineName, Namespace: TestNamespace},
				Spec: longhorn.EngineSpec{
					InstanceSpec: longhorn.InstanceSpec{VolumeName: TestVolumeName},
					Active:       tc.active,
				},
				Status: longhorn.EngineStatus{Snapshots: tc.snapshots},
			}
			eIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Engines().Informer().GetIndexer()
			assert.NoError(eIndexer.Add(engine))

			assert.NoError(ec.syncSnapshotCRs(engine.Name))

			_, err = lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(context.TODO(), snapName, metav1.GetOptions{})
			if tc.expectCRDeleted {
				assert.True(apierrors.IsNotFound(err), "expected snapshot CR to be deleted, got %v", err)
			} else {
				assert.NoError(err)
			}
		})
	}
}

type purgeMockProxy struct {
	*engineapi.EngineSimulator
	failUntil      int
	firstCallDelay time.Duration
	purgeCalls     int
	closeCalls     int
	onPurge        func()
}

var _ engineapi.EngineClientProxy = (*purgeMockProxy)(nil)

func (p *purgeMockProxy) Close() {
	p.closeCalls++
}

func (p *purgeMockProxy) SnapshotPurge(_ engineapi.DataEngineObject) error {
	p.purgeCalls++
	if p.onPurge != nil {
		p.onPurge()
	}
	if p.purgeCalls == 1 && p.firstCallDelay > 0 {
		time.Sleep(p.firstCallDelay)
	}
	if p.purgeCalls <= p.failUntil {
		return errors.Newf("transient failure starting purge (call %d)", p.purgeCalls)
	}
	return nil
}

func drainFakeEvents(r *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case e := <-r.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}

func newPostRebuildPurgeTestController(t *testing.T, recorder *record.FakeRecorder) (*EngineController, *util.InformerFactories) {
	t.Helper()
	datastore.SkipListerCheck = true

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)

	ec := &EngineController{
		baseController:               newBaseController("longhorn-engine", logrus.New()),
		controllerID:                 TestNode1,
		ds:                           datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories),
		eventRecorder:                recorder,
		snapshotConcurrentLimiter:    NewSnapshotConcurrentLimiter(),
		postRebuildPurgeResults:      map[string]*postRebuildPurgeResult{},
		postRebuildPurgeResultsMutex: &sync.Mutex{},
	}
	return ec, informerFactories
}

func TestStartPostRebuildPurge(t *testing.T) {
	// Use short, test-controlled timings instead of production values.
	origCount, origInterval := postRebuildPurgeRetryCount, postRebuildPurgeRetryInterval
	origPollInterval, origPollTimeout := EnginePollInterval, EnginePollTimeout
	postRebuildPurgeRetryCount = 3
	postRebuildPurgeRetryInterval = time.Millisecond
	// Keep the engine poll deadline far shorter than a slow attempt so that a regression back
	// to a deadline-bounded poll loop would skip the retry and fail the slow-attempt case.
	EnginePollInterval = time.Millisecond
	EnginePollTimeout = 10 * time.Millisecond
	defer func() {
		postRebuildPurgeRetryCount, postRebuildPurgeRetryInterval = origCount, origInterval
		EnginePollInterval, EnginePollTimeout = origPollInterval, origPollTimeout
	}()

	tests := map[string]struct {
		failUntil           int
		firstCallDelay      time.Duration
		engineOwner         string
		prevFailedCondition bool // engine already has PostRebuildPurgeFailed=True
		expectCalls         int
		expectEvent         bool
		expectErrorText     string // substring expected in the event and the True condition; "" means False
		expectAttempts      int
	}{
		"succeeds on first attempt": {
			engineOwner: TestNode1,
			expectCalls: 1,
		},
		// A transient failure must be retried; once it succeeds, no further attempts are made.
		"failures followed by success stop retrying": {
			failUntil:   2,
			engineOwner: TestNode1,
			expectCalls: 3,
		},
		// Exhausting all attempts reports the last purge error in the event and the condition.
		"persistent failure exhausts attempts and records condition": {
			failUntil:       1000,
			engineOwner:     TestNode1,
			expectCalls:     3,
			expectEvent:     true,
			expectErrorText: "(call 3)",
			expectAttempts:  3,
		},
		// A slow attempt that fails after a deadline-style budget has elapsed must still be
		// followed by another attempt (the proxy RPC timeout is independent of any poll deadline).
		"slow failing attempt is still retried": {
			failUntil:      1,
			firstCallDelay: 50 * time.Millisecond,
			engineOwner:    TestNode1,
			expectCalls:    2,
		},
		// A node that no longer owns the engine must not purge or hand over a result.
		"no attempt and no result when engine ownership is lost": {
			failUntil:   1000,
			engineOwner: TestNode2,
			expectCalls: 0,
		},
		// A later successful purge start clears a previously recorded failure.
		"success clears a previous failure condition": {
			engineOwner:         TestNode1,
			prevFailedCondition: true,
			expectCalls:         1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert := require.New(t)

			recorder := record.NewFakeRecorder(100)
			ec, informerFactories := newPostRebuildPurgeTestController(t, recorder)

			// Empty DataEngine means v1: GetDataEngineObject returns the engine itself.
			engine := &longhorn.Engine{
				ObjectMeta: metav1.ObjectMeta{Name: TestEngineName, Namespace: TestNamespace},
				Spec: longhorn.EngineSpec{
					InstanceSpec: longhorn.InstanceSpec{VolumeName: TestVolumeName},
				},
				Status: longhorn.EngineStatus{
					InstanceStatus: longhorn.InstanceStatus{OwnerID: tc.engineOwner},
				},
			}
			if tc.prevFailedCondition {
				engine.Status.Conditions = types.SetCondition(engine.Status.Conditions,
					longhorn.EngineConditionTypePostRebuildPurgeFailed, longhorn.ConditionStatusTrue,
					longhorn.EngineConditionReasonPostRebuildPurgeStartFailed, "previous failure")
			}
			eIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Engines().Informer().GetIndexer()
			assert.NoError(eIndexer.Add(engine))

			proxy := &purgeMockProxy{
				EngineSimulator: &engineapi.EngineSimulator{},
				failUntil:       tc.failUntil,
				firstCallDelay:  tc.firstCallDelay,
			}

			logger := logrus.New()
			logger.Out = io.Discard
			ec.startPostRebuildPurge(logrus.NewEntry(logger), engine, func(_ *longhorn.Engine) (engineapi.EngineClientProxy, error) {
				return proxy, nil
			})

			assert.Equal(tc.expectCalls, proxy.purgeCalls, "SnapshotPurge attempt count")
			assert.Equal(tc.expectCalls, proxy.closeCalls, "each attempt must close its proxy")

			events := drainFakeEvents(recorder)
			if tc.expectEvent {
				assert.Len(events, 1, "expected exactly one failure event")
				assert.Contains(events[0], constant.EventReasonFailedStartingSnapshotPurge)
				assert.Contains(events[0], tc.expectErrorText, "event should report the last purge error")
			} else {
				assert.Empty(events, "no FailedStartingSnapshotPurge event expected")
			}

			if tc.engineOwner != TestNode1 {
				assert.Empty(ec.postRebuildPurgeResults, "a non-owner must not record a result")
				assert.Zero(ec.queue.Len(), "a non-owner must not enqueue the engine")
				return
			}

			// The result is handed to syncEngine: kept in memory and the engine enqueued.
			assert.Equal(1, ec.queue.Len(), "engine should be enqueued so syncEngine records the result")
			applied := engine.DeepCopy()
			assert.NotNil(ec.applyPostRebuildPurgeResult(applied), "a pending result is expected")

			cond := types.GetCondition(applied.Status.Conditions, longhorn.EngineConditionTypePostRebuildPurgeFailed)
			if tc.expectErrorText != "" {
				assert.Equal(longhorn.ConditionStatusTrue, cond.Status)
				assert.Equal(longhorn.EngineConditionReasonPostRebuildPurgeStartFailed, cond.Reason)
				assert.Contains(cond.Message, tc.expectErrorText, "condition should report the last purge error")
				assert.Contains(cond.Message, "("+strconv.Itoa(tc.expectAttempts)+" attempts)")
			} else {
				assert.Equal(longhorn.ConditionStatusFalse, cond.Status)
				assert.Equal(longhorn.EngineConditionReasonPostRebuildPurgeStarted, cond.Reason)
			}
			// PurgeStatus is engine-reported per replica and must never be written by this path.
			assert.Empty(applied.Status.PurgeStatus, "PurgeStatus must not be touched")
		})
	}
}

// A result must survive until syncEngine persists it: a conflict leaves it in place for the next
// sync, and clearing an older result must not drop a newer one recorded in the meantime.
func TestPostRebuildPurgeResultIsKeptUntilPersisted(t *testing.T) {
	assert := require.New(t)
	ec, _ := newPostRebuildPurgeTestController(t, record.NewFakeRecorder(10))
	engine := &longhorn.Engine{ObjectMeta: metav1.ObjectMeta{Name: TestEngineName, Namespace: TestNamespace}}

	ec.recordPostRebuildPurgeResult(engine, 3, errors.New("injected purge failure"))

	// First sync applies the result but its status update conflicts: the result is not cleared.
	first := ec.applyPostRebuildPurgeResult(engine.DeepCopy())
	assert.NotNil(first)

	// A newer rebuild finishes before the retried sync: clearing the old result must keep the new one.
	ec.recordPostRebuildPurgeResult(engine, 1, nil)
	ec.clearPostRebuildPurgeResult(engine.Name, first)
	assert.NotNil(ec.postRebuildPurgeResults[engine.Name], "a newer result must not be dropped")

	// The retried sync applies the newer result and clears it after a successful update.
	applied := engine.DeepCopy()
	second := ec.applyPostRebuildPurgeResult(applied)
	assert.Equal(longhorn.ConditionStatusFalse,
		types.GetCondition(applied.Status.Conditions, longhorn.EngineConditionTypePostRebuildPurgeFailed).Status)
	ec.clearPostRebuildPurgeResult(engine.Name, second)
	assert.Empty(ec.postRebuildPurgeResults)

	// An engine that is gone or no longer owned drops any pending result.
	ec.recordPostRebuildPurgeResult(engine, 3, errors.New("injected purge failure"))
	ec.deletePostRebuildPurgeResult(engine.Name)
	assert.Empty(ec.postRebuildPurgeResults)
}

const (
	postRebuildPurgeFailedAt   = "2026-11-26T10:00:00Z"
	postRebuildPurgeObservedAt = "2026-11-26T10:00:05Z"
)

// A failed post-rebuild purge may be followed by a manual or recurring purge that removes the
// leftovers; the condition must then go back to False instead of staying True forever.
func TestClearPostRebuildPurgeFailureIfCleaned(t *testing.T) {
	head := etypes.VolumeHeadName
	failed := func() []longhorn.Condition {
		return types.SetCondition(nil, longhorn.EngineConditionTypePostRebuildPurgeFailed,
			longhorn.ConditionStatusTrue, longhorn.EngineConditionReasonPostRebuildPurgeStartFailed, "previous failure")
	}

	tests := map[string]struct {
		dataEngine     longhorn.DataEngineType
		conditions     []longhorn.Condition
		snapshots      map[string]*longhorn.SnapshotInfo
		snapshotsError string
		// observedAt is PostRebuildPurgeFailed.LastProbeTime: the start of a successful
		// SnapshotList. Empty means no post-failure observation has been recorded.
		observedAt   string
		expectStatus longhorn.ConditionStatus
	}{
		"no failure condition is left untouched": {
			snapshots:    map[string]*longhorn.SnapshotInfo{head: {Name: head}},
			expectStatus: longhorn.ConditionStatusUnknown,
		},
		"only user snapshots left clears the failure": {
			conditions: failed(),
			snapshots: map[string]*longhorn.SnapshotInfo{
				"user": {Name: "user", UserCreated: true, Children: map[string]bool{head: true}},
				head:   {Name: head, Parent: "user"},
			},
			observedAt:   postRebuildPurgeObservedAt,
			expectStatus: longhorn.ConditionStatusFalse,
		},
		// The same clean list, observed before the failure, is still the pre-rebuild view.
		"clean snapshots observed before the failure keep it": {
			conditions: failed(),
			snapshots: map[string]*longhorn.SnapshotInfo{
				"user": {Name: "user", UserCreated: true, Children: map[string]bool{head: true}},
				head:   {Name: head, Parent: "user"},
			},
			observedAt:   "2026-11-26T09:59:59Z",
			expectStatus: longhorn.ConditionStatusTrue,
		},
		"system snapshot still present keeps the failure": {
			conditions: failed(),
			snapshots: map[string]*longhorn.SnapshotInfo{
				"system": {Name: "system", Children: map[string]bool{"user": true}},
				"user":   {Name: "user", UserCreated: true, Parent: "system", Children: map[string]bool{head: true}},
				head:     {Name: head, Parent: "user"},
			},
			observedAt:   postRebuildPurgeObservedAt,
			expectStatus: longhorn.ConditionStatusTrue,
		},
		"removed snapshot still present keeps the failure": {
			conditions: failed(),
			snapshots: map[string]*longhorn.SnapshotInfo{
				"removed": {Name: "removed", UserCreated: true, Removed: true, Children: map[string]bool{"user": true}},
				"user":    {Name: "user", UserCreated: true, Parent: "removed", Children: map[string]bool{head: true}},
				head:      {Name: head, Parent: "user"},
			},
			observedAt:   postRebuildPurgeObservedAt,
			expectStatus: longhorn.ConditionStatusTrue,
		},
		// V1: the parent of the volume head cannot be purged yet, so it must not keep the condition stuck.
		"v1 system snapshot behind the volume head does not block clearing": {
			dataEngine: longhorn.DataEngineTypeV1,
			conditions: failed(),
			snapshots: map[string]*longhorn.SnapshotInfo{
				"system": {Name: "system", Children: map[string]bool{head: true}},
				head:     {Name: head, Parent: "system"},
			},
			observedAt:   postRebuildPurgeObservedAt,
			expectStatus: longhorn.ConditionStatusFalse,
		},
		"v1 unremoved backing-image export snapshot does not block clearing": {
			dataEngine: longhorn.DataEngineTypeV1,
			conditions: failed(),
			snapshots: map[string]*longhorn.SnapshotInfo{
				"export": {
					Name: "export", UserCreated: false,
					Labels:   map[string]string{types.GetLonghornLabelKey(types.LonghornLabelSnapshotForExportingBackingImage): "bids"},
					Children: map[string]bool{"user": true},
				},
				"user": {Name: "user", UserCreated: true, Parent: "export", Children: map[string]bool{head: true}},
				head:   {Name: head, Parent: "user"},
			},
			observedAt:   postRebuildPurgeObservedAt,
			expectStatus: longhorn.ConditionStatusFalse,
		},
		"v1 removed backing-image export snapshot keeps the failure": {
			dataEngine: longhorn.DataEngineTypeV1,
			conditions: failed(),
			snapshots: map[string]*longhorn.SnapshotInfo{
				"export": {
					Name: "export", UserCreated: false, Removed: true,
					Labels:   map[string]string{types.GetLonghornLabelKey(types.LonghornLabelSnapshotForExportingBackingImage): "bids"},
					Children: map[string]bool{"user": true},
				},
				"user": {Name: "user", UserCreated: true, Parent: "export", Children: map[string]bool{head: true}},
				head:   {Name: head, Parent: "user"},
			},
			observedAt:   postRebuildPurgeObservedAt,
			expectStatus: longhorn.ConditionStatusTrue,
		},
		// V2: head parent with a single child is purgeable, so the failure must stay.
		"v2 system snapshot behind the volume head keeps the failure": {
			dataEngine: longhorn.DataEngineTypeV2,
			conditions: failed(),
			snapshots: map[string]*longhorn.SnapshotInfo{
				"system": {Name: "system", Children: map[string]bool{head: true}},
				head:     {Name: head, Parent: "system"},
			},
			observedAt:   postRebuildPurgeObservedAt,
			expectStatus: longhorn.ConditionStatusTrue,
		},
		// V2: branched system snapshots are skipped by purge, so they must not block clearing.
		"v2 branched system snapshot does not block clearing": {
			dataEngine: longhorn.DataEngineTypeV2,
			conditions: failed(),
			snapshots: map[string]*longhorn.SnapshotInfo{
				"system": {Name: "system", Children: map[string]bool{"a": true, "b": true}},
				"a":      {Name: "a", UserCreated: true, Parent: "system", Children: map[string]bool{head: true}},
				"b":      {Name: "b", UserCreated: true, Parent: "system"},
				head:     {Name: head, Parent: "a"},
			},
			observedAt:   postRebuildPurgeObservedAt,
			expectStatus: longhorn.ConditionStatusFalse,
		},
		"unknown snapshot list keeps the failure": {
			conditions:   failed(),
			snapshots:    nil,
			expectStatus: longhorn.ConditionStatusTrue,
		},
		"snapshot list error keeps the failure": {
			conditions:     failed(),
			snapshots:      map[string]*longhorn.SnapshotInfo{head: {Name: head}},
			snapshotsError: "failed to list snapshots",
			expectStatus:   longhorn.ConditionStatusTrue,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert := require.New(t)
			engine := &longhorn.Engine{
				Spec: longhorn.EngineSpec{DataEngine: tc.dataEngine},
				Status: longhorn.EngineStatus{
					InstanceStatus: longhorn.InstanceStatus{Conditions: tc.conditions},
					Snapshots:      tc.snapshots,
					SnapshotsError: tc.snapshotsError,
				},
			}
			for i := range engine.Status.Conditions {
				if engine.Status.Conditions[i].Type != longhorn.EngineConditionTypePostRebuildPurgeFailed {
					continue
				}
				engine.Status.Conditions[i].LastTransitionTime = postRebuildPurgeFailedAt
				engine.Status.Conditions[i].LastProbeTime = tc.observedAt
			}

			clearPostRebuildPurgeFailureIfCleaned(engine)

			cond := types.GetCondition(engine.Status.Conditions, longhorn.EngineConditionTypePostRebuildPurgeFailed)
			assert.Equal(tc.expectStatus, cond.Status)
			if tc.expectStatus == longhorn.ConditionStatusFalse {
				assert.Equal(longhorn.EngineConditionReasonPostRebuildPurgeCleaned, cond.Reason)
			}
		})
	}
}

// An instance-manager replacement that changes the engine's IM name between attempts must
// force a new client; reusing the pre-rebuild proxy would keep targeting the old endpoint.
func TestStartPostRebuildPurgeUsesFreshProxyAfterInstanceManagerReplacement(t *testing.T) {
	origCount, origInterval := postRebuildPurgeRetryCount, postRebuildPurgeRetryInterval
	postRebuildPurgeRetryCount = 3
	postRebuildPurgeRetryInterval = time.Millisecond
	defer func() {
		postRebuildPurgeRetryCount, postRebuildPurgeRetryInterval = origCount, origInterval
	}()

	assert := require.New(t)
	recorder := record.NewFakeRecorder(100)
	ec, informerFactories := newPostRebuildPurgeTestController(t, recorder)

	engine := &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{Name: TestEngineName, Namespace: TestNamespace},
		Spec: longhorn.EngineSpec{
			InstanceSpec: longhorn.InstanceSpec{VolumeName: TestVolumeName},
		},
		Status: longhorn.EngineStatus{
			InstanceStatus: longhorn.InstanceStatus{
				OwnerID:             TestNode1,
				InstanceManagerName: "im-old",
			},
		},
	}
	eIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Engines().Informer().GetIndexer()
	assert.NoError(eIndexer.Add(engine))

	type trackedProxy struct {
		*purgeMockProxy
		imName string
	}
	var created []*trackedProxy

	logger := logrus.New()
	logger.Out = io.Discard

	ec.startPostRebuildPurge(logrus.NewEntry(logger), engine, func(e *longhorn.Engine) (engineapi.EngineClientProxy, error) {
		p := &trackedProxy{
			purgeMockProxy: &purgeMockProxy{EngineSimulator: &engineapi.EngineSimulator{}},
			imName:         e.Status.InstanceManagerName,
		}
		if len(created) == 0 {
			p.failUntil = 1
			p.onPurge = func() {
				latest, err := ec.ds.GetEngine(engine.Name)
				assert.NoError(err)
				latest.Status.InstanceManagerName = "im-new"
				assert.NoError(eIndexer.Update(latest))
			}
		}
		created = append(created, p)
		return p, nil
	})

	assert.Len(created, 2)
	assert.Equal("im-old", created[0].imName)
	assert.Equal("im-new", created[1].imName)
	assert.Equal(1, created[0].purgeCalls)
	assert.Equal(1, created[0].closeCalls)
	assert.Equal(1, created[1].purgeCalls)
	assert.Equal(1, created[1].closeCalls)
	assert.Empty(drainFakeEvents(recorder))

	applied := engine.DeepCopy()
	assert.NotNil(ec.applyPostRebuildPurgeResult(applied))
	assert.Equal(longhorn.ConditionStatusFalse,
		types.GetCondition(applied.Status.Conditions, longhorn.EngineConditionTypePostRebuildPurgeFailed).Status)
}

// A pending failure must win over a stale clean snapshot list in the same reconcile.
func TestPendingPurgeFailureWinsOverStaleCleanSnapshots(t *testing.T) {
	assert := require.New(t)
	ec, _ := newPostRebuildPurgeTestController(t, record.NewFakeRecorder(10))
	head := etypes.VolumeHeadName

	engine := &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{Name: TestEngineName, Namespace: TestNamespace},
		Status: longhorn.EngineStatus{
			// Pre-rebuild view: only a non-purgeable head parent remains.
			Snapshots: map[string]*longhorn.SnapshotInfo{
				"system": {Name: "system", Children: map[string]bool{head: true}},
				head:     {Name: head, Parent: "system"},
			},
		},
	}
	ec.recordPostRebuildPurgeResult(engine, 3, errors.New("injected purge failure"))

	clearPostRebuildPurgeFailureIfCleaned(engine)
	result := ec.applyPostRebuildPurgeResult(engine)
	assert.NotNil(result)
	cond := types.GetCondition(engine.Status.Conditions, longhorn.EngineConditionTypePostRebuildPurgeFailed)
	assert.Equal(longhorn.ConditionStatusTrue, cond.Status)
	assert.Equal(longhorn.EngineConditionReasonPostRebuildPurgeStartFailed, cond.Reason)

	// Persist the result, then reconcile again with the same pre-rebuild snapshot list.
	ec.clearPostRebuildPurgeResult(engine.Name, result)
	clearPostRebuildPurgeFailureIfCleaned(engine)
	cond = types.GetCondition(engine.Status.Conditions, longhorn.EngineConditionTypePostRebuildPurgeFailed)
	assert.Equal(longhorn.ConditionStatusTrue, cond.Status)
	assert.Equal(longhorn.EngineConditionReasonPostRebuildPurgeStartFailed, cond.Reason)
}

// A SnapshotList that started before the failure must not count, and a later one is recorded once.
func TestNotePostRebuildSnapshotObservation(t *testing.T) {
	assert := require.New(t)
	failedAt := postRebuildPurgeFailedAt
	engine := &longhorn.Engine{
		Status: longhorn.EngineStatus{
			InstanceStatus: longhorn.InstanceStatus{
				Conditions: types.SetCondition(nil, longhorn.EngineConditionTypePostRebuildPurgeFailed,
					longhorn.ConditionStatusTrue, longhorn.EngineConditionReasonPostRebuildPurgeStartFailed, "previous failure"),
			},
		},
	}
	for i := range engine.Status.Conditions {
		if engine.Status.Conditions[i].Type == longhorn.EngineConditionTypePostRebuildPurgeFailed {
			engine.Status.Conditions[i].LastTransitionTime = failedAt
		}
	}

	notePostRebuildSnapshotObservation(engine, "2026-11-26T09:59:59Z")
	cond := types.GetCondition(engine.Status.Conditions, longhorn.EngineConditionTypePostRebuildPurgeFailed)
	assert.Empty(cond.LastProbeTime)

	notePostRebuildSnapshotObservation(engine, postRebuildPurgeObservedAt)
	cond = types.GetCondition(engine.Status.Conditions, longhorn.EngineConditionTypePostRebuildPurgeFailed)
	assert.Equal(postRebuildPurgeObservedAt, cond.LastProbeTime)

	notePostRebuildSnapshotObservation(engine, "2026-11-26T10:00:10Z")
	cond = types.GetCondition(engine.Status.Conditions, longhorn.EngineConditionTypePostRebuildPurgeFailed)
	assert.Equal(postRebuildPurgeObservedAt, cond.LastProbeTime)
}

// A second failure while PostRebuildPurgeFailed is already True must not be cleared by a clean
// SnapshotList that started after the first failure and before this one.
func TestConsecutivePostRebuildFailureIgnoresInFlightCleanList(t *testing.T) {
	assert := require.New(t)
	ec, _ := newPostRebuildPurgeTestController(t, record.NewFakeRecorder(10))
	head := etypes.VolumeHeadName

	engine := &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{Name: TestEngineName, Namespace: TestNamespace},
		Status: longhorn.EngineStatus{
			Snapshots: map[string]*longhorn.SnapshotInfo{
				"user": {Name: "user", UserCreated: true, Children: map[string]bool{head: true}},
				head:   {Name: head, Parent: "user"},
			},
			InstanceStatus: longhorn.InstanceStatus{
				Conditions: types.SetCondition(nil, longhorn.EngineConditionTypePostRebuildPurgeFailed,
					longhorn.ConditionStatusTrue, longhorn.EngineConditionReasonPostRebuildPurgeStartFailed, "first failure"),
			},
		},
	}
	for i := range engine.Status.Conditions {
		if engine.Status.Conditions[i].Type != longhorn.EngineConditionTypePostRebuildPurgeFailed {
			continue
		}
		engine.Status.Conditions[i].LastTransitionTime = "2020-01-01T00:00:00Z"
		// Clean list that started after the first failure, before the second.
		engine.Status.Conditions[i].LastProbeTime = "2020-01-01T00:00:05Z"
	}

	ec.recordPostRebuildPurgeResult(engine, 3, errors.New("second failure"))
	clearPostRebuildPurgeFailureIfCleaned(engine)
	result := ec.applyPostRebuildPurgeResult(engine)
	assert.NotNil(result)

	cond := types.GetCondition(engine.Status.Conditions, longhorn.EngineConditionTypePostRebuildPurgeFailed)
	assert.Equal(longhorn.ConditionStatusTrue, cond.Status)
	assert.Equal(longhorn.EngineConditionReasonPostRebuildPurgeStartFailed, cond.Reason)
	assert.Empty(cond.LastProbeTime)
	assert.NotEqual("2020-01-01T00:00:00Z", cond.LastTransitionTime)

	// The in-flight list still predates this failure, so it must not be recorded.
	notePostRebuildSnapshotObservation(engine, "2020-01-01T00:00:05Z")
	clearPostRebuildPurgeFailureIfCleaned(engine)
	cond = types.GetCondition(engine.Status.Conditions, longhorn.EngineConditionTypePostRebuildPurgeFailed)
	assert.Equal(longhorn.ConditionStatusTrue, cond.Status)
	assert.Equal(longhorn.EngineConditionReasonPostRebuildPurgeStartFailed, cond.Reason)
	assert.Empty(cond.LastProbeTime)
}
