package controller

import (
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"

	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
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
