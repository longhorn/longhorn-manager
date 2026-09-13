package controller

import (
	"io"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

func TestCurrentEngineFrontendForEngine(t *testing.T) {
	volume := &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "vol"},
	}
	engine := &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{Name: "vol-e-0"},
	}
	newEngineFrontends := func(engineName string, state longhorn.InstanceState) map[string]*longhorn.EngineFrontend {
		return map[string]*longhorn.EngineFrontend{
			"vol-ef-0": {
				ObjectMeta: metav1.ObjectMeta{Name: "vol-ef-0"},
				Spec: longhorn.EngineFrontendSpec{
					EngineName: engineName,
					Active:     true,
				},
				Status: longhorn.EngineFrontendStatus{InstanceStatus: longhorn.InstanceStatus{
					CurrentState: state,
					StorageIP:    "10.0.0.1",
					// A frontend is an initiator and listens on no port.
					Port: 0,
				}},
			},
		}
	}

	engineFrontend, err := currentEngineFrontendForEngine(volume, engine, newEngineFrontends("vol-e-0", longhorn.InstanceStateRunning))
	require.NoError(t, err)
	require.NotNil(t, engineFrontend)
	require.Equal(t, "vol-ef-0", engineFrontend.Name)

	// A frontend of another engine must not be used for this engine.
	engineFrontend, err = currentEngineFrontendForEngine(volume, engine, newEngineFrontends("vol-e-1", longhorn.InstanceStateRunning))
	require.NoError(t, err)
	require.Nil(t, engineFrontend)

	// A frontend that is not running cannot report the frontend size.
	engineFrontend, err = currentEngineFrontendForEngine(volume, engine, newEngineFrontends("vol-e-0", longhorn.InstanceStateStopped))
	require.NoError(t, err)
	require.Nil(t, engineFrontend)

	// A rebuild suspends the frontend, but it still serves the current size.
	engineFrontend, err = currentEngineFrontendForEngine(volume, engine, newEngineFrontends("vol-e-0", longhorn.InstanceStateSuspended))
	require.NoError(t, err)
	require.NotNil(t, engineFrontend)
	require.Equal(t, "vol-ef-0", engineFrontend.Name)

	// An errored frontend still holds the size it reached and the expansion error.
	engineFrontend, err = currentEngineFrontendForEngine(volume, engine, newEngineFrontends("vol-e-0", longhorn.InstanceStateError))
	require.NoError(t, err)
	require.NotNil(t, engineFrontend)
	require.Equal(t, "vol-ef-0", engineFrontend.Name)

	engineFrontend, err = currentEngineFrontendForEngine(volume, engine, map[string]*longhorn.EngineFrontend{})
	require.NoError(t, err)
	require.Nil(t, engineFrontend)

	// The instance handler drops the service address of a failed frontend, and
	// without it the proxy would answer from the engine instead.
	engineFrontends := newEngineFrontends("vol-e-0", longhorn.InstanceStateError)
	engineFrontends["vol-ef-0"].Status.StorageIP = ""
	engineFrontend, err = currentEngineFrontendForEngine(volume, engine, engineFrontends)
	require.NoError(t, err)
	require.Nil(t, engineFrontend)
}

func TestEngineFrontendReportedVolumeInfo(t *testing.T) {
	engineFrontend := &longhorn.EngineFrontend{
		ObjectMeta: metav1.ObjectMeta{Name: "vol-ef-0"},
	}

	// The proxy overlays the frontend endpoint only when it reached the frontend.
	require.True(t, engineFrontendReportedVolumeInfo(engineFrontend,
		&engineapi.Volume{Endpoint: "/dev/longhorn/vol", Size: 4 * util.GiB}))

	// A frontend was asked for, but the proxy answered from the engine, which
	// carries no endpoint and is already at the new size mid-expansion.
	require.False(t, engineFrontendReportedVolumeInfo(engineFrontend,
		&engineapi.Volume{Endpoint: "", Size: 4 * util.GiB}))

	require.False(t, engineFrontendReportedVolumeInfo(nil,
		&engineapi.Volume{Endpoint: "/dev/longhorn/vol"}))
	require.False(t, engineFrontendReportedVolumeInfo(engineFrontend, nil))
}

func TestEngineServesFrontend(t *testing.T) {
	engine := &longhorn.Engine{
		Spec: longhorn.EngineSpec{Frontend: longhorn.VolumeFrontendBlockDev},
	}
	require.True(t, engineServesFrontend(engine))

	engine.Spec.DisableFrontend = true
	require.False(t, engineServesFrontend(engine))

	engine.Spec.DisableFrontend = false
	engine.Spec.Frontend = longhorn.VolumeFrontendEmpty
	require.False(t, engineServesFrontend(engine))
}

func TestResolveFrontendSizeReporting(t *testing.T) {
	type testCase struct {
		sizeFollowsFrontend bool
		frontendReported    bool
		frontendFailed      bool
		haveCurrentSize     bool
		expectPublishSize   bool
		expectPublishState  bool
		expectSizeConfirmed bool
	}
	tests := map[string]testCase{
		// v1 or a frontendless engine never depends on the frontend, so the
		// engine-reported size is always trusted and published.
		"size does not follow frontend": {
			sizeFollowsFrontend: false, frontendReported: false, haveCurrentSize: true,
			expectPublishSize: true, expectPublishState: true, expectSizeConfirmed: true,
		},
		// A v1 engine keeps the original behavior even before any size is known:
		// the reported size is always confirmed and published, never suppressed.
		"v1 with no known size still publishes and confirms": {
			sizeFollowsFrontend: false, frontendReported: false, haveCurrentSize: false,
			expectPublishSize: true, expectPublishState: true, expectSizeConfirmed: true,
		},
		"size does not follow frontend with expansion error": {
			sizeFollowsFrontend: false, frontendReported: false, frontendFailed: true, haveCurrentSize: true,
			expectPublishSize: true, expectPublishState: true, expectSizeConfirmed: true,
		},
		// v2 frontend answered: the size is confirmed and published.
		"frontend confirmed the size": {
			sizeFollowsFrontend: true, frontendReported: true, haveCurrentSize: true,
			expectPublishSize: true, expectPublishState: true, expectSizeConfirmed: true,
		},
		// A v2 frontend with an expansion error can carry the backend size even
		// though the host device did not finish resizing. Keep the last known size
		// but publish the terminal expanding state so retry is not blocked.
		"frontend reported failed expansion with a known size": {
			sizeFollowsFrontend: true, frontendReported: true, frontendFailed: true, haveCurrentSize: true,
			expectPublishSize: false, expectPublishState: true, expectSizeConfirmed: false,
		},
		// If no size is recorded yet, still initialize it without treating it as
		// confirmed expansion success.
		"frontend reported failed expansion without a known size": {
			sizeFollowsFrontend: true, frontendReported: true, frontendFailed: true, haveCurrentSize: false,
			expectPublishSize: true, expectPublishState: true, expectSizeConfirmed: false,
		},
		// v2 proxy fell back to the engine mid-expansion: the size is not
		// confirmed and the last known size is kept.
		"frontend fell back with a known size": {
			sizeFollowsFrontend: true, frontendReported: false, haveCurrentSize: true,
			expectPublishSize: false, expectPublishState: false, expectSizeConfirmed: false,
		},
		// Same fallback but no size recorded yet: still not confirmed, but the
		// first size has to be published so the engine is not left at 0.
		"frontend fell back without a known size": {
			sizeFollowsFrontend: true, frontendReported: false, haveCurrentSize: false,
			expectPublishSize: true, expectPublishState: false, expectSizeConfirmed: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			publishSize, publishExpansionState, sizeConfirmed := resolveFrontendSizeReporting(tc.sizeFollowsFrontend, tc.frontendReported, tc.frontendFailed, tc.haveCurrentSize)
			require.Equal(t, tc.expectPublishSize, publishSize, "publishSize")
			require.Equal(t, tc.expectPublishState, publishExpansionState, "publishExpansionState")
			require.Equal(t, tc.expectSizeConfirmed, sizeConfirmed, "sizeConfirmed")
		})
	}
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
