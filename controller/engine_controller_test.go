package controller

import (
	"io"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-manager/engineapi"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/util"
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

func TestMarkFailedRebuildsAsErr(t *testing.T) {
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
		rebuildStatus            map[string]*longhorn.RebuildStatus
		replicaModeMap           map[string]longhorn.ReplicaMode
		existingReplicaModeMap   map[string]longhorn.ReplicaMode
		addressReplicaMap        map[string]string
		currentReplicaAddressMap map[string]string            // engine.Status.CurrentReplicaAddressMap (replica -> addr)
		replicas                 map[string]*longhorn.Replica // stub for GetReplicaRO
		expectModeMap            map[string]longhorn.ReplicaMode
		expectTransitionUpdate   bool // true if TransitionTimeMap[replicaWO|replicaRW] should be bumped
	}

	tests := map[string]testCase{
		// Core support-bundle case: sync agent gave up, engine reports state=error, replica
		// still WO in ReplicaModeMap. Must transition to ERR so rebuildNewReplica unblocks.
		"state=error on WO replica transitions to ERR": {
			rebuildStatus: map[string]*longhorn.RebuildStatus{
				urlWO: {
					State:        engineapi.ProcessStateError,
					IsRebuilding: false,
					Error:        "syncing has failed for 3 times, will error out",
				},
			},
			replicaModeMap:         map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			existingReplicaModeMap: map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			addressReplicaMap:      map[string]string{addrWO: replicaWO},
			expectModeMap:          map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeERR},
			expectTransitionUpdate: true,
		},
		// Error message set without explicit state field — still counts as failed.
		"error field alone on WO replica transitions to ERR": {
			rebuildStatus: map[string]*longhorn.RebuildStatus{
				urlWO: {Error: "connection reset by peer"},
			},
			replicaModeMap:         map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			existingReplicaModeMap: map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			addressReplicaMap:      map[string]string{addrWO: replicaWO},
			expectModeMap:          map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeERR},
			expectTransitionUpdate: true,
		},
		// State=error without an Error message — still terminal. The reason string passed to
		// flipWOReplicaToErr must surface the state so the event/log isn't empty.
		"state=error with empty Error still transitions": {
			rebuildStatus: map[string]*longhorn.RebuildStatus{
				urlWO: {State: engineapi.ProcessStateError, IsRebuilding: false},
			},
			replicaModeMap:         map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			existingReplicaModeMap: map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			addressReplicaMap:      map[string]string{addrWO: replicaWO},
			expectModeMap:          map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeERR},
			expectTransitionUpdate: true,
		},
		// Nil rebuildStatus (engine unreachable, caller ran Path 2 only) must still flip WO->ERR
		// via Path 2 when the Replica CR reports FailedAt.
		"nil rebuildStatus runs Path 2 only": {
			rebuildStatus:            nil,
			replicaModeMap:           map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			existingReplicaModeMap:   map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			addressReplicaMap:        map[string]string{addrWO: replicaWO},
			currentReplicaAddressMap: map[string]string{replicaWO: addrWO},
			replicas: map[string]*longhorn.Replica{
				replicaWO: {Spec: longhorn.ReplicaSpec{FailedAt: "2026-04-23T07:29:33Z"}},
			},
			expectModeMap:          map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeERR},
			expectTransitionUpdate: true,
		},
		// Rebuild still in progress — must not touch the mode even if Error is set transiently.
		"in-progress rebuild with error leaves WO alone": {
			rebuildStatus: map[string]*longhorn.RebuildStatus{
				urlWO: {State: engineapi.ProcessStateInProgress, IsRebuilding: true, Error: "transient"},
			},
			replicaModeMap:         map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			existingReplicaModeMap: map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			addressReplicaMap:      map[string]string{addrWO: replicaWO},
			expectModeMap:          map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
		},
		// Successful completion must not flip to ERR — verifyCompletedRebuild handles that path.
		"complete rebuild leaves WO alone": {
			rebuildStatus: map[string]*longhorn.RebuildStatus{
				urlWO: {State: engineapi.ProcessStateComplete, IsRebuilding: false},
			},
			replicaModeMap:         map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			existingReplicaModeMap: map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			addressReplicaMap:      map[string]string{addrWO: replicaWO},
			expectModeMap:          map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
		},
		// Stale rebuild error for a replica that is already RW (shouldn't happen, but be safe).
		"error on already-RW replica is ignored": {
			rebuildStatus: map[string]*longhorn.RebuildStatus{
				urlRW: {State: engineapi.ProcessStateError, Error: "stale"},
			},
			replicaModeMap:         map[string]longhorn.ReplicaMode{replicaRW: longhorn.ReplicaModeRW},
			existingReplicaModeMap: map[string]longhorn.ReplicaMode{replicaRW: longhorn.ReplicaModeRW},
			addressReplicaMap:      map[string]string{addrRW: replicaRW},
			expectModeMap:          map[string]longhorn.ReplicaMode{replicaRW: longhorn.ReplicaModeRW},
		},
		// URL maps to no known replica — skip silently.
		"unknown replica url is skipped": {
			rebuildStatus: map[string]*longhorn.RebuildStatus{
				urlWO: {State: engineapi.ProcessStateError, Error: "boom"},
			},
			replicaModeMap:         map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			existingReplicaModeMap: map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			addressReplicaMap:      map[string]string{}, // no mapping
			expectModeMap:          map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
		},
		// Engine keeps reporting WO every cycle, so without a guard we would re-emit the event
		// and reset the transition time forever. If the previous persisted mode was already ERR,
		// flip the mode again but skip event/transition-time updates.
		"re-applied ERR override does not bump transition time": {
			rebuildStatus: map[string]*longhorn.RebuildStatus{
				urlWO: {State: engineapi.ProcessStateError, Error: "still failing"},
			},
			replicaModeMap:         map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			existingReplicaModeMap: map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeERR},
			addressReplicaMap:      map[string]string{addrWO: replicaWO},
			expectModeMap:          map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeERR},
			expectTransitionUpdate: false,
		},
		// Mixed map: one replica errored, one completed. The errored one transitions; the
		// completed one is handled by verifyCompletedRebuild and must be left untouched here.
		"mixed errored and completed": {
			rebuildStatus: map[string]*longhorn.RebuildStatus{
				urlWO: {State: engineapi.ProcessStateError, Error: "fail"},
				urlRW: {State: engineapi.ProcessStateComplete, IsRebuilding: false},
			},
			replicaModeMap: map[string]longhorn.ReplicaMode{
				replicaWO: longhorn.ReplicaModeWO,
				replicaRW: longhorn.ReplicaModeWO,
			},
			existingReplicaModeMap: map[string]longhorn.ReplicaMode{
				replicaWO: longhorn.ReplicaModeWO,
				replicaRW: longhorn.ReplicaModeWO,
			},
			addressReplicaMap: map[string]string{addrWO: replicaWO, addrRW: replicaRW},
			expectModeMap: map[string]longhorn.ReplicaMode{
				replicaWO: longhorn.ReplicaModeERR,
				replicaRW: longhorn.ReplicaModeWO,
			},
			expectTransitionUpdate: true,
		},
		// Nil status entry must not panic or trigger anything.
		"nil status entry is skipped": {
			rebuildStatus:          map[string]*longhorn.RebuildStatus{urlWO: nil},
			replicaModeMap:         map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			existingReplicaModeMap: map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			addressReplicaMap:      map[string]string{addrWO: replicaWO},
			expectModeMap:          map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
		},
		// Path 2 — node-down case: ReplicaRebuildStatus is empty (sync agent unreachable), but
		// the Replica CR is already marked FailedAt. Must flip WO -> ERR regardless.
		"replica CR FailedAt transitions stuck WO to ERR": {
			rebuildStatus:            map[string]*longhorn.RebuildStatus{}, // engine can't reach sync agent
			replicaModeMap:           map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			existingReplicaModeMap:   map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			addressReplicaMap:        map[string]string{addrWO: replicaWO},
			currentReplicaAddressMap: map[string]string{replicaWO: addrWO},
			replicas: map[string]*longhorn.Replica{
				replicaWO: {Spec: longhorn.ReplicaSpec{FailedAt: "2026-04-22T07:29:33Z"}},
			},
			expectModeMap:          map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeERR},
			expectTransitionUpdate: true,
		},
		// Path 2 — CurrentState=Error also counts as failed even when FailedAt not yet set.
		"replica CR in InstanceStateError transitions stuck WO to ERR": {
			rebuildStatus:          map[string]*longhorn.RebuildStatus{},
			replicaModeMap:         map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			existingReplicaModeMap: map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			addressReplicaMap:      map[string]string{addrWO: replicaWO},
			replicas: map[string]*longhorn.Replica{
				replicaWO: {Status: longhorn.ReplicaStatus{InstanceStatus: longhorn.InstanceStatus{CurrentState: longhorn.InstanceStateError}}},
			},
			expectModeMap:          map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeERR},
			expectTransitionUpdate: true,
		},
		// Path 2 — healthy Replica CR must not be touched even if engine still reports WO.
		"healthy replica CR leaves WO alone": {
			rebuildStatus:          map[string]*longhorn.RebuildStatus{},
			replicaModeMap:         map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			existingReplicaModeMap: map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			addressReplicaMap:      map[string]string{addrWO: replicaWO},
			replicas: map[string]*longhorn.Replica{
				replicaWO: {Status: longhorn.ReplicaStatus{InstanceStatus: longhorn.InstanceStatus{CurrentState: longhorn.InstanceStateRunning}}},
			},
			expectModeMap: map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
		},
		// Path 2 — replica CR missing from datastore (NotFound): skip, don't flip.
		"missing replica CR leaves WO alone": {
			rebuildStatus:          map[string]*longhorn.RebuildStatus{},
			replicaModeMap:         map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			existingReplicaModeMap: map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			addressReplicaMap:      map[string]string{addrWO: replicaWO},
			replicas:               nil, // getter returns error for all names
			expectModeMap:          map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
		},
		// Path 2 — re-apply guard: replica already ERR in existing, still WO in current (engine
		// keeps reporting WO). Flip mode again but don't bump transition time.
		"replica CR failed but already ERR skips transition bump": {
			rebuildStatus:          map[string]*longhorn.RebuildStatus{},
			replicaModeMap:         map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeWO},
			existingReplicaModeMap: map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeERR},
			addressReplicaMap:      map[string]string{addrWO: replicaWO},
			replicas: map[string]*longhorn.Replica{
				replicaWO: {Spec: longhorn.ReplicaSpec{FailedAt: "2026-04-22T07:29:33Z"}},
			},
			expectModeMap:          map[string]longhorn.ReplicaMode{replicaWO: longhorn.ReplicaModeERR},
			expectTransitionUpdate: false,
		},
	}

	const priorTime = "2000-01-01T00:00:00Z"
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert := require.New(t)

			transitionTimeMap := map[string]string{}
			for r := range tc.replicaModeMap {
				transitionTimeMap[r] = priorTime
			}
			engine := &longhorn.Engine{
				Status: longhorn.EngineStatus{
					ReplicaModeMap:           tc.replicaModeMap,
					ReplicaTransitionTimeMap: transitionTimeMap,
					CurrentReplicaAddressMap: tc.currentReplicaAddressMap,
				},
			}
			existingEngine := &longhorn.Engine{
				Status: longhorn.EngineStatus{
					ReplicaModeMap: tc.existingReplicaModeMap,
				},
			}
			getReplica := func(name string) (*longhorn.Replica, error) {
				r, ok := tc.replicas[name]
				if !ok {
					return nil, errors.New("replica not found")
				}
				return r, nil
			}

			newMonitor().markFailedRebuildsAsErr(engine, existingEngine, tc.addressReplicaMap, tc.rebuildStatus, getReplica)
			assert.Equal(tc.expectModeMap, engine.Status.ReplicaModeMap, "ReplicaModeMap mismatch")

			for replicaName, mode := range tc.expectModeMap {
				if mode != longhorn.ReplicaModeERR {
					continue
				}
				if tc.expectTransitionUpdate {
					assert.NotEqual(priorTime, engine.Status.ReplicaTransitionTimeMap[replicaName],
						"TransitionTime should be updated for %v", replicaName)
				} else {
					assert.Equal(priorTime, engine.Status.ReplicaTransitionTimeMap[replicaName],
						"TransitionTime should not be touched for %v", replicaName)
				}
			}
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
