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
	clienttesting "k8s.io/client-go/testing"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	imclient "github.com/longhorn/longhorn-instance-manager/pkg/client"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
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

// purgeMockProxy wraps EngineSimulator and controls SnapshotPurge results: the first
// failUntil calls fail (the first one optionally after firstCallDelay to simulate a slow
// RPC that times out), later calls succeed. All attempts are counted.
type purgeMockProxy struct {
	*engineapi.EngineSimulator
	failUntil      int
	firstCallDelay time.Duration
	purgeCalls     int
}

var _ engineapi.EngineClientProxy = (*purgeMockProxy)(nil)

func (p *purgeMockProxy) Close() {}

func (p *purgeMockProxy) SnapshotPurge(_ engineapi.DataEngineObject) error {
	p.purgeCalls++
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
		failUntil       int
		firstCallDelay  time.Duration
		engineOwner     string
		prevPurgeError  string // engine.Status.PostRebuildPurgeError before the call
		expectCalls     int
		expectEvent     bool
		expectErrorText string // substring expected in the event and engine status; "" means status must be empty
	}{
		"succeeds on first attempt": {
			failUntil:   0,
			engineOwner: TestNode1,
			expectCalls: 1,
		},
		// A transient failure must be retried; once it succeeds, no further attempts are made
		// and nothing is recorded.
		"failures followed by success stop retrying": {
			failUntil:   2,
			engineOwner: TestNode1,
			expectCalls: 3,
		},
		// Exhausting all attempts reports the last purge error in the event and engine status.
		"persistent failure exhausts attempts and records error": {
			failUntil:       1000,
			engineOwner:     TestNode1,
			expectCalls:     3,
			expectEvent:     true,
			expectErrorText: "(call 3)",
		},
		// A slow attempt that fails after a deadline-style budget has elapsed must still be
		// followed by another attempt (the proxy RPC timeout is independent of any poll deadline).
		"slow failing attempt is still retried": {
			failUntil:      1,
			firstCallDelay: 50 * time.Millisecond,
			engineOwner:    TestNode1,
			expectCalls:    2,
		},
		// Ownership is checked before every attempt: a node that no longer owns the engine
		// must not purge or record anything on its behalf.
		"no attempt when engine ownership is lost": {
			failUntil:   1000,
			engineOwner: TestNode2,
			expectCalls: 0,
		},
		// A later successful purge start clears a previously recorded failure.
		"success clears a previous error": {
			failUntil:      0,
			engineOwner:    TestNode1,
			prevPurgeError: "previous failure",
			expectCalls:    1,
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

			recorder := record.NewFakeRecorder(100)
			ec := &EngineController{
				baseController:            newBaseController("longhorn-engine", logrus.New()),
				controllerID:              TestNode1,
				ds:                        datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories),
				eventRecorder:             recorder,
				snapshotConcurrentLimiter: NewSnapshotConcurrentLimiter(),
			}

			// Empty DataEngine means v1: GetDataEngineObject returns the engine itself.
			engine := &longhorn.Engine{
				ObjectMeta: metav1.ObjectMeta{Name: TestEngineName, Namespace: TestNamespace},
				Spec: longhorn.EngineSpec{
					InstanceSpec: longhorn.InstanceSpec{VolumeName: TestVolumeName},
				},
				Status: longhorn.EngineStatus{
					InstanceStatus:        longhorn.InstanceStatus{OwnerID: tc.engineOwner},
					PostRebuildPurgeError: tc.prevPurgeError,
				},
			}
			// The engine must exist in the API (for UpdateEngineStatus) and in the lister (for GetEngine).
			created, err := lhClient.LonghornV1beta2().Engines(TestNamespace).Create(context.TODO(), engine, metav1.CreateOptions{})
			assert.NoError(err)
			eIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Engines().Informer().GetIndexer()
			assert.NoError(eIndexer.Add(created))

			proxy := &purgeMockProxy{
				EngineSimulator: &engineapi.EngineSimulator{},
				failUntil:       tc.failUntil,
				firstCallDelay:  tc.firstCallDelay,
			}

			logger := logrus.New()
			logger.Out = io.Discard
			ec.startPostRebuildPurge(logrus.NewEntry(logger), created, proxy)

			assert.Equal(tc.expectCalls, proxy.purgeCalls, "SnapshotPurge attempt count")

			// Event
			events := drainFakeEvents(recorder)
			if tc.expectEvent {
				assert.Len(events, 1, "expected exactly one failure event")
				assert.Contains(events[0], constant.EventReasonFailedStartingSnapshotPurge)
				assert.Contains(events[0], tc.expectErrorText, "event should report the last purge error")
			} else {
				assert.Empty(events, "no FailedStartingSnapshotPurge event expected")
			}

			// Engine status
			updated, err := lhClient.LonghornV1beta2().Engines(TestNamespace).Get(context.TODO(), TestEngineName, metav1.GetOptions{})
			assert.NoError(err)
			switch {
			case tc.expectErrorText != "":
				assert.Contains(updated.Status.PostRebuildPurgeError, tc.expectErrorText, "engine status should report the last purge error")
				assert.Contains(updated.Status.PostRebuildPurgeError, "(3 attempts)")
			case tc.engineOwner != TestNode1:
				// Not the owner: status must be left untouched.
				assert.Equal(tc.prevPurgeError, updated.Status.PostRebuildPurgeError)
			default:
				assert.Empty(updated.Status.PostRebuildPurgeError, "engine status should be clear after a successful purge start")
			}
			// PurgeStatus is engine-reported per replica and must never be written by this path.
			assert.Empty(updated.Status.PurgeStatus, "PurgeStatus must not be touched")
		})
	}
}

// setPostRebuildPurgeError runs once per rebuild, so a conflict must not drop the result.
// The informer cache can lag behind the API server; retries must read the latest object
// instead of resubmitting the stale cached resourceVersion, and the ownership and equality
// checks must use that latest object too.
func TestSetPostRebuildPurgeErrorUsesFreshEngine(t *testing.T) {
	// The lister is intentionally stale in this test and never catches up, so keep the
	// datastore's post-update verification short.
	origVerifyCounts := datastore.VerificationRetryCounts
	datastore.VerificationRetryCounts = 1
	defer func() { datastore.VerificationRetryCounts = origVerifyCounts }()

	engineGVR := longhorn.SchemeGroupVersion.WithResource("engines")

	tests := map[string]struct {
		apiOwner       string // owner in the newer API object (the lister always says TestNode1)
		apiPrevError   string // PostRebuildPurgeError in the newer API object (the lister says "")
		purgeErr       error
		expectValue    string // expected PostRebuildPurgeError in the API after the call
		expectNoUpdate bool
	}{
		// Old behavior: every retry resubmits resourceVersion "1" from the lister and conflicts,
		// so the failure is never recorded.
		"records failure against a newer API object": {
			apiOwner:    TestNode1,
			purgeErr:    errors.New("injected purge failure"),
			expectValue: "failed to start snapshot purge after rebuilding (3 attempts): injected purge failure",
		},
		// Old behavior: the stale lister already shows "" so the clear is skipped entirely.
		"clears a previous error that only the API object has": {
			apiOwner:     TestNode1,
			apiPrevError: "previous failure",
			purgeErr:     nil,
			expectValue:  "",
		},
		// The lister still says this node owns the engine, but the API says otherwise.
		"does not write when the latest owner is another node": {
			apiOwner:       TestNode2,
			purgeErr:       errors.New("injected purge failure"),
			expectValue:    "",
			expectNoUpdate: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert := require.New(t)
			datastore.SkipListerCheck = true

			origCount := postRebuildPurgeRetryCount
			postRebuildPurgeRetryCount = 3
			defer func() { postRebuildPurgeRetryCount = origCount }()

			kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
			lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
			extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
			informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)

			ec := &EngineController{
				baseController: newBaseController("longhorn-engine", logrus.New()),
				controllerID:   TestNode1,
				ds:             datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories),
				eventRecorder:  record.NewFakeRecorder(100),
			}

			// Stale copy: in the lister at resourceVersion "1", owned by this node, no error.
			stale := &longhorn.Engine{
				ObjectMeta: metav1.ObjectMeta{Name: TestEngineName, Namespace: TestNamespace, ResourceVersion: "1"},
				Spec: longhorn.EngineSpec{
					InstanceSpec: longhorn.InstanceSpec{VolumeName: TestVolumeName},
				},
				Status: longhorn.EngineStatus{
					InstanceStatus: longhorn.InstanceStatus{OwnerID: TestNode1},
				},
			}
			_, err := lhClient.LonghornV1beta2().Engines(TestNamespace).Create(context.TODO(), stale.DeepCopy(), metav1.CreateOptions{})
			assert.NoError(err)
			eIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Engines().Informer().GetIndexer()
			assert.NoError(eIndexer.Add(stale.DeepCopy()))

			// A concurrent writer (e.g. the engine monitor) moved the API object to resourceVersion "2".
			latest := stale.DeepCopy()
			latest.ResourceVersion = "2"
			latest.Status.OwnerID = tc.apiOwner
			latest.Status.PostRebuildPurgeError = tc.apiPrevError
			latest.Status.CurrentState = longhorn.InstanceStateRunning // must be preserved by our update
			assert.NoError(lhClient.Tracker().Update(engineGVR, latest, TestNamespace))

			// Emulate API server optimistic concurrency: reject status updates with a stale resourceVersion.
			statusUpdates := 0
			lhClient.PrependReactor("update", "engines", func(action clienttesting.Action) (bool, runtime.Object, error) {
				update, ok := action.(clienttesting.UpdateAction)
				if !ok || update.GetSubresource() != "status" {
					return false, nil, nil
				}
				statusUpdates++
				submitted, ok := update.GetObject().(*longhorn.Engine)
				if !ok {
					return false, nil, nil
				}
				current, err := lhClient.Tracker().Get(engineGVR, TestNamespace, submitted.Name)
				if err != nil {
					return true, nil, err
				}
				currentEngine, ok := current.(*longhorn.Engine)
				if !ok || submitted.ResourceVersion != currentEngine.ResourceVersion {
					return true, nil, apierrors.NewConflict(longhorn.Resource("engines"), submitted.Name,
						errors.Newf("stale resourceVersion %q", submitted.ResourceVersion))
				}
				return false, nil, nil
			})

			logger := logrus.New()
			logger.Out = io.Discard
			ec.setPostRebuildPurgeError(logrus.NewEntry(logger), TestEngineName, tc.purgeErr)

			updated, err := lhClient.LonghornV1beta2().Engines(TestNamespace).Get(context.TODO(), TestEngineName, metav1.GetOptions{})
			assert.NoError(err)
			assert.Equal(tc.expectValue, updated.Status.PostRebuildPurgeError, "PostRebuildPurgeError in the API object")
			assert.Equal(longhorn.InstanceStateRunning, updated.Status.CurrentState, "concurrent writer's status must be preserved")
			if tc.expectNoUpdate {
				assert.Zero(statusUpdates, "a non-owner must not update engine status")
			} else {
				assert.Equal(1, statusUpdates, "the first attempt should already use the latest resourceVersion")
			}
		})
	}
}
