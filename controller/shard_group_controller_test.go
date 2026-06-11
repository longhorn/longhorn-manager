package controller

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	spdkrpc "github.com/longhorn/types/pkg/generated/spdkrpc"

	. "gopkg.in/check.v1"
)

// ShardGroupControllerSuite covers the syncProcess paths that do not require
// mocking the InstanceManager gRPC client. These are the readiness-gate guards
// and Status-management helpers - the points where we want strong guarantees
// against accidental "no-shards-yet but provision anyway" regressions.
//
// RPC-issuing paths (provision, refresh, teardown) are exercised by the
// integration test suite in longhorn-tests, not here.
type ShardGroupControllerSuite struct {
	kubeClient       *fake.Clientset
	lhClient         *lhfake.Clientset
	extensionsClient *apiextensionsfake.Clientset

	informerFactories *util.InformerFactories

	lhShardGroupIndexer cache.Indexer
	lhShardIndexer      cache.Indexer
	lhVolumeIndexer     cache.Indexer

	controller *ShardGroupController
}

var _ = Suite(&ShardGroupControllerSuite{})

func (s *ShardGroupControllerSuite) SetUpTest(c *C) {
	s.kubeClient = fake.NewSimpleClientset()                    // nolint: staticcheck
	s.lhClient = lhfake.NewSimpleClientset()                    // nolint: staticcheck
	s.extensionsClient = apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	s.informerFactories = util.NewInformerFactories(TestNamespace, s.kubeClient, s.lhClient, controller.NoResyncPeriodFunc())

	s.lhShardGroupIndexer = s.informerFactories.LhInformerFactory.Longhorn().V1beta2().ShardGroups().Informer().GetIndexer()
	s.lhShardIndexer = s.informerFactories.LhInformerFactory.Longhorn().V1beta2().Shards().Informer().GetIndexer()
	s.lhVolumeIndexer = s.informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()

	var err error
	s.controller, err = newTestShardGroupController(s.lhClient, s.kubeClient, s.extensionsClient, s.informerFactories, TestNode1)
	c.Assert(err, IsNil)
}

func newTestShardGroupController(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, controllerID string) (*ShardGroupController, error) {

	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	logger := logrus.StandardLogger()
	c, err := NewShardGroupController(logger, ds, scheme.Scheme, kubeClient, controllerID, TestNamespace)
	if err != nil {
		return nil, err
	}

	c.eventRecorder = record.NewFakeRecorder(eventRecorderBufferSize)
	for index := range c.cacheSyncs {
		c.cacheSyncs[index] = alwaysReady
	}
	return c, nil
}

func newTestShardGroup(name string, k, m int, nodeID string) *longhorn.ShardGroup {
	return &longhorn.ShardGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
		},
		Spec: longhorn.ShardGroupSpec{
			VolumeName:   name,
			DataChunks:   k,
			ParityChunks: m,
			StripSizeKB:  64,
			NodeID:       nodeID,
		},
	}
}

// TestSyncProcessNoOpWhenNodeIDEmpty verifies that syncProcess is a no-op
// before the Volume controller has bound the ShardGroup to a node. This
// guards against accidental InstanceCreate calls in the freshly-created
// detached state.
func (s *ShardGroupControllerSuite) TestSyncProcessNoOpWhenNodeIDEmpty(c *C) {
	sg := newTestShardGroup("vol-no-node", 4, 2, "")
	created, err := s.lhClient.LonghornV1beta2().ShardGroups(TestNamespace).Create(
		context.TODO(), sg, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(s.lhShardGroupIndexer.Add(created), IsNil)

	rctx := &sgReconcileCtx{
		shardGroup: created,
		log:        logrus.NewEntry(logrus.StandardLogger()),
	}

	c.Assert(s.controller.syncProcess(rctx), IsNil)
	c.Assert(created.Status.ProcessState, Equals, longhorn.InstanceState(""))
	c.Assert(created.Status.StorageIP, Equals, "")
}

// TestSyncProcessNoOpWhenAddressMapIncomplete verifies the readiness gate's
// first leg: even with Spec.NodeID set, syncProcess must not provision the
// ShardGroup process until ECShardAddressMap has all k+m entries. This guards
// against the "stale address from a stopped SPDK process" regression.
func (s *ShardGroupControllerSuite) TestSyncProcessNoOpWhenAddressMapIncomplete(c *C) {
	sg := newTestShardGroup("vol-incomplete-map", 4, 2, TestNode1)
	// Only 5 entries; expected k+m = 6.
	sg.Status.ECShardAddressMap = map[string]string{
		"0": "10.0.0.10:20011",
		"1": "10.0.0.11:20011",
		"2": "10.0.0.12:20011",
		"3": "10.0.0.13:20011",
		"4": "10.0.0.14:20011",
	}
	created, err := s.lhClient.LonghornV1beta2().ShardGroups(TestNamespace).Create(
		context.TODO(), sg, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(s.lhShardGroupIndexer.Add(created), IsNil)

	rctx := &sgReconcileCtx{
		shardGroup: created,
		log:        logrus.NewEntry(logrus.StandardLogger()),
	}

	c.Assert(s.controller.syncProcess(rctx), IsNil)
	c.Assert(created.Status.ProcessState, Equals, longhorn.InstanceState(""))
}

// TestSyncProcessNoOpWhenShardNotNormal verifies the readiness gate's second
// leg: even with all k+m address entries present, syncProcess must not
// provision when any Shard CR is not in ShardStateNormal. Address presence
// alone is not a liveness signal - Status.StorageIP/Port can persist across
// SPDK restarts after the IM has re-allocated the port.
func (s *ShardGroupControllerSuite) TestSyncProcessNoOpWhenShardNotNormal(c *C) {
	sg := newTestShardGroup("vol-shard-not-normal", 2, 1, TestNode1)
	sg.Status.ECShardAddressMap = map[string]string{
		"0": "10.0.0.10:20011",
		"1": "10.0.0.11:20011",
		"2": "10.0.0.12:20011",
	}
	created, err := s.lhClient.LonghornV1beta2().ShardGroups(TestNamespace).Create(
		context.TODO(), sg, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(s.lhShardGroupIndexer.Add(created), IsNil)

	// Two normal shards, one in Failed state.
	shards := map[string]*longhorn.Shard{
		"vol-shard-not-normal-0": {Status: longhorn.ShardStatus{State: longhorn.ShardStateNormal}},
		"vol-shard-not-normal-1": {Status: longhorn.ShardStatus{State: longhorn.ShardStateNormal}},
		"vol-shard-not-normal-2": {Status: longhorn.ShardStatus{State: longhorn.ShardStateFailed}},
	}

	rctx := &sgReconcileCtx{
		shardGroup: created,
		shards:     shards,
		log:        logrus.NewEntry(logrus.StandardLogger()),
	}

	c.Assert(s.controller.syncProcess(rctx), IsNil)
	c.Assert(created.Status.ProcessState, Equals, longhorn.InstanceState(""))
}

// TestClearShardGroupProcessStatusPreservesLvstoreIdentity verifies the helper used after
// teardown / re-bind correctly resets all process-state Status fields. Stale
// IP/Port/NQN values surviving past teardown would mislead the volume
// controller's EC readiness check into thinking the process is still up.
// LvstoreUUID and HeadLvolUUID are deliberately
// preserved across teardown - they identify the persistent on-disk lvstore
// living encoded across the shards, and the next reconcile reads them to
// drive salvage=true on re-provision.
func (s *ShardGroupControllerSuite) TestClearShardGroupProcessStatusPreservesLvstoreIdentity(c *C) {
	sg := newTestShardGroup("vol-clear", 2, 1, TestNode1)
	sg.Status.InstanceManagerName = "old-im"
	sg.Status.StorageIP = "10.0.0.99"
	sg.Status.Port = 20100
	sg.Status.NQN = "nqn.longhorn:shardgroup-vol-clear"
	sg.Status.LvstoreUUID = "deadbeef"
	sg.Status.HeadLvolUUID = "cafebabe"
	sg.Status.ProcessState = longhorn.InstanceStateRunning

	rctx := &sgReconcileCtx{
		shardGroup: sg,
		log:        logrus.NewEntry(logrus.StandardLogger()),
	}

	s.controller.clearShardGroupProcessStatus(rctx)

	// Runtime fields cleared.
	c.Assert(sg.Status.InstanceManagerName, Equals, "")
	c.Assert(sg.Status.StorageIP, Equals, "")
	c.Assert(sg.Status.Port, Equals, int32(0))
	c.Assert(sg.Status.NQN, Equals, "")
	c.Assert(sg.Status.ProcessState, Equals, longhorn.InstanceState(""))

	// Persistent lvstore identity preserved - the lvstore lives on the
	// encoded shards and survives any IM/process teardown.
	c.Assert(sg.Status.LvstoreUUID, Equals, "deadbeef")
	c.Assert(sg.Status.HeadLvolUUID, Equals, "cafebabe")
}

// TestClearShardGroupProcessStatusZerosIntentionalDeleteSlots verifies the
// defensive clear of ShardGroup.Status.IntentionalDeleteSlots on process
// teardown / re-bind. Without this, a stale bypass entry from a pre-rebind
// force-fail would erroneously skip the debounce on an unintentional failure
// targeting the same slot index after the rebind.
func (s *ShardGroupControllerSuite) TestClearShardGroupProcessStatusZerosIntentionalDeleteSlots(c *C) {
	sg := newTestShardGroup("vol-clear-intent", 2, 1, TestNode1)
	sg.Status.IntentionalDeleteSlots = []int{1, 2}
	sg.Status.ProcessState = longhorn.InstanceStateRunning

	rctx := &sgReconcileCtx{
		shardGroup: sg,
		log:        logrus.NewEntry(logrus.StandardLogger()),
	}

	s.controller.clearShardGroupProcessStatus(rctx)

	c.Assert(sg.Status.IntentionalDeleteSlots, IsNil)
}

// fakeForceFailClient is a minimal SPDKServiceClient stub for forceFailIfIntentional
// tests. Embedding the interface satisfies the type without implementing every
// method; only ShardGroupShardForceFail is overridden, and any unexpected method
// call would panic on the nil embedded interface - which is the intended fail-loud
// behavior for tests that should not exercise other RPCs.
type fakeForceFailClient struct {
	spdkrpc.SPDKServiceClient

	called   int
	lastReq  *spdkrpc.ShardGroupShardForceFailRequest
	respCode codes.Code // codes.OK means success
}

func (f *fakeForceFailClient) ShardGroupShardForceFail(ctx context.Context, in *spdkrpc.ShardGroupShardForceFailRequest, opts ...grpc.CallOption) (*spdkrpc.ShardGroupShardForceFailResponse, error) {
	f.called++
	f.lastReq = in
	if f.respCode != codes.OK {
		return nil, status.Error(f.respCode, "fake error")
	}
	return &spdkrpc.ShardGroupShardForceFailResponse{
		SlotState: spdkrpc.EcSlotState_EC_SLOT_STATE_FAILED,
	}, nil
}

func newTestShard(sg *longhorn.ShardGroup, slotIndex int, state longhorn.ShardState) *longhorn.Shard {
	return &longhorn.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      newShardName(sg.Name, slotIndex),
			Namespace: TestNamespace,
		},
		Spec: longhorn.ShardSpec{
			ShardGroupName: sg.Name,
			SlotIndex:      slotIndex,
		},
		Status: longhorn.ShardStatus{
			State: state,
		},
	}
}

func newShardName(sgName string, slot int) string {
	return fmt.Sprintf("%s-%d", sgName, slot)
}

// TestForceFailIfIntentionalNoOpWhenSpdkClientNil verifies the fast path is
// skipped silently when the engine is detached (no SPDK client). Slow path
// proceeds via the existing cleanupShard logic.
func (s *ShardGroupControllerSuite) TestForceFailIfIntentionalNoOpWhenSpdkClientNil(c *C) {
	sg := newTestShardGroup("vol-no-spdk", 2, 1, TestNode1)
	created, err := s.lhClient.LonghornV1beta2().ShardGroups(TestNamespace).Create(
		context.TODO(), sg, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	shard := newTestShard(created, 0, longhorn.ShardStateNormal)

	rctx := &sgReconcileCtx{
		shardGroup: created,
		log:        logrus.NewEntry(logrus.StandardLogger()),
		// spdkClient deliberately nil
	}

	c.Assert(s.controller.forceFailIfIntentional(rctx, shard), IsNil)
	c.Assert(created.Status.IntentionalDeleteSlots, IsNil)
}

// TestForceFailIfIntentionalNoOpWhenSlotFailed verifies the fast path is a
// no-op when the slot is already FAILED - no acceleration is needed and the
// standard slow path (cleanupShard -> InstanceDelete) handles teardown.
func (s *ShardGroupControllerSuite) TestForceFailIfIntentionalNoOpWhenSlotFailed(c *C) {
	sg := newTestShardGroup("vol-slot-failed", 2, 1, TestNode1)
	created, err := s.lhClient.LonghornV1beta2().ShardGroups(TestNamespace).Create(
		context.TODO(), sg, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	shard := newTestShard(created, 0, longhorn.ShardStateFailed)

	fake := &fakeForceFailClient{}
	rctx := &sgReconcileCtx{
		shardGroup: created,
		spdkClient: fake,
		log:        logrus.NewEntry(logrus.StandardLogger()),
	}

	c.Assert(s.controller.forceFailIfIntentional(rctx, shard), IsNil)
	c.Assert(fake.called, Equals, 0)
	c.Assert(created.Status.IntentionalDeleteSlots, IsNil)
}

// TestForceFailIfIntentionalRecordsSlotAndCallsRPC verifies the happy path:
// slot Normal + spdk client present -> annotation set on Shard, slot recorded
// in Status.IntentionalDeleteSlots, and ShardGroupShardForceFail issued.
func (s *ShardGroupControllerSuite) TestForceFailIfIntentionalRecordsSlotAndCallsRPC(c *C) {
	sg := newTestShardGroup("vol-force-fail", 2, 1, TestNode1)
	created, err := s.lhClient.LonghornV1beta2().ShardGroups(TestNamespace).Create(
		context.TODO(), sg, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	shard := newTestShard(created, 1, longhorn.ShardStateNormal)
	createdShard, err := s.lhClient.LonghornV1beta2().Shards(TestNamespace).Create(
		context.TODO(), shard, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(s.lhShardIndexer.Add(createdShard), IsNil)

	fake := &fakeForceFailClient{respCode: codes.OK}
	rctx := &sgReconcileCtx{
		shardGroup: created,
		spdkClient: fake,
		log:        logrus.NewEntry(logrus.StandardLogger()),
	}

	c.Assert(s.controller.forceFailIfIntentional(rctx, createdShard), IsNil)

	// RPC was called with the right arguments.
	c.Assert(fake.called, Equals, 1)
	c.Assert(fake.lastReq.ShardGroupName, Equals, "vol-force-fail")
	c.Assert(fake.lastReq.ShardName, Equals, createdShard.Name)

	// Slot recorded in status (so the replacement bypasses the debounce).
	c.Assert(created.Status.IntentionalDeleteSlots, DeepEquals, []int{1})

	// Annotation persisted on the dying Shard CR.
	persisted, err := s.lhClient.LonghornV1beta2().Shards(TestNamespace).Get(
		context.TODO(), createdShard.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(persisted.Annotations[types.ShardAnnotationIntentionalDelete], Equals, "true")
}

// TestForceFailIfIntentionalFallsThroughOnFailedPrecondition verifies that a
// FailedPrecondition from spdk-engine (e.g. the slot is already mid-failure) does
// not surface as an error: the slow path proceeds and the slot is still recorded.
func (s *ShardGroupControllerSuite) TestForceFailIfIntentionalFallsThroughOnFailedPrecondition(c *C) {
	sg := newTestShardGroup("vol-failed-precond", 2, 1, TestNode1)
	created, err := s.lhClient.LonghornV1beta2().ShardGroups(TestNamespace).Create(
		context.TODO(), sg, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	shard := newTestShard(created, 0, longhorn.ShardStateNormal)
	createdShard, err := s.lhClient.LonghornV1beta2().Shards(TestNamespace).Create(
		context.TODO(), shard, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(s.lhShardIndexer.Add(createdShard), IsNil)

	fake := &fakeForceFailClient{respCode: codes.FailedPrecondition}
	rctx := &sgReconcileCtx{
		shardGroup: created,
		spdkClient: fake,
		log:        logrus.NewEntry(logrus.StandardLogger()),
	}

	// FailedPrecondition must not surface as an error to the caller (cleanupShard).
	c.Assert(s.controller.forceFailIfIntentional(rctx, createdShard), IsNil)
	c.Assert(fake.called, Equals, 1)
	// Slot is recorded before the RPC, so the replacement still skips the debounce.
	c.Assert(created.Status.IntentionalDeleteSlots, DeepEquals, []int{0})
}

// TestForceFailIfIntentionalPropagatesUnexpectedRPCError checks that an RPC error other
// than FailedPrecondition propagates to the caller, and that the slot is still recorded
// because recording happens before the RPC.
func (s *ShardGroupControllerSuite) TestForceFailIfIntentionalPropagatesUnexpectedRPCError(c *C) {
	sg := newTestShardGroup("vol-rpc-error", 2, 1, TestNode1)
	created, err := s.lhClient.LonghornV1beta2().ShardGroups(TestNamespace).Create(
		context.TODO(), sg, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	shard := newTestShard(created, 0, longhorn.ShardStateNormal)
	createdShard, err := s.lhClient.LonghornV1beta2().Shards(TestNamespace).Create(
		context.TODO(), shard, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(s.lhShardIndexer.Add(createdShard), IsNil)

	fake := &fakeForceFailClient{respCode: codes.Internal}
	rctx := &sgReconcileCtx{
		shardGroup: created,
		spdkClient: fake,
		log:        logrus.NewEntry(logrus.StandardLogger()),
	}

	// An unexpected RPC error must propagate so the reconcile retries.
	c.Assert(s.controller.forceFailIfIntentional(rctx, createdShard), NotNil)
	c.Assert(fake.called, Equals, 1)
	// Slot is recorded before the RPC is attempted.
	c.Assert(created.Status.IntentionalDeleteSlots, DeepEquals, []int{0})
}

// TestShouldDelayReplaceBypassesForIntentionalSlot verifies that the debounce
// is skipped for slots in IntentionalDeleteSlots even when LastFailureTimestamp
// is fresh and the replenishment-wait setting would otherwise force a delay.
func (s *ShardGroupControllerSuite) TestShouldDelayReplaceBypassesForIntentionalSlot(c *C) {
	sg := newTestShardGroup("vol-bypass", 2, 1, TestNode1)
	sg.Status.IntentionalDeleteSlots = []int{2}

	shard := newTestShard(sg, 2, longhorn.ShardStateFailed)
	// Freshly observed failure - without the bypass, shouldDelayReplace would
	// return true and stall the replace until the replenishment interval elapses.
	shard.Status.LastFailureTimestamp = util.Now()

	rctx := &sgReconcileCtx{
		shardGroup: sg,
		log:        logrus.NewEntry(logrus.StandardLogger()),
	}

	c.Assert(s.controller.shouldDelayReplace(rctx, shard), Equals, false)
}

// TestClearCompletedIntentionalDeleteSlotsDropsHealthySlots verifies that
// once a replacement Shard CR reaches Normal+StorageIP, its slot is removed
// from ShardGroup.Status.IntentionalDeleteSlots so the bypass does not
// linger past the recovery cycle.
func (s *ShardGroupControllerSuite) TestClearCompletedIntentionalDeleteSlotsDropsHealthySlots(c *C) {
	sg := newTestShardGroup("vol-clear-completed", 2, 1, TestNode1)
	sg.Status.IntentionalDeleteSlots = []int{0, 1, 2}
	created, err := s.lhClient.LonghornV1beta2().ShardGroups(TestNamespace).Create(
		context.TODO(), sg, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	// slot 0: replacement shard is Normal with StorageIP - should drop.
	healthy := newTestShard(created, 0, longhorn.ShardStateNormal)
	healthy.Status.StorageIP = "10.0.0.10"

	// slot 1: replacement shard exists but is still Failed - should keep.
	stillFailing := newTestShard(created, 1, longhorn.ShardStateFailed)
	stillFailing.Status.StorageIP = "10.0.0.11"

	// slot 2: replacement shard not yet provisioned (missing from map) - should keep.

	rctx := &sgReconcileCtx{
		shardGroup: created,
		shards: map[string]*longhorn.Shard{
			healthy.Name:      healthy,
			stillFailing.Name: stillFailing,
		},
		log: logrus.NewEntry(logrus.StandardLogger()),
	}

	c.Assert(s.controller.clearCompletedIntentionalDeleteSlots(rctx), IsNil)

	c.Assert(created.Status.IntentionalDeleteSlots, DeepEquals, []int{1, 2})
}

// TestConcurrentShardRebuildLimit verifies the per-node rebuild limit is enforced
// strictly: an in-memory reservation blocks a sibling ShardGroup even before the first
// group's RebuildInProgress flag has propagated, and the slot frees once the rebuild
// finishes.
func (s *ShardGroupControllerSuite) TestConcurrentShardRebuildLimit(c *C) {
	settingIndexer := s.informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	c.Assert(settingIndexer.Add(newSetting(string(types.SettingNameConcurrentReplicaRebuildPerNodeLimit), "1")), IsNil)

	sgA := newTestShardGroup("vol-rebuild-a", 4, 2, TestNode1)
	sgB := newTestShardGroup("vol-rebuild-b", 4, 2, TestNode1)
	c.Assert(s.lhShardGroupIndexer.Add(sgA), IsNil)
	c.Assert(s.lhShardGroupIndexer.Add(sgB), IsNil)

	// A takes the only slot via an in-memory reservation. Its RebuildInProgress is still
	// false, so a bare cache count would not see it - the reservation is what holds the slot.
	can, err := s.controller.canStartShardRebuild(sgA)
	c.Assert(err, IsNil)
	c.Assert(can, Equals, true)

	// B is blocked strictly by A's reservation, before any durable flag is set.
	can, err = s.controller.canStartShardRebuild(sgB)
	c.Assert(err, IsNil)
	c.Assert(can, Equals, false)

	// A asking again is idempotent - it already holds the slot, does not consume a second.
	can, err = s.controller.canStartShardRebuild(sgA)
	c.Assert(err, IsNil)
	c.Assert(can, Equals, true)

	// A's rebuild is now actually running: the durable flag carries the count and B stays blocked.
	sgA.Status.RebuildInProgress = true
	c.Assert(s.lhShardGroupIndexer.Update(sgA), IsNil)
	can, err = s.controller.canStartShardRebuild(sgB)
	c.Assert(err, IsNil)
	c.Assert(can, Equals, false)

	// A finishes: the freed slot lets B proceed.
	sgA.Status.RebuildInProgress = false
	c.Assert(s.lhShardGroupIndexer.Update(sgA), IsNil)
	can, err = s.controller.canStartShardRebuild(sgB)
	c.Assert(err, IsNil)
	c.Assert(can, Equals, true)
}
