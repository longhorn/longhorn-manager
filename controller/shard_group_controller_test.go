package controller

import (
	"context"

	"github.com/sirupsen/logrus"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"

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
