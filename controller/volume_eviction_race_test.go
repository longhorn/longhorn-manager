package controller

import (
	"context"
	"sort"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

// evictionRaceTrials increases the chance of exposing inconsistent replica
// selection across two passes when the choice depends on map iteration order.
const evictionRaceTrials = 50

var evictionRaceNodes = []string{TestNode1, TestNode2, "test-node-name-3"}

type evictionCleanupFixture struct {
	vc       *VolumeController
	ds       *datastore.DataStore
	lhClient *lhfake.Clientset
	indexer  cache.Indexer
	volume   *longhorn.Volume
	engine   *longhorn.Engine
	replicas []*longhorn.Replica
}

// newEvictionCleanupFixture creates the given number of healthy evicting replicas
// sorted by name for a volume requesting numberOfReplicas. If provided, prepare modifies the
// sorted replicas before they enter the fake client and informer cache.
func newEvictionCleanupFixture(c *C, replicas, numberOfReplicas int, prepare func([]*longhorn.Replica)) *evictionCleanupFixture {
	volume := newVolume(TestVolumeName, numberOfReplicas)
	volume.Status.State = longhorn.VolumeStateAttached
	volume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	engine := newEngineForVolume(volume)

	var created []*longhorn.Replica
	for _, nodeID := range evictionRaceNodes[:replicas] {
		r := newReplicaForVolume(volume, engine, nodeID, TestDiskID1)
		r.Namespace = TestNamespace
		r.Spec.HealthyAt = TestTimeNow
		r.Spec.LastHealthyAt = TestTimeNow
		r.Spec.EvictionRequested = true
		r.Status.CurrentState = longhorn.InstanceStateRunning
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		created = append(created, r)
	}
	sort.Slice(created, func(i, j int) bool { return created[i].Name < created[j].Name })
	if prepare != nil {
		prepare(created)
	}

	objects := []runtime.Object{volume, engine}
	for _, r := range created {
		objects = append(objects, r)
	}
	lhClient := lhfake.NewSimpleClientset(objects...)          // nolint: staticcheck
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
	indexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
	for _, r := range created {
		c.Assert(indexer.Add(r), IsNil)
	}

	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
	vc := &VolumeController{
		baseController: newBaseController("test-volume", logrus.StandardLogger()),
		ds:             ds,
		eventRecorder:  record.NewFakeRecorder(10),
		nowHandler:     getTestNow,
	}
	return &evictionCleanupFixture{vc: vc, ds: ds, lhClient: lhClient, indexer: indexer, volume: volume, engine: engine, replicas: created}
}

// cleanup runs one cleanup pass over the replicas in the informer cache, as
// syncVolume lists them.
func (f *evictionCleanupFixture) cleanup(c *C) error {
	rs, err := f.ds.ListVolumeReplicas(f.volume.Name)
	c.Assert(err, IsNil)
	return f.vc.cleanupExtraHealthyReplicas(f.volume, f.engine, rs)
}

func (f *evictionCleanupFixture) liveReplicas(c *C) map[string]bool {
	live, err := f.lhClient.LonghornV1beta2().Replicas(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	names := map[string]bool{}
	for i := range live.Items {
		names[live.Items[i].Name] = true
	}
	return names
}

// runEvictionCleanupTwice runs two cleanup passes over healthy evicting replicas
// and reports whether the final replica count and second pass error match expectations.
// The fake client should retain numberOfReplicas replicas. If refreshCache is true,
// the cache reflects the first deletion, so the second pass has nothing to do and
// returns nil. Otherwise, it retries the deletion already applied by the fake client
// and returns NotFound.
func runEvictionCleanupTwice(c *C, replicas, numberOfReplicas int, refreshCache bool) bool {
	f := newEvictionCleanupFixture(c, replicas, numberOfReplicas, nil)
	c.Assert(f.cleanup(c), IsNil)

	if refreshCache {
		live := f.liveReplicas(c)
		for _, r := range f.replicas {
			if !live[r.Name] {
				c.Assert(f.indexer.Delete(r), IsNil)
			}
		}
	}

	err := f.cleanup(c)
	errAsExpected := err == nil
	if !refreshCache {
		errAsExpected = apierrors.IsNotFound(err)
	}
	return errAsExpected && len(f.liveReplicas(c)) == numberOfReplicas
}

func assertEvictionCleanupKeeps(c *C, replicas, numberOfReplicas int, refreshCache bool) {
	wrong := 0
	for i := 0; i < evictionRaceTrials; i++ {
		if !runEvictionCleanupTwice(c, replicas, numberOfReplicas, refreshCache) {
			wrong++
		}
	}
	c.Assert(wrong, Equals, 0, Commentf("%d of %d trials ended with a replica count other than %d or an unexpected second pass error", wrong, evictionRaceTrials, numberOfReplicas))
}

func (s *TestSuite) TestEvictionCleanupKeepsAReplicaWhenTheCacheLagsBehindADeletion(c *C) {
	assertEvictionCleanupKeeps(c, 2, 1, false)
}

func (s *TestSuite) TestEvictionCleanupDeletesOneReplicaWhenTheCacheLagsBehindADeletion(c *C) {
	assertEvictionCleanupKeeps(c, 3, 2, false)
}

func (s *TestSuite) TestEvictionCleanupKeepsAReplicaWhenTheCacheIsCurrent(c *C) {
	assertEvictionCleanupKeeps(c, 2, 1, true)
}

func (s *TestSuite) TestEvictionCleanupSkipsATerminatingReplica(c *C) {
	// The first replica is retained and the second is already terminating, so
	// the pass must move on to the third.
	f := newEvictionCleanupFixture(c, 3, 1, func(rs []*longhorn.Replica) {
		now := metav1.Now()
		rs[1].DeletionTimestamp = &now
		rs[1].Finalizers = []string{longhornFinalizerKey}
	})
	c.Assert(f.cleanup(c), IsNil)

	live := f.liveReplicas(c)
	c.Assert(live[f.replicas[0].Name], Equals, true)
	c.Assert(live[f.replicas[1].Name], Equals, true)
	c.Assert(live[f.replicas[2].Name], Equals, false)
}
