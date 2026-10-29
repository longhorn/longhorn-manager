package scheduler

import (
	"context"
	"fmt"

	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/go-common-libs/multierr"

	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

// antiAffinityFixture is a three-node cluster with one schedulable disk per
// node, into which volumes and placed replicas can be registered.
type antiAffinityFixture struct {
	c        *C
	lhClient *lhfake.Clientset
	rcs      *ReplicaScheduler
	nIndexer cache.Indexer
	vIndexer cache.Indexer
	rIndexer cache.Indexer
}

func newAntiAffinityFixture(c *C) *antiAffinityFixture {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	eiIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

	f := &antiAffinityFixture{
		c:        c,
		lhClient: lhClient,
		rcs:      newReplicaScheduler(lhClient, kubeClient, extensionsClient, informerFactories),
		nIndexer: informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer(),
		vIndexer: informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer(),
		rIndexer: informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer(),
	}

	engineImage := newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed)
	for i, nodeName := range []string{TestNode1, TestNode2, TestNode3} {
		daemon := newDaemonPod(corev1.PodRunning, fmt.Sprintf("longhorn-manager-%d", i), TestNamespace, nodeName, fmt.Sprintf("10.0.0.%d", i))
		p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), daemon, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(pIndexer.Add(p), IsNil)

		node := newNode(nodeName, TestNamespace, TestZone1, true, longhorn.ConditionStatusTrue)
		node.Spec.Disks = map[string]longhorn.DiskSpec{}
		node.Status.DiskStatus = map[string]*longhorn.DiskStatus{}
		addAntiAffinityTestDisk(node, "1")
		n, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(f.nIndexer.Add(n), IsNil)
		engineImage.Status.NodeDeploymentMap[nodeName] = true

		im, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), newInstanceManager(nodeName), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(imIndexer.Add(im), IsNil)
	}
	ei, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), engineImage, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(eiIndexer.Add(ei), IsNil)
	setSettings(generateSchedulerTestCase(), lhClient, sIndexer, c)
	return f
}

func addAntiAffinityTestDisk(node *longhorn.Node, index string) {
	diskID := getDiskID(node.Name, index)
	node.Spec.Disks[diskID] = newDisk(TestDefaultDataPath+index, true, 0)
	node.Status.DiskStatus[diskID] = &longhorn.DiskStatus{
		StorageAvailable: TestDiskAvailableSize,
		StorageMaximum:   TestDiskSize,
		Conditions:       []longhorn.Condition{newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue)},
		DiskUUID:         diskID,
		Type:             longhorn.DiskTypeFilesystem,
	}
}

func appTerm(app string) longhorn.VolumeAntiAffinityTerm {
	return longhorn.VolumeAntiAffinityTerm{
		Namespaces:    []string{"prod"},
		LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
	}
}

func requiredFromPod(pod string, terms ...longhorn.VolumeAntiAffinityTerm) *longhorn.VolumeAntiAffinityFromPod {
	return &longhorn.VolumeAntiAffinityFromPod{Pod: "prod/" + pod, RequiredTerms: terms}
}

func preferredFromPod(pod string, terms ...longhorn.VolumeAntiAffinityTerm) *longhorn.VolumeAntiAffinityFromPod {
	return &longhorn.VolumeAntiAffinityFromPod{Pod: "prod/" + pod, PreferredTerms: terms}
}

// addVolume registers a volume with the feature enabled when fromPod is set;
// each node name in placedOn gets one placed replica of it.
func (f *antiAffinityFixture) addVolume(name string, fromPod *longhorn.VolumeAntiAffinityFromPod, placedOn ...string) (*longhorn.Volume, map[string]*longhorn.Replica) {
	v := newVolume(name, 1)
	if fromPod != nil {
		v.Spec.VolumeAntiAffinity = longhorn.VolumeAntiAffinityEnabled
		v.Spec.VolumeAntiAffinityFromPod = fromPod
	}
	v, err := f.lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(context.TODO(), v, metav1.CreateOptions{})
	f.c.Assert(err, IsNil)
	f.c.Assert(f.vIndexer.Add(v), IsNil)

	replicas := map[string]*longhorn.Replica{}
	for _, nodeName := range placedOn {
		r := newReplicaForVolume(v)
		r.Spec.NodeID = nodeName
		r.Spec.DiskID = getDiskID(nodeName, "1")
		r.Spec.HealthyAt = getTestNow().String()
		replicas[r.Name] = f.addReplica(r)
	}
	return v, replicas
}

func (f *antiAffinityFixture) addReplica(r *longhorn.Replica) *longhorn.Replica {
	r, err := f.lhClient.LonghornV1beta2().Replicas(TestNamespace).Create(context.TODO(), r, metav1.CreateOptions{})
	f.c.Assert(err, IsNil)
	f.c.Assert(f.rIndexer.Add(r), IsNil)
	return r
}

func (f *antiAffinityFixture) updateReplica(r *longhorn.Replica) {
	f.c.Assert(f.rIndexer.Update(r), IsNil)
}

// schedule creates a new replica of the volume and schedules it next to the
// existing replicas of the volume.
func (f *antiAffinityFixture) schedule(v *longhorn.Volume, existing map[string]*longhorn.Replica) (*longhorn.Replica, multierr.MultiError) {
	r := f.addReplica(newReplicaForVolume(v))
	replicas := map[string]*longhorn.Replica{r.Name: r}
	for name, replica := range existing {
		replicas[name] = replica
	}
	return f.rcs.ScheduleReplica(r, replicas, v)
}

func (f *antiAffinityFixture) scheduledNode(v *longhorn.Volume) string {
	r, errs := f.schedule(v, nil)
	f.c.Assert(errs, HasLen, 0)
	f.c.Assert(r, NotNil)
	f.c.Assert(r.Spec.NodeID, Not(Equals), "")
	return r.Spec.NodeID
}

func (s *TestSuite) TestVolumeAntiAffinityRequired(c *C) {
	f := newAntiAffinityFixture(c)
	f.addVolume("kafka-0", requiredFromPod("kafka-0", appTerm("kafka")), TestNode1)
	v1, _ := f.addVolume("kafka-1", requiredFromPod("kafka-1", appTerm("kafka")))
	node := f.scheduledNode(v1)
	c.Assert(node, Not(Equals), TestNode1)
	f.addVolume("kafka-1-placed", requiredFromPod("kafka-1", appTerm("kafka")), node)

	v2, _ := f.addVolume("kafka-2", requiredFromPod("kafka-2", appTerm("kafka")))
	last := f.scheduledNode(v2)
	c.Assert(last, Not(Equals), TestNode1)
	c.Assert(last, Not(Equals), node)
	f.addVolume("kafka-2-placed", requiredFromPod("kafka-2", appTerm("kafka")), last)

	// Related volumes occupy every node: the replica is not scheduled, and the
	// failure names volume anti-affinity.
	v3, _ := f.addVolume("kafka-3", requiredFromPod("kafka-3", appTerm("kafka")))
	r, errs := f.schedule(v3, nil)
	c.Assert(r, IsNil)
	c.Assert(errs.ErrorByReason(longhorn.ErrorReplicaScheduleVolumeAntiAffinityNotSatisfied), Not(Equals), "")

	// The volume itself is let go by disabling the feature on it.
	v3.Spec.VolumeAntiAffinity = longhorn.VolumeAntiAffinityDisabled
	c.Assert(f.scheduledNode(v3), Not(Equals), "")
}

func (s *TestSuite) TestVolumeAntiAffinityPreferred(c *C) {
	f := newAntiAffinityFixture(c)
	f.addVolume("redis-0", preferredFromPod("redis-0", appTerm("redis")), TestNode1)
	f.addVolume("redis-1", preferredFromPod("redis-1", appTerm("redis")), TestNode1)
	f.addVolume("redis-2", preferredFromPod("redis-2", appTerm("redis")), TestNode2)
	v, _ := f.addVolume("redis-3", preferredFromPod("redis-3", appTerm("redis")))
	c.Assert(f.scheduledNode(v), Equals, TestNode3)

	// Every node holds a related replica: the fewest wins and nothing fails.
	f.addVolume("redis-3-placed", preferredFromPod("redis-3", appTerm("redis")), TestNode3)
	v, _ = f.addVolume("redis-4", preferredFromPod("redis-4", appTerm("redis")))
	c.Assert(f.scheduledNode(v), Not(Equals), TestNode1)
}

func (s *TestSuite) TestVolumeAntiAffinityRelation(c *C) {
	testCases := map[string]struct {
		sibling  *longhorn.Volume
		related  bool
		required bool
	}{
		"same term": {
			sibling: &longhorn.Volume{Spec: longhorn.VolumeSpec{VolumeAntiAffinity: longhorn.VolumeAntiAffinityEnabled,
				VolumeAntiAffinityFromPod: requiredFromPod("kafka-0", appTerm("kafka"))}},
			related: true, required: true,
		},
		"same term written as an expression": {
			sibling: &longhorn.Volume{Spec: longhorn.VolumeSpec{VolumeAntiAffinity: longhorn.VolumeAntiAffinityEnabled,
				VolumeAntiAffinityFromPod: requiredFromPod("kafka-0", longhorn.VolumeAntiAffinityTerm{
					Namespaces: []string{"prod"},
					LabelSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
						{Key: "app", Operator: metav1.LabelSelectorOpIn, Values: []string{"kafka"}},
					}},
				})}},
			related: true, required: true,
		},
		"same term preferred by the sibling": {
			sibling: &longhorn.Volume{Spec: longhorn.VolumeSpec{VolumeAntiAffinity: longhorn.VolumeAntiAffinityEnabled,
				VolumeAntiAffinityFromPod: preferredFromPod("kafka-0", appTerm("kafka"))}},
			related: true, required: true,
		},
		"different term": {
			sibling: &longhorn.Volume{Spec: longhorn.VolumeSpec{VolumeAntiAffinity: longhorn.VolumeAntiAffinityEnabled,
				VolumeAntiAffinityFromPod: requiredFromPod("zk-0", appTerm("zookeeper"))}},
		},
		"same pod": {
			sibling: &longhorn.Volume{Spec: longhorn.VolumeSpec{VolumeAntiAffinity: longhorn.VolumeAntiAffinityEnabled,
				VolumeAntiAffinityFromPod: requiredFromPod("kafka-9", appTerm("kafka"))}},
		},
		"feature disabled on the sibling": {
			sibling: &longhorn.Volume{Spec: longhorn.VolumeSpec{VolumeAntiAffinity: longhorn.VolumeAntiAffinityDisabled,
				VolumeAntiAffinityFromPod: requiredFromPod("kafka-0", appTerm("kafka"))}},
		},
		"another namespace": {
			sibling: &longhorn.Volume{Spec: longhorn.VolumeSpec{VolumeAntiAffinity: longhorn.VolumeAntiAffinityEnabled,
				VolumeAntiAffinityFromPod: requiredFromPod("kafka-0", longhorn.VolumeAntiAffinityTerm{
					Namespaces: []string{"stg"}, LabelSelector: appTerm("kafka").LabelSelector,
				})}},
		},
	}
	for name, tc := range testCases {
		f := newAntiAffinityFixture(c)
		f.addVolume("sibling", tc.sibling.Spec.VolumeAntiAffinityFromPod, TestNode1)
		sibling, err := f.rcs.ds.GetVolumeRO("sibling")
		c.Assert(err, IsNil)
		sibling = sibling.DeepCopy()
		sibling.Spec.VolumeAntiAffinity = tc.sibling.Spec.VolumeAntiAffinity
		c.Assert(f.vIndexer.Update(sibling), IsNil)

		v, _ := f.addVolume("kafka-9", requiredFromPod("kafka-9", appTerm("kafka")))
		replica := newReplicaForVolume(v)
		antiAffinity, err := f.rcs.getVolumeAntiAffinity(replica, v)
		c.Assert(err, IsNil)
		if !tc.related {
			c.Assert(antiAffinity, IsNil, Commentf("case %v", name))
			continue
		}
		c.Assert(antiAffinity, NotNil, Commentf("case %v", name))
		c.Assert(antiAffinity.requiredNodes.Has(TestNode1), Equals, tc.required, Commentf("case %v", name))
	}

	// The strength of the term in the scheduled volume decides: a term that is
	// preferred here only ranks the node of a sibling that requires it.
	f := newAntiAffinityFixture(c)
	f.addVolume("kafka-0", requiredFromPod("kafka-0", appTerm("kafka")), TestNode1)
	v, _ := f.addVolume("kafka-1", preferredFromPod("kafka-1", appTerm("kafka")))
	antiAffinity, err := f.rcs.getVolumeAntiAffinity(newReplicaForVolume(v), v)
	c.Assert(err, IsNil)
	c.Assert(antiAffinity.requiredNodes.Len(), Equals, 0)
	c.Assert(antiAffinity.preferredReplicas, DeepEquals, map[string]int{TestNode1: 1})
}

func (s *TestSuite) TestVolumeAntiAffinityOccupancy(c *C) {
	f := newAntiAffinityFixture(c)
	_, failed := f.addVolume("kafka-0", requiredFromPod("kafka-0", appTerm("kafka")), TestNode1)
	for _, r := range failed {
		r = r.DeepCopy()
		r.Spec.FailedAt = getTestNow().String()
		f.updateReplica(r)
	}
	_, deleting := f.addVolume("kafka-1", requiredFromPod("kafka-1", appTerm("kafka")), TestNode2)
	for _, r := range deleting {
		r = r.DeepCopy()
		now := metav1.Now()
		r.DeletionTimestamp = &now
		f.updateReplica(r)
	}
	_, evicting := f.addVolume("kafka-2", requiredFromPod("kafka-2", appTerm("kafka")), TestNode3)
	for _, r := range evicting {
		r = r.DeepCopy()
		r.Spec.EvictionRequested = true
		f.updateReplica(r)
	}

	v, _ := f.addVolume("kafka-3", requiredFromPod("kafka-3", appTerm("kafka")))
	antiAffinity, err := f.rcs.getVolumeAntiAffinity(newReplicaForVolume(v), v)
	c.Assert(err, IsNil)
	c.Assert(antiAffinity.requiredNodes.UnsortedList(), DeepEquals, []string{TestNode3})
}

func (s *TestSuite) TestVolumeAntiAffinityExemptions(c *C) {
	// Best-effort keeps the replica on the attach node when a disk there is a
	// candidate, as its local migration would move it back there anyway.
	f := newAntiAffinityFixture(c)
	f.addVolume("kafka-0", requiredFromPod("kafka-0", appTerm("kafka")), TestNode1)
	v, _ := f.addVolume("kafka-1", requiredFromPod("kafka-1", appTerm("kafka")))
	v.Spec.DataLocality = longhorn.DataLocalityBestEffort
	v.Spec.NodeID = TestNode1
	c.Assert(f.scheduledNode(v), Equals, TestNode1)

	// A best-effort volume that already has a healthy replica on the attach
	// node does not place this replica there, so volume anti-affinity applies.
	f = newAntiAffinityFixture(c)
	f.addVolume("kafka-0", requiredFromPod("kafka-0", appTerm("kafka")), TestNode2)
	v, local := f.addVolume("kafka-1", requiredFromPod("kafka-1", appTerm("kafka")), TestNode1)
	v.Spec.DataLocality = longhorn.DataLocalityBestEffort
	v.Spec.NodeID = TestNode1
	r, errs := f.schedule(v, local)
	c.Assert(errs, HasLen, 0)
	c.Assert(r.Spec.NodeID, Equals, TestNode3)

	// A replica with a hard node affinity already has its node.
	f = newAntiAffinityFixture(c)
	f.addVolume("kafka-0", requiredFromPod("kafka-0", appTerm("kafka")), TestNode1)
	v, _ = f.addVolume("kafka-1", requiredFromPod("kafka-1", appTerm("kafka")))
	replica := newReplicaForVolume(v)
	replica.Spec.HardNodeAffinity = TestNode1
	antiAffinity, err := f.rcs.getVolumeAntiAffinity(replica, v)
	c.Assert(err, IsNil)
	c.Assert(antiAffinity, IsNil)

	// The failed-replica reuse path keeps the node that has the data.
	nodes, err := f.rcs.ListSchedulableNodes(v.Spec.DataEngine)
	c.Assert(err, IsNil)
	disks := map[string]map[string]struct{}{}
	for nodeName := range nodes {
		disks[nodeName] = map[string]struct{}{getDiskID(nodeName, "1"): {}}
	}
	reuseCandidates, _ := f.rcs.getDiskCandidates(nodes, disks, map[string]*longhorn.Replica{}, v, false, true, nil)
	c.Assert(reuseCandidates, HasLen, 3)

	// A strict-local replica stays on the attach node, even before its hard node affinity is set.
	f = newAntiAffinityFixture(c)
	f.addVolume("kafka-0", requiredFromPod("kafka-0", appTerm("kafka")), TestNode1)
	v, _ = f.addVolume("kafka-1", requiredFromPod("kafka-1", appTerm("kafka")))
	v.Spec.DataLocality = longhorn.DataLocalityStrictLocal
	antiAffinity, err = f.rcs.getVolumeAntiAffinity(newReplicaForVolume(v), v)
	c.Assert(err, IsNil)
	c.Assert(antiAffinity, IsNil)
}

func (s *TestSuite) TestVolumeAntiAffinityDiskEviction(c *C) {
	f := newAntiAffinityFixture(c)
	f.addVolume("kafka-0", requiredFromPod("kafka-0", appTerm("kafka")), TestNode2)
	f.addVolume("kafka-1", requiredFromPod("kafka-1", appTerm("kafka")), TestNode3)

	obj, exists, err := f.nIndexer.GetByKey(TestNamespace + "/" + TestNode1)
	c.Assert(err, IsNil)
	c.Assert(exists, Equals, true)
	node := obj.(*longhorn.Node).DeepCopy()
	addAntiAffinityTestDisk(node, "2")
	evictingDisk := node.Spec.Disks[getDiskID(TestNode1, "1")]
	evictingDisk.EvictionRequested = true
	node.Spec.Disks[getDiskID(TestNode1, "1")] = evictingDisk
	c.Assert(f.nIndexer.Update(node), IsNil)

	v, existing := f.addVolume("kafka-2", requiredFromPod("kafka-2", appTerm("kafka")), TestNode1)
	for name, r := range existing {
		r = r.DeepCopy()
		r.Spec.EvictionRequested = true
		f.updateReplica(r)
		existing[name] = r
	}

	// The other nodes hold related replicas, so the replica moves to another
	// disk of the evicting node, which the later candidate group offers.
	r, errs := f.schedule(v, existing)
	c.Assert(errs, HasLen, 0)
	c.Assert(r, NotNil)
	c.Assert(r.Spec.NodeID, Equals, TestNode1)
	c.Assert(r.Spec.DiskID, Equals, getDiskID(TestNode1, "2"))
}

func (s *TestSuite) TestVolumeAntiAffinityWithinVolumeTopology(c *C) {
	// The topology requirement narrows the nodes before volume anti-affinity
	// filters them, so a related replica never moves a volume out of its zone.
	f := newAntiAffinityFixture(c)
	obj, exists, err := f.nIndexer.GetByKey(TestNamespace + "/" + TestNode3)
	c.Assert(err, IsNil)
	c.Assert(exists, Equals, true)
	node := obj.(*longhorn.Node).DeepCopy()
	node.Status.Zone = TestZone2
	c.Assert(f.nIndexer.Update(node), IsNil)
	zonal := []longhorn.VolumeTopologyTerm{{Zone: TestZone1}}

	f.addVolume("kafka-0", requiredFromPod("kafka-0", appTerm("kafka")), TestNode1)
	v, _ := f.addVolume("kafka-1", requiredFromPod("kafka-1", appTerm("kafka")))
	v.Spec.TopologyRequirement = zonal
	c.Assert(f.scheduledNode(v), Equals, TestNode2)

	// Related volumes occupy every node of the zone, and the node of the other
	// zone is not a candidate.
	f.addVolume("kafka-1-placed", requiredFromPod("kafka-1", appTerm("kafka")), TestNode2)
	v, _ = f.addVolume("kafka-2", requiredFromPod("kafka-2", appTerm("kafka")))
	v.Spec.TopologyRequirement = zonal
	r, errs := f.schedule(v, nil)
	c.Assert(r, IsNil)
	c.Assert(errs.ErrorByReason(longhorn.ErrorReplicaScheduleVolumeAntiAffinityNotSatisfied), Not(Equals), "")
}
