package scheduler

import (
	"context"

	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	miB = int64(1) << 20
	giB = int64(1) << 30
)

// TestScheduleShardTopologyRequirement covers the volume topology requirement as
// a hard filter on shard placement: EC volumes have no Replica CRs, so the
// requirement has to be honored on this path as well as in FindDiskCandidates.
func (s *TestSuite) TestScheduleShardTopologyRequirement(c *C) {
	newShardNode := func(name, zone, region string) *longhorn.Node {
		node := newNode(name, TestNamespace, zone, true, longhorn.ConditionStatusTrue)
		node.Status.Region = region
		diskID := getDiskID(name, "1")
		diskSpec := newDisk(TestDefaultDataPath, true, 0)
		diskSpec.Type = longhorn.DiskTypeBlock
		node.Spec.Disks = map[string]longhorn.DiskSpec{diskID: diskSpec}
		node.Status.DiskStatus = map[string]*longhorn.DiskStatus{
			diskID: {
				StorageAvailable: TestDiskAvailableSize,
				StorageScheduled: 0,
				StorageMaximum:   TestDiskSize,
				Conditions: []longhorn.Condition{
					newCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue),
				},
				DiskUUID: diskID,
				DiskName: diskID,
			},
		}
		return node
	}

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	addSetting := func(name types.SettingName, value string) {
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), initSettings(string(name), value), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(sIndexer.Add(setting), IsNil)
	}
	addSetting(types.SettingNameAllowEmptyDiskSelectorVolume, "true")
	addSetting(types.SettingNameStorageOverProvisioningPercentage, "100")
	addSetting(types.SettingNameStorageMinimalAvailablePercentage, "10")

	ss := &ShardScheduler{
		ds:  datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories),
		rcs: NewReplicaScheduler(datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)),
	}

	nodes := map[string]*longhorn.Node{
		TestNode1: newShardNode(TestNode1, TestZone1, "test-region-1"),
		TestNode2: newShardNode(TestNode2, TestZone2, "test-region-1"),
	}
	shardGroup := &longhorn.ShardGroup{
		Spec: longhorn.ShardGroupSpec{DataChunks: 2, ParityChunks: 1, StripSizeKB: 64},
	}
	volume := newVolume(TestVolumeName, 1)
	volume.Spec.DataEngine = longhorn.DataEngineTypeV2

	// Unconstrained volume: any node may host the shard.
	placement, _, err := ss.ScheduleShard(shardGroup, volume, map[string]bool{}, nodes)
	c.Assert(err, IsNil)
	c.Assert(placement, NotNil)

	// Zone-pinned volume: only the node in that zone is eligible.
	volume.Spec.TopologyRequirement = []longhorn.VolumeTopologyTerm{{Zone: TestZone2, Region: "test-region-1"}}
	placement, _, err = ss.ScheduleShard(shardGroup, volume, map[string]bool{}, nodes)
	c.Assert(err, IsNil)
	c.Assert(placement, NotNil)
	c.Assert(placement.NodeID, Equals, TestNode2)

	// No node in the required failure domain: no placement, and the caller gets
	// the reason instead of a fallback outside the requirement.
	volume.Spec.TopologyRequirement = []longhorn.VolumeTopologyTerm{{Zone: "test-zone-3"}}
	placement, skipReasons, err := ss.ScheduleShard(shardGroup, volume, map[string]bool{}, nodes)
	c.Assert(err, IsNil)
	c.Assert(placement, IsNil)
	c.Assert(len(skipReasons) > 0, Equals, true)
	c.Assert(skipReasons.ErrorByReason("node is outside the volume topology requirement"), Not(Equals), "")
}

func (s *TestSuite) TestComputeShardSize(c *C) {
	type testCase struct {
		volumeSize  int64
		k           int
		stripSizeKB int
	}
	testCases := map[string]testCase{
		"single shard holds the whole volume": {
			volumeSize: giB, k: 1, stripSizeKB: 64,
		},
		"even division across four shards": {
			volumeSize: 4 * giB, k: 4, stripSizeKB: 64,
		},
		"uneven division rounds each shard up": {
			volumeSize: giB + 1, k: 4, stripSizeKB: 64,
		},
		"small volume where the reservation dominates": {
			volumeSize: 10 * miB, k: 2, stripSizeKB: 64,
		},
		"minimum strip size": {
			volumeSize: 2 * giB, k: 3, stripSizeKB: 4,
		},
		"maximum strip size": {
			volumeSize: 8 * giB, k: 8, stripSizeKB: 1024,
		},
	}

	for name, tc := range testCases {
		got := ComputeShardSize(tc.volumeSize, tc.k, tc.stripSizeKB)

		// The body delegates to the shared go-spdk-helper formula; the full sizing
		// sweep lives in that repo. Guard the delegation and the manager-level
		// invariants here.
		c.Assert(got, Equals, spdktypes.ComputeShardSize(tc.volumeSize, tc.k, tc.stripSizeKB),
			Commentf("case %q: diverged from the shared formula", name))
		// Every shard lvol must be 2 MiB aligned so SPDK accepts it on create and expand.
		c.Assert(got%util.SizeAlignment, Equals, int64(0), Commentf("case %q: shard size not 2 MiB aligned", name))
		// The k shards' user regions together must at least cover the volume.
		// The lvstore metadata budget on top of it is internal to the shared
		// formula and covered by go-spdk-helper's own tests.
		reservation := int64(spdktypes.EcFrontReservationBytes(uint32(tc.stripSizeKB)))
		c.Assert((got-reservation)*int64(tc.k) >= tc.volumeSize, Equals, true,
			Commentf("case %q: shards do not cover the volume", name))
	}

	// k <= 0 cannot divide the volume; the guard returns the size unchanged.
	c.Assert(ComputeShardSize(giB, 0, 64), Equals, giB)
	c.Assert(ComputeShardSize(giB, -1, 64), Equals, giB)
}
