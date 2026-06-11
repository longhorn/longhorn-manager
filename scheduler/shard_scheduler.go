package scheduler

import (
	"fmt"

	"github.com/longhorn/go-common-libs/multierr"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// ShardPlacement is the output of a single shard scheduling decision.
type ShardPlacement struct {
	NodeID   string
	DiskUUID string
	DiskPath string
	Size     int64
}

// ShardScheduler selects nodes and disks for EC shard slots.
// It delegates disk filtering and capacity checks to the embedded ReplicaScheduler
// so the same scheduling infrastructure (overprovisioning, availability, disk tags) is reused.
type ShardScheduler struct {
	ds  *datastore.DataStore
	rcs *ReplicaScheduler
}

func NewShardScheduler(ds *datastore.DataStore) *ShardScheduler {
	return &ShardScheduler{
		ds:  ds,
		rcs: NewReplicaScheduler(ds),
	}
}

// ListSchedulableNodes returns the schedulable v2 node set for shard placement.
func (ss *ShardScheduler) ListSchedulableNodes() (map[string]*longhorn.Node, error) {
	return ss.rcs.ListSchedulableNodes(longhorn.DataEngineTypeV2)
}

// ScheduleShard picks a node and disk for one EC shard slot.
//
// usedNodeIDs are the nodes already used by other shards in this ShardGroup; they are
// skipped so no two shards of the same group land on the same node. nodes is the set
// of schedulable nodes to choose from.
//
// If nothing fits, it returns a nil placement and a MultiError explaining why each
// candidate was rejected. The error return is set only for a real failure (for
// example, a setting that cannot be read), not when nothing can be scheduled yet.
func (ss *ShardScheduler) ScheduleShard(sg *longhorn.ShardGroup, vol *longhorn.Volume, usedNodeIDs map[string]bool, nodes map[string]*longhorn.Node) (*ShardPlacement, multierr.MultiError, error) {
	shardSize := ComputeShardSize(vol.Spec.Size, sg.Spec.DataChunks, sg.Spec.StripSizeKB)

	skipReasons := multierr.NewMultiError()

	allowEmptyDiskSelector, err := ss.ds.GetSettingAsBool(types.SettingNameAllowEmptyDiskSelectorVolume)
	if err != nil {
		return nil, nil, err
	}

	for nodeName, node := range nodes {
		// Hard node anti-affinity: skip nodes already used by this ShardGroup.
		if usedNodeIDs[nodeName] {
			skipReasons.Append("node already used by this shard group", fmt.Errorf("node %v", nodeName))
			continue
		}

		for diskName, diskStatus := range node.Status.DiskStatus {
			diskSpec, ok := node.Spec.Disks[diskName]
			if !ok {
				skipReasons.Append("disk missing from node spec", fmt.Errorf("disk %v on node %v", diskName, nodeName))
				continue
			}
			// Shards require V2 block disks.
			if diskSpec.Type != longhorn.DiskTypeBlock {
				skipReasons.Append("disk is not a v2 block disk", fmt.Errorf("disk %v on node %v", diskName, nodeName))
				continue
			}
			if !diskSpec.AllowScheduling || diskSpec.EvictionRequested {
				skipReasons.Append("disk scheduling disabled or eviction requested", fmt.Errorf("disk %v on node %v", diskName, nodeName))
				continue
			}
			// Disk must be in schedulable condition.
			if types.GetCondition(diskStatus.Conditions, longhorn.DiskConditionTypeSchedulable).Status != longhorn.ConditionStatusTrue {
				skipReasons.Append("disk not in schedulable state", fmt.Errorf("disk %v on node %v", diskName, nodeName))
				continue
			}
			// Respect disk selector labels from the volume spec.
			if !types.IsSelectorsInTags(diskSpec.Tags, vol.Spec.DiskSelector, allowEmptyDiskSelector) {
				skipReasons.Append("disk tags do not match disk selector", fmt.Errorf("disk %v on node %v", diskName, nodeName))
				continue
			}
			// Capacity check via the shared scheduling-info helper.
			info, err := ss.rcs.GetDiskSchedulingInfo(diskSpec, diskStatus)
			if err != nil {
				// Fails only on the cluster-wide over-provisioning settings, not on
				// anything disk-specific; surface it instead of skipping the disk as
				// unschedulable.
				return nil, nil, err
			}
			if schedulable, msg := ss.rcs.IsSchedulableToDisk(shardSize, 0, info); !schedulable {
				skipReasons.Append("insufficient disk capacity", fmt.Errorf("disk %v on node %v: %s", diskName, nodeName, msg))
				continue
			}

			return &ShardPlacement{
				NodeID:   nodeName,
				DiskUUID: diskStatus.DiskUUID,
				DiskPath: diskStatus.DiskPath,
				Size:     shardSize,
			}, nil, nil
		}
	}

	// No eligible node/disk this cycle; skipReasons explains why for the caller.
	return nil, skipReasons, nil
}

// ComputeShardSize returns the size in bytes that each EC shard lvol must be:
// ceil(volumeSize/k) plus the per-disk SPDK front reservation, rounded up to a 2 MiB
// (util.SizeAlignment) boundary so SPDK accepts it on create and expand.
func ComputeShardSize(volumeSize int64, k, stripSizeKB int) int64 {
	if k <= 0 {
		return volumeSize
	}
	perDiskUser := (volumeSize + int64(k) - 1) / int64(k) // ceil(volumeSize/k)
	reservation := int64(spdktypes.EcFrontReservationBytes(uint32(stripSizeKB)))
	return util.RoundUpSize(perDiskUser + reservation)
}
