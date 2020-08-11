package scheduler

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/sirupsen/logrus"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

type ReplicaScheduler struct {
	ds *datastore.DataStore
}

type Disk struct {
	types.DiskSpec
	NodeID string
}

type DiskSchedulingInfo struct {
	StorageAvailable           int64
	StorageMaximum             int64
	StorageReserved            int64
	StorageScheduled           int64
	OverProvisioningPercentage int64
	MinimalAvailablePercentage int64
}

func NewReplicaScheduler(ds *datastore.DataStore) *ReplicaScheduler {
	rcScheduler := &ReplicaScheduler{
		ds: ds,
	}
	return rcScheduler
}

// ScheduleReplica will return (nil, nil) for unschedulable replica
func (rcs *ReplicaScheduler) ScheduleReplica(replica *longhorn.Replica, replicas map[string]*longhorn.Replica, volume *longhorn.Volume) (*longhorn.Replica, error) {
	// only called when replica is starting for the first time
	if replica.Spec.NodeID != "" {
		return nil, fmt.Errorf("BUG: Replica %v has been scheduled to node %v", replica.Name, replica.Spec.NodeID)
	}

	// get all hosts
	nodeInfo, err := rcs.getNodeInfo()
	if err != nil {
		return nil, err
	}
	if len(nodeInfo) == 0 {
		logrus.Errorf("There's no available node for replica %v, size %v", replica.ObjectMeta.Name, replica.Spec.VolumeSize)
		return nil, nil
	}

	// find proper node and disk
	diskCandidates := rcs.chooseDiskCandidates(nodeInfo, replicas, replica, volume)

	// there's no disk that fit for current replica
	if len(diskCandidates) == 0 {
		logrus.Errorf("There's no available disk for replica %v, size %v", replica.ObjectMeta.Name, replica.Spec.VolumeSize)
		return nil, nil
	}

	// schedule replica to disk
	rcs.scheduleReplicaToDisk(replica, diskCandidates)

	return replica, nil
}

// ScheduleReplicaToNode will return (nil, nil) for unschedulable replica
func (rcs *ReplicaScheduler) ScheduleReplicaToNode(replica *longhorn.Replica, replicas map[string]*longhorn.Replica, volume *longhorn.Volume, nodeID string) (*longhorn.Replica, error) {
	// only called when replica is starting for the first time
	if replica.Spec.NodeID != "" {
		return nil, fmt.Errorf("BUG: Replica %v has been scheduled to node %v", replica.Name, replica.Spec.NodeID)
	}

	// get all hosts
	schedulableNodesInfo, err := rcs.getNodeInfo()
	if err != nil {
		return nil, err
	}

	node, exist := schedulableNodesInfo[nodeID]
	if !exist {
		return nil, nil
	}

	nodeInfo := make(map[string]*longhorn.Node)
	nodeInfo[nodeID] = node

	// find proper disk
	diskCandidates := rcs.chooseDiskCandidates(nodeInfo, replicas, replica, volume)

	// there's no disk that fit for current replica
	if len(diskCandidates) == 0 {
		return nil, nil
	}

	// schedule replica to disk
	rcs.scheduleReplicaToDisk(replica, diskCandidates)

	return replica, nil
}

func (rcs *ReplicaScheduler) chooseDiskCandidates(nodeInfo map[string]*longhorn.Node, replicas map[string]*longhorn.Replica, replica *longhorn.Replica, volume *longhorn.Volume) map[string]*Disk {
	nodeSoftAntiAffinity, err :=
		rcs.ds.GetSettingAsBool(types.SettingNameReplicaSoftAntiAffinity)
	if err != nil {
		logrus.Errorf("error getting replica soft anti-affinity setting: %v", err)
	}

	zoneSoftAntiAffinity, err :=
		rcs.ds.GetSettingAsBool(types.SettingNameReplicaZoneSoftAntiAffinity)
	if err != nil {
		logrus.Errorf("Error getting replica zone soft anti-affinity setting: %v", err)
	}

	usedNodes := map[string]*longhorn.Node{}
	usedZones := map[string]bool{}

	// Get current nodes and zones
	for _, r := range replicas {
		if r.Spec.NodeID != "" && r.DeletionTimestamp == nil && r.Spec.FailedAt == "" {
			if node, ok := nodeInfo[r.Spec.NodeID]; ok {
				usedNodes[r.Spec.NodeID] = node
				// For empty zone label, we treat them as
				// one zone.
				usedZones[node.Status.Zone] = true
			}
		}
	}

	unusedNodeCandidates := map[string]*longhorn.Node{}
	unusedNodeWithNewZoneCandidates := map[string]*longhorn.Node{}

	// Get new nodes with new zones
	for nodeName, node := range nodeInfo {
		if _, ok := usedNodes[nodeName]; !ok {
			// Filter Nodes. If the Nodes don't match the tags, don't bother marking them as candidates.
			if !rcs.checkTagsAreFulfilled(node.Spec.Tags, volume.Spec.NodeSelector) {
				continue
			}
			unusedNodeCandidates[nodeName] = node
			if _, ok := usedZones[node.Status.Zone]; !ok {
				unusedNodeWithNewZoneCandidates[nodeName] = node
			}
		}
	}

	diskCandidates := map[string]*Disk{}

	// First check if we can schedule replica on new nodes with new zone
	for _, node := range unusedNodeWithNewZoneCandidates {
		diskCandidates = rcs.filterNodeDisksForReplica(node, replica, replicas, volume)
		if len(diskCandidates) > 0 {
			return diskCandidates
		}
	}

	// Hard on zone and hard on node
	if (!nodeSoftAntiAffinity) && (!zoneSoftAntiAffinity) {
		return diskCandidates
	}

	// Then check if we can schedule replica on new nodes with same zone
	for _, node := range unusedNodeCandidates {
		diskCandidates = rcs.filterNodeDisksForReplica(node, replica, replicas, volume)
		if len(diskCandidates) > 0 {
			break
		}
	}

	// Soft on zone and hard on node
	if !nodeSoftAntiAffinity {
		return diskCandidates
	}

	// On new nodes with soft on zone and soft on node
	if (zoneSoftAntiAffinity) && (len(diskCandidates) > 0) {
		return diskCandidates
	}

	for _, node := range nodeInfo {
		if _, ok := usedZones[node.Status.Zone]; !ok {
			// Filter tag unmatch nodes
			if !rcs.checkTagsAreFulfilled(node.Spec.Tags, volume.Spec.NodeSelector) {
				continue
			}
			diskCandidates = rcs.filterNodeDisksForReplica(node, replica, replicas, volume)
			if len(diskCandidates) > 0 {
				break
			}
		}
	}

	// Hard on zone and soft on node
	if !zoneSoftAntiAffinity {
		return diskCandidates
	}

	// On new zones with soft on zone and soft on node
	if len(diskCandidates) > 0 {
		return diskCandidates
	}

	// Last check if we can schedule replica on existing node regardless the zone
	// Soft on zone and soft on node
	for _, node := range usedNodes {
		diskCandidates = rcs.filterNodeDisksForReplica(node, replica, replicas, volume)
	}

	return diskCandidates
}

func (rcs *ReplicaScheduler) filterNodeDisksForReplica(node *longhorn.Node, replica *longhorn.Replica, replicas map[string]*longhorn.Replica, volume *longhorn.Volume) map[string]*Disk {
	preferredDisk := map[string]*Disk{}
	// find disk that fit for current replica
	disks := node.Spec.Disks
	diskStatus := node.Status.DiskStatus
	for fsid, disk := range disks {
		status := diskStatus[fsid]
		info, err := rcs.GetDiskSchedulingInfo(disk, status)
		if err != nil {
			logrus.Errorf("Fail to get settings when scheduling replica: %v", err)
			return preferredDisk
		}
		scheduledReplica := status.ScheduledReplica
		// check other replicas for the same volume has been accounted on current node
		var storageScheduled int64
		for rName, r := range replicas {
			if _, ok := scheduledReplica[rName]; !ok && r.Spec.NodeID != "" && r.Spec.NodeID == node.Name {
				storageScheduled += r.Spec.VolumeSize
			}
		}
		if storageScheduled > 0 {
			info.StorageScheduled += storageScheduled
		}
		diskReadyCondition := types.GetCondition(status.Conditions, types.DiskConditionTypeReady)
		if diskReadyCondition.Status == types.ConditionStatusFalse || !disk.AllowScheduling ||
			!rcs.IsSchedulableToDisk(replica.Spec.VolumeSize, volume.Status.ActualSize, info) {
			continue
		}
		// Check if the Disk's Tags are valid.
		if !rcs.checkTagsAreFulfilled(disk.Tags, volume.Spec.DiskSelector) {
			continue
		}

		suggestDisk := &Disk{
			DiskSpec: disk,
			NodeID:   node.Name,
		}
		preferredDisk[fsid] = suggestDisk
	}

	return preferredDisk
}

func (rcs *ReplicaScheduler) checkTagsAreFulfilled(itemTags, volumeTags []string) bool {
	if !sort.StringsAreSorted(itemTags) {
		logrus.Warnf("BUG: Tags are not sorted, sort now")
		sort.Strings(itemTags)
	}

	for _, tag := range volumeTags {
		if index := sort.SearchStrings(itemTags, tag); index >= len(itemTags) || itemTags[index] != tag {
			return false
		}
	}

	return true
}

func (rcs *ReplicaScheduler) getNodeInfo() (map[string]*longhorn.Node, error) {
	nodeInfo, err := rcs.ds.ListNodes()
	if err != nil {
		return nil, err
	}
	scheduledNode := map[string]*longhorn.Node{}

	for _, node := range nodeInfo {
		// First check node ready condition
		nodeReadyCondition := types.GetCondition(node.Status.Conditions, types.NodeConditionTypeReady)
		// Get Schedulable condition
		nodeSchedulableCondition :=
			types.GetCondition(node.Status.Conditions,
				types.NodeConditionTypeSchedulable)
		if node != nil && node.DeletionTimestamp == nil &&
			nodeReadyCondition.Status == types.ConditionStatusTrue &&
			nodeSchedulableCondition.Status == types.ConditionStatusTrue &&
			node.Spec.AllowScheduling {
			scheduledNode[node.Name] = node
		}
	}
	return scheduledNode, nil
}

func (rcs *ReplicaScheduler) scheduleReplicaToDisk(replica *longhorn.Replica, diskCandidates map[string]*Disk) {
	// get a random disk from diskCandidates
	var fsid string
	var disk *Disk
	for fsid, disk = range diskCandidates {
		break
	}
	replica.Spec.NodeID = disk.NodeID
	replica.Spec.DiskID = fsid
	replica.Spec.DataPath = filepath.Join(disk.Path, "replicas", replica.Spec.VolumeName+"-"+util.RandomID())
	logrus.Debugf("Schedule replica %v to node %v, disk %v, datapath %v",
		replica.Name, replica.Spec.NodeID, replica.Spec.DiskID, replica.Spec.DataPath)
}

func (rcs *ReplicaScheduler) IsSchedulableToDisk(size int64, requiredStorage int64, info *DiskSchedulingInfo) bool {
	// StorageReserved = the space is already used by 3rd party + the space will be used by 3rd party.
	// StorageAvailable = the space can be used by 3rd party or Longhorn system.
	// There is no (direct) relationship between StorageReserved and StorageAvailable.
	return info.StorageMaximum > 0 && info.StorageAvailable > 0 &&
		info.StorageAvailable-requiredStorage > info.StorageMaximum*info.MinimalAvailablePercentage/100 &&
		(size+info.StorageScheduled) <= (info.StorageMaximum-info.StorageReserved)*(info.OverProvisioningPercentage/100)
}

func (rcs *ReplicaScheduler) GetDiskSchedulingInfo(disk types.DiskSpec, diskStatus *types.DiskStatus) (*DiskSchedulingInfo, error) {
	// get StorageOverProvisioningPercentage and StorageMinimalAvailablePercentage settings
	overProvisioningPercentage, err := rcs.ds.GetSettingAsInt(types.SettingNameStorageOverProvisioningPercentage)
	if err != nil {
		return nil, err
	}
	minimalAvailablePercentage, err := rcs.ds.GetSettingAsInt(types.SettingNameStorageMinimalAvailablePercentage)
	if err != nil {
		return nil, err
	}
	info := &DiskSchedulingInfo{
		StorageAvailable:           diskStatus.StorageAvailable,
		StorageScheduled:           diskStatus.StorageScheduled,
		StorageReserved:            disk.StorageReserved,
		StorageMaximum:             diskStatus.StorageMaximum,
		OverProvisioningPercentage: overProvisioningPercentage,
		MinimalAvailablePercentage: minimalAvailablePercentage,
	}
	return info, nil
}
