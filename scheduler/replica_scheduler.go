package scheduler

import (
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

const (
	FailedReplicaMaxRetryCount = 5
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
	nodesInfo, err := rcs.getNodeInfo()
	if err != nil {
		return nil, err
	}

	if replica.Spec.HardNodeAffinity != "" {
		node, exist := nodesInfo[replica.Spec.HardNodeAffinity]
		if !exist {
			return nil, nil
		}
		nodesInfo = make(map[string]*longhorn.Node)
		nodesInfo[replica.Spec.HardNodeAffinity] = node
	}

	if len(nodesInfo) == 0 {
		logrus.Errorf("There's no available node for replica %v, size %v", replica.ObjectMeta.Name, replica.Spec.VolumeSize)
		return nil, nil
	}

	nodeDisksMap := map[string]map[string]struct{}{}
	for _, node := range nodesInfo {
		disks := map[string]struct{}{}
		for fsid, diskStatus := range node.Status.DiskStatus {
			diskSpec, exists := node.Spec.Disks[fsid]
			if !exists {
				continue
			}
			if !diskSpec.AllowScheduling || diskSpec.EvictionRequested {
				continue
			}
			if types.GetCondition(diskStatus.Conditions, types.DiskConditionTypeSchedulable).Status != types.ConditionStatusTrue {
				continue
			}
			disks[fsid] = struct{}{}
		}
		nodeDisksMap[node.Name] = disks
	}

	// find proper node and disk
	diskCandidates := rcs.getDiskCandidates(nodesInfo, nodeDisksMap, replicas, volume, true)

	// there's no disk that fit for current replica
	if len(diskCandidates) == 0 {
		logrus.Errorf("There's no available disk for replica %v, size %v", replica.ObjectMeta.Name, replica.Spec.VolumeSize)
		return nil, nil
	}

	// schedule replica to disk
	rcs.scheduleReplicaToDisk(replica, diskCandidates)

	return replica, nil
}

func (rcs *ReplicaScheduler) getDiskCandidates(nodeInfo map[string]*longhorn.Node, nodeDisksMap map[string]map[string]struct{}, replicas map[string]*longhorn.Replica, volume *longhorn.Volume, requireSchedulingCheck bool) map[string]*Disk {
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
		diskCandidates = rcs.filterNodeDisksForReplica(node, nodeDisksMap[node.Name], replicas, volume, requireSchedulingCheck)
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
		diskCandidates = rcs.filterNodeDisksForReplica(node, nodeDisksMap[node.Name], replicas, volume, requireSchedulingCheck)
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
			// Filter tag unmatched nodes
			if !rcs.checkTagsAreFulfilled(node.Spec.Tags, volume.Spec.NodeSelector) {
				continue
			}
			diskCandidates = rcs.filterNodeDisksForReplica(node, nodeDisksMap[node.Name], replicas, volume, requireSchedulingCheck)
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
		diskCandidates = rcs.filterNodeDisksForReplica(node, nodeDisksMap[node.Name], replicas, volume, requireSchedulingCheck)
	}

	return diskCandidates
}

func (rcs *ReplicaScheduler) filterNodeDisksForReplica(node *longhorn.Node, disks map[string]struct{}, replicas map[string]*longhorn.Replica, volume *longhorn.Volume, requireSchedulingCheck bool) map[string]*Disk {
	preferredDisk := map[string]*Disk{}
	// find disk that fit for current replica
	for fsid := range disks {
		diskSpec := node.Spec.Disks[fsid]
		diskStatus := node.Status.DiskStatus[fsid]
		if requireSchedulingCheck {
			info, err := rcs.GetDiskSchedulingInfo(diskSpec, diskStatus)
			if err != nil {
				logrus.Errorf("Fail to get settings when scheduling replica: %v", err)
				return preferredDisk
			}
			scheduledReplica := diskStatus.ScheduledReplica
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
			if !rcs.IsSchedulableToDisk(volume.Spec.Size, volume.Status.ActualSize, info) {
				continue
			}
		}

		// Check if the Disk's Tags are valid.
		if !rcs.checkTagsAreFulfilled(diskSpec.Tags, volume.Spec.DiskSelector) {
			continue
		}

		suggestDisk := &Disk{
			DiskSpec: diskSpec,
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

func (rcs *ReplicaScheduler) CheckAndReuseFailedReplica(replicas map[string]*longhorn.Replica, volume *longhorn.Volume, hardNodeAffinity string) (*longhorn.Replica, error) {
	allNodesInfo, err := rcs.getNodeInfo()
	if err != nil {
		return nil, err
	}

	availableNodesInfo := map[string]*longhorn.Node{}
	availableNodeDisksMap := map[string]map[string]struct{}{}
	reusableNodeReplicasMap := map[string][]*longhorn.Replica{}
	for _, r := range replicas {
		if !rcs.isFailedReplicaReusable(r, volume, allNodesInfo, hardNodeAffinity) {
			continue
		}

		disks, exists := availableNodeDisksMap[r.Spec.NodeID]
		if exists {
			disks[r.Spec.DiskID] = struct{}{}
		} else {
			disks = map[string]struct{}{r.Spec.DiskID: struct{}{}}
		}
		availableNodesInfo[r.Spec.NodeID] = allNodesInfo[r.Spec.NodeID]
		availableNodeDisksMap[r.Spec.NodeID] = disks

		if _, exists := reusableNodeReplicasMap[r.Spec.NodeID]; exists {
			reusableNodeReplicasMap[r.Spec.NodeID] = append(reusableNodeReplicasMap[r.Spec.NodeID], r)
		} else {
			reusableNodeReplicasMap[r.Spec.NodeID] = []*longhorn.Replica{r}
		}
	}

	diskCandidates := rcs.getDiskCandidates(availableNodesInfo, availableNodeDisksMap, replicas, volume, false)

	var reusedReplica *longhorn.Replica
	for fsid, suggestDisk := range diskCandidates {
		for _, r := range reusableNodeReplicasMap[suggestDisk.NodeID] {
			if r.Spec.DiskID != fsid {
				continue
			}
			if reusedReplica == nil {
				reusedReplica = r
				continue
			}
			reusedReplica = GetLatestFailedReplica(reusedReplica, r)
		}
	}
	if reusedReplica == nil {
		logrus.Errorf("Cannot pick up a reusable failed replicas")
		return nil, nil
	}

	return reusedReplica, nil
}

// RequireNewReplica is used to check if creating new replica immediately is necessary **after a reusable failed replica is not found**.
// A new replica needs to be created when:
//   1. the volume is a new volume (volume.Status.Robustness is Empty)
//   2. data locality is required (hardNodeAffinity is not Empty and volume.Status.Robustness is Healthy)
//   3. replica eviction happens (volume.Status.Robustness is Healthy)
//   4. there is no potential reusable replica
//   5. there is potential reusable replica but the replica replenishment wait interval is passed.
func (rcs *ReplicaScheduler) RequireNewReplica(replicas map[string]*longhorn.Replica, volume *longhorn.Volume, hardNodeAffinity string) bool {
	if volume.Status.Robustness != types.VolumeRobustnessDegraded {
		return true
	}
	if hardNodeAffinity != "" {
		return true
	}

	hasPotentiallyReusableReplica := false
	for _, r := range replicas {
		if IsPotentiallyReusableReplica(r, hardNodeAffinity) {
			hasPotentiallyReusableReplica = true
			break
		}
	}
	if !hasPotentiallyReusableReplica {
		return true
	}

	// Otherwise Longhorn will relay the new replica creation then there is a chance to reuse failed replicas later.
	settingValue, err := rcs.ds.GetSettingAsInt(types.SettingNameReplicaReplenishmentWaitInterval)
	if err != nil {
		logrus.Errorf("Failed to get Setting ReplicaReplenishmentWaitInterval, will directly replenish a new replica: %v", err)
		return true
	}
	waitInterval := time.Duration(settingValue) * time.Second
	lastDegradedAt, err := util.ParseTime(volume.Status.LastDegradedAt)
	if err != nil {
		logrus.Errorf("Failed to get parse volume last degraded timestamp %v, will directly replenish a new replica: %v", volume.Status.LastDegradedAt, err)
		return true
	}
	if time.Now().After(lastDegradedAt.Add(waitInterval)) {
		return true
	}

	logrus.Debugf("Replica replenishment is delayed until %v", lastDegradedAt.Add(waitInterval))
	return false
}

func (rcs *ReplicaScheduler) isFailedReplicaReusable(r *longhorn.Replica, v *longhorn.Volume, nodeInfo map[string]*longhorn.Node, hardNodeAffinity string) bool {
	if r.Spec.FailedAt == "" {
		return false
	}
	if r.Spec.NodeID == "" || r.Spec.DiskID == "" {
		return false
	}
	if r.Spec.RebuildRetryCount >= FailedReplicaMaxRetryCount {
		return false
	}
	if r.Status.EvictionRequested {
		return false
	}
	if hardNodeAffinity != "" && r.Spec.NodeID != hardNodeAffinity {
		return false
	}

	node, exists := nodeInfo[r.Spec.NodeID]
	if !exists {
		return false
	}
	diskSpec, exists := node.Spec.Disks[r.Spec.DiskID]
	if !exists {
		return false
	}
	if !diskSpec.AllowScheduling || diskSpec.EvictionRequested {
		return false
	}
	diskStatus, exists := node.Status.DiskStatus[r.Spec.DiskID]
	if !exists {
		return false
	}
	if types.GetCondition(diskStatus.Conditions, types.DiskConditionTypeSchedulable).Status != types.ConditionStatusTrue {
		return false
	}
	if !rcs.checkTagsAreFulfilled(diskSpec.Tags, v.Spec.DiskSelector) {
		return false
	}

	im, err := rcs.ds.GetInstanceManagerByInstance(r)
	if err != nil {
		logrus.Errorf("failed to get instance manager when checking replica %v is reusable: %v", r.Name, err)
		return false
	}
	if im.DeletionTimestamp != nil || im.Status.CurrentState != types.InstanceManagerStateRunning {
		return false
	}

	return true
}

// IsPotentiallyReusableReplica is used to check if a failed replica is potentially reusable.
// A potentially reusable replica means this failed replica may be able to reuse it later but it’s not valid now due to node/disk down issue.
func IsPotentiallyReusableReplica(r *longhorn.Replica, hardNodeAffinity string) bool {
	if r.Spec.FailedAt == "" {
		return false
	}
	if r.Spec.NodeID == "" || r.Spec.DiskID == "" {
		return false
	}
	if r.Spec.RebuildRetryCount >= FailedReplicaMaxRetryCount {
		return false
	}
	if r.Status.EvictionRequested {
		return false
	}
	if hardNodeAffinity != "" && r.Spec.NodeID != hardNodeAffinity {
		return false
	}
	return true
}

func GetLatestFailedReplica(rs ...*longhorn.Replica) (res *longhorn.Replica) {
	if rs == nil {
		return nil
	}

	var latestFailedAt time.Time
	for _, r := range rs {
		failedAt, err := util.ParseTime(r.Spec.FailedAt)
		if err != nil {
			logrus.Errorf("Failed to check replica %v failure timestamp %v: %v", r.Name, r.Spec.FailedAt, err)
			continue
		}
		if res == nil || failedAt.After(latestFailedAt) {
			res = r
			latestFailedAt = failedAt
		}
	}
	return res
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
