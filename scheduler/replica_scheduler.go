package scheduler

import (
	"fmt"
	"path/filepath"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
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
func (rcs *ReplicaScheduler) ScheduleReplica(replica *longhorn.Replica, replicas map[string]*longhorn.Replica) (*longhorn.Replica, error) {
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
		logrus.Errorf("There's no available node for replica %+v", replica)
		return nil, nil
	}

	// find proper node and disk
	diskCandidates := rcs.chooseDiskCandidates(nodeInfo, replicas, replica)

	// there's no disk that fit for current replica
	if len(diskCandidates) == 0 {
		logrus.Errorf("There's no available disk for replica %+v", replica)
		return nil, nil
	}

	// schedule replica to disk
	rcs.scheduleReplicaToDisk(replica, diskCandidates)

	return replica, nil
}

func (rcs *ReplicaScheduler) chooseDiskCandidates(nodeInfo map[string]*longhorn.Node, replicas map[string]*longhorn.Replica, replica *longhorn.Replica) map[string]*Disk {
	diskCandidates := map[string]*Disk{}
	filterdNode := []*longhorn.Node{}
	for nodeName, node := range nodeInfo {
		isFilterd := false
		for _, r := range replicas {
			// filter replica in deleting process
			if r.Spec.NodeID != "" && r.Spec.NodeID == nodeName && r.DeletionTimestamp == nil {
				filterdNode = append(filterdNode, node)
				isFilterd = true
				break
			}
		}
		if !isFilterd {
			diskCandidates = rcs.filterNodeDisksForReplica(node, replica)
			if len(diskCandidates) > 0 {
				return diskCandidates
			}
		}
	}
	// If there's no disk fit for replica on other nodes,
	// try to schedule to node that has been scheduled replicas.
	for _, node := range filterdNode {
		diskCandidates = rcs.filterNodeDisksForReplica(node, replica)
	}

	return diskCandidates
}

func (rcs *ReplicaScheduler) filterNodeDisksForReplica(node *longhorn.Node, replica *longhorn.Replica) map[string]*Disk {
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
		if !disk.AllowScheduling ||
			!rcs.IsSchedulableToDisk(replica.Spec.VolumeSize, info) {
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

func (rcs *ReplicaScheduler) getNodeInfo() (map[string]*longhorn.Node, error) {
	nodeInfo, err := rcs.ds.ListNodes()
	if err != nil {
		return nil, err
	}
	scheduledNode := map[string]*longhorn.Node{}
	for _, node := range nodeInfo {
		nodeReadyCondition := types.GetNodeConditionFromStatus(node.Status, types.NodeConditionTypeReady)
		if node != nil && node.DeletionTimestamp == nil && nodeReadyCondition.Status == types.ConditionStatusTrue && node.Spec.AllowScheduling {
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
}

func (rcs *ReplicaScheduler) IsSchedulableToDisk(size int64, info *DiskSchedulingInfo) bool {
	return info.StorageMaximum > 0 && info.StorageAvailable > 0 &&
		(size+info.StorageScheduled) <= (info.StorageMaximum-info.StorageReserved)*(info.OverProvisioningPercentage/100) &&
		size <= (info.StorageAvailable-info.StorageMaximum*info.MinimalAvailablePercentage/100)*info.OverProvisioningPercentage/100
}

func (rcs *ReplicaScheduler) GetDiskSchedulingInfo(disk types.DiskSpec, diskStatus types.DiskStatus) (*DiskSchedulingInfo, error) {
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
