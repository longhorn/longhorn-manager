package manager

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

func (m *VolumeManager) GetNode(name string) (*longhorn.Node, error) {
	return m.ds.GetNode(name)
}

func (m *VolumeManager) UpdateNode(name string, allowScheduling bool) (*longhorn.Node, error) {
	node, err := m.ds.GetNode(name)
	if err != nil {
		return nil, err
	}
	node.Spec.AllowScheduling = allowScheduling
	return m.ds.UpdateNode(node)
}

func (m *VolumeManager) ListNodes() (map[string]*longhorn.Node, error) {
	nodeList, err := m.ds.ListNodes()
	if err != nil {
		return nil, err
	}
	return nodeList, nil
}

func (m *VolumeManager) ListNodesSorted() ([]*longhorn.Node, error) {
	nodeMap, err := m.ListNodes()
	if err != nil {
		return []*longhorn.Node{}, err
	}

	nodes := make([]*longhorn.Node, len(nodeMap))
	nodeNames, err := sortKeys(nodeMap)
	if err != nil {
		return []*longhorn.Node{}, err
	}
	for i, nodeName := range nodeNames {
		nodes[i] = nodeMap[nodeName]
	}
	return nodes, nil
}

func (m *VolumeManager) DiskUpdate(name string, updateDisks []types.DiskSpec) (*longhorn.Node, error) {
	node, err := m.ds.GetNode(name)
	if err != nil {
		return nil, err
	}

	originDisks := node.Spec.Disks
	diskUpdateMap := map[string]types.DiskSpec{}

	for _, uDisk := range updateDisks {
		diskInfo, err := util.GetDiskInfo(uDisk.Path)
		if err != nil {
			return nil, err
		}
		isInvalid := false
		for fsid, oDisk := range originDisks {
			if oDisk.Path == uDisk.Path && fsid != diskInfo.Fsid {
				isInvalid = true
				logrus.Warnf("Update disk on node %v warning: The disk %v has changed file system, please mount it back or remove it", name, oDisk.Path)
				diskUpdateMap[fsid] = oDisk
				break
			}
		}
		if !isInvalid {
			if uDisk.StorageReserved < 0 || uDisk.StorageReserved > diskInfo.StorageMaximum {
				return nil, fmt.Errorf("Update disk on node %v error: The storageReserved setting of disk %v is not valid, should be positive and no more than storageMaximum and storageAvailable", name, uDisk.Path)
			}
			// update disks
			if oDisk, ok := originDisks[diskInfo.Fsid]; ok {
				if oDisk.Path != uDisk.Path {
					// current disk is the same file system with exist disk
					return nil, fmt.Errorf("Add Disk on node %v error: The disk %v is the same file system with %v ", name, uDisk.Path, oDisk.Path)
				}
				diskUpdateMap[diskInfo.Fsid] = uDisk
			} else {
				// add disks
				diskUpdateMap[diskInfo.Fsid] = uDisk
			}
		}
	}

	// delete disks
	for fsid, oDisk := range originDisks {
		if _, ok := diskUpdateMap[fsid]; !ok {
			if oDisk.AllowScheduling || node.Status.DiskStatus[fsid].StorageScheduled != 0 {
				return nil, fmt.Errorf("Delete Disk on node %v error: Please disable the disk %v and remove all replicas first ", name, oDisk.Path)
			}
		}
	}
	node.Spec.Disks = diskUpdateMap

	return m.ds.UpdateNode(node)
}

func (m *VolumeManager) DeleteNode(name string) error {
	node, err := m.ds.GetNode(name)
	if err != nil {
		return err
	}
	// only remove node from longhorn without any volumes on it
	replicas, err := m.ds.ListReplicasByNode(name)
	if err != nil {
		return err
	}
	engines, err := m.ds.ListEnginesByNode(name)
	if err != nil {
		return err
	}
	condition := types.GetNodeConditionFromStatus(node.Status, types.NodeConditionTypeReady)
	// Only could delete node from longhorn if kubernetes node missing
	if condition.Status == types.ConditionStatusTrue || condition.Reason != types.NodeConditionReasonKubernetesNodeDown ||
		node.Spec.AllowScheduling || len(replicas) > 0 || len(engines) > 0 {
		return fmt.Errorf("Could not delete node %v with node ready condition is %v, reason is %v, node schedulable %v, and %v replica, %v engine running on it", name,
			condition.Status, condition.Reason, node.Spec.AllowScheduling, len(replicas), len(engines))
	}
	// before delete, clear ownerID of volumes and engine images handle by removed node
	eiList, err := m.ds.ListEngineImages()
	if err != nil {
		return err
	}
	for _, ei := range eiList {
		if ei.Spec.OwnerID == name {
			ei.Spec.OwnerID = ""
			if _, err := m.ds.UpdateEngineImage(ei); err != nil {
				return err
			}
		}
	}
	volumeList, err := m.ds.ListVolumes()
	for _, volume := range volumeList {
		if volume.Spec.OwnerID == name {
			volume.Spec.OwnerID = ""
			if _, err := m.ds.UpdateVolume(volume); err != nil {
				return err
			}
		}
	}
	return m.ds.DeleteNode(name)
}
