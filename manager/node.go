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
		// update disks
		if oDisk, ok := originDisks[diskInfo.Fsid]; ok {
			if oDisk.Path != uDisk.Path {
				// current disk is the same file system with exist disk
				return nil, fmt.Errorf("Add Disk on node %v error: The disk %v is the same file system with %v ", name, uDisk.Path, oDisk.Path)
			} else if oDisk.StorageMaximum != uDisk.StorageMaximum && uDisk.StorageMaximum != diskInfo.StorageMaximum {
				logrus.Warnf("StorageMaximum has been changed for disk %v of node %v. Detected maximum storage %v, current setting %v", diskInfo.Path, name, diskInfo.StorageMaximum, uDisk.StorageMaximum)
			}
		} else {
			// add disks
			if uDisk.StorageMaximum != 0 && uDisk.StorageMaximum != diskInfo.StorageMaximum {
				logrus.Warnf("StorageMaximum has been changed for disk %v of node %v. Detected maximum storage %v, current setting %v", diskInfo.Path, name, diskInfo.StorageMaximum, uDisk.StorageMaximum)
			} else {
				uDisk.StorageMaximum = diskInfo.StorageMaximum
			}
		}
		diskUpdateMap[diskInfo.Fsid] = uDisk
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
