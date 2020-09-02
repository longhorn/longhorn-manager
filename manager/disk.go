package manager

import (
	"fmt"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

func (m *VolumeManager) GetDisk(name string) (*longhorn.Disk, error) {
	return m.ds.GetDisk(name)
}

func (m *VolumeManager) ListDisks() (map[string]*longhorn.Disk, error) {
	diskMap, err := m.ds.ListDisks()
	if err != nil {
		return nil, err
	}
	return diskMap, nil
}

func (m *VolumeManager) ListDisksSorted() ([]*longhorn.Disk, error) {
	diskMap, err := m.ListDisks()
	if err != nil {
		return []*longhorn.Disk{}, err
	}

	disks := []*longhorn.Disk{}
	nodeDiskMap := map[string]map[string]*longhorn.Disk{}
	for _, disk := range diskMap {
		if nodeDiskMap[disk.Status.NodeID] == nil {
			nodeDiskMap[disk.Status.NodeID] = map[string]*longhorn.Disk{}
		}
		nodeDiskMap[disk.Status.NodeID][disk.Name] = disk
	}
	sortedNodeNameList, err := sortKeys(nodeDiskMap)
	if err != nil {
		return []*longhorn.Disk{}, err
	}
	for _, nodeName := range sortedNodeNameList {
		sortedDiskNameList, err := sortKeys(nodeDiskMap[nodeName])
		if err != nil {
			return []*longhorn.Disk{}, err
		}
		for _, diskName := range sortedDiskNameList {
			disks = append(disks, diskMap[diskName])
		}
	}

	return disks, nil
}

func (m *VolumeManager) CreateDisk(nodeID, path string, diskSpec *types.DiskSpec) (res *longhorn.Disk, err error) {
	if err := m.validateDiskSpec(diskSpec); err != nil {
		return nil, err
	}

	if path == "" {
		return nil, fmt.Errorf("invalid paramater: empty disk path")
	}
	path = filepath.Clean(path)

	if nodeID == "" {
		return nil, fmt.Errorf("invalid paramater: empty node ID")
	}

	logrus.Debugf("Prepare to connect a new disk in path %v on node %v", path, nodeID)

	node, err := m.GetNode(nodeID)
	if err != nil {
		return nil, err
	}
	readyCondition := types.GetCondition(node.Status.Conditions, types.NodeConditionTypeReady)
	if readyCondition.Status != types.ConditionStatusTrue {
		return nil, fmt.Errorf("node %v is not ready, couldn't add disks for it", node.Name)
	}
	if _, exists := node.Spec.DiskPathMap[path]; exists {
		if _, exists := node.Status.DiskPathIDMap[path]; exists {
			return nil, fmt.Errorf("Disk path %v is already on node %v", path, node.Name)
		}
	} else {
		node.Spec.DiskPathMap[path] = struct{}{}
		if _, err := m.ds.UpdateNode(node); err != nil {
			logrus.Errorf("Failed to update node %v disk map during the disk creation: %v", node.Name, err)
			return nil, err
		}
	}

	defer func() {
		if err != nil {
			if node, err = m.ds.GetNode(node.Name); err != nil {
				logrus.Errorf("failed to get node after the disk creation failure: %v", err)
				return
			}
			delete(node.Spec.DiskPathMap, path)
			if _, err = m.ds.UpdateNode(node); err != nil {
				logrus.Errorf("failed to rollback disk map after the disk creation failure: %v", err)
				return
			}
		}
	}()

	disk, err := m.ds.WaitForDiskCreation(nodeID, path)
	if err != nil {
		return nil, err
	}

	disk.Spec = *diskSpec
	if disk.Spec.Tags == nil {
		disk.Spec.Tags = []string{}
	}
	if disk, err = m.ds.UpdateDisk(disk); err != nil {
		logrus.Errorf("Failed to apply all parameters during the disk %v creation: %v", disk.Name, err)
		return nil, err
	}

	nodes, err := m.ds.ListNodes()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list all nodes for the expired disk map cleanup after the disk creation")
	}
	for _, node := range nodes {
		for diskPath, diskName := range node.Status.DiskPathIDMap {
			if diskName == disk.Name && diskPath == path {
				delete(node.Spec.DiskPathMap, path)
				if _, err := m.ds.UpdateNode(node); err != nil {
					logrus.Errorf("Failed to clean up the expired disk info in node %v disk map after the disk %v creation: %v", node.Name, disk.Name, err)
					return nil, err
				}
				break
			}
		}
	}

	defer logrus.Infof("Created a new disk %v for node %v", disk.Name, nodeID)
	return disk, nil
}

func (m *VolumeManager) validateDiskSpec(diskSpec *types.DiskSpec) error {
	if diskSpec.StorageReserved < 0 {
		return fmt.Errorf("the reserved storage %v is invalid, should be positive", diskSpec.StorageReserved)
	}

	tags, err := util.ValidateTags(diskSpec.Tags)
	if err != nil {
		return err
	}
	diskSpec.Tags = tags

	return nil
}

func (m *VolumeManager) DeleteDisk(name string) error {
	disk, err := m.ds.GetDisk(name)
	if err != nil {
		return err
	}
	if disk.Status.State == types.DiskStateConnected && disk.Spec.AllowScheduling {
		disk.Spec.AllowScheduling = false
		if disk, err = m.ds.UpdateDisk(disk); err != nil {
			logrus.Errorf("Failed to disable the scheduling before deleting disk %v: %v", name, err)
			return err
		}
	}

	defer logrus.Debugf("Prepare to delete disk %v on node %v", name, disk.Status.NodeID)
	if disk.Status.NodeID == "" {
		return m.ds.DeleteDisk(name)
	}
	node, err := m.ds.GetNode(disk.Status.NodeID)
	if err != nil {
		return err
	}
	delete(node.Spec.DiskPathMap, disk.Status.Path)
	if _, err := m.ds.UpdateNode(node); err != nil {
		return err
	}

	return nil
}

func (m *VolumeManager) UpdateDisk(d *longhorn.Disk) (*longhorn.Disk, error) {
	if err := m.validateDiskSpec(&d.Spec); err != nil {
		return nil, err
	}
	disk, err := m.ds.UpdateDisk(d)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Updated disk %v to %+v", disk.Name, disk.Spec)
	return disk, nil
}

func (m *VolumeManager) GetDiskTags() ([]string, error) {
	foundTags := make(map[string]struct{})
	var tags []string

	diskList, err := m.ds.ListDisks()
	if err != nil {
		return nil, errors.Wrapf(err, "fail to list disks")
	}
	for _, disk := range diskList {
		for _, tag := range disk.Spec.Tags {
			if _, ok := foundTags[tag]; !ok {
				foundTags[tag] = struct{}{}
				tags = append(tags, tag)
			}
		}
	}

	return tags, nil
}
