package manager

import (
	"fmt"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

func (m *VolumeManager) GetInstanceManager(name string) (*longhorn.InstanceManager, error) {
	return m.ds.GetInstanceManager(name)
}

func (m *VolumeManager) ListInstanceManagers() (map[string]*longhorn.InstanceManager, error) {
	return m.ds.ListInstanceManagers()
}

func (m *VolumeManager) GetNode(name string) (*longhorn.Node, error) {
	return m.ds.GetNode(name)
}

func (m *VolumeManager) GetNodeTags() ([]string, error) {
	foundTags := make(map[string]struct{})
	var tags []string

	nodeList, err := m.ListNodesSorted()
	if err != nil {
		return nil, errors.Wrapf(err, "fail to list nodes")
	}
	for _, node := range nodeList {
		for _, tag := range node.Spec.Tags {
			if _, ok := foundTags[tag]; !ok {
				foundTags[tag] = struct{}{}
				tags = append(tags, tag)
			}
		}
	}
	return tags, nil
}

func (m *VolumeManager) UpdateNode(n *longhorn.Node) (*longhorn.Node, error) {
	// We need to make sure the tags passed in are valid before updating the node.
	tags, err := util.ValidateTags(n.Spec.Tags)
	if err != nil {
		return nil, err
	}
	n.Spec.Tags = tags

	node, err := m.ds.UpdateNode(n)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Updated node %v to %+v", node.Spec.Name, node.Spec)
	return node, nil
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

func (m *VolumeManager) DeleteNode(name string) error {
	node, err := m.ds.GetNode(name)
	if err != nil {
		return err
	}
	engines, err := m.ds.ListEnginesByNode(name)
	if err != nil {
		return err
	}
	condition := types.GetCondition(node.Status.Conditions, types.NodeConditionTypeReady)
	// Only could delete node from longhorn if kubernetes node missing or manager pod is missing
	if condition.Status == types.ConditionStatusTrue ||
		(condition.Reason != types.NodeConditionReasonKubernetesNodeGone &&
			condition.Reason != types.NodeConditionReasonManagerPodMissing) ||
		node.Spec.AllowScheduling || len(engines) > 0 {
		return fmt.Errorf("Could not delete node %v with node ready condition is %v, reason is %v, node schedulable %v, and %v engine running on it", name,
			condition.Status, condition.Reason, node.Spec.AllowScheduling, len(engines))
	}

	retainDisksEnabled, err := m.ds.GetSettingAsBool(types.SettingNameRetainDisksDuringNodeDeletion)
	if err != nil {
		return err
	}
	if !retainDisksEnabled {
		disks, err := m.ds.ListDisksByNode(node.Name)
		if err != nil {
			return errors.Wrapf(err, "failed to list disks before node removal")
		}
		for _, disk := range disks {
			if err := m.DeleteDisk(disk.Name); err != nil {
				return errors.Wrapf(err, "failed to delete disk %v before node removal", disk.Name)
			}
		}
	}

	if err := m.ds.DeleteNode(name); err != nil {
		return err
	}
	logrus.Debugf("Deleted node %v", name)
	return nil
}
