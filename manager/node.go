package manager

import (
	"fmt"

	"github.com/yasker/lm-rewrite/kvstore"
)

func (m *VolumeManager) RegisterNode() error {
	currentInfo := m.orch.GetCurrentNode()

	existInfo, err := m.kv.GetNode(currentInfo.ID)
	if err != nil {
		return err
	}
	if existInfo == nil {
		if err := m.kv.CreateNode(currentInfo); err != nil {
			return err
		}
	} else {
		if err := kvstore.UpdateKVIndex(currentInfo, existInfo); err != nil {
			return err
		}
		if err := m.kv.UpdateNode(currentInfo); err != nil {
			return err
		}
	}
	m.currentNode = &Node{
		NodeInfo: *currentInfo,
		m:        m,
	}
	return nil
}

func (m *VolumeManager) GetCurrentNode() *Node {
	return m.currentNode
}

func (m *VolumeManager) GetNode(nodeID string) (*Node, error) {
	info, err := m.kv.GetNode(nodeID)
	if err != nil {
		return nil, err
	}
	node := &Node{
		NodeInfo: *info,
		m:        m,
	}
	if err := node.Connect(); err != nil {
		return nil, fmt.Errorf("fail to connect to node %v", info.ID)
	}
	return node, nil
}

func (m *VolumeManager) GetRandomNode() (*Node, error) {
	return m.GetCurrentNode(), nil
}

func (n *Node) Connect() error {
	return nil
}

func (n *Node) Notify(volumeName string) error {
	event := Event{
		Type:       EventTypeNotify,
		VolumeName: volumeName,
	}
	n.m.EventChan <- event
	return nil
}
