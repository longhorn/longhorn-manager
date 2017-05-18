package manager

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/yasker/lm-rewrite/kvstore"
)

var (
	NodeCheckinIntervalInSeconds = 60
	NodeCheckinMaximumGap        = 2 * NodeCheckinIntervalInSeconds
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
	go m.nodeHealthCheckin()
	go m.rpc.startServer(currentInfo.Address)
	return nil
}

func (m *VolumeManager) nodeHealthCheckin() {
	info := m.currentNode.NodeInfo
	for {
		info.LastCheckin = util.Now()
		if err := m.kv.UpdateNode(info); err != nil {
			logrus.Errorf("cannot update node checkin in kvstore: %v", err)
		}
		time.Sleep(NodeCheckinIntervalInSeconds * time.Second)
	}
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
	return node, nil
}

func (m *VolumeManager) GetRandomNode() (*Node, error) {
	var node *types.NodeInfo
	nodes, err := m.kv.ListNodes()
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(perm); i++ {
		node = nodes[perm[i]]
		if !util.TimestampAfterTimeout(node.LastCheckin, NodeCheckinMaximumGap) {
			break
		}
		logrus.Warnf("node %v(%v) is not healthy, last checkin at %v, trying next",
			node.Name, node.Address, node.LastCheckin)
	}

	return node, nil
}

func (n *Node) Notify(volumeName string) error {
	conn, err := m.rpc.Connect(node.Address)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.NodeNotify(&Event{
		Type:       EventTypeNotify,
		VolumeName: volumeName,
	}); err != nil {
		return err
	}
	return nil
}
