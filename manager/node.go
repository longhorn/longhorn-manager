package manager

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/yasker/lm-rewrite/kvstore"
	"github.com/yasker/lm-rewrite/scheduler"
	"github.com/yasker/lm-rewrite/types"
	"github.com/yasker/lm-rewrite/util"
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
	if err := m.rpc.StartServer(currentInfo.Address); err != nil {
		return err
	}
	go m.nodeHealthCheckin()
	return nil
}

func (m *VolumeManager) nodeHealthCheckin() {
	info := &m.currentNode.NodeInfo
	for {
		//TODO If KVIndex of the node changed outside of this node, it will fail to update
		info.LastCheckin = util.Now()
		if err := m.kv.UpdateNode(info); err != nil {
			logrus.Errorf("cannot update node checkin in kvstore: %v", err)
		}
		time.Sleep(time.Duration(NodeCheckinIntervalInSeconds) * time.Second)
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
	// map is random in Go
	for _, n := range nodes {
		if !util.TimestampAfterTimeout(n.LastCheckin, NodeCheckinMaximumGap) {
			node = n
			break
		}
		logrus.Warnf("node %v(%v) is not healthy, last checkin at %v, trying next",
			n.Name, n.Address, n.LastCheckin)
	}

	if node == nil {
		return nil, fmt.Errorf("cannot find healthy node")
	}
	return &Node{
		NodeInfo: *node,
		m:        m,
	}, nil
}

func (m *VolumeManager) ListSchedulingNodes() (map[string]*scheduler.Node, error) {
	nodes, err := m.kv.ListNodes()
	if err != nil {
		return nil, err
	}
	ret := map[string]*scheduler.Node{}
	for id := range nodes {
		ret[id] = &scheduler.Node{
			ID: id,
		}
	}
	return ret, nil
}

func (n *Node) Notify(volumeName string) error {
	if err := n.m.rpc.NodeNotify(n.Address,
		&Event{
			Type:       EventTypeNotify,
			VolumeName: volumeName,
		}); err != nil {
		return err
	}
	return nil
}
