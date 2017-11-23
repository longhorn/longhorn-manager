package manager

import (
	"fmt"
	"strconv"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/rancher/longhorn-manager/kvstore"
	"github.com/rancher/longhorn-manager/scheduler"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

var (
	NodeCheckinIntervalInSeconds = 60
	NodeCheckinMaximumGap        = 2 * NodeCheckinIntervalInSeconds
)

func (m *VolumeManager) RegisterNode(port int) error {
	currentInfo := m.orch.GetCurrentNode()
	currentInfo.ManagerPort = port

	existInfo, err := m.ds.GetNode(currentInfo.ID)
	if err != nil {
		return err
	}
	if existInfo == nil {
		if err := m.ds.CreateNode(currentInfo); err != nil {
			return err
		}
	} else {
		if err := kvstore.UpdateResourceVersion(currentInfo, existInfo); err != nil {
			return err
		}
		if err := m.ds.UpdateNode(currentInfo); err != nil {
			return err
		}
	}
	m.currentNode = &Node{
		NodeInfo: *currentInfo,
		m:        m,
	}
	if err := m.rpc.StartServer(m.currentNode.GetManagerAddress(), m.EventChan); err != nil {
		return err
	}
	go m.nodeHealthCheckin()
	return nil
}

func (m *VolumeManager) nodeHealthCheckin() {
	info := &m.currentNode.NodeInfo
	for {
		//TODO If ResourceVersion of the node changed outside of this node, it will fail to update
		info.LastCheckin = util.Now()
		if err := m.ds.UpdateNode(info); err != nil {
			logrus.Errorf("cannot update node checkin in data store: %v", err)
		}
		time.Sleep(time.Duration(NodeCheckinIntervalInSeconds) * time.Second)
	}
}

func (m *VolumeManager) GetCurrentNode() *Node {
	return m.currentNode
}

func (m *VolumeManager) GetNode(nodeID string) (*Node, error) {
	info, err := m.ds.GetNode(nodeID)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, fmt.Errorf("cannot find node %v", nodeID)
	}
	node := &Node{
		NodeInfo: *info,
		m:        m,
	}
	return node, nil
}

func (m *VolumeManager) GetRandomNode() (*Node, error) {
	var node *types.NodeInfo
	nodes, err := m.ds.ListNodes()
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
			n.Name, n.IP, n.LastCheckin)
	}

	if node == nil {
		return nil, fmt.Errorf("cannot find healthy node")
	}
	return &Node{
		NodeInfo: *node,
		m:        m,
	}, nil
}

func (m *VolumeManager) ListNodes() (map[string]*types.NodeInfo, error) {
	return m.ds.ListNodes()
}

func (m *VolumeManager) ListSchedulingNodes() (map[string]*scheduler.Node, error) {
	nodes, err := m.ds.ListNodes()
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

func (m *VolumeManager) Node2OrchestratorAddress(nodeID string) (string, error) {
	node, err := m.GetNode(nodeID)
	if err != nil {
		return "", err
	}
	return node.GetOrchestratorAddress(), nil
}

func (n *Node) GetOrchestratorAddress() string {
	return n.IP + ":" + strconv.Itoa(n.OrchestratorPort)
}

func (n *Node) GetManagerAddress() string {
	return n.IP + ":" + strconv.Itoa(n.ManagerPort)
}

func (n *Node) GetAPIAddress() string {
	return n.IP + ":" + strconv.Itoa(types.DefaultAPIPort)
}

func (n *Node) Notify(volumeName string) error {
	if err := n.m.rpc.NodeNotify(n.GetManagerAddress(),
		&Event{
			Type:       EventTypeNotify,
			VolumeName: volumeName,
		}); err != nil {
		return err
	}
	return nil
}

func (m *VolumeManager) GetCurrentNodeID() string {
	return m.currentNode.ID
}

func (m *VolumeManager) Node2APIAddress(nodeID string) (string, error) {
	node, err := m.GetNode(nodeID)
	if err != nil {
		return "", err
	}
	return node.GetAPIAddress(), nil
}
