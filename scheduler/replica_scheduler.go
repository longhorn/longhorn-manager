package scheduler

import (
	"fmt"

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

type ReplicaScheduler struct {
	ds *datastore.DataStore
}

func NewReplicaScheduler(ds *datastore.DataStore) *ReplicaScheduler {
	rcScheduler := &ReplicaScheduler{
		ds: ds,
	}
	return rcScheduler
}

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
		return nil, fmt.Errorf("There's no available node for replica %v", replica)
	}
	// TODO Need to add capacity.
	// Just make sure replica of the same volume be scheduled to different nodes for now.
	preferredNodes, err := rcs.preferredNodes(nodeInfo, replicas)
	if err != nil {
		return nil, err
	}
	// if other replica has allocated to different nodes, then choose a random one
	var preferredNode *longhorn.Node
	if len(preferredNodes) == 0 {
		preferredNode = rcs.getRandomNode(nodeInfo)
	} else {
		preferredNode = rcs.getRandomNode(preferredNodes)
	}

	replica.Spec.NodeID = preferredNode.Name
	// TODO just set default directory for now
	replica.Spec.DataPath = types.DefaultLonghornDirectory + "/replicas/" + replica.Spec.VolumeName + "-" + util.RandomID()

	return replica, nil
}

func (rcs *ReplicaScheduler) getRandomNode(nodeMap map[string]*longhorn.Node) *longhorn.Node {
	var node *longhorn.Node

	// map is random in Go
	for _, node = range nodeMap {
		break
	}

	return node
}

func (rcs *ReplicaScheduler) preferredNodes(nodeInfo map[string]*longhorn.Node, replicas map[string]*longhorn.Replica) (map[string]*longhorn.Node, error) {
	preferredNode := map[string]*longhorn.Node{}
	for nodeName, node := range nodeInfo {
		isFilterd := false
		for _, r := range replicas {
			if r.Spec.NodeID != "" && r.Spec.NodeID == nodeName {
				isFilterd = true
				break
			}
		}
		if !isFilterd {
			preferredNode[nodeName] = node
		}
	}
	return preferredNode, nil
}

func (rcs *ReplicaScheduler) getNodeInfo() (map[string]*longhorn.Node, error) {
	nodeInfo, err := rcs.ds.ListNodes()
	if err != nil {
		return nil, err
	}
	scheduledNode := map[string]*longhorn.Node{}
	for _, node := range nodeInfo {
		if node != nil && node.DeletionTimestamp == nil && node.Status.State == types.NodeStateUp && node.Spec.AllowScheduling {
			scheduledNode[node.Name] = node
		}
	}
	return scheduledNode, nil
}
