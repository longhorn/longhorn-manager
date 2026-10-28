package scheduler

import (
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// volumeAntiAffinity holds the nodes occupied by the replicas of the volumes
// related to the volume being scheduled, as observed in the cache at the time.
type volumeAntiAffinity struct {
	// requiredNodes hold a replica of a volume related through a term in the
	// required terms of the scheduled volume.
	requiredNodes sets.Set[string]
	// preferredReplicas counts the replicas on each node of the volumes related
	// only through a preferred term.
	preferredReplicas map[string]int

	// excludedNodes are the candidate nodes that requiredNodes removed.
	excludedNodes sets.Set[string]
}

// getVolumeAntiAffinity returns nil when volume anti-affinity does not apply
// to the replica, or when no related volume exists.
func (rcs *ReplicaScheduler) getVolumeAntiAffinity(replica *longhorn.Replica, volume *longhorn.Volume) (*volumeAntiAffinity, error) {
	fromPod := volume.Spec.VolumeAntiAffinityFromPod
	// A strict-local replica stays on the attach node, even before its hard node
	// affinity is set. Other replicas with a hard node affinity already have their
	// node: the local migration of best-effort, or a move to another disk.
	if !types.IsVolumeAntiAffinityEnabled(volume) || fromPod == nil ||
		volume.Spec.DataLocality == longhorn.DataLocalityStrictLocal || replica.Spec.HardNodeAffinity != "" {
		return nil, nil
	}
	required := sets.New[string]()
	for _, term := range fromPod.RequiredTerms {
		required.Insert(types.VolumeAntiAffinityTermKey(term))
	}
	preferred := sets.New[string]()
	for _, term := range fromPod.PreferredTerms {
		preferred.Insert(types.VolumeAntiAffinityTermKey(term))
	}
	if required.Len() == 0 && preferred.Len() == 0 {
		return nil, nil
	}

	volumes, err := rcs.ds.ListVolumesRO()
	if err != nil {
		return nil, err
	}
	// The value is true for a volume related through a required term.
	related := map[string]bool{}
	for _, v := range volumes {
		other := v.Spec.VolumeAntiAffinityFromPod
		// The volumes of one pod already share the fate of the pod.
		if v.Name == volume.Name || !types.IsVolumeAntiAffinityEnabled(v) || other == nil || other.Pod == fromPod.Pod {
			continue
		}
		for _, term := range slices.Concat(other.RequiredTerms, other.PreferredTerms) {
			key := types.VolumeAntiAffinityTermKey(term)
			if required.Has(key) {
				related[v.Name] = true
				break
			}
			if preferred.Has(key) {
				related[v.Name] = false
			}
		}
	}
	if len(related) == 0 {
		return nil, nil
	}

	replicas, err := rcs.ds.ListReplicasRO()
	if err != nil {
		return nil, err
	}
	antiAffinity := &volumeAntiAffinity{
		requiredNodes:     sets.New[string](),
		preferredReplicas: map[string]int{},
		excludedNodes:     sets.New[string](),
	}
	for _, r := range replicas {
		requiredTerm, ok := related[r.Spec.VolumeName]
		// A replica that is being evicted still holds its data.
		if !ok || r.Spec.NodeID == "" || r.Spec.FailedAt != "" || r.DeletionTimestamp != nil {
			continue
		}
		if requiredTerm {
			antiAffinity.requiredNodes.Insert(r.Spec.NodeID)
		} else {
			antiAffinity.preferredReplicas[r.Spec.NodeID]++
		}
	}
	return antiAffinity, nil
}

// filterNodes removes the nodes that hold a replica of a volume related
// through a required term.
func (aa *volumeAntiAffinity) filterNodes(nodes map[string]*longhorn.Node) map[string]*longhorn.Node {
	if aa == nil || aa.requiredNodes.Len() == 0 {
		return nodes
	}
	filtered := map[string]*longhorn.Node{}
	for name, node := range nodes {
		if aa.requiredNodes.Has(name) {
			aa.excludedNodes.Insert(name)
			continue
		}
		filtered[name] = node
	}
	return filtered
}

// rankDisks keeps the disks on the nodes that hold the fewest replicas of the
// volumes related through a preferred term. It never empties the candidates.
func (aa *volumeAntiAffinity) rankDisks(disks map[string]*Disk) map[string]*Disk {
	if aa == nil || len(aa.preferredReplicas) == 0 || len(disks) == 0 {
		return disks
	}
	fewest := -1
	for _, disk := range disks {
		if count := aa.preferredReplicas[disk.NodeID]; fewest < 0 || count < fewest {
			fewest = count
		}
	}
	ranked := map[string]*Disk{}
	for name, disk := range disks {
		if aa.preferredReplicas[disk.NodeID] == fewest {
			ranked[name] = disk
		}
	}
	return ranked
}

func (aa *volumeAntiAffinity) notSatisfiedError() error {
	if aa == nil || aa.excludedNodes.Len() == 0 {
		return nil
	}
	return fmt.Errorf("nodes %v hold replicas of related volumes", sets.List(aa.excludedNodes))
}

// isLocalReplicaWanted reports whether the scheduler places the replica on
// the attach node of a best-effort volume when a disk there is a candidate.
// Volume anti-affinity does not change that placement, because the local
// migration of best-effort would move the replica back to the attach node.
func isLocalReplicaWanted(replicas map[string]*longhorn.Replica, volume *longhorn.Volume) bool {
	if volume.Spec.DataLocality != longhorn.DataLocalityBestEffort || volume.Spec.NodeID == "" {
		return false
	}
	for _, r := range replicas {
		if r.Spec.NodeID == volume.Spec.NodeID && r.Spec.FailedAt == "" {
			return false
		}
	}
	return true
}

func hasDiskOnNode(disks map[string]*Disk, nodeID string) bool {
	for _, disk := range disks {
		if disk.NodeID == nodeID {
			return true
		}
	}
	return false
}
