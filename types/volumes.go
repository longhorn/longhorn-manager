package types

import (
	"fmt"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func IsVolumeReady(v *longhorn.Volume, vrs []*longhorn.Replica) (ready bool, msg string) {
	var allReplicaScheduled = true
	if len(vrs) == 0 {
		allReplicaScheduled = false
	}
	for _, r := range vrs {
		if r.Spec.NodeID == "" {
			allReplicaScheduled = false
		}
	}
	scheduledCondition := GetCondition(v.Status.Conditions, longhorn.VolumeConditionTypeScheduled)
	isCloningDesired := IsDataFromVolume(v.Spec.DataSource)
	isCloningCompleted := v.Status.CloneStatus.State == longhorn.VolumeCloneStateCompleted
	if v.Spec.NodeID == "" && v.Status.State != longhorn.VolumeStateDetached {
		return false, fmt.Sprintf("waiting for the volume to fully detach. current state: %v", v.Status.State)
	}
	if v.Status.State == longhorn.VolumeStateDetached && scheduledCondition.Status != longhorn.ConditionStatusTrue && !allReplicaScheduled {
		return false, "volume is currently in detached state with some un-schedulable replicas"
	}
	if v.Status.Robustness == longhorn.VolumeRobustnessFaulted {
		return false, "volume is faulted"
	}
	if v.Status.RestoreRequired {
		return false, "restore is required"
	}
	if isCloningDesired && !isCloningCompleted {
		return false, "volume has not finished cloning data"
	}
	return true, ""
}
