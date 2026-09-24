package snapshothook

import (
	"fmt"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// MemberCheckFailure is why a volume cannot be a hooked member right now:
// the reason for clients that act on it, and the message admission rejects
// with.
type MemberCheckFailure struct {
	Reason  AdmissionFailureReason
	Message string
}

// CheckMemberClusterState verifies that a volume is in a state where its
// hooks can run: it has a running pod, its PVC is not in the Longhorn
// namespace, and no other hooked group is still holding it or one of its
// pods. It returns the first check that fails, along with any warnings,
// such as a pod left paused by an earlier group.
func CheckMemberClusterState(volume *longhorn.Volume, policyNamespacedName string, otherGroups []*longhorn.SnapshotGroup, longhornNamespace string) (*MemberCheckFailure, []string) {
	if len(RunningWorkloadPods(volume)) == 0 {
		return &MemberCheckFailure{
			Reason:  AdmissionFailureNoRunningPod,
			Message: fmt.Sprintf("member volume %v (policy %v) has no running pod to run its hooks in", volume.Name, policyNamespacedName),
		}, nil
	}
	if volume.Status.KubernetesStatus.Namespace == longhornNamespace {
		return &MemberCheckFailure{
			Reason:  AdmissionFailureLonghornNamespace,
			Message: fmt.Sprintf("member volume %v: %v", volume.Name, LonghornNamespaceMessage(longhornNamespace)),
		}, nil
	}
	for _, other := range otherGroups {
		if GroupHasUnreleasedMember(other, volume.Name) {
			return &MemberCheckFailure{
				Reason:  AdmissionFailureVolumeConflict,
				Message: VolumeConflictMessage(volume.Name, other.Name),
			}, nil
		}
	}
	// A pod mounting several volumes can belong to groups that share no
	// volume. The workloadsStatus records no pod UID, so this matches by
	// name; the executor re-checks by UID before pausing.
	for _, podName := range RunningWorkloadPods(volume) {
		for _, other := range otherGroups {
			if GroupHasUnreleasedPod(other, volume.Status.KubernetesStatus.Namespace, podName, "") {
				return &MemberCheckFailure{
					Reason:  AdmissionFailurePodConflict,
					Message: fmt.Sprintf("member volume %v: %v", volume.Name, PodConflictMessage(volume.Status.KubernetesStatus.Namespace, podName, other.Name)),
				}, nil
			}
		}
	}
	return nil, PostFailedHookPodWarnings(otherGroups, volume.Name)
}
