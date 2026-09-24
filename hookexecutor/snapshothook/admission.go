package snapshothook

import (
	"fmt"
)

// AdmissionFailureReason names why hook admission rejects a member, so a
// client can act on the reason instead of parsing the message.
type AdmissionFailureReason string

const (
	// AdmissionFailureNoPVC means the volume has no live PVC, so no policy
	// can apply to it and there is no namespace whose pods could be paused.
	AdmissionFailureNoPVC AdmissionFailureReason = "NoPVC"
	// AdmissionFailureNoPolicy means no policy in the PVC's namespace
	// matches it.
	AdmissionFailureNoPolicy AdmissionFailureReason = "NoPolicy"
	// AdmissionFailureMultiplePolicies means several policies match the PVC.
	AdmissionFailureMultiplePolicies AdmissionFailureReason = "MultiplePolicies"
	// AdmissionFailureNoHookForOperation means the policy that matches the
	// PVC has no hook that runs for the group's operation.
	AdmissionFailureNoHookForOperation AdmissionFailureReason = "NoHookForOperation"
	// AdmissionFailureNoRunningPod means the volume's workloadsStatus
	// reports no running pod to run the hooks in.
	AdmissionFailureNoRunningPod AdmissionFailureReason = "NoRunningPod"
	// AdmissionFailureVolumeConflict means another hooked group includes the
	// volume and has not released it.
	AdmissionFailureVolumeConflict AdmissionFailureReason = "VolumeConflict"
	// AdmissionFailureLonghornNamespace means the PVC is in the Longhorn
	// namespace, where hooks never run.
	AdmissionFailureLonghornNamespace AdmissionFailureReason = "LonghornNamespace"
)

// LonghornNamespaceMessage explains why hooks never run in the Longhorn
// namespace: its pods are privileged, so a command in one of them is a
// command on the node.
func LonghornNamespaceMessage(longhornNamespace string) string {
	return fmt.Sprintf("hooks never run in the Longhorn namespace %v: its pods are privileged, and a command in one of them is a command on the node", longhornNamespace)
}

// PolicyResolutionError means policy resolution found no usable policy
// for the volume: it has no PVC, or zero or several policies match it.
type PolicyResolutionError struct {
	Reason  AdmissionFailureReason
	Message string
}

func (e *PolicyResolutionError) Error() string {
	return e.Message
}
