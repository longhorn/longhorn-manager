package snapshothook

import (
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// IsSnapshotGroupMember reports whether the labels mark a snapshot or backup
// created by a SnapshotGroup.
func IsSnapshotGroupMember(labels map[string]string) bool {
	_, member := labels[types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroup)]
	return member
}

// BypassWarning returns the warning to record when a snapshot or backup was
// taken outside a SnapshotGroup and therefore skipped the volume's hook
// policy. It returns "" when no policy covers the operation.
func BypassWarning(policy *longhorn.VolumeHookPolicy, resolveErr error, volumeName string, operation longhorn.HookOperation) (string, error) {
	if resolveErr != nil {
		var resolution *PolicyResolutionError
		if !errors.As(resolveErr, &resolution) {
			return "", resolveErr
		}
		if resolution.Reason == AdmissionFailureMultiplePolicies {
			// Conflicting policies show the user intended hooks for this
			// volume, so warn even though resolution failed.
			return fmt.Sprintf("%v; this %v was taken outside a SnapshotGroup, so no hooks ran and the data is only crash-consistent; keep exactly one policy covering the PVC and use a SnapshotGroup with hooks for an application-consistent %v",
				resolution.Message, operation, operation), nil
		}
		// No PVC or no matching policy: a group would not have run hooks either.
		return "", nil
	}
	if !PolicyCoversOperation(policy, operation) {
		return "", nil
	}
	return fmt.Sprintf("volume %v is covered by volume hook policy %v, but this %v was taken outside a SnapshotGroup, so no hooks ran and the data is only crash-consistent; use a SnapshotGroup with hooks for an application-consistent %v",
		volumeName, PolicyNamespacedName(policy), operation, operation), nil
}
