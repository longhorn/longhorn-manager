package snapshothook

import (
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// PolicyNamespacedName renders the policy as namespace/name.
func PolicyNamespacedName(policy *longhorn.VolumeHookPolicy) string {
	return policy.Namespace + "/" + policy.Name
}

// PolicyRef builds the hookPolicyRef that admission records on a
// SnapshotGroup member for a resolved policy.
func PolicyRef(policy *longhorn.VolumeHookPolicy) *longhorn.VolumeHookPolicyRef {
	return &longhorn.VolumeHookPolicyRef{
		Namespace:  policy.Namespace,
		Name:       policy.Name,
		UID:        policy.UID,
		Generation: policy.Generation,
	}
}

// PolicyRefNamespacedName renders a recorded policy reference as namespace/name, or
// "none" when unset.
func PolicyRefNamespacedName(ref *longhorn.VolumeHookPolicyRef) string {
	if ref == nil {
		return "none"
	}
	return ref.Namespace + "/" + ref.Name
}

// PVCRefNamespacedName renders a recorded claim as namespace/name, or "" when unset.
func PVCRefNamespacedName(ref *longhorn.PVCRef) string {
	if ref == nil {
		return ""
	}
	return ref.Namespace + "/" + ref.Name
}
