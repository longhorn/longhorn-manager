package manager

import (
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// ListVolumeHookPolicies lists the hook policies of one namespace from the
// datastore cache.
func (m *VolumeManager) ListVolumeHookPolicies(namespace string) ([]*longhorn.VolumeHookPolicy, error) {
	return m.ds.ListVolumeHookPoliciesRO(namespace)
}
