package manager

import (
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// ListVolumeHookPolicies lists the hook policies of one namespace from the
// datastore cache. An empty namespace lists the policies of every namespace.
func (m *VolumeManager) ListVolumeHookPolicies(namespace string) ([]*longhorn.VolumeHookPolicy, error) {
	if namespace == "" {
		return m.ds.ListAllVolumeHookPoliciesRO()
	}
	return m.ds.ListVolumeHookPoliciesRO(namespace)
}

// GetVolumeHookPolicy reads one hook policy.
func (m *VolumeManager) GetVolumeHookPolicy(namespace, name string) (*longhorn.VolumeHookPolicy, error) {
	return m.ds.GetVolumeHookPolicyRO(namespace, name)
}
