package manager

import corev1 "k8s.io/api/core/v1"

func (m *VolumeManager) ListSecrets() ([]*corev1.Secret, error) {
	namespace, err := m.ds.GetLonghornNamespace()
	if err != nil {
		return []*corev1.Secret{}, err
	}

	secrets, err := m.ds.ListSecret(namespace.Name)
	if err != nil {
		return []*corev1.Secret{}, err
	}
	return secrets, err
}
