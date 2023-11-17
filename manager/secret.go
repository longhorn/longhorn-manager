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

func (m *VolumeManager) GetSecretRO(namespace, name string) (*corev1.Secret, error) {
	return m.ds.GetSecretRO(namespace, name)
}

func (m *VolumeManager) CreateSecret(secret *corev1.Secret) (*corev1.Secret, error) {
	return m.ds.CreateSecret(m.GetLonghornNamespace(), secret)
}

func (m *VolumeManager) UpdateSecret(secret *corev1.Secret) (*corev1.Secret, error) {
	return m.ds.UpdateSecret(m.GetLonghornNamespace(), secret)
}
