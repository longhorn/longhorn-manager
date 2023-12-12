package manager

import (
	"reflect"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/util"
)

func (m *VolumeManager) GetSecret(name string) (*corev1.Secret, error) {
	namespace, err := m.ds.GetLonghornNamespace()
	if err != nil {
		return nil, err
	}
	return m.ds.GetSecretRO(namespace.Name, name)
}

func (m *VolumeManager) ListSecretsSorted() ([]*corev1.Secret, error) {
	namespace, err := m.ds.GetLonghornNamespace()
	if err != nil {
		return nil, err
	}
	secretList, err := m.ds.ListSecretsRO(namespace.Name)
	if err != nil {
		return []*corev1.Secret{}, err
	}

	itemMap := map[string]*corev1.Secret{}
	for _, itemRO := range secretList {
		itemMap[itemRO.Name] = itemRO
	}

	secrets := make([]*corev1.Secret, len(secretList))
	sortedSecrets, err := util.SortKeys(itemMap)
	if err != nil {
		return []*corev1.Secret{}, err
	}
	for i, name := range sortedSecrets {
		secrets[i] = itemMap[name]
	}
	return secrets, nil
}

func (m *VolumeManager) CreateSecret(secret *corev1.Secret) (*corev1.Secret, error) {
	namespace, err := m.ds.GetLonghornNamespace()
	if err != nil {
		return nil, err
	}

	s, err := m.ds.CreateSecret(namespace.Name, secret)
	if err != nil {
		return nil, err
	}
	logrus.Infof("Created secret %v", secret.Name)
	return s, nil
}

func (m *VolumeManager) UpdateSecret(secret *corev1.Secret) (*corev1.Secret, error) {
	var err error
	defer func() {
		err = errors.Wrapf(err, "unable to update %v secret", secret.Name)
	}()
	namespace, err := m.ds.GetLonghornNamespace()
	if err != nil {
		return nil, err
	}

	existingSecret, err := m.ds.GetSecret(namespace.Name, secret.Name)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get secret %v", secret.Name)
	}
	existingSecretDataSorted, err := util.SortKeys(existingSecret.Data)
	if err != nil {
		return nil, err
	}
	secretDataSorted, err := util.SortKeys(secret.Data)
	if err != nil {
		return nil, err
	}
	if existingSecret.Type == secret.Type &&
		reflect.DeepEqual(existingSecretDataSorted, secretDataSorted) {
		return secret, nil
	}

	return m.ds.UpdateSecret(namespace.Name, secret)
}

func (m *VolumeManager) DeleteSecret(name string) error {
	namespace, err := m.ds.GetLonghornNamespace()
	if err != nil {
		return err
	}
	if err := m.ds.DeleteSecret(namespace.Name, name); err != nil {
		return err
	}
	logrus.Infof("Deleted secret %v", name)
	return nil
}
