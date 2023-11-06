package manager

import (
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/sirupsen/logrus"
)

func (m *VolumeManager) ListObjectStoresSorted() ([]*longhorn.ObjectStore, error) {
	storeMap, err := m.ds.ListObjectStores()
	if err != nil {
		return []*longhorn.ObjectStore{}, err
	}

	stores := make([]*longhorn.ObjectStore, len(storeMap))
	storeNames, err := util.SortKeys(storeMap)
	if err != nil {
		return []*longhorn.ObjectStore{}, err
	}

	for i, name := range storeNames {
		stores[i] = storeMap[name]
	}
	return stores, nil
}

func (m *VolumeManager) GetObjectStore(name string) (*longhorn.ObjectStore, error) {
	return m.ds.GetObjectStore(name)
}

func (m *VolumeManager) CreateObjectStore(store *longhorn.ObjectStore) (*longhorn.ObjectStore, error) {
	store, err := m.ds.CreateObjectStore(store)
	if err != nil {
		return nil, err
	}
	logrus.Infof("Created Object Store %v", store.ObjectMeta.Name)
	return store, nil
}

func (m *VolumeManager) DeleteObjectStore(name string) error {
	if err := m.ds.DeleteObjectStore(name); err != nil {
		return err
	}
	logrus.Infof("Deleted Object Store %v", name)
	return nil
}

func (m *VolumeManager) UpdateObjectStore(store *longhorn.ObjectStore) (*longhorn.ObjectStore, error) {
	store, err := m.ds.UpdateObjectStore(store)
	if err != nil {
		return nil, err
	}

	logrus.Infof("Updated Object Store %v", store.ObjectMeta.Name)
	return store, nil
}
