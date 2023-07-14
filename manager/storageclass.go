package manager

import (
	"github.com/longhorn/longhorn-manager/util"
	storagev1 "k8s.io/api/storage/v1"
)

func (m *VolumeManager) ListStorageClassesSorted() ([]*storagev1.StorageClass, error) {
	classMap, err := m.ds.ListStorageClasses()
	if err != nil {
		return []*storagev1.StorageClass{}, err
	}

	classes := make([]*storagev1.StorageClass, len(classMap))
	classNames, err := util.SortKeys(classMap)
	if err != nil {
		return []*storagev1.StorageClass{}, err
	}

	for i, name := range classNames {
		classes[i] = classMap[name]
	}
	return classes, nil
}
