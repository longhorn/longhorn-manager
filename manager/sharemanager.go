package manager

import (
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (m *VolumeManager) ListShareManagers() (map[string]*longhorn.ShareManager, error) {
	return m.ds.ListShareManagers()
}

func (m *VolumeManager) ListShareManagersSorted() ([]*longhorn.ShareManager, error) {
	shareManagerMap, err := m.ds.ListShareManagers()
	if err != nil {
		return []*longhorn.ShareManager{}, err
	}

	shareManagers := make([]*longhorn.ShareManager, len(shareManagerMap))
	shareManagerNames, err := util.SortKeys(shareManagerMap)
	if err != nil {
		return []*longhorn.ShareManager{}, err
	}
	for i, shareManagerName := range shareManagerNames {
		shareManagers[i] = shareManagerMap[shareManagerName]
	}
	return shareManagers, nil
}

func (m *VolumeManager) GetShareManager(name string) (*longhorn.ShareManager, error) {
	return m.ds.GetShareManager(name)
}
