package manager

import (
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/rancher/longhorn-manager/types"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

func (m *VolumeManager) GetSettingValueExisted(sName types.SettingName) (string, error) {
	setting, err := m.GetSetting(sName)
	if err != nil {
		return "", err
	}
	if setting.Value == "" {
		return "", fmt.Errorf("setting %v is empty", sName)
	}
	return setting.Value, nil
}

func (m *VolumeManager) GetSetting(sName types.SettingName) (*longhorn.Setting, error) {
	return m.ds.GetSetting(sName)
}

func (m *VolumeManager) ListSettings() (map[types.SettingName]*longhorn.Setting, error) {
	return m.ds.ListSettings()
}

func (m *VolumeManager) ListSettingsSorted() ([]*longhorn.Setting, error) {
	settingMap, err := m.ListSettings()
	if err != nil {
		return []*longhorn.Setting{}, err
	}

	settings := make([]*longhorn.Setting, len(settingMap))
	settingNames, err := sortKeys(settingMap)
	if err != nil {
		return []*longhorn.Setting{}, err
	}
	for i, settingName := range settingNames {
		settings[i] = settingMap[types.SettingName(settingName)]
	}
	return settings, nil
}

func (m *VolumeManager) CreateOrUpdateSetting(s *longhorn.Setting) (*longhorn.Setting, error) {
	setting, err := m.ds.UpdateSetting(s)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return m.ds.CreateSetting(s)
		}
		return nil, err
	}
	return setting, nil
}
