package manager

import (
	"github.com/pkg/errors"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

func (m *VolumeManager) GetSetting() (*longhorn.Setting, error) {
	return m.ds.GetSetting()
}

func (m *VolumeManager) getBackupTargetURL() (string, error) {
	settings, err := m.ds.GetSetting()
	if err != nil {
		return "", errors.New("cannot backup: unable to read settings")
	}
	backupTarget := settings.BackupTarget
	if backupTarget == "" {
		return "", errors.New("cannot backup: backupTarget not set")
	}
	return backupTarget, nil
}

func (m *VolumeManager) GetDefaultEngineImage() (string, error) {
	setting, err := m.ds.GetSetting()
	if err != nil {
		return "", errors.New("cannot backup: unable to read settings")
	}
	engineImage := setting.DefaultEngineImage
	if engineImage == "" {
		return "", errors.New("cannot backup: default engine image not set")
	}
	return engineImage, nil
}

func (m *VolumeManager) UpdateSetting(s *longhorn.Setting) (*longhorn.Setting, error) {
	return m.ds.UpdateSetting(s)
}
