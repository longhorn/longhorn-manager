package manager

import (
	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

func (m *VolumeManager) GetSetting() (*longhorn.Setting, error) {
	return m.ds.GetSetting()
}

func (m *VolumeManager) UpdateSetting(s *longhorn.Setting) (*longhorn.Setting, error) {
	return m.ds.UpdateSetting(s)
}
