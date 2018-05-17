package manager

import (
	"github.com/pkg/errors"
	"strings"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

func (m *VolumeManager) GetSetting() (*longhorn.Setting, error) {
	return m.ds.GetSetting()
}

func (m *VolumeManager) UpdateSetting(s *longhorn.Setting) (*longhorn.Setting, error) {
	if err := m.syncEngineUpgradeImage(s.EngineUpgradeImage); err != nil {
		return nil, err
	}
	return m.ds.UpdateSetting(s)
}

func (m *VolumeManager) syncEngineUpgradeImage(image string) error {
	deployed, err := m.listEngineUpgradeImage()
	if err != nil {
		return errors.Wrapf(err, "failed to get engine upgrade images")
	}

	toDeploy := make(map[string]struct{})
	toDelete := make(map[string]struct{})

	images := strings.Split(image, ",")
	for _, image := range images {
		if deployed[image] == "" {
			toDeploy[image] = struct{}{}
		} else {
			delete(deployed, image)
		}
	}
	for image := range deployed {
		toDelete[image] = struct{}{}
	}

	for image := range toDelete {
		if err := m.deleteEngineUpgradeImage(image); err != nil {
			return errors.Wrapf(err, "failed to delete engine upgrade image")
		}
	}
	for image := range toDeploy {
		if err := m.createEngineUpgradeImage(image); err != nil {
			return errors.Wrapf(err, "failed to create engine upgrade image")
		}
	}
	return nil
}

func (m *VolumeManager) listEngineUpgradeImage() (map[string]string, error) {
	return nil, nil
}

func (m *VolumeManager) deleteEngineUpgradeImage(image string) error {
	return nil
}

func (m *VolumeManager) createEngineUpgradeImage(image string) error {
	return nil
}
