package manager

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

func (m *VolumeManager) GetSettingValueExisted(sName types.SettingName) (string, error) {
	return m.ds.GetSettingValueExisted(sName)
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
	err := m.SettingValidation(s.Name, s.Value)
	if err != nil {
		return nil, err
	}
	setting, err := m.ds.UpdateSetting(s)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return m.ds.CreateSetting(s)
		}
		return nil, err
	}
	logrus.Debugf("Updated setting %v to %v", s.Name, setting.Value)
	return setting, nil
}

func (m *VolumeManager) SettingValidation(name, value string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to set settings with invalid %v", name)
	}()
	sName := types.SettingName(name)

	definition, ok := types.SettingDefinitions[sName]
	if !ok {
		return fmt.Errorf("setting %v is not supported", sName)
	}
	if definition.Required == true && value == "" {
		return fmt.Errorf("required setting %v shouldn't be empty", sName)
	}

	switch sName {
	case types.SettingNameBackupTarget:
		vs, err := m.ds.ListStandbyVolumesRO()
		if err != nil {
			return errors.Wrapf(err, "failed to list standby volume when modifying BackupTarget")
		}
		if len(vs) != 0 {
			standbyVolumeNames := make([]string, len(vs))
			for k := range vs {
				standbyVolumeNames = append(standbyVolumeNames, k)
			}
			return fmt.Errorf("cannot modify BackupTarget since there are existing standby volumes: %v", standbyVolumeNames)
		}
		// additional check whether have $ or , have been set in BackupTarget
		regStr := `[\$\,]`
		reg := regexp.MustCompile(regStr)
		findStr := reg.FindAllString(value, -1)
		if len(findStr) != 0 {
			return fmt.Errorf("value %s, contains %v", value, strings.Join(findStr, " or "))
		}
	case types.SettingNameStorageOverProvisioningPercentage:
		// additional check whether over provisioning percentage is positive
		value, err := util.ConvertSize(value)
		if err != nil || value < 0 {
			return fmt.Errorf("value %v should be positive", value)
		}
	case types.SettingNameStorageMinimalAvailablePercentage:
		// additional check whether minimal available percentage is between 0 to 100
		value, err := util.ConvertSize(value)
		if err != nil || value < 0 || value > 100 {
			return fmt.Errorf("value %v should between 0 to 100", value)
		}
	case types.SettingNameDefaultReplicaCount:
		c, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("value %v is not int: %v", types.SettingNameDefaultReplicaCount, err)
		}
		if err := m.validateReplicaCount(c); err != nil {
			return fmt.Errorf("value %v: %v", c, err)
		}
	case types.SettingNameGuaranteedEngineCPU:
		if _, err := resource.ParseQuantity(value); err != nil {
			return errors.Wrapf(err, "invalid value %v as CPU resource", value)
		}
	case types.SettingNameBackupstorePollInterval:
		interval, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("value of %v is not int: %v", types.SettingNameBackupstorePollInterval, err)
		}
		if interval < 0 {
			return fmt.Errorf("backupstore poll interval %v shouldn't be less than 0", value)
		}

	}
	return nil
}
