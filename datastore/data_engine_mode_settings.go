package datastore

import (
	"github.com/cockroachdb/errors"

	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

func isDataEngineSetting(name types.SettingName) bool {
	return name == types.SettingNameV1DataEngine || name == types.SettingNameV2DataEngine
}

func (s *DataStore) validateDataEngineModeSetting(name types.SettingName, value string) error {
	mode, err := types.ParseDataEngineMode(string(s.dataEngineMode))
	if err != nil {
		return err
	}
	if mode == types.DataEngineModeV2Only && isDataEngineSetting(name) && value != v2OnlyEngineSettingValue(name) {
		return errors.New("v2-only capability requires v1-data-engine=false and v2-data-engine=true")
	}
	return nil
}

func (s *DataStore) validateDataEngineModeCustomizedSettings(configMap *corev1.ConfigMap) error {
	mode, err := types.ParseDataEngineMode(string(s.dataEngineMode))
	if err != nil {
		return err
	}
	if mode != types.DataEngineModeV2Only {
		return nil
	}
	if configMap == nil || configMap.Name != types.DefaultDefaultSettingConfigMapName || configMap.Namespace != s.namespace || configMap.UID == "" || configMap.ResourceVersion == "" || configMap.DeletionTimestamp != nil {
		return errors.New("v2-only mode requires an identified default Settings ConfigMap")
	}
	raw, err := util.GetDataContentFromYAML([]byte(configMap.Data[types.DefaultSettingYAMLFileName]))
	if err != nil {
		return err
	}
	for _, name := range []types.SettingName{types.SettingNameV1DataEngine, types.SettingNameV2DataEngine} {
		if value, present := raw[string(name)]; present {
			if err := s.validateDataEngineModeSetting(name, value); err != nil {
				return errors.Wrap(err, "invalid raw engine intent in default Settings ConfigMap")
			}
		}
	}
	return nil
}
