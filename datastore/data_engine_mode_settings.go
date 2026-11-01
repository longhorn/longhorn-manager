package datastore

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

// ValidateDataEngineModeStartup checks raw chart intent and uncached storage
// before upgrades or default-Setting writes. Missing members remain a separate
// create-only bootstrap phase.
func (s *DataStore) ValidateDataEngineModeStartup(ctx context.Context) error {
	mode, err := types.ParseDataEngineMode(string(s.dataEngineMode))
	if err != nil {
		return err
	}
	if mode != types.DataEngineModeV2Only {
		return nil
	}
	if ctx == nil || s.namespace == "" || s.lhClient == nil || s.kubeClient == nil {
		return errors.New("v2-only startup requires a context, namespace and both API clients")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	readIntent := func() (*corev1.ConfigMap, error) {
		configMap, err := s.kubeClient.CoreV1().ConfigMaps(s.namespace).Get(ctx, types.DefaultDefaultSettingConfigMapName, metav1.GetOptions{})
		if err != nil {
			return nil, errors.Wrap(err, "failed to read raw default Settings for v2-only startup")
		}
		if err := s.validateDataEngineModeCustomizedSettings(configMap); err != nil {
			return nil, err
		}
		return configMap, nil
	}
	before, err := readIntent()
	if err != nil {
		return err
	}
	if err := ValidateDataEngineModeBootstrapState(ctx, mode, s.namespace, s.lhClient, s.kubeClient); err != nil {
		return err
	}
	after, err := readIntent()
	if err != nil {
		return err
	}
	if before.UID != after.UID || before.ResourceVersion != after.ResourceVersion {
		return errors.New("default Settings ConfigMap changed during v2-only startup validation")
	}
	return ctx.Err()
}

func (s *DataStore) InitializeDataEngineModeSettings(ctx context.Context) error {
	return InitializeDataEngineModeSettings(ctx, s.dataEngineMode, s.namespace, s.lhClient, s.kubeClient)
}

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
