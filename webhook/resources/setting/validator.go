package setting

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type settingValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &settingValidator{ds: ds}
}

func (v *settingValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "settings",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Setting{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
			admissionregv1.Delete,
		},
	}
}

func (v *settingValidator) Create(request *admission.Request, newObj runtime.Object) error {
	return v.validateSetting(newObj)
}

func (v *settingValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	setting, ok := newObj.(*longhorn.Setting)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("newObj %v is not a *longhorn.Setting", newObj), "")
	}

	settingDef, isExist := types.GetSettingDefinition(types.SettingName(setting.Name))
	if !isExist {
		return werror.NewInvalidError(fmt.Sprintf("setting %s does not exist", setting.Name), "metadata.name")
	}
	existingSetting := oldObj.(*longhorn.Setting)
	_, isFromLHOld := existingSetting.Annotations[types.GetLonghornLabelKey(types.UpdateSettingFromLonghorn)]
	_, isFromLH := setting.Annotations[types.GetLonghornLabelKey(types.UpdateSettingFromLonghorn)]
	if settingDef.ReadOnly && !isFromLHOld && !isFromLH {
		return werror.NewInvalidError(fmt.Sprintf("setting %s is read-only", setting.Name), "metadata.name")
	}
	if setting.Name == string(types.SettingNameDataEngineProvisioningMode) {
		changedDataEngines, err := changedDataEngineValues(types.SettingNameDataEngineProvisioningMode, existingSetting.Value, setting.Value)
		if err != nil {
			return werror.NewInvalidError(fmt.Sprintf("failed to compare provisioning mode values: %v", err), "value")
		}
		for _, dataEngine := range changedDataEngines {
			selector := labels.SelectorFromSet(labels.Set{
				types.GetLonghornLabelKey(types.LonghornLabelDataEngine): string(dataEngine),
			})
			volumes, err := v.ds.ListVolumesBySelectorRO(selector)
			if err != nil {
				return werror.NewInvalidError(fmt.Sprintf("failed to list %v volumes before changing the provisioning mode: %v", dataEngine, err), "value")
			}
			if len(volumes) > 0 {
				return werror.NewInvalidError(fmt.Sprintf("cannot change the %v data engine provisioning mode while %d %v volumes exist; delete them before changing the mode", dataEngine, len(volumes), dataEngine), "value")
			}
		}
	}
	return v.validateSetting(newObj)
}

func (v *settingValidator) Delete(request *admission.Request, oldObj runtime.Object) error {
	setting, ok := oldObj.(*longhorn.Setting)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("oldObj %v is not a *longhorn.Setting", oldObj), "")
	}

	if _, ok := types.GetSettingDefinition(types.SettingName(setting.Name)); ok {
		return werror.NewInvalidError(fmt.Sprintf("setting %s can be modified but not deleted", setting.Name), "metadata.name")
	}
	// If we reach this point, the setting is either from a previous version or is otherwise erroneous. Allow deletion.
	return nil
}

func (v *settingValidator) validateSetting(newObj runtime.Object) error {
	setting, ok := newObj.(*longhorn.Setting)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("newObj %v is not a *longhorn.Setting", newObj), "")
	}

	err := v.ds.ValidateSetting(setting.Name, setting.Value)
	if err == nil {
		return nil
	}

	// TODO: https://github.com/longhorn/longhorn/issues/5018
	//       This is a work around for the setting restoration blocking.
	if types.ErrorIsNotSupport(err) {
		if systemRestore, e := v.ds.GetSystemRestoreInProgress(""); e != nil && !datastore.ErrorIsNotFound(e) {
			return werror.NewInvalidError(err.Error(), "")
		} else if systemRestore != nil {
			return nil
		}
	}

	return werror.NewInvalidError(err.Error(), "value")
}

// changedDataEngineValues returns, sorted, the data engines whose value of a
// data-engine-specific setting differs between oldValue and newValue. An empty
// value stands for the definition default; a plain value applies to every data
// engine the setting supports, like the setting parser does.
func changedDataEngineValues(settingName types.SettingName, oldValue, newValue string) ([]longhorn.DataEngineType, error) {
	definition, ok := types.GetSettingDefinition(settingName)
	if !ok {
		return nil, fmt.Errorf("setting %v is not supported", settingName)
	}
	parse := func(value string) (map[longhorn.DataEngineType]any, error) {
		value = strings.TrimSpace(value)
		if value == "" {
			value = definition.Default
		}
		if types.IsJSONFormat(value) {
			return types.ParseDataEngineSpecificSetting(definition, value)
		}
		return types.ParseSettingSingleValue(definition, value)
	}
	oldValues, err := parse(oldValue)
	if err != nil {
		return nil, err
	}
	newValues, err := parse(newValue)
	if err != nil {
		return nil, err
	}

	changed := []longhorn.DataEngineType{}
	for dataEngine, value := range newValues {
		if oldValues[dataEngine] != value {
			changed = append(changed, dataEngine)
		}
	}
	sort.Slice(changed, func(i, j int) bool { return changed[i] < changed[j] })
	return changed, nil
}
