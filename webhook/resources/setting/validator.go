package setting

import (
	"fmt"

	"github.com/pkg/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
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
	oldSetting, ok := oldObj.(*longhorn.Setting)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("oldOjb %v is not a *longhorn.Setting", oldObj), "")
	}
	newSetting, ok := newObj.(*longhorn.Setting)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("newObj %v is not a *longhorn.Setting", newObj), "")
	}

	settingDefinition, isExist := types.GetSettingDefinition(types.SettingName(newSetting.Name))
	if !isExist {
		return werror.NewInvalidError(fmt.Sprintf("setting %s definition does not exist", newSetting.Name), "metadata.name")
	}

	if settingDefinition.ApplicableDataEngines == nil {
		return werror.NewInvalidError(fmt.Sprintf("setting %s definition does not have applicable engines defined", newSetting.Name), "metadata.name")
	}

	if ok := settingDefinition.ApplicableDataEngines[longhorn.DataEngineTypeAll]; ok {
		if len(newSetting.DefaultsByDataEngine) > 0 {
			return werror.NewInvalidError("defaultsByDataEngine cannot be set when applicableDataEngines includes 'all'. Use the 'default' field instead", "defaultsByDataEngine")
		}
	}

	numOfApplicableDataEngines := getNumOfApplicableDataEngines(settingDefinition.ApplicableDataEngines)
	if numOfApplicableDataEngines == 1 {
		if len(newSetting.DefaultsByDataEngine) > 0 {
			return werror.NewInvalidError("defaultsByDataEngine cannot be set when there is only one applicable engine. Use the 'default' field instead", "defaultsByDataEngine")
		}
	}

	for engine := range newSetting.DefaultsByDataEngine {
		_, applicable := settingDefinition.ApplicableDataEngines[engine]
		if !applicable {
			return werror.NewInvalidError(fmt.Sprintf("defaultsByDataEngine for %s cannot be set because the setting is not applicable to %s", engine, engine), "defaultsByDataEngine")
		}
	}

	definition, isExist := types.GetSettingDefinition(types.SettingName(newSetting.Name))
	if !isExist {
		return werror.NewInvalidError(fmt.Sprintf("setting %s does not exist", newSetting.Name), "metadata.name")
	}

	_, isFromLHOld := oldSetting.Annotations[types.GetLonghornLabelKey(types.UpdateSettingFromLonghorn)]
	_, isFromLH := newSetting.Annotations[types.GetLonghornLabelKey(types.UpdateSettingFromLonghorn)]
	if definition.ReadOnly && !isFromLHOld && !isFromLH {
		return werror.NewInvalidError(fmt.Sprintf("setting %s is read-only", newSetting.Name), "metadata.name")
	}

	return v.validateSetting(newObj)
}

func (v *settingValidator) Delete(request *admission.Request, oldObj runtime.Object) error {
	setting, ok := oldObj.(*longhorn.Setting)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("oldObj %v is not a *longhorn.Setting", oldObj), "")
	}

	if _, ok := types.GetSettingDefinition(types.SettingName(setting.Name)); ok {
		return werror.NewInvalidError(fmt.Sprintf("setting %s can be modified but not deleted", setting.Name),
			"metadata.name")
	}
	// If we reach this point, the setting is either from a previous version or is otherwise erroneous. Allow deletion.
	return nil
}

func (v *settingValidator) validateSetting(newObj runtime.Object) error {
	setting, ok := newObj.(*longhorn.Setting)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("newObj %v is not a *longhorn.Setting", newObj), "")
	}

	multiError := util.NewMultiError()

	// Validate the default value
	if err := v.ds.ValidateSetting(setting.Name, setting.Value); err != nil {
		multiError.Append(util.NewMultiError(err.Error()))
	}

	// Validate the per data engine defaults
	for engine, value := range setting.DefaultsByDataEngine {
		if err := v.ds.ValidateSetting(setting.Name, value); err != nil {
			multiError.Append(util.NewMultiError(fmt.Sprintf("defaultsByDataEngine for data engine %s is invalid: %s", engine, err.Error())))
		}
	}
	if len(multiError) == 0 {
		return nil
	}

	// TODO: https://github.com/longhorn/longhorn/issues/5018
	//       This is a work around for the setting restoration blocking.
	err := errors.New(multiError.Join())
	if types.ErrorIsNotSupport(err) {
		if systemRestore, e := v.ds.GetSystemRestoreInProgress(""); e != nil && !datastore.ErrorIsNotFound(e) {
			return werror.NewInvalidError(err.Error(), "")
		} else if systemRestore != nil {
			return nil
		}
	}

	return werror.NewInvalidError(err.Error(), "value")
}

func getNumOfApplicableDataEngines(applicableDataEngines map[longhorn.DataEngineType]bool) int {
	count := 0
	for dataEngine, applicable := range applicableDataEngines {
		if dataEngine == longhorn.DataEngineTypeAll {
			continue
		}
		if applicable {
			count++
		}
	}
	return count
}
