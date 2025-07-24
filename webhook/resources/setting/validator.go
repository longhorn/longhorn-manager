package setting

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
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

	if !cmp.Equal(oldSetting.ApplicableEngines, newSetting.ApplicableEngines) {
		return werror.NewInvalidError("applicableEngines cannot be modified", "applicableEngines")
	}

	numOfApplicableEngines := getNumOfApplicableEngines(oldSetting.ApplicableEngines)
	if numOfApplicableEngines == 0 || numOfApplicableEngines == 1 {
		if len(newSetting.DefaultsByEngine) > 0 {
			return werror.NewInvalidError("defaultsByEngine cannot be set when applicableEngines is empty or has only one data engine. Use the 'default' field instead", "defaultsByEngine")
		}
	}

	for engine := range newSetting.DefaultsByEngine {
		_, applicable := oldSetting.ApplicableEngines[engine]
		if !applicable {
			return werror.NewInvalidError(fmt.Sprintf("defaultsByEngine for %s cannot be set because the setting is not applicable to %s", engine, engine), "defaultsByEngine")
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
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Setting", oldObj), "")
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
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Setting", newObj), "")
	}

	multiError := util.NewMultiError()

	// Validate the default value
	if err := v.ds.ValidateSetting(setting.Name, setting.Value); err != nil {
		multiError.Append(util.NewMultiError(err.Error()))
	}

	// Validate the per data engine defaults
	for engine, value := range setting.DefaultsByEngine {
		if err := v.ds.ValidateSetting(setting.Name, value); err != nil {
			multiError.Append(util.NewMultiError(fmt.Sprintf("defaultsByEngine for data engine %s is invalid: %s", engine, err.Error())))
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

func getNumOfApplicableEngines(applicableEngines map[longhorn.DataEngineType]bool) int {
	numApplicable := 0
	for _, applicable := range applicableEngines {
		if applicable {
			numApplicable++
		}
	}
	return numApplicable
}
