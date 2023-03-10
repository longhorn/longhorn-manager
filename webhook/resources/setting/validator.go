package setting

import (
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
		},
	}
}

func (v *settingValidator) Create(request *admission.Request, newObj runtime.Object) error {
	return v.validateSetting(newObj)
}

func (v *settingValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	return v.validateSetting(newObj)
}

func (v *settingValidator) validateSetting(newObj runtime.Object) error {
	setting := newObj.(*longhorn.Setting)

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
