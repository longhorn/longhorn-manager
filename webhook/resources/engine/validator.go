package engine

import (
	"fmt"

	"github.com/pkg/errors"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type engineValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &engineValidator{ds: ds}
}

func (e *engineValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "engines",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Engine{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (e *engineValidator) Create(request *admission.Request, newObj runtime.Object) error {
	engine := newObj.(*longhorn.Engine)

	if engine.Spec.BackendStoreDriver == longhorn.BackendStoreDriverTypeSPDK {
		spdkEnabled, err := e.ds.GetSettingAsBool(types.SettingNameSpdk)
		if err != nil {
			err = errors.Wrapf(err, "failed to get spdk setting")
			return werror.NewInvalidError(err.Error(), "")
		}
		if !spdkEnabled {
			return werror.NewInvalidError("SPDK data engine is not enabled", "")
		}
	}

	return nil
}

func (e *engineValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldEngine := oldObj.(*longhorn.Engine)
	newEngine := newObj.(*longhorn.Engine)

	if oldEngine.Spec.BackendStoreDriver != "" {
		if oldEngine.Spec.BackendStoreDriver != newEngine.Spec.BackendStoreDriver {
			err := fmt.Errorf("changing backend store driver for engine %v is not supported", oldEngine.Name)
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	return nil
}
