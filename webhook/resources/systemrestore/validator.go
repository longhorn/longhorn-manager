package systemrestore

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type systemRestoreValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &systemRestoreValidator{ds: ds}
}

func (v *systemRestoreValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "systemrestores",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.SystemRestore{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

func (v *systemRestoreValidator) Create(request *admission.Request, newObj runtime.Object) error {
	systemRestore, ok := newObj.(*longhorn.SystemRestore)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.SystemRestore", newObj), "")
	}

	areAllVolumesDetached, err := v.ds.AreAllVolumesDetachedState()
	if err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if !areAllVolumesDetached {
		return werror.NewInvalidError("all volumes need to be detached before creating SystemRestore", "")
	}

	systemRestores, err := v.ds.ListSystemRestoresInProgress()
	if err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	count := len(systemRestores)
	if count != 0 {
		return werror.NewInvalidError(fmt.Sprintf("found %v SystemRestore in progress", count), "")
	}

	_, err = v.ds.GetSystemBackupRO(systemRestore.Spec.SystemBackup)
	if err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}
