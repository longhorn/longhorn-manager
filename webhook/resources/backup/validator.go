package backup

import (
	"fmt"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type backupValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &backupValidator{ds: ds}
}

func (b *backupValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:           "backups",
		Scope:          admissionregv1.NamespacedScope,
		APIGroup:       longhorn.SchemeGroupVersion.Group,
		APIVersion:     longhorn.SchemeGroupVersion.Version,
		ObjectType:     &longhorn.Backup{},
		OperationTypes: []admissionregv1.OperationType{},
	}
}

func (b *backupValidator) Create(request *admission.Request, newObj runtime.Object) error {
	backup, ok := newObj.(*longhorn.Backup)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Backup", newObj), "")
	}

	if backup.Spec.BackupMode != longhorn.BackupModeFull &&
		backup.Spec.BackupMode != longhorn.BackupModeIncremental {
		return werror.NewInvalidError(fmt.Sprintf("BackupMode %v is not a valid option", backup.Spec.BackupMode), "")
	}

	return nil
}
