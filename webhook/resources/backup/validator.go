package backup

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/pkg/errors"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
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
		Name:       "backups",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Backup{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

func (b *backupValidator) Create(request *admission.Request, newObj runtime.Object) error {
	backup := newObj.(*longhorn.Backup)

	if !util.ValidateName(backup.Name) {
		return werror.NewInvalidError(fmt.Sprintf("invalid name %v", backup.Name), "")
	}

	backupTarget, err := b.ds.GetBackupTargetRO(backup.Spec.BackupTargetName)
	if err != nil {
		return werror.NewInvalidError(errors.Wrapf(err, "failed to get backup target").Error(), "")
	}
	if backupTarget.Spec.ReadOnly && backup.Spec.SnapshotName != "" {
		return werror.NewInvalidError(fmt.Sprintf("failed to create a new backup %v on a read-only backup target %v ", backup.Name, backupTarget.Name), "")
	}

	return nil

}
