package systembackup

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type systemBackupValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &systemBackupValidator{ds: ds}
}

func (v *systemBackupValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "systembackups",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.SystemBackup{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

func (v *systemBackupValidator) Create(request *admission.Request, newObj runtime.Object) error {
	systemBackup, ok := newObj.(*longhorn.SystemBackup)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackupBackingImage", newObj), "")
	}

	backupTarget, err := v.ds.GetBackupTargetRO(types.DefaultBackupTargetName)
	if err != nil {
		return werror.NewBadRequest(err.Error())
	}

	backupType, err := util.CheckBackupType(backupTarget.Spec.BackupTargetURL)
	if err != nil {
		return werror.NewBadRequest(err.Error())
	}

	if types.BackupStoreRequireCredential(backupType) {
		if backupTarget.Spec.CredentialSecret == "" {
			return werror.NewBadRequest(fmt.Sprintf("cannot access %s without credential secret", backupType))
		}
	}

	isLonghornCreated, err := datastore.IsLabelLonghornCreateCustomResourceFromLonghornExisting(systemBackup)
	if err != nil {
		return werror.NewInvalidError(fmt.Sprintf("failed to get system backup %s label: %v, ", systemBackup.Name, err), "")
	}
	if !isLonghornCreated && !backupTarget.Status.Available {
		return werror.NewInvalidError(fmt.Sprintf("backup target %s is not available", types.DefaultBackupTargetName), "")
	}

	return nil
}
