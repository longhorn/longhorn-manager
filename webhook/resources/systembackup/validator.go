package systembackup

import (
	"github.com/pkg/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
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

func (v *systemBackupValidator) Create(request *admission.Request, newObj runtime.Object) (err error) {
	defer func() {
		if err == nil {
			return
		}
		err = werror.NewInternalError(err.Error())
	}()

	systemBackupInCluster := newObj.(*longhorn.SystemBackup)
	if longhornVersion := systemBackupInCluster.Labels[types.GetVersionLabelKey()]; longhornVersion != "" {
		// Do not validate SystemBackup created by the backup target controller
		return nil
	}

	backupTarget, err := v.ds.GetDefaultBackupTargetRO()
	if err != nil {
		return err
	}

	backupTargetClient, err := engineapi.NewBackupTargetClientFromBackupTarget(backupTarget, v.ds)
	if err != nil {
		return err
	}

	systemBackupsInBackupstore, err := backupTargetClient.ListSystemBackup()
	if err != nil {
		return errors.Wrapf(err, "failed to list system backups in %v", backupTargetClient.URL)
	}

	for systemBackupInBackupStore := range systemBackupsInBackupstore {
		if string(systemBackupInBackupStore) == systemBackupInCluster.Name {
			return errors.Errorf("%v already exists in %v: %v", systemBackupInCluster.Name, backupTargetClient.URL, systemBackupsInBackupstore)
		}
	}

	return nil
}
