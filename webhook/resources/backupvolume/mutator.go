package backupvolume

import (
	"fmt"

	"github.com/pkg/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type backupVolumeMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &backupVolumeMutator{ds: ds}
}

func (b *backupVolumeMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "backupvolumes",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.BackupVolume{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (b *backupVolumeMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

func (b *backupVolumeMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	backupVolume, ok := newObj.(*longhorn.BackupVolume)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackupVolume", newObj), "")
	}

	deleteCustomResourceOnlyLabelExists, err := datastore.IsLabelLonghornDeleteCustomResourceOnlyExisting(backupVolume)
	if err != nil {
		err := errors.Wrap(err, "failed to check if the label longhorn.io/delete-custom-resource-only exists")
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if deleteCustomResourceOnlyLabelExists {
		if err := b.addBackupsDeleteCustomResourceLabel(backupVolume); err != nil {
			err := errors.Wrapf(err, "failed to add the label longhorn.io/delete-custom-resource-only to backups of the backup volume %v", backupVolume.Name)
			return nil, werror.NewInvalidError(err.Error(), "")
		}
	}

	return mutate(newObj)
}

func (b *backupVolumeMutator) addBackupsDeleteCustomResourceLabel(bv *longhorn.BackupVolume) error {
	backups, err := b.ds.ListBackupsWithBackupVolumeName(bv.Spec.BackupTargetName, bv.Spec.VolumeName)
	if err != nil {
		return errors.Wrap(err, "failed to list backups of the backup volume")
	}
	for _, backup := range backups {
		if err = datastore.AddBackupDeleteCustomResourceOnlyLabel(b.ds, backup.Name); err != nil {
			return errors.Wrapf(err, "failed to add the label longhorn.io/delete-custom-resource-only to backup %s", backup.Name)
		}
	}

	return nil
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	backupVolume, ok := newObj.(*longhorn.BackupVolume)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackupVolume", newObj), "")
	}

	var patchOps admission.PatchOps

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(backupVolume)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for backupVolume %v", backupVolume.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}
