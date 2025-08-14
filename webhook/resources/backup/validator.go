package backup

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
			admissionregv1.Update,
		},
	}
}

func (b *backupValidator) Create(request *admission.Request, newObj runtime.Object) error {
	backup, ok := newObj.(*longhorn.Backup)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Backup", newObj), "")
	}

	if !util.ValidateName(backup.Name) {
		return werror.NewInvalidError(fmt.Sprintf("invalid name %v", backup.Name), "")
	}

	if backup.Spec.BackupMode != longhorn.BackupModeFull &&
		backup.Spec.BackupMode != longhorn.BackupModeIncremental {
		return werror.NewInvalidError(fmt.Sprintf("BackupMode %v is not a valid option", backup.Spec.BackupMode), "")
	}

	// Check if backup target exists and is available
	backupTargetName := backup.Labels[types.LonghornLabelBackupTarget]
	if backupTargetName == "" {
		return werror.NewInvalidError("missing backup target label on backup object", "")
	}
	backupTarget, err := b.ds.GetBackupTarget(backupTargetName)
	if err != nil {
		return werror.NewInvalidError(fmt.Sprintf("failed to get backup target %s: %v", backupTargetName, err), "")
	}

	if err := b.validateBackupBlockSize(backup, true); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if !backupTarget.Status.Available {
		return werror.NewInvalidError(fmt.Sprintf("backup target %s is not available", backupTargetName), "")
	}

	if backup.Spec.SnapshotName != "" {
		//check if label volume name matches snapshot volume name
		labelVolumeName := backup.Labels[types.LonghornLabelBackupVolume]
		snapshot, err := b.ds.GetSnapshotRO(backup.Spec.SnapshotName)
		if err != nil {
			return werror.NewInvalidError(fmt.Sprintf("snapshot %v is invalid", snapshot), "")
		}
		volume, err := b.ds.GetVolumeRO(snapshot.Spec.Volume)
		if err != nil {
			return werror.NewInvalidError(fmt.Sprintf("volume %v is invalid", snapshot), "")
		}
		snapshotVolumeName := volume.Name

		if labelVolumeName != snapshotVolumeName {
			return werror.NewInvalidError(fmt.Sprintf("snapshot volume name %s and label volume name %s does not match", snapshotVolumeName, labelVolumeName), "")
		}

		//check if volume backup target matches labelbackup target
		volumeBackupTargetName := volume.Spec.BackupTargetName
		if volumeBackupTargetName != backupTargetName {
			return werror.NewInvalidError(fmt.Sprintf("volume backup target %s and label backup target %s does not match", volumeBackupTargetName, backupTargetName), "")
		}
	}

	return nil
}

func (b *backupValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldBackup, ok := oldObj.(*longhorn.Backup)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("oldObj %v is not a longhorn.Backup", oldObj), "")
	}
	newBackup, ok := newObj.(*longhorn.Backup)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("newObj %v is not a longhorn.Backup", newObj), "")
	}

	// Allow backup block size mutation only when the existing obj is not set, or correcting the existed invalid value
	if oldBackup.Spec.BackupBlockSize != newBackup.Spec.BackupBlockSize {
		if b.validateBackupBlockSize(oldBackup, false) == nil {
			err := fmt.Errorf("changing backup block size for backup %v is not supported; only changing invalid backup block size is supported", oldBackup.Name)
			return werror.NewInvalidError(err.Error(), "")
		}
		if b.validateBackupBlockSize(newBackup, false) != nil {
			err := fmt.Errorf("invalid backup block size %v for backup %v", newBackup.Spec.BackupBlockSize, oldBackup.Name)
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	return nil
}

func (b *backupValidator) validateBackupBlockSize(backup *longhorn.Backup, allowInvalid bool) error {
	// types.BackupBlockSizeInvalid indicates the block size information is unavailable. This broken backup exists but is unable to restore a volume.
	if allowInvalid && backup.Spec.BackupBlockSize == types.BackupBlockSizeInvalid {
		return nil
	}

	var volumeSize int64 = -1
	if backupVolumeName, exist := backup.Labels[types.LonghornLabelBackupVolume]; exist {
		volume, err := b.ds.GetVolumeRO(backupVolumeName)
		if err == nil {
			volumeSize = volume.Spec.Size
		}
	}
	return types.ValidateBackupBlockSize(volumeSize, backup.Spec.BackupBlockSize)
}
