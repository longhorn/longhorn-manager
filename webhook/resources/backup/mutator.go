package backup

import (
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/errors"
	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type backupMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &backupMutator{ds: ds}
}

func (b *backupMutator) Resource() admission.Resource {
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

func (b *backupMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	backup, ok := newObj.(*longhorn.Backup)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Backup", newObj), "")
	}

	var patchOps admission.PatchOps

	var err error
	if patchOps, err = mutate(newObj); err != nil {
		return nil, err
	}

	specLabels := backup.Spec.Labels
	if specLabels == nil {
		specLabels = make(map[string]string)
	}

	metaLabels := backup.Labels
	if metaLabels == nil {
		metaLabels = map[string]string{}
	}

	//check snapshot name, if not exist, return error

	volumeName, isExist := backup.Labels[types.LonghornLabelBackupVolume]
	if !isExist {
		if backup.Spec.SnapshotName == "" {
			err := errors.Wrapf(err, "cannot find the backup volume label for backup %v", backup.Name)
			return nil, werror.NewInvalidError(err.Error(), "")
		}

		// Try to get volume name from snapshot
		snapshot, err := b.ds.GetSnapshotRO(backup.Spec.SnapshotName)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get the snapshot %v", backup.Spec.SnapshotName)
		}

		volume, err := b.ds.GetVolumeRO(snapshot.Spec.Volume)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get the volume %v of the snapshot %v of the backup %v", snapshot.Spec.Volume, snapshot.Name, backup.Name)
		}

		volumeName = volume.Name
		metaLabels[types.LonghornLabelBackupVolume] = volumeName
	}

	if _, isExist := specLabels[types.GetLonghornLabelKey(types.LonghornLabelVolumeAccessMode)]; !isExist {
		volumeAccessMode := longhorn.AccessModeReadWriteOnce
		if volume, err := b.ds.GetVolumeRO(volumeName); err == nil {
			if volume.Spec.AccessMode != "" {
				volumeAccessMode = volume.Spec.AccessMode
			}
		}
		specLabels[types.GetLonghornLabelKey(types.LonghornLabelVolumeAccessMode)] = string(volumeAccessMode)
	}

	valueBackupLabels, err := json.Marshal(specLabels)
	if err != nil {
		return nil, werror.NewInvalidError(errors.Wrapf(err, "failed to convert backup labels into JSON string").Error(), "")
	}
	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/labels", "value": %s}`, string(valueBackupLabels)))

	if backup.Spec.BackupMode == longhorn.BackupModeIncrementalNone {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/backupMode", "value": "%s"}`, string(longhorn.BackupModeIncremental)))
	}

	if backup.Spec.SnapshotName != "" {
		volume, err := b.ds.GetVolumeRO(volumeName)
		if err != nil {
			return nil, werror.NewInvalidError(errors.Wrapf(err, "failed to get backup block size from the volume %v of backup %v", volumeName, backup.Name).Error(), "")
		}
		if backup.Spec.BackupBlockSize != volume.Spec.BackupBlockSize {
			patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/backupBlockSize", "value": "%d"}`, volume.Spec.BackupBlockSize))
		}
	}

	backupTargetName, ok := backup.Labels[types.LonghornLabelBackupTarget]
	if !ok {
		volume, err := b.ds.GetVolumeRO(volumeName)
		if err != nil {
			err := errors.Wrapf(err, "failed to get the volume %v of backup %v", volumeName, backup.Name)
			return nil, werror.NewInvalidError(err.Error(), "")
		}
		backupTargetName = volume.Spec.BackupTargetName
	}

	metaLabels[types.LonghornLabelBackupTarget] = backupTargetName

	patchOp, err := common.GetLonghornLabelsPatchOp(backup, metaLabels, nil)
	if err != nil {
		err := errors.Wrapf(err, "failed to get label patch for backup %v", backup.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	return patchOps, nil
}

func (b *backupMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	backup, ok := newObj.(*longhorn.Backup)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Backup", newObj), "")
	}
	var patchOps admission.PatchOps

	if backup.Spec.Labels == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/labels", "value": {}}`)
	}

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(backup)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for backup %v", backup.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}
