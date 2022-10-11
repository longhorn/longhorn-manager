package backup

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	common "github.com/longhorn/longhorn-manager/webhook/common"
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
	backup := newObj.(*longhorn.Backup)

	patchOps, err := mutate(newObj)
	if err != nil {
		return nil, werror.NewInvalidError(err.Error(), "")
	}

	patchOp, err := common.GetLonghornFinalizerPatchOp(backup)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for backup %v", backup.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	backupLabels := backup.Spec.Labels
	if backupLabels == nil {
		backupLabels = make(map[string]string)
	}
	volumeName, isExist := backup.Labels[types.LonghornLabelBackupVolume]
	if !isExist {
		err := errors.Wrapf(err, "cannot find the backup volume label for backup %v", backup.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}

	if _, isExist := backupLabels[types.GetLonghornLabelKey(types.LonghornLabelVolumeAccessMode)]; !isExist {
		volumeAccessMode := longhorn.AccessModeReadWriteOnce
		if volume, err := b.ds.GetVolumeRO(volumeName); err == nil {
			if volume.Spec.AccessMode != "" {
				volumeAccessMode = volume.Spec.AccessMode
			}
		}
		backupLabels[types.GetLonghornLabelKey(types.LonghornLabelVolumeAccessMode)] = string(volumeAccessMode)
	}

	valueBackupLabels, err := json.Marshal(backupLabels)
	if err != nil {
		return nil, werror.NewInvalidError(errors.Wrapf(err, "failed to convert backup labels into JSON string").Error(), "")
	}
	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/labels", "value": %s}`, string(valueBackupLabels)))

	return patchOps, nil
}

func (b *backupMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	backup := newObj.(*longhorn.Backup)
	var patchOps admission.PatchOps

	if backup.Spec.Labels == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/labels", "value": {}}`)
	}

	return patchOps, nil
}
