package backupbackingimage

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	common "github.com/longhorn/longhorn-manager/webhook/common"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type backupBackingImageMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &backupBackingImageMutator{ds: ds}
}

func (b *backupBackingImageMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "backupbackingimages",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.BackupBackingImage{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (b *backupBackingImageMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	backupBackingImage, ok := newObj.(*longhorn.BackupBackingImage)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackupBackingImage", newObj), "")
	}

	var patchOps admission.PatchOps

	var err error
	if patchOps, err = mutate(newObj); err != nil {
		return nil, err
	}

	backupTarget, err := b.ds.GetBackupTargetRO(backupBackingImage.Spec.BackupTargetName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, werror.NewInvalidError(errors.Wrapf(err, "failed to get backup target of backup backing image").Error(), "")
		}
		if backupBackingImage.Spec.BackupTargetURL != "" {
			backupTarget, err = b.ds.GetBackupTargetWithURLRO(backupBackingImage.Spec.BackupTargetURL)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return nil, werror.NewInvalidError(errors.Wrapf(err, "failed to get backup target of backup backing image by backup target URL").Error(), "")
				}
			}
		}
		if backupTarget == nil {
			backupTarget, err = b.ds.GetDefaultBackupTargetRO()
			if err != nil {
				return nil, werror.NewInvalidError(errors.Wrapf(err, "failed to get default backup target").Error(), "")
			}
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/backupTargetName", "value": %s}`, string(backupTarget.Name)))
	}
	if _, isExist := backupBackingImage.Labels[types.LonghornLabelBackupTarget]; !isExist {
		backupTargetLabels, err := json.Marshal(types.GetBackupTargetLabels(backupTarget.Name))
		if err != nil {
			return nil, werror.NewInvalidError(errors.Wrapf(err, "failed to convert backup backing image labels into JSON string").Error(), "")
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/labels", "value": %s}`, string(backupTargetLabels)))
	}

	if backupBackingImage.Spec.BackupTargetURL != backupTarget.Spec.BackupTargetURL {
		backupTargetURL, err := json.Marshal(backupTarget.Spec.BackupTargetURL)
		if err != nil {
			return nil, werror.NewInvalidError(errors.Wrapf(err, "failed to convert backup target url into JSON string").Error(), "")
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/backupTargetURL", "value": %s}`, string(backupTargetURL)))
	}

	return patchOps, nil
}

func (b *backupBackingImageMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	backupBackingImage, ok := newObj.(*longhorn.BackupBackingImage)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackupBackingImage", newObj), "")
	}

	var patchOps admission.PatchOps

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(backupBackingImage)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for backupBackingImage %v", backupBackingImage.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}
