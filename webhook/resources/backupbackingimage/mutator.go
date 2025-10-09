package backupbackingimage

import (
	"fmt"

	"github.com/cockroachdb/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
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

	mutatePatchOps, err := mutate(newObj)
	if err != nil {
		return nil, err
	}
	patchOps = append(patchOps, mutatePatchOps...)

	backupTargetName := backupBackingImage.Spec.BackupTargetName
	if backupTargetName == "" {
		backupTargetName = types.DefaultBackupTargetName
	}
	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/backupTargetName", "value": "%s"}`, backupTargetName))

	backupTarget, err := b.ds.GetBackupTargetRO(backupTargetName)
	if apierrors.IsNotFound(err) && backupTargetName != types.DefaultBackupTargetName {
		// If the backup target is not found, we will try to use the default backup target.
		backupTarget, err = b.ds.GetDefaultBackupTargetRO()
		if err != nil {
			return nil, werror.NewInvalidError(errors.Wrapf(err, "failed to get default backup target").Error(), "")
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/backupTargetName", "value": "%s"}`, backupTarget.Name))
	} else if err != nil {
		return nil, werror.NewInvalidError(errors.Wrapf(err, "failed to get backup target of backup backing image").Error(), "")
	}

	moreLabels := make(map[string]string)
	if backupBackingImage.Labels != nil {
		for k, v := range backupBackingImage.Labels {
			moreLabels[k] = v
		}
	}
	moreLabels[types.LonghornLabelBackupTarget] = backupTarget.Name
	moreLabels[types.LonghornLabelBackingImage] = backupBackingImage.Spec.BackingImage
	patchOp, err := common.GetLonghornLabelsPatchOp(backupBackingImage, moreLabels, nil)
	if err != nil {
		return nil, werror.NewInvalidError(errors.Wrapf(err, "failed to get labels patch for backupBackingImage %v", backupBackingImage.Name).Error(), "")
	}
	patchOps = append(patchOps, patchOp)

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
