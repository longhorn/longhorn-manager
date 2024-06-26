package backupbackingimage

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
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

	backupTargetPatchOps, returningBackupTargetName, err := common.GetBackupTargetInfoPatchOp(b.ds, backupBackingImage.Spec.BackupTargetName, backupBackingImage.Spec.BackupTargetURL, backupBackingImage.Labels)
	if err != nil {
		return nil, werror.NewInvalidError(errors.Wrapf(err, "failed to update backup target information of backup backing image").Error(), "")
	}
	patchOps = append(patchOps, backupTargetPatchOps...)

	if !strings.HasSuffix(backupBackingImage.Name, returningBackupTargetName) {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/name", "value": "%s"}`, string(backupBackingImage.Name+"-"+returningBackupTargetName)))
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
