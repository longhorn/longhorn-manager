package backingimagemanager

import (
	"github.com/pkg/errors"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	common "github.com/longhorn/longhorn-manager/webhook/common"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type backingImageManagerMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &backingImageManagerMutator{ds: ds}
}

func (b *backingImageManagerMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "backingImageManagers",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.BackingImageManager{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (b *backingImageManagerMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	backingImageManager := newObj.(*longhorn.BackingImageManager)

	patchOps, err := mutate(newObj)
	if err != nil {
		return nil, werror.NewInvalidError(err.Error(), "")
	}

	patchOp, err := common.GetLonghornFinalizerPatchOp(backingImageManager)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for backingImageManager %v", backingImageManager.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	return patchOps, nil
}

func (b *backingImageManagerMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	backingImageManager := newObj.(*longhorn.BackingImageManager)
	var patchOps admission.PatchOps

	if backingImageManager.Spec.BackingImages == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/backingImages", "value": {}}`)
	}

	return patchOps, nil
}
