package backingimagemanager

import (
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
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
	return mutateBackingImageManager(newObj)
}

func (b *backingImageManagerMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutateBackingImageManager(newObj)
}

func mutateBackingImageManager(newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	backingImageManager := newObj.(*longhorn.BackingImageManager)

	if backingImageManager.Spec.BackingImages == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/backingImages", "value": {}}`)
	}

	return patchOps, nil
}
