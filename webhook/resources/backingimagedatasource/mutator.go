package backingimagedatasource

import (
	"fmt"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
)

type backingImageDataSourceMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &backingImageDataSourceMutator{ds: ds}
}

func (b *backingImageDataSourceMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "backingimagedatasources",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.BackingImageDataSource{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (b *backingImageDataSourceMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutateBackingImageDatasource(newObj)
}

func (b *backingImageDataSourceMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutateBackingImageDatasource(newObj)
}

func mutateBackingImageDatasource(newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	backingimagedatasource := newObj.(*longhorn.BackingImageDataSource)

	if backingimagedatasource.Spec.SourceType == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/sourceType", "value": "%s"}`, longhorn.BackingImageDataSourceTypeDownload))
	}

	if backingimagedatasource.Spec.Parameters == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/parameters", "value": {}}`)
	}

	return patchOps, nil
}
