package backingimagedatasource

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
	return mutate(newObj)
}

func (b *backingImageDataSourceMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	backingImageDataSource := newObj.(*longhorn.BackingImageDataSource)

	if backingImageDataSource.Spec.SourceType == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/sourceType", "value": "%s"}`, longhorn.BackingImageDataSourceTypeDownload))
	}

	if backingImageDataSource.Spec.Parameters == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/parameters", "value": {}}`)
	}

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(backingImageDataSource)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for backingImageDataSource %v", backingImageDataSource.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}
