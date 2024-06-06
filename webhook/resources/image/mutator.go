package image

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type imageMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &imageMutator{ds: ds}
}

func (e *imageMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "images",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Image{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (i *imageMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	image, ok := newObj.(*longhorn.Image)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Image", newObj), "")
	}

	var patchOps admission.PatchOps
	var err error
	if patchOps, err = mutate(newObj); err != nil {
		return nil, err
	}

	imageURL := strings.TrimSpace(image.Spec.ImageURL)
	if imageURL != image.Spec.ImageURL {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/image", "value": "%s"}`, imageURL))
	}

	longhornLabels := types.GetImageLabels(image.Name)
	patchOp, err := common.GetLonghornLabelsPatchOp(image, longhornLabels, nil)
	if err != nil {
		err := errors.Wrapf(err, "failed to get label patch for Image %v", image.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	return patchOps, nil
}

func (i *imageMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	image, ok := newObj.(*longhorn.Image)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Image", newObj), "")
	}

	var patchOps admission.PatchOps

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(image)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for image %v", image.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}
