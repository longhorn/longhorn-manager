package engineimage

import (
	"fmt"
	"strings"

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

type engineImageMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &engineImageMutator{ds: ds}
}

func (e *engineImageMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "engineimages",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.EngineImage{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (e *engineImageMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	engineImage, ok := newObj.(*longhorn.EngineImage)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.EngineImage", newObj), "")
	}

	var patchOps admission.PatchOps

	var err error
	if patchOps, err = mutate(newObj); err != nil {
		return nil, err
	}

	image := strings.TrimSpace(engineImage.Spec.Image)
	if image != engineImage.Spec.Image {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/image", "value": "%s"}`, image))
	}

	name := types.GetEngineImageChecksumName(image)
	if name != engineImage.Name {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/name", "value": "%s"}`, name))
	}

	longhornLabels := types.GetEngineImageLabels(name)
	patchOp, err := common.GetLonghornLabelsPatchOp(engineImage, longhornLabels, nil)
	if err != nil {
		err := errors.Wrapf(err, "failed to get label patch for engineImage %v", engineImage.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	return patchOps, nil
}

func (e *engineImageMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	engineImage, ok := newObj.(*longhorn.EngineImage)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.EngineImage", newObj), "")
	}

	var patchOps admission.PatchOps

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(engineImage)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for engineImage %v", engineImage.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}
