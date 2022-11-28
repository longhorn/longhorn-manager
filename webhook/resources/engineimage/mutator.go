package engineimage

import (
	"fmt"
	"strings"

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
		},
	}
}

func (e *engineImageMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	engineImage := newObj.(*longhorn.EngineImage)
	var patchOps admission.PatchOps

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

	patchOp, err = common.GetLonghornFinalizerPatchOp(engineImage)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for engineImage %v", engineImage.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	return patchOps, nil
}
