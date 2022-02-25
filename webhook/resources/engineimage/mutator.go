package engineimage

import (
	"encoding/json"
	"fmt"
	"strings"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
	"github.com/pkg/errors"
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
	var patchOps admission.PatchOps

	engineImage := newObj.(*longhorn.EngineImage)

	image := strings.TrimSpace(engineImage.Spec.Image)
	if image != engineImage.Spec.Image {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/image", "value": "%s"}`, image))
	}

	name := types.GetEngineImageChecksumName(image)
	if name != engineImage.Name {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/name", "value": "%s"}`, name))
	}

	// Merge the user created and longhorn specific labels
	labels := engineImage.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	longhornLabels := types.GetEngineImageLabels(name)
	for k, v := range longhornLabels {
		labels[k] = v
	}
	bytes, err := json.Marshal(labels)
	if err != nil {
		err = errors.Wrapf(err, "failed to get JSON encoding for engine image %v labels", engineImage.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/labels", "value": %v}`, string(bytes)))

	return patchOps, nil
}
