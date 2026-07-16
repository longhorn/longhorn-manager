package enginefrontend

import (
	"fmt"

	"github.com/cockroachdb/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type engineFrontendMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &engineFrontendMutator{ds: ds}
}

func (e *engineFrontendMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "enginefrontends",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.EngineFrontend{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (e *engineFrontendMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

func (e *engineFrontendMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	engineFrontend, ok := newObj.(*longhorn.EngineFrontend)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.EngineFrontend", newObj), "")
	}

	var patchOps admission.PatchOps

	labels := engineFrontend.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	patchOp, err := common.GetLonghornLabelsPatchOp(engineFrontend, labels, nil)
	if err != nil {
		err := errors.Wrapf(err, "failed to get label patch for engine frontend %v", engineFrontend.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	patchOp, err = common.GetLonghornFinalizerPatchOpIfNeeded(engineFrontend)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for engine frontend %v", engineFrontend.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	if string(engineFrontend.Spec.DataEngine) == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/dataEngine", "value": "%s"}`, longhorn.DataEngineTypeV2))
	}

	return patchOps, nil
}
