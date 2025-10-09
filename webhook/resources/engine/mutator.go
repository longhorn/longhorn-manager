package engine

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

type engineMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &engineMutator{ds: ds}
}

func (e *engineMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "engines",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Engine{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (e *engineMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

func (e *engineMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	engine, ok := newObj.(*longhorn.Engine)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Engine", newObj), "")
	}

	var patchOps admission.PatchOps

	if engine.Spec.ReplicaAddressMap == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/replicaAddressMap", "value": {}}`)
	}

	if engine.Spec.UpgradedReplicaAddressMap == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/upgradedReplicaAddressMap", "value": {}}`)
	}

	labels := engine.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	patchOp, err := common.GetLonghornLabelsPatchOp(engine, labels, nil)
	if err != nil {
		err := errors.Wrapf(err, "failed to get label patch for engine %v", engine.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	patchOp, err = common.GetLonghornFinalizerPatchOpIfNeeded(engine)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for engine %v", engine.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	if string(engine.Spec.DataEngine) == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/dataEngine", "value": "%s"}`, longhorn.DataEngineTypeV1))
	}

	return patchOps, nil
}
