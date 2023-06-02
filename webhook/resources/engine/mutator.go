package engine

import (
	"fmt"

	"github.com/pkg/errors"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"
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
	return mutateEngine(newObj, true)
}

func (e *engineMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutateEngine(newObj, false)
}

func mutateEngine(newObj runtime.Object, needFinalizer bool) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	engine := newObj.(*longhorn.Engine)

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

	if needFinalizer {
		patchOp, err = common.GetLonghornFinalizerPatchOp(engine)
		if err != nil {
			err := errors.Wrapf(err, "failed to get finalizer patch for engine %v", engine.Name)
			return nil, werror.NewInvalidError(err.Error(), "")
		}
		patchOps = append(patchOps, patchOp)
	}

	if string(engine.Spec.BackendStoreDriver) == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/backendStoreDriver", "value": "%s"}`, longhorn.BackendStoreDriverTypeLonghorn))
	}

	return patchOps, nil
}
