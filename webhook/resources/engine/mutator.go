package engine

import (
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
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
	return mutateEngine(newObj)
}

func (e *engineMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutateEngine(newObj)
}

func mutateEngine(newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	engine := newObj.(*longhorn.Engine)

	if engine.Spec.ReplicaAddressMap == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/replicaAddressMap", "value": {}}`)
	}

	if engine.Spec.UpgradedReplicaAddressMap == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/upgradedReplicaAddressMap", "value": {}}`)
	}

	return patchOps, nil
}
