package engine

import (
	"fmt"
	"strconv"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
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
	var patchOps admission.PatchOps

	ops, err := mutateEngine(newObj)
	if err != nil {
		return nil, werror.NewInvalidError(err.Error(), "")
	}

	patchOps = append(patchOps, ops...)

	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/blockSize", "value": %s}`, strconv.FormatInt(util.DefaultBlockSize, 10)))

	return patchOps, nil
}

func (e *engineMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	oldEngine := oldObj.(*longhorn.Engine)

	var patchOps admission.PatchOps

	ops, err := mutateEngine(newObj)
	if err != nil {
		return nil, werror.NewInvalidError(err.Error(), "")
	}

	patchOps = append(patchOps, ops...)
	if oldEngine.Spec.BlockSize == 0 {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/blockSize", "value": %s}`, strconv.FormatInt(util.LegacyBlockSize, 10)))
	}

	return patchOps, nil
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
