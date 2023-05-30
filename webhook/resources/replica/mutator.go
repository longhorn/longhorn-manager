package replica

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

type replicaMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &replicaMutator{ds: ds}
}

func (e *replicaMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "replicas",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Replica{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (e *replicaMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutateReplica(newObj, true)
}

func (e *replicaMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutateReplica(newObj, false)
}

func mutateReplica(newObj runtime.Object, needFinalizer bool) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	replica := newObj.(*longhorn.Replica)

	labels := replica.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	patchOp, err := common.GetLonghornLabelsPatchOp(replica, labels, nil)
	if err != nil {
		err := errors.Wrapf(err, "failed to get label patch for replica %v", replica.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	if needFinalizer {
		patchOp, err = common.GetLonghornFinalizerPatchOp(replica)
		if err != nil {
			err := errors.Wrapf(err, "failed to get finalizer patch for replica %v", replica.Name)
			return nil, werror.NewInvalidError(err.Error(), "")
		}
		patchOps = append(patchOps, patchOp)
	}

	if string(replica.Spec.BackendStoreDriver) == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/backendStoreDriver", "value": "%s"}`, longhorn.BackendStoreDriverTypeLonghorn))
	}

	return patchOps, nil
}
