package diskschedule

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

type diskScheduleMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &diskScheduleMutator{ds: ds}
}

func (m *diskScheduleMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "diskschedules",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.DiskSchedule{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (m *diskScheduleMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

func (m *diskScheduleMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	diskSchedule, ok := newObj.(*longhorn.DiskSchedule)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.DiskSchedule", newObj), "")
	}
	var patchOps admission.PatchOps

	if diskSchedule.Spec.Replicas == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/replicas", "value": {}}`)
	}

	if diskSchedule.Spec.BackingImages == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/backingImages", "value": {}}`)
	}

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(diskSchedule)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for diskSchedule %v", diskSchedule.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}
