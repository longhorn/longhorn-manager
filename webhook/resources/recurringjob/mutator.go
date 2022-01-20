package recurringjob

import (
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
)

type recurringJobMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &recurringJobMutator{ds: ds}
}

func (r *recurringJobMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "recurringjobs",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.RecurringJob{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (r *recurringJobMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutateRecurringJob(newObj)
}

func (r *recurringJobMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutateRecurringJob(newObj)
}

func mutateRecurringJob(newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	recurringjob := newObj.(*longhorn.RecurringJob)

	if recurringjob.Spec.Groups == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/groups", "value": []}`)
	}
	if recurringjob.Spec.Labels == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/labels", "value": {}}`)
	}

	return patchOps, nil
}
