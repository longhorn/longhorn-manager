package volume

import (
	"fmt"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
)

type volumeMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &volumeMutator{ds: ds}
}

func (v *volumeMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "volumes",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Volume{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (v *volumeMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutateVolume(newObj)
}

func (v *volumeMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutateVolume(newObj)
}

func mutateVolume(newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	volume := newObj.(*longhorn.Volume)

	if volume.Spec.ReplicaAutoBalance == "" {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/replicaAutoBalance", "value": "ignored"}`)
	}
	if volume.Spec.DiskSelector == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/diskSelector", "value": []}`)
	}
	if volume.Spec.NodeSelector == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/nodeSelector", "value": []}`)
	}
	if volume.Spec.RecurringJobs == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/recurringJobs", "value": []}`)
	}
	for id, job := range volume.Spec.RecurringJobs {
		if job.Groups == nil {
			patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/recurringJobs/%d/groups", "value": []}`, id))
		}
		if job.Labels == nil {
			patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/recurringJobs/%d/labels", "value": {}}`, id))
		}
	}

	return patchOps, nil
}
