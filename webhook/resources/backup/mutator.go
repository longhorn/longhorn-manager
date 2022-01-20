package backup

import (
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
)

type backupMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &backupMutator{ds: ds}
}

func (b *backupMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "backups",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Backup{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (b *backupMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutateBackup(newObj)
}

func (b *backupMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutateBackup(newObj)
}

func mutateBackup(newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	backup := newObj.(*longhorn.Backup)

	if backup.Spec.Labels == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/labels", "value": {}}`)
	}

	return patchOps, nil
}
