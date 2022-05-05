package backup

import (
	"github.com/pkg/errors"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	common "github.com/longhorn/longhorn-manager/webhook/common"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
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
	backup := newObj.(*longhorn.Backup)

	patchOps, err := mutate(newObj)
	if err != nil {
		return nil, werror.NewInvalidError(err.Error(), "")
	}

	patchOp, err := common.GetLonghornFinalizerPatchOp(backup)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for backup %v", backup.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	return patchOps, nil
}

func (b *backupMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	backup := newObj.(*longhorn.Backup)
	var patchOps admission.PatchOps

	if backup.Spec.Labels == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/labels", "value": {}}`)
	}

	return patchOps, nil
}
