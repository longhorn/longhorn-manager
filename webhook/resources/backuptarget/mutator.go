package backuptarget

import (
	"fmt"

	"github.com/pkg/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	common "github.com/longhorn/longhorn-manager/webhook/common"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type backupTargetMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &backupTargetMutator{ds: ds}
}

func (b *backupTargetMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "backuptargets",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.BackupTarget{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (b *backupTargetMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

func (b *backupTargetMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	backupTarget, ok := newObj.(*longhorn.BackupTarget)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackupTarget", newObj), "")
	}

	var patchOps admission.PatchOps

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(backupTarget)
	if err != nil {
		err = errors.Wrapf(err, "failed to get finalizer patch for backupTarget %v", backupTarget.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}
