package systembackup

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

type systemBackupMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &systemBackupMutator{ds: ds}
}

func (m *systemBackupMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "systembackups",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.SystemBackup{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (m *systemBackupMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

func (m *systemBackupMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	systemBackup := newObj.(*longhorn.SystemBackup)
	var patchOps admission.PatchOps

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(systemBackup)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for SystemBackup %v", systemBackup.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	if systemBackup.Spec.VolumeBackupPolicy == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/volumeBackupPolicy", "value": "%s"}`, longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent))
	}

	return patchOps, nil
}
