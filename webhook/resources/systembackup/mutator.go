package systembackup

import (
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/errors"

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
	sb, ok := newObj.(*longhorn.SystemBackup)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.SystemBackup", newObj), "")
	}

	var patchOps admission.PatchOps
	var err error
	if patchOps, err = mutate(newObj); err != nil {
		return nil, err
	}

	if len(sb.OwnerReferences) == 0 {
		// system backup is always created with the default backup target.
		backupTarget, err := m.ds.GetDefaultBackupTargetRO()
		if err != nil {
			return nil, err
		}
		btRef := datastore.GetOwnerReferencesForBackupTarget(backupTarget)
		bytes, err := json.Marshal(btRef)
		if err != nil {
			err = errors.Wrapf(err, "failed to get JSON encoding for system backup %v ownerReferences", sb.Name)
			return nil, werror.NewInvalidError(err.Error(), "")
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/ownerReferences", "value": %v}`, string(bytes)))
	}

	return patchOps, nil
}

func (m *systemBackupMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	systemBackup, ok := newObj.(*longhorn.SystemBackup)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.SystemBackup", newObj), "")
	}

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
