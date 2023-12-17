package snapshot

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
)

type snapShotMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &snapShotMutator{ds: ds}
}

func (s *snapShotMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "snapshots",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Snapshot{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (s *snapShotMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	snapshot := newObj.(*longhorn.Snapshot)
	var patchOps admission.PatchOps

	var err error
	if patchOps, err = mutate(newObj); err != nil {
		return nil, err
	}

	volume, err := s.ds.GetVolumeRO(snapshot.Spec.Volume)
	if err != nil {
		err = errors.Wrapf(err, "failed to get volume %v", snapshot.Spec.Volume)
		return nil, werror.NewInvalidError(err.Error(), "spec.Volume")
	}

	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/backendStoreDriver", "value": "%s"}`, volume.Spec.BackendStoreDriver))

	patchOp, err := common.GetLonghornLabelsPatchOp(snapshot, types.GetVolumeLabels(volume.Name), nil)
	if err != nil {
		err := errors.Wrapf(err, "failed to get labels patch for snapshot %v", snapshot.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	if len(snapshot.OwnerReferences) == 0 {
		volumeRef := datastore.GetOwnerReferencesForVolume(volume)
		bytes, err := json.Marshal(volumeRef)
		if err != nil {
			err = errors.Wrapf(err, "failed to get JSON encoding for snapshot %v ownerReferences", snapshot.Name)
			return nil, werror.NewInvalidError(err.Error(), "")
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/ownerReferences", "value": %v}`, string(bytes)))
	}

	return patchOps, nil
}

func (s *snapShotMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	snapshot := newObj.(*longhorn.Snapshot)
	var patchOps admission.PatchOps

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(snapshot)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for snapshot %v", snapshot.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}
