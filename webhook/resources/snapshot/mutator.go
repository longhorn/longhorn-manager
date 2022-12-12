package snapshot

import (
	"encoding/json"
	"fmt"
	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
	"github.com/pkg/errors"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	longhornFinalizerKey = longhorn.SchemeGroupVersion.Group
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
		},
	}
}

func (s *snapShotMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	snapshot, ok := newObj.(*longhorn.Snapshot)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Snapshot", newObj), "")
	}

	volume, err := s.ds.GetVolumeRO(snapshot.Spec.Volume)
	if err != nil {
		err = errors.Wrapf(err, "failed to get volume %v", snapshot.Spec.Volume)
		return nil, werror.NewInvalidError(err.Error(), "spec.Volume")
	}

	patchOp, err := common.GetLonghornLabelsPatchOp(snapshot, types.GetVolumeLabels(volume.Name), nil)
	if err != nil {
		err := errors.Wrapf(err, "failed to get labels patch for snapshot %v", snapshot.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	patchOp, err = common.GetLonghornFinalizerPatchOp(snapshot)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for snapshot %v", snapshot.Name)
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
