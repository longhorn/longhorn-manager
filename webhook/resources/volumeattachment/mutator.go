package volumeattachment

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

type volumeAttachmentMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &volumeAttachmentMutator{ds: ds}
}

func (o *volumeAttachmentMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "volumeattachments",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.VolumeAttachment{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

func (s *volumeAttachmentMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	va, ok := newObj.(*longhorn.VolumeAttachment)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.VolumeAttachment", newObj), "")
	}

	volume, err := s.ds.GetVolumeRO(va.Spec.Volume)
	if err != nil {
		err = errors.Wrapf(err, "failed to get volume %v", va.Spec.Volume)
		return nil, werror.NewInvalidError(err.Error(), "spec.Volume")
	}

	patchOp, err := common.GetLonghornLabelsPatchOp(va, types.GetVolumeLabels(volume.Name))
	if err != nil {
		err := errors.Wrapf(err, "failed to get labels patch for VolumeAttachment %v", va.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	patchOp, err = common.GetLonghornFinalizerPatchOp(va)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for VolumeAttachment %v", va.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	if len(va.OwnerReferences) == 0 {
		volumeRef := datastore.GetOwnerReferencesForVolume(volume)
		bytes, err := json.Marshal(volumeRef)
		if err != nil {
			err = errors.Wrapf(err, "failed to get JSON encoding for VolumeAttachment %v ownerReferences", va.Name)
			return nil, werror.NewInvalidError(err.Error(), "")
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/ownerReferences", "value": %v}`, string(bytes)))
	}

	return patchOps, nil
}
