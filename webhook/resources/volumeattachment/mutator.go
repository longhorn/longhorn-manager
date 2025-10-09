package volumeattachment

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/cockroachdb/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type volumeAttachmentMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &volumeAttachmentMutator{ds: ds}
}

func (m *volumeAttachmentMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "volumeattachments",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.VolumeAttachment{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (m *volumeAttachmentMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	va, ok := newObj.(*longhorn.VolumeAttachment)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.VolumeAttachment", newObj), "")
	}
	var patchOps admission.PatchOps

	var err error
	if patchOps, err = mutate(newObj); err != nil {
		return nil, err
	}

	volume, err := m.ds.GetVolumeRO(va.Spec.Volume)
	if err != nil {
		err = errors.Wrapf(err, "failed to get volume %v", va.Spec.Volume)
		return nil, werror.NewInvalidError(err.Error(), "spec.Volume")
	}

	patchOp, err := common.GetLonghornLabelsPatchOp(va, types.GetVolumeLabels(volume.Name), nil)
	if err != nil {
		err := errors.Wrapf(err, "failed to get labels patch for VolumeAttachment %v", va.Name)
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

func (m *volumeAttachmentMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	oldVa, ok := oldObj.(*longhorn.VolumeAttachment)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.VolumeAttachment", oldVa), "")
	}
	newVa, ok := newObj.(*longhorn.VolumeAttachment)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.VolumeAttachment", newVa), "")
	}

	var patchOps admission.PatchOps

	var err error
	if patchOps, err = mutate(newObj); err != nil {
		return nil, err
	}

	if newVa.Spec.AttachmentTickets == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/attachmentTickets", "value": {}}`)
	}

	attachmentTickets := map[string]*longhorn.AttachmentTicket{}
	for ticketID, ticket := range newVa.Spec.AttachmentTickets {
		attachmentTickets[ticketID] = ticket.DeepCopy()
	}

	for ticketID, ticket := range attachmentTickets {
		if _, ok := oldVa.Spec.AttachmentTickets[ticketID]; !ok {
			if ticketStatus, ok := newVa.Status.AttachmentTicketStatuses[ticketID]; ok {
				ticket.Generation = ticketStatus.Generation + 1
			}
		} else if !reflect.DeepEqual(ticket, oldVa.Spec.AttachmentTickets[ticketID]) {
			ticket.Generation++
		}

		// handle integer overflow
		if ticket.Generation < 0 {
			ticket.Generation = 0
		}

	}

	if !reflect.DeepEqual(attachmentTickets, newVa.Spec.AttachmentTickets) {
		bytes, err := json.Marshal(attachmentTickets)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get JSON encoding of attachmentTickets for volumeattachment %v ", newVa.Name)
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/attachmentTickets", "value": %v}`, string(bytes)))
	}

	return patchOps, nil
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	va := newObj.(*longhorn.VolumeAttachment)
	var patchOps admission.PatchOps

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(va)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for VolumeAttachment %v", va.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}
