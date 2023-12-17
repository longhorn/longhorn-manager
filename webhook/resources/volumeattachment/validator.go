package volumeattachment

import (
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type volumeAttachmentValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &volumeAttachmentValidator{ds: ds}
}

func (v *volumeAttachmentValidator) Resource() admission.Resource {
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

func (v *volumeAttachmentValidator) Create(request *admission.Request, newObj runtime.Object) error {
	va := newObj.(*longhorn.VolumeAttachment)

	return verifyAttachmentTicketIDConsistency(va.Spec.AttachmentTickets)
}

func (v *volumeAttachmentValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldVA := oldObj.(*longhorn.VolumeAttachment)
	newVA := newObj.(*longhorn.VolumeAttachment)

	if newVA.Spec.Volume != oldVA.Spec.Volume {
		return werror.NewInvalidError("spec.volume field is immutable", "spec.volume")
	}

	if len(oldVA.OwnerReferences) != 0 && !reflect.DeepEqual(newVA.OwnerReferences, oldVA.OwnerReferences) {
		return werror.NewInvalidError("VolumeAttachment's OwnerReferences field is immutable", "metadata.ownerReferences")
	}

	if _, ok := oldVA.Labels[types.LonghornLabelVolume]; ok && newVA.Labels[types.LonghornLabelVolume] != oldVA.Labels[types.LonghornLabelVolume] {
		return werror.NewInvalidError(fmt.Sprintf("label %v is immutable", types.LonghornLabelVolume), "metadata.labels")
	}

	return verifyAttachmentTicketIDConsistency(newVA.Spec.AttachmentTickets)
}

func verifyAttachmentTicketIDConsistency(attachmentTickets map[string]*longhorn.AttachmentTicket) error {
	for ticketID, ticket := range attachmentTickets {
		if ticketID != ticket.ID {
			return werror.NewInvalidError(fmt.Sprintf("the attachmentTickets map contains inconsistent attachment ticket ID: %v vs %v", ticketID, ticket.ID), "")
		}
	}
	return nil
}
