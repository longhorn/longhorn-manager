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
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"

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
	va, ok := newObj.(*longhorn.VolumeAttachment)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.VolumeAttachment", newObj), "")
	}

	return verifyAttachmentTicketIDConsistency(va.Spec.AttachmentTickets)
}

func (v *volumeAttachmentValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldVA, ok := oldObj.(*longhorn.VolumeAttachment)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.VolumeAttachment", oldObj), "")
	}
	newVA, ok := newObj.(*longhorn.VolumeAttachment)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.VolumeAttachment", newObj), "")
	}
	isRemovingLonghornFinalizer, err := common.IsRemovingLonghornFinalizer(oldObj, newObj)
	if err != nil {
		err = errors.Wrap(err, "failed to check if removing longhorn.io finalizer from deleted object")
		return werror.NewInvalidError(err.Error(), "")
	} else if isRemovingLonghornFinalizer {
		// We always allow the removal of the longhorn.io finalizer while an object is being deleted. It is the
		// controller's responsibility to wait for the correct conditions to attempt to remove it.
		return nil
	}

	if newVA.Spec.Volume != oldVA.Spec.Volume {
		return werror.NewInvalidError("spec.volume field is immutable", "spec.volume")
	}

	if len(oldVA.OwnerReferences) != 0 && !reflect.DeepEqual(newVA.OwnerReferences, oldVA.OwnerReferences) {
		return werror.NewInvalidError("VolumeAttachment's OwnerReferences field is immutable", "metadata.ownerReferences")
	}

	if _, ok := oldVA.Labels[types.LonghornLabelVolume]; ok && newVA.Labels[types.LonghornLabelVolume] != oldVA.Labels[types.LonghornLabelVolume] {
		return werror.NewInvalidError(fmt.Sprintf("label %v is immutable", types.LonghornLabelVolume), "metadata.labels")
	}

	if err := v.verifyTicketCountForMigratableVolume(newVA); err != nil {
		return err
	}

	if err := v.verifyStrictLocalVolumeAttachment(newVA); err != nil {
		return err
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

func (v *volumeAttachmentValidator) verifyTicketCountForMigratableVolume(va *longhorn.VolumeAttachment) error {
	vol, err := v.ds.GetVolumeRO(va.Spec.Volume)
	if err != nil {
		err = errors.Wrapf(err, "failed to get volume %v for attachment", va.Spec.Volume)
		return werror.NewInvalidError(err.Error(), "spec.volume")
	}

	if !util.IsMigratableVolume(vol) {
		return nil
	}

	numCSITickets := 0
	for _, ticket := range va.Spec.AttachmentTickets {
		if ticket.Type == longhorn.AttacherTypeCSIAttacher {
			numCSITickets++
		}
	}

	switch {
	case numCSITickets < 2:
		return nil
	case numCSITickets == 2:
		if vol.Status.State != longhorn.VolumeStateAttached {
			msg := fmt.Sprintf("cannot have second CSI ticket for migratable volume %v while it is in state %v", vol.Name, vol.Status.State)
			return werror.NewInvalidError(msg, "spec.attachmentTickets")
		}
		return nil
	default:
		ticketsJson, _ := json.Marshal(va.Spec.AttachmentTickets)
		msg := fmt.Sprintf("cannot have more than 2 CSI tickets for migratable volume %v: %s", vol.Name, ticketsJson)
		return werror.NewInvalidError(msg, "spec.attachmentTickets")
	}
}

func (v *volumeAttachmentValidator) verifyStrictLocalVolumeAttachment(va *longhorn.VolumeAttachment) error {
	vol, err := v.ds.GetVolumeRO(va.Spec.Volume)
	if err != nil {
		err = errors.Wrapf(err, "failed to get volume %v for attachment", va.Spec.Volume)
		return werror.NewInvalidError(err.Error(), "spec.volume")
	}

	if vol.Spec.DataLocality != longhorn.DataLocalityStrictLocal {
		return nil
	}

	replicas, err := v.ds.ListVolumeReplicas(vol.Name)
	if err != nil {
		err = errors.Wrapf(err, "failed to get replicas for volume %v", vol.Name)
		return werror.NewInvalidError(err.Error(), "spec.volume")
	}

	if len(replicas) != 1 {
		err := fmt.Errorf("BUG: replica should be 1 for %v volume %v", longhorn.DataLocalityStrictLocal, vol.Name)
		return werror.NewInvalidError(err.Error(), "spec.volume")
	}

	var replica *longhorn.Replica
	for _, replica = range replicas {
		break
	}

	// Allow initial attachment when replica is not yet bound to a node.
	if replica.Spec.NodeID == "" {
		return nil
	}

	if vol.Spec.NodeID != "" && replica.Spec.NodeID != vol.Spec.NodeID {
		err := fmt.Errorf("moving a %v volume %v to another node is not supported", longhorn.DataLocalityStrictLocal, vol.Name)
		return werror.NewInvalidError(err.Error(), "spec.nodeID")
	}

	for ticketID, ticket := range va.Spec.AttachmentTickets {
		if ticket.NodeID != "" && ticket.NodeID != replica.Spec.NodeID {
			err := fmt.Errorf("moving a %v volume %v to another node is not supported", longhorn.DataLocalityStrictLocal, vol.Name)
			return werror.NewInvalidError(err.Error(), fmt.Sprintf("spec.attachmentTickets[%s].nodeID", ticketID))
		}
	}

	return nil
}
