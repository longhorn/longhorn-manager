package volumeattachment

import (
	"fmt"
	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"reflect"
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
	_, ok := newObj.(*longhorn.VolumeAttachment)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.VolumeAttachment", newObj), "")
	}

	return nil
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

	if newVA.Spec.Volume != oldVA.Spec.Volume {
		return werror.NewInvalidError(fmt.Sprintf("spec.volume field is immutable"), "spec.volume")
	}

	if len(oldVA.OwnerReferences) != 0 && !reflect.DeepEqual(newVA.OwnerReferences, oldVA.OwnerReferences) {
		return werror.NewInvalidError(fmt.Sprintf("VolumeAttachment's OwnerReferences field is immutable"), "metadata.ownerReferences")
	}

	if _, ok := oldVA.Labels[types.LonghornLabelVolume]; ok && newVA.Labels[types.LonghornLabelVolume] != oldVA.Labels[types.LonghornLabelVolume] {
		return werror.NewInvalidError(fmt.Sprintf("label %v is immutable", types.LonghornLabelVolume), "metadata.labels")
	}

	return nil
}
