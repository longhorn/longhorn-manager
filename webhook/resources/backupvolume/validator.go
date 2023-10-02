package backupvolume

import (
	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type backupVolumeValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &backupVolumeValidator{ds: ds}
}

func (b *backupVolumeValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:           "backupvolumes",
		Scope:          admissionregv1.NamespacedScope,
		APIGroup:       longhorn.SchemeGroupVersion.Group,
		APIVersion:     longhorn.SchemeGroupVersion.Version,
		ObjectType:     &longhorn.BackupVolume{},
		OperationTypes: []admissionregv1.OperationType{},
	}
}
