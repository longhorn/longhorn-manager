package backup

import (
	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type backupValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &backupValidator{ds: ds}
}

func (b *backupValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:           "backups",
		Scope:          admissionregv1.NamespacedScope,
		APIGroup:       longhorn.SchemeGroupVersion.Group,
		APIVersion:     longhorn.SchemeGroupVersion.Version,
		ObjectType:     &longhorn.Backup{},
		OperationTypes: []admissionregv1.OperationType{},
	}
}
