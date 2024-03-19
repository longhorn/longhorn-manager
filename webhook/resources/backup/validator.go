package backup

import (
	"fmt"
	"strings"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	btypes "github.com/longhorn/backupstore/types"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"

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

func (b *backupValidator) Create(request *admission.Request, newObj runtime.Object) error {
	backup, ok := newObj.(*longhorn.Backup)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Backup", newObj), "")
	}

	if backup.Spec.Labels != nil {
		for key, value := range backup.Spec.Labels {
			if !strings.HasPrefix(key, types.LonghornLabelKeyPrefix) {
				continue
			}
			if err := validateBackupOption(key, value); err != nil {
				return werror.NewInvalidError(err.Error(), "")
			}
		}
	}

	return nil

}

func validateBackupOption(label, value string) error {
	option := label[strings.LastIndex(label, "/")+1:]

	switch option {
	case btypes.LonghornBackupOptionBackupMode:
		if value != btypes.LonghornBackupModeFull &&
			value != btypes.LonghornBackupModeIncremental {
			return fmt.Errorf("%v:%v is not a valid option", label, value)
		}
	case types.LonghornLabelVolumeAccessMode:
		return nil
	default:
		return fmt.Errorf("%v:%v is not a valid option", label, value)
	}

	return nil
}
