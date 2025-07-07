package backupvolume

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type backupVolumeValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &backupVolumeValidator{ds: ds}
}

func (bv *backupVolumeValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "backupvolumes",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.BackupVolume{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (bv *backupVolumeValidator) Create(request *admission.Request, obj runtime.Object) error {
	backupVolume := obj.(*longhorn.BackupVolume)

	if err := validateRequiredFields(backupVolume); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}

func validateRequiredFields(backupVolume *longhorn.BackupVolume) error {
	if backupVolume.Spec.BackupTargetName == "" {
		return fmt.Errorf("backup target name cannot be empty")
	}

	if backupVolume.Spec.VolumeName == "" {
		return fmt.Errorf("volume name cannot be empty")
	}

	return nil
}

func (bv *backupVolumeValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	newBackupVolume := newObj.(*longhorn.BackupVolume)

	if err := validateRequiredFields(newBackupVolume); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}
