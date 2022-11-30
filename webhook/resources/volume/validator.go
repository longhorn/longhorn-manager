package volume

import (
	"fmt"

	"github.com/pkg/errors"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type volumeValidator struct {
	admission.DefaultValidator
	ds            *datastore.DataStore
	currentNodeID string
}

func NewValidator(ds *datastore.DataStore, currentNodeID string) admission.Validator {
	return &volumeValidator{ds: ds, currentNodeID: currentNodeID}
}

func (v *volumeValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "volumes",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Volume{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (v *volumeValidator) Create(request *admission.Request, newObj runtime.Object) error {
	volume := newObj.(*longhorn.Volume)

	if !util.ValidateName(volume.Name) {
		return werror.NewInvalidError(fmt.Sprintf("invalid name %v", volume.Name), "")
	}

	if err := types.ValidateDataLocality(volume.Spec.DataLocality); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := types.ValidateAccessMode(volume.Spec.AccessMode); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := validateReplicaCount(volume.Spec.DataLocality, volume.Spec.NumberOfReplicas); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := types.ValidateDataLocalityAndReplicaCount(volume.Spec.DataLocality, volume.Spec.NumberOfReplicas); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := types.ValidateReplicaAutoBalance(volume.Spec.ReplicaAutoBalance); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := types.ValidateUnmapMarkSnapChainRemoved(volume.Spec.UnmapMarkSnapChainRemoved); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if volume.Spec.BackingImage != "" {
		if _, err := v.ds.GetBackingImage(volume.Spec.BackingImage); err != nil {
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	if volume.Spec.EngineImage == "" {
		return werror.NewInvalidError("BUG: Invalid empty Setting.EngineImage", "")
	}

	if !volume.Spec.Standby {
		if volume.Spec.Frontend != longhorn.VolumeFrontendBlockDev && volume.Spec.Frontend != longhorn.VolumeFrontendISCSI {
			return werror.NewInvalidError(fmt.Sprintf("invalid volume frontend specified: %v", volume.Spec.Frontend), "")
		}
	}

	if volume.Spec.Migratable && volume.Spec.AccessMode != longhorn.AccessModeReadWriteMany {
		return werror.NewInvalidError("migratable volumes are only supported in ReadWriteMany (rwx) access mode", "")
	}

	// Check engine version before disable revision counter
	if volume.Spec.RevisionCounterDisabled {
		if ok, err := v.canDisableRevisionCounter(volume.Spec.EngineImage); !ok {
			err := errors.Wrapf(err, "can not create volume with current engine image that doesn't support disable revision counter")
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	if err := datastore.CheckVolume(volume); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}

func (v *volumeValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldVolume := oldObj.(*longhorn.Volume)
	newVolume := newObj.(*longhorn.Volume)

	if err := validateDataLocalityUpdate(oldVolume, newVolume); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := validateReplicaCount(newVolume.Spec.DataLocality, newVolume.Spec.NumberOfReplicas); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := types.ValidateAccessMode(newVolume.Spec.AccessMode); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := types.ValidateReplicaAutoBalance(newVolume.Spec.ReplicaAutoBalance); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := types.ValidateUnmapMarkSnapChainRemoved(newVolume.Spec.UnmapMarkSnapChainRemoved); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := datastore.CheckVolume(newVolume); err != nil {
		return err
	}

	return nil
}

func validateDataLocalityUpdate(oldVolume *longhorn.Volume, newVolume *longhorn.Volume) error {
	if err := types.ValidateDataLocality(newVolume.Spec.DataLocality); err != nil {
		return err
	}

	if oldVolume.Status.State == longhorn.VolumeStateDetached {
		return nil
	}

	if oldVolume.Spec.DataLocality != newVolume.Spec.DataLocality &&
		(oldVolume.Spec.DataLocality == longhorn.DataLocalityStrictLocal || newVolume.Spec.DataLocality == longhorn.DataLocalityStrictLocal) {
		return werror.NewInvalidError(fmt.Sprintf("data locality cannot be converted between %v and other modes when volume is not detached",
			longhorn.DataLocalityStrictLocal), "")
	}
	return nil
}

func validateReplicaCount(dataLocality longhorn.DataLocality, replicaCount int) error {
	if err := types.ValidateReplicaCount(replicaCount); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}
	if dataLocality == longhorn.DataLocalityStrictLocal {
		if replicaCount != 1 {
			return werror.NewInvalidError(fmt.Sprintf("number of replica count should be 1 whe data locality is %v", longhorn.DataLocalityStrictLocal), "")
		}
	}
	return nil
}

func (v *volumeValidator) canDisableRevisionCounter(engineImage string) (bool, error) {
	cliAPIVersion, err := v.ds.GetEngineImageCLIAPIVersion(engineImage)
	if err != nil {
		return false, err
	}
	if cliAPIVersion < engineapi.CLIVersionFour {
		return false, fmt.Errorf("current engine image version %v doesn't support disable revision counter", cliAPIVersion)
	}

	return true, nil
}
