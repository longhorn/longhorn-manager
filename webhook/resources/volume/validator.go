package volume

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

	if err := types.ValidateReplicaSoftAntiAffinity(volume.Spec.ReplicaSoftAntiAffinity); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := types.ValidateReplicaZoneSoftAntiAffinity(volume.Spec.ReplicaZoneSoftAntiAffinity); err != nil {
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
		if volume.Spec.Frontend != longhorn.VolumeFrontendBlockDev &&
			volume.Spec.Frontend != longhorn.VolumeFrontendISCSI &&
			volume.Spec.Frontend != longhorn.VolumeFrontendNvmf {
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

	if volume.Spec.BackendStoreDriver == longhorn.BackendStoreDriverTypeV2 {
		v2DataEngineEnabled, err := v.ds.GetSettingAsBool(types.SettingNameV2DataEngine)
		if err != nil {
			err = errors.Wrapf(err, "failed to get spdk setting")
			return werror.NewInvalidError(err.Error(), "")
		}
		if !v2DataEngineEnabled {
			return werror.NewInvalidError("v2 data engine is not enabled", "")
		}

		if volume.Spec.Frontend == longhorn.VolumeFrontendISCSI {
			return werror.NewInvalidError("v2 data engine does not support iSCSI frontend", "")
		}
	}

	return nil
}

func (v *volumeValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldVolume := oldObj.(*longhorn.Volume)
	newVolume := newObj.(*longhorn.Volume)

	if err := v.validateExpansionSize(oldVolume, newVolume); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

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

	if err := types.ValidateReplicaSoftAntiAffinity(newVolume.Spec.ReplicaSoftAntiAffinity); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := types.ValidateReplicaZoneSoftAntiAffinity(newVolume.Spec.ReplicaZoneSoftAntiAffinity); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if newVolume.Spec.DataLocality == longhorn.DataLocalityStrictLocal {
		// Check if the strict-local volume can attach to newVolume.Spec.NodeID
		if oldVolume.Spec.NodeID != newVolume.Spec.NodeID && newVolume.Spec.NodeID != "" {
			ok, err := v.hasLocalReplicaOnSameNodeAsStrictLocalVolume(newVolume)
			if !ok {
				err = errors.Wrapf(err, "failed to check if %v volume %v and its replica are on the same node",
					longhorn.DataLocalityStrictLocal, newVolume.Name)
				return werror.NewInvalidError(err.Error(), "")
			}
		}
	}

	if err := datastore.CheckVolume(newVolume); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if oldVolume.Spec.BackupCompressionMethod != "" {
		if oldVolume.Spec.BackupCompressionMethod != newVolume.Spec.BackupCompressionMethod {
			err := fmt.Errorf("changing backup compression method for volume %v is not supported", oldVolume.Name)
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	if oldVolume.Spec.BackendStoreDriver != "" {
		if oldVolume.Spec.BackendStoreDriver != newVolume.Spec.BackendStoreDriver {
			err := fmt.Errorf("changing backend store driver for volume %v is not supported", oldVolume.Name)
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	if newVolume.Spec.BackendStoreDriver == longhorn.BackendStoreDriverTypeV2 {
		// TODO: remove this check when we support the following features for SPDK volumes
		if oldVolume.Spec.Size != newVolume.Spec.Size {
			err := fmt.Errorf("changing volume size for volume %v is not supported for backend store driver %v",
				newVolume.Name, newVolume.Spec.BackendStoreDriver)
			return werror.NewInvalidError(err.Error(), "")
		}

		if oldVolume.Spec.NumberOfReplicas != newVolume.Spec.NumberOfReplicas {
			err := fmt.Errorf("changing number of replicas for volume %v is not supported for backend store driver %v",
				newVolume.Name, newVolume.Spec.BackendStoreDriver)
			return werror.NewInvalidError(err.Error(), "")
		}

		if oldVolume.Spec.BackingImage != newVolume.Spec.BackingImage {
			err := fmt.Errorf("changing backing image for volume %v is not supported for backend store driver %v",
				newVolume.Name, newVolume.Spec.BackendStoreDriver)
			return werror.NewInvalidError(err.Error(), "")
		}

		if oldVolume.Spec.Encrypted != newVolume.Spec.Encrypted {
			err := fmt.Errorf("changing encryption for volume %v is not supported for backend store driver %v",
				newVolume.Name, newVolume.Spec.BackendStoreDriver)
			return werror.NewInvalidError(err.Error(), "")
		}

		if oldVolume.Spec.DataLocality != newVolume.Spec.DataLocality {
			err := fmt.Errorf("changing data locality for volume %v is not supported for backend store driver %v",
				newVolume.Name, newVolume.Spec.BackendStoreDriver)
			return werror.NewInvalidError(err.Error(), "")
		}

		if oldVolume.Spec.SnapshotDataIntegrity != newVolume.Spec.SnapshotDataIntegrity {
			err := fmt.Errorf("changing snapshot data integrity for volume %v is not supported for backend store driver %v",
				newVolume.Name, newVolume.Spec.BackendStoreDriver)
			return werror.NewInvalidError(err.Error(), "")
		}

		if oldVolume.Spec.ReplicaAutoBalance != newVolume.Spec.ReplicaAutoBalance {
			err := fmt.Errorf("changing replica auto balance for volume %v is not supported for backend store driver %v",
				newVolume.Name, newVolume.Spec.BackendStoreDriver)
			return werror.NewInvalidError(err.Error(), "")
		}

		if oldVolume.Spec.RestoreVolumeRecurringJob != newVolume.Spec.RestoreVolumeRecurringJob {
			err := fmt.Errorf("changing restore volume recurring job for volume %v is not supported for backend store driver %v",
				newVolume.Name, newVolume.Spec.BackendStoreDriver)
			return werror.NewInvalidError(err.Error(), "")
		}

		if oldVolume.Spec.ReplicaSoftAntiAffinity != newVolume.Spec.ReplicaSoftAntiAffinity {
			err := fmt.Errorf("changing replica soft anti-affinity for volume %v is not supported for backend store driver %v",
				newVolume.Name, newVolume.Spec.BackendStoreDriver)
			return werror.NewInvalidError(err.Error(), "")
		}

		if oldVolume.Spec.ReplicaZoneSoftAntiAffinity != newVolume.Spec.ReplicaZoneSoftAntiAffinity {
			err := fmt.Errorf("changing replica zone soft anti-affinity for volume %v is not supported for backend store driver %v",
				newVolume.Name, newVolume.Spec.BackendStoreDriver)
			return werror.NewInvalidError(err.Error(), "")
		}
	} else {
		if newVolume.Spec.OfflineReplicaRebuilding != longhorn.OfflineReplicaRebuildingDisabled {
			err := fmt.Errorf("changing offline replica rebuilding for volume %v is not supported for backend store driver %v",
				newVolume.Name, newVolume.Spec.BackendStoreDriver)
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	// prevent the changing v.Spec.MigrationNodeID to different node when the volume is doing live migration (when v.Status.CurrentMigrationNodeID != "")
	if newVolume.Status.CurrentMigrationNodeID != "" &&
		newVolume.Spec.MigrationNodeID != oldVolume.Spec.MigrationNodeID &&
		newVolume.Spec.MigrationNodeID != newVolume.Status.CurrentMigrationNodeID &&
		newVolume.Spec.MigrationNodeID != "" {
		err := fmt.Errorf("cannot change v.Spec.MigrationNodeID to node %v when the volume is doing live migration to node %v ", newVolume.Spec.MigrationNodeID, newVolume.Status.CurrentMigrationNodeID)
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}

func (v *volumeValidator) validateExpansionSize(oldVolume *longhorn.Volume, newVolume *longhorn.Volume) error {
	oldSize := oldVolume.Spec.Size
	newSize := newVolume.Spec.Size
	if newSize == oldSize {
		return nil
	}
	if newSize < oldSize && !newVolume.Status.ExpansionRequired {
		return fmt.Errorf("shrinking volume %v size from %v to %v is not supported", newVolume.Name, oldSize, newSize)
	}

	newKubernetesStatus := &newVolume.Status.KubernetesStatus
	namespace := newKubernetesStatus.Namespace
	pvcName := newKubernetesStatus.PVCName
	if pvcName == "" || newKubernetesStatus.LastPVCRefAt != "" {
		return nil
	}

	pvc, err := v.ds.GetPersistentVolumeClaim(namespace, pvcName)
	if err != nil {
		return err
	}

	pvcSCName := *pvc.Spec.StorageClassName
	pvcStorageClass, err := v.ds.GetStorageClassRO(pvcSCName)
	if err != nil {
		return err
	}
	if pvcStorageClass.AllowVolumeExpansion != nil && !*pvcStorageClass.AllowVolumeExpansion {
		return fmt.Errorf("storage class %v of PVC %v does not allow the volume expansion", pvcSCName, pvcName)
	}

	pvcSpecValue, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if !ok {
		return fmt.Errorf("cannot get request storage of PVC %v", pvcName)
	}

	requestedSize := resource.MustParse(strconv.FormatInt(newSize, 10))
	if pvcSpecValue.Cmp(requestedSize) < 0 {
		return fmt.Errorf("PVC %v size should be expanded from %v to %v first", pvcName, pvcSpecValue.Value(), requestedSize.Value())
	}

	return nil
}

func (v *volumeValidator) hasLocalReplicaOnSameNodeAsStrictLocalVolume(volume *longhorn.Volume) (bool, error) {
	replicas, err := v.ds.ListVolumeReplicas(volume.Name)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get replicas for volume %v", volume.Name)
	}

	if len(replicas) != 1 {
		return false, fmt.Errorf("BUG: replica should be 1 for %v volume %v", longhorn.DataLocalityStrictLocal, volume.Name)
	}

	var replica *longhorn.Replica
	for _, r := range replicas {
		replica = r
	}

	// For a newly created volume, the replica.Status.OwnerID should be ""
	// The attach should be successful.
	if replica.Spec.NodeID == "" {
		return true, nil
	}

	if replica.Spec.NodeID == volume.Spec.NodeID {
		return true, nil
	}

	return false, fmt.Errorf("moving a %v volume %v to another node is not supported", longhorn.DataLocalityStrictLocal, volume.Name)
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
			return werror.NewInvalidError(fmt.Sprintf("number of replica count should be 1 when data locality is %v", longhorn.DataLocalityStrictLocal), "")
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
