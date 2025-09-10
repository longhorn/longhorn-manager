package volume

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	wcommon "github.com/longhorn/longhorn-manager/webhook/common"
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
	volume, ok := newObj.(*longhorn.Volume)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Volume", newObj), "")
	}

	if !util.ValidateName(volume.Name) {
		return werror.NewInvalidError(fmt.Sprintf("invalid name %v", volume.Name), "")
	}

	if err := types.ValidateDataLocality(volume.Spec.DataLocality); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.dataLocality")
	}

	if err := types.ValidateAccessMode(volume.Spec.AccessMode); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.accessMode")
	}

	if err := validateReplicaCount(volume.Spec.CloneMode, volume.Spec.DataLocality, volume.Spec.NumberOfReplicas); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.numberOfReplicas")
	}

	if err := types.ValidateDataLocalityAndReplicaCount(volume.Spec.DataLocality, volume.Spec.NumberOfReplicas); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.dataLocality and spec.numberOfReplicas")
	}

	if err := types.ValidateDataLocalityAndAccessMode(volume.Spec.DataLocality, volume.Spec.Migratable, volume.Spec.AccessMode); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.dataLocality and spec.migratable and spec.accessMode")
	}

	if err := types.ValidateReplicaAutoBalance(volume.Spec.ReplicaAutoBalance); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.replicaAutoBalance")
	}

	if err := types.ValidateUnmapMarkSnapChainRemoved(volume.Spec.DataEngine, volume.Spec.UnmapMarkSnapChainRemoved); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.unmapMarkSnapChainRemoved")
	}

	if err := types.ValidateReplicaSoftAntiAffinity(volume.Spec.ReplicaSoftAntiAffinity); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.replicaSoftAntiAffinity")
	}

	if err := types.ValidateReplicaZoneSoftAntiAffinity(volume.Spec.ReplicaZoneSoftAntiAffinity); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.replicaZoneSoftAntiAffinity")
	}

	if err := types.ValidateReplicaDiskSoftAntiAffinity(volume.Spec.ReplicaDiskSoftAntiAffinity); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.replicaDiskSoftAntiAffinity")
	}

	if err := types.ValidateOfflineRebuild(volume.Spec.OfflineRebuilding); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.offlineRebuilding")
	}

	if err := types.ValidateBackupBlockSize(volume.Spec.Size, volume.Spec.BackupBlockSize); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.backupBlockSize")
	}

	if err := types.ValidateReplicaRebuildingBandwidthLimit(volume.Spec.DataEngine, volume.Spec.ReplicaRebuildingBandwidthLimit); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.replicaRebuildingBandwidthLimit")
	}

	if volume.Spec.BackingImage != "" {
		backingImage, err := v.ds.GetBackingImage(volume.Spec.BackingImage)
		if err != nil {
			return werror.NewInvalidError(err.Error(), "")
		}
		if backingImage != nil {
			if backingImage.Spec.DataEngine != volume.Spec.DataEngine {
				return werror.NewInvalidError("volume should have the same data engine as the backing image", "")
			}
		}
		// For qcow2 files, VirtualSize may be larger than the physical image size on disk.
		// For raw files, `qemu-img info` will report VirtualSize as being the same as the physical file size.
		// Volume size should not be smaller than the backing image size.
		if volume.Spec.Size < backingImage.Status.VirtualSize {
			return werror.NewInvalidError("volume size should be larger than the backing image size", "")
		}
	}

	if volume.Spec.Image == "" {
		return werror.NewInvalidError("BUG: Invalid empty Setting.EngineImage", "")
	}

	if volume.Spec.DataSource == "" && volume.Spec.CloneMode != longhorn.CloneModeNone {
		return werror.NewInvalidError("BUG: CloneMode is non-empty while DataSource is empty", ".spec.cloneMode")
	}
	if types.IsDataEngineV1(volume.Spec.DataEngine) && volume.Spec.CloneMode == longhorn.CloneModeLinkedClone {
		return werror.NewInvalidError(fmt.Sprintf("BUG: v1 data engine does not support clone mode %v", longhorn.CloneModeLinkedClone), ".spec.cloneMode")
	}

	if err := verifyVolumeDataSource(v.ds, volume); err != nil {
		return err
	}
	if err := validateRecurringJobLabels(volume); err != nil {
		return err
	}

	if !volume.Spec.Standby {
		if types.IsDataEngineV1(volume.Spec.DataEngine) &&
			volume.Spec.Frontend != longhorn.VolumeFrontendBlockDev &&
			volume.Spec.Frontend != longhorn.VolumeFrontendISCSI {
			return werror.NewInvalidError(fmt.Sprintf("invalid volume frontend specified: %v for data engine %v ", volume.Spec.Frontend, volume.Spec.DataEngine), "")
		}
		if types.IsDataEngineV2(volume.Spec.DataEngine) &&
			volume.Spec.Frontend != longhorn.VolumeFrontendBlockDev &&
			volume.Spec.Frontend != longhorn.VolumeFrontendNvmf &&
			volume.Spec.Frontend != longhorn.VolumeFrontendUblk {
			return werror.NewInvalidError(fmt.Sprintf("invalid volume frontend specified: %v for data engine %v ", volume.Spec.Frontend, volume.Spec.DataEngine), "")
		}
	}

	if volume.Spec.Migratable && volume.Spec.AccessMode != longhorn.AccessModeReadWriteMany {
		return werror.NewInvalidError("migratable volumes are only supported in ReadWriteMany (rwx) access mode", "")
	}

	// Check engine version before disable revision counter
	if volume.Spec.RevisionCounterDisabled {
		if ok, err := v.canDisableRevisionCounter(volume.Spec.Image, volume.Spec.DataEngine); !ok {
			err := errors.Wrapf(err, "can not create volume with current engine image that doesn't support disable revision counter")
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	if err := datastore.CheckVolume(volume); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	err := wcommon.ValidateRequiredDataEngineEnabled(v.ds, volume.Spec.DataEngine)
	if err != nil {
		return err
	}

	if err := validateSnapshotMaxCount(volume.Spec.SnapshotMaxCount); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.snapshotMaxCount")
	}

	if err := validateSnapshotMaxSize(volume.Spec.Size, volume.Spec.SnapshotMaxSize); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.snapshotMaxSize")
	}

	if err := v.ds.CheckDataEngineImageCompatiblityByImage(volume.Spec.Image, volume.Spec.DataEngine); err != nil {
		return werror.NewInvalidError(err.Error(), "volume.spec.image")
	}

	if err := v.validateBackupTarget("", volume.Spec.BackupTargetName); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.backupTargetName")
	}

	return nil
}

func (v *volumeValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldVolume, ok := oldObj.(*longhorn.Volume)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("oldObj %v is not a longhorn.Volume", oldObj), "")
	}
	newVolume, ok := newObj.(*longhorn.Volume)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("newObj %v is not a longhorn.Volume", newObj), "")
	}

	if err := v.validateExpansionSize(oldVolume, newVolume); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.size")
	}

	if err := validateDataLocalityUpdate(oldVolume, newVolume); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.dataLocality")
	}

	if err := validateReplicaCount(newVolume.Spec.CloneMode, newVolume.Spec.DataLocality, newVolume.Spec.NumberOfReplicas); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.numberOfReplicas")
	}

	if err := types.ValidateAccessMode(newVolume.Spec.AccessMode); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.accessMode")
	}

	if err := types.ValidateReplicaAutoBalance(newVolume.Spec.ReplicaAutoBalance); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.replicaAutoBalance")
	}

	if err := types.ValidateUnmapMarkSnapChainRemoved(newVolume.Spec.DataEngine, newVolume.Spec.UnmapMarkSnapChainRemoved); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.unmapMarkSnapChainRemoved")
	}

	if err := types.ValidateReplicaSoftAntiAffinity(newVolume.Spec.ReplicaSoftAntiAffinity); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.replicaSoftAntiAffinity")
	}

	if err := types.ValidateReplicaZoneSoftAntiAffinity(newVolume.Spec.ReplicaZoneSoftAntiAffinity); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.replicaZoneSoftAntiAffinity")
	}

	if err := types.ValidateReplicaDiskSoftAntiAffinity(newVolume.Spec.ReplicaDiskSoftAntiAffinity); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.replicaDiskSoftAntiAffinity")
	}

	if err := types.ValidateOfflineRebuild(newVolume.Spec.OfflineRebuilding); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.offlineRebuilding")
	}

	if err := validateImmutable(".spec.dataSource", oldVolume.Spec.DataSource, newVolume.Spec.DataSource); err != nil {
		return werror.NewInvalidError(err.Error(), ".spec.dataSource")
	}

	if err := validateImmutable(".spec.cloneMode", oldVolume.Spec.CloneMode, newVolume.Spec.CloneMode); err != nil {
		return werror.NewInvalidError(err.Error(), ".spec.cloneMode")
	}

	if oldVolume.Spec.Image != newVolume.Spec.Image {
		if err := v.ds.CheckDataEngineImageCompatiblityByImage(newVolume.Spec.Image, newVolume.Spec.DataEngine); err != nil {
			return werror.NewInvalidError(err.Error(), "volume.spec.image")
		}
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
			return werror.NewInvalidError(err.Error(), "spec.backupCompressionMethod")
		}
	}

	// Allow backup block size mutation only when the existing obj is not set, or correcting the existed invalid value
	isValidOldBackupBlockSize := types.ValidateBackupBlockSize(oldVolume.Spec.Size, oldVolume.Spec.BackupBlockSize) == nil
	if isValidOldBackupBlockSize && oldVolume.Spec.BackupBlockSize != newVolume.Spec.BackupBlockSize {
		err := fmt.Errorf("changing backup block size for volume %v is not supported", oldVolume.Name)
		return werror.NewInvalidError(err.Error(), "spec.backupBlockSize")
	}
	if err := types.ValidateBackupBlockSize(newVolume.Spec.Size, newVolume.Spec.BackupBlockSize); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.backupBlockSize")
	}

	if err := types.ValidateReplicaRebuildingBandwidthLimit(newVolume.Spec.DataEngine, newVolume.Spec.ReplicaRebuildingBandwidthLimit); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.replicaRebuildingBandwidthLimit")
	}

	if oldVolume.Spec.DataEngine != "" {
		if oldVolume.Spec.DataEngine != newVolume.Spec.DataEngine {
			err := fmt.Errorf("changing data engine for volume %v is not supported", oldVolume.Name)
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	if oldVolume.Spec.BackingImage != newVolume.Spec.BackingImage {
		err := fmt.Errorf("changing backing image for volume %v is not supported for data engine %v",
			newVolume.Name, newVolume.Spec.DataEngine)
		return werror.NewInvalidError(err.Error(), "")
	}

	if oldVolume.Spec.Encrypted != newVolume.Spec.Encrypted {
		err := fmt.Errorf("changing encryption for volume %v is not supported for data engine %v",
			newVolume.Name, newVolume.Spec.DataEngine)
		return werror.NewInvalidError(err.Error(), "")
	}

	if types.IsDataEngineV2(newVolume.Spec.DataEngine) {
		if newVolume.Spec.Frontend == longhorn.VolumeFrontendUblk {
			if oldVolume.Spec.Size != newVolume.Spec.Size {
				err := fmt.Errorf("changing volume size for ublk volume %v is not supported for data engine %v",
					newVolume.Name, newVolume.Spec.DataEngine)
				return werror.NewInvalidError(err.Error(), "")
			}
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

	if err := validateSnapshotMaxCount(newVolume.Spec.SnapshotMaxCount); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.snapshotMaxCount")
	}

	if err := validateSnapshotMaxSize(newVolume.Spec.Size, newVolume.Spec.SnapshotMaxSize); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.snapshotMaxSize")
	}

	if err := v.validateBackupTarget(oldVolume.Spec.BackupTargetName, newVolume.Spec.BackupTargetName); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.backupTargetName")
	}

	if (oldVolume.Spec.SnapshotMaxCount != newVolume.Spec.SnapshotMaxCount) ||
		(oldVolume.Spec.SnapshotMaxSize != newVolume.Spec.SnapshotMaxSize) {
		if err := v.validateUpdatingSnapshotMaxCountAndSize(oldVolume, newVolume); err != nil {
			return err
		}
	}
	if err := validateRecurringJobLabels(newVolume); err != nil {
		return err
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

	replicaMap, err := v.ds.ListVolumeReplicasRO(newVolume.Name)
	if err != nil {
		return err
	}

	replicaList := make([]*longhorn.Replica, 0, len(replicaMap))
	for _, r := range replicaMap {
		replicaList = append(replicaList, r)
	}
	ready, msg := types.IsVolumeReady(oldVolume, replicaList, types.VolumeOperationSizeExpansion)
	if !ready {
		return fmt.Errorf("volume %v is not ready: %v", oldVolume.Name, msg)
	}

	for _, replica := range replicaMap {
		diskUUID := replica.Spec.DiskID
		node, diskName, err := v.ds.GetReadyDiskNode(diskUUID)
		if err != nil {
			return err
		}
		diskStatus := node.Status.DiskStatus[diskName]
		if !datastore.IsSupportedVolumeSize(replica.Spec.DataEngine, diskStatus.FSType, newSize) {
			return fmt.Errorf("file system %s does not support volume size %v", diskStatus.Type, newSize)
		}
		break
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

func validateReplicaCount(cloneMode longhorn.CloneMode, dataLocality longhorn.DataLocality, replicaCount int) error {
	if err := types.ValidateReplicaCount(replicaCount); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}
	if dataLocality == longhorn.DataLocalityStrictLocal {
		if replicaCount != 1 {
			return werror.NewInvalidError(fmt.Sprintf("number of replica count should be 1 when data locality is %v", longhorn.DataLocalityStrictLocal), "")
		}
	}
	if cloneMode == longhorn.CloneModeLinkedClone {
		if replicaCount != 1 {
			return werror.NewInvalidError(fmt.Sprintf("number of replica count must be 1 when clone mode %v", longhorn.CloneModeLinkedClone), "")
		}
	}
	return nil
}

func (v *volumeValidator) canDisableRevisionCounter(image string, dataEngine longhorn.DataEngineType) (bool, error) {
	if types.IsDataEngineV2(dataEngine) {
		// v2 volume does not have revision counter
		return true, nil
	}

	cliAPIVersion, err := v.ds.GetEngineImageCLIAPIVersion(image)
	if err != nil {
		return false, err
	}
	if cliAPIVersion < engineapi.CLIVersionFour {
		return false, fmt.Errorf("current engine image version %v doesn't support disable revision counter", cliAPIVersion)
	}

	return true, nil
}

func validateSnapshotMaxCount(snapshotMaxCount int) error {
	if snapshotMaxCount < 2 || snapshotMaxCount > 250 {
		return fmt.Errorf("snapshot max count should be between 2 to 250")
	}
	return nil
}

func validateSnapshotMaxSize(size, snapshotMaxSize int64) error {
	if snapshotMaxSize != 0 && snapshotMaxSize < size*2 {
		return fmt.Errorf("snapshot max size can not be 0 and at least twice of volume size")
	}
	return nil
}

func (v *volumeValidator) validateBackupTarget(oldBackupTarget, newBackupTarget string) error {
	if newBackupTarget == "" {
		return fmt.Errorf("backup target name cannot be empty when creating a volume or updating from an existing backup target")
	}
	if oldBackupTarget == newBackupTarget {
		return nil
	}
	if _, err := v.ds.GetBackupTargetRO(newBackupTarget); err != nil {
		return errors.Wrapf(err, "failed to get backup target %v", newBackupTarget)
	}
	return nil
}

func (v *volumeValidator) validateUpdatingSnapshotMaxCountAndSize(oldVolume, newVolume *longhorn.Volume) error {
	var (
		currentSnapshotCount     int
		currentTotalSnapshotSize int64
		engine                   *longhorn.Engine
	)
	engines, err := v.ds.ListVolumeEngines(newVolume.Name)
	if err != nil && !datastore.ErrorIsNotFound(err) {
		return werror.NewInternalError(fmt.Sprintf("can't list engines for volume %s, err %v", newVolume.Name, err))
	} else if len(engines) == 0 {
		return nil
	}

	// It is dangerous to update snapshotMaxCount and snapshotMaxSize while  migrating. However, when upgrading to a
	// Longhorn version that includes these fields from one that does not, we automatically set snapshotMaxCount = 250,
	// and we do not want a validation failure here to stop the upgrade. We accept the change, but do not propagate it
	// to engines or replicas until the migration is complete.
	if len(engines) >= 2 {
		if oldVolume.Spec.SnapshotMaxCount == 0 &&
			newVolume.Spec.SnapshotMaxCount == types.MaxSnapshotNum &&
			oldVolume.Spec.SnapshotMaxSize == newVolume.Spec.SnapshotMaxSize {
			logrus.WithField("volumeName", newVolume.Name).Debugf("Allowing snapshotMaxCount of a migrating volume to change from 0 to %v during upgrade", types.MaxSnapshotNum)
		} else {
			return werror.NewInvalidError("can't update snapshotMaxCount or snapshotMaxSize during migration", "")
		}
	}

	for _, e := range engines {
		engine = e
	}

	for _, snapshotInfo := range engine.Status.Snapshots {
		if snapshotInfo == nil || snapshotInfo.Removed || snapshotInfo.Name == "volume-head" {
			continue
		}
		currentSnapshotCount++
		snapshotSize, err := strconv.ParseInt(snapshotInfo.Size, 10, 64)
		if err != nil {
			return werror.NewInternalError(fmt.Sprintf("can't parse size %s from snapshot %s in volume %s, err %v", snapshotInfo.Size, snapshotInfo.Name, newVolume.Name, err))
		}
		currentTotalSnapshotSize += snapshotSize
	}

	if currentSnapshotCount > newVolume.Spec.SnapshotMaxCount || (newVolume.Spec.SnapshotMaxSize != 0 && currentTotalSnapshotSize > newVolume.Spec.SnapshotMaxSize) {
		return werror.NewInvalidError("can't make snapshotMaxCount or snapshotMaxSize be smaller than current usage, please remove snapshots first", "")
	}
	return nil
}

func validateImmutable(field string, oldVal, newVal any) error {
	if !apiequality.Semantic.DeepEqual(oldVal, newVal) {
		return fmt.Errorf("%s is immutable (old=%+v, new=%+v)", field, oldVal, newVal)
	}
	return nil
}

func verifyVolumeDataSource(ds *datastore.DataStore, vol *longhorn.Volume) error {
	if vol.Spec.DataSource == "" {
		return nil
	}
	if !types.IsValidVolumeDataSource(vol.Spec.DataSource) {
		return werror.NewInvalidError(fmt.Sprintf("invalid volume data source %v", vol.Spec.DataSource), ".spec.dataSource")
	}
	srcVolName := types.GetVolumeName(vol.Spec.DataSource) // Note that srcVolName is non empty
	srcVol, err := ds.GetVolumeRO(srcVolName)
	if err != nil {
		return err
	}
	if vol.Spec.DataEngine != srcVol.Spec.DataEngine {
		return werror.NewInvalidError(fmt.Sprintf("cannot clone volume with data engine %v into a volume with data engine %v", srcVol.Spec.DataEngine, vol.Spec.DataEngine), ".spec.dataSource")
	}
	if srcVol.Spec.CloneMode == longhorn.CloneModeLinkedClone {
		return werror.NewInvalidError(fmt.Sprintf("cannot create a new volume from a linked-clone volume %v", srcVolName), ".spec.dataSource")
	}
	if vol.Spec.CloneMode != longhorn.CloneModeLinkedClone {
		return nil
	}
	volumesRO, err := ds.ListVolumesRO()
	if err != nil {
		return werror.NewInvalidError(err.Error(), ".spec.dataSource")
	}
	for _, v := range volumesRO {
		if types.GetVolumeName(v.Spec.DataSource) == srcVolName && v.Spec.CloneMode == longhorn.CloneModeLinkedClone {
			return werror.NewInvalidError(fmt.Sprintf("BUG: there already exist a linked-cloned volume %v from the source volume %v", v.Name, srcVolName), ".spec.dataSource")
		}
	}

	return nil
}

func validateRecurringJobLabels(vol *longhorn.Volume) error {
	if vol.Spec.CloneMode != longhorn.CloneModeLinkedClone {
		return nil
	}

	metadata, err := meta.Accessor(vol)
	if err != nil {
		return err
	}

	labels := metadata.GetLabels()

	jobPrefix := fmt.Sprintf(types.LonghornLabelRecurringJobKeyPrefixFmt, types.LonghornLabelRecurringJob)
	groupPrefix := fmt.Sprintf(types.LonghornLabelRecurringJobKeyPrefixFmt, types.LonghornLabelRecurringJobGroup)

	jobLabels := []string{}
	for label := range labels {
		if !strings.HasPrefix(label, jobPrefix) &&
			!strings.HasPrefix(label, groupPrefix) {
			continue
		}
		jobLabels = append(jobLabels, label)
	}

	if len(jobLabels) > 0 {
		return werror.NewInvalidError(fmt.Sprintf("cannot add recurring jobs to linked-clone volume: %+v ", jobLabels), ".metadata.label")
	}

	return nil
}
