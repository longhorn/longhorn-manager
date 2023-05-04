package volume

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/backupstore"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type volumeMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &volumeMutator{ds: ds}
}

func (v *volumeMutator) Resource() admission.Resource {
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

func (v *volumeMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	volume := newObj.(*longhorn.Volume)

	name := util.AutoCorrectName(volume.Name, datastore.NameMaximumLength)
	if name != volume.Name {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/name", "value": "%s"}`, name))
	}

	if volume.Spec.ReplicaAutoBalance == "" {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/replicaAutoBalance", "value": "ignored"}`)
	}

	if volume.Spec.DiskSelector == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/diskSelector", "value": []}`)
	}

	if volume.Spec.NodeSelector == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/nodeSelector", "value": []}`)
	}

	if volume.Spec.RecurringJobs == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/recurringJobs", "value": []}`)
	}

	for id, job := range volume.Spec.RecurringJobs {
		if job.Groups == nil {
			patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/recurringJobs/%d/groups", "value": []}`, id))
		}
		if job.Labels == nil {
			patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/recurringJobs/%d/labels", "value": {}}`, id))
		}
	}

	if volume.Spec.NumberOfReplicas == 0 {
		numberOfReplicas, err := v.getDefaultReplicaCount()
		if err != nil {
			err = errors.Wrap(err, "BUG: cannot get valid number for setting default replica count")
			return nil, werror.NewInvalidError(err.Error(), "")
		}
		logrus.Infof("Use the default number of replicas %v", numberOfReplicas)
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/numberOfReplicas", "value": %v}`, numberOfReplicas))
	}

	if string(volume.Spec.DataLocality) == "" {
		defaultDataLocality, err := v.ds.GetSettingValueExisted(types.SettingNameDefaultDataLocality)
		if err != nil {
			err = errors.Wrapf(err, "cannot get valid mode for setting default data locality for volume: %v", name)
			return nil, werror.NewInvalidError(err.Error(), "")
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/dataLocality", "value": "%s"}`, defaultDataLocality))
	} else if volume.Spec.DataLocality == longhorn.DataLocalityStrictLocal {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/revisionCounterDisabled", "value": true}`)
	}

	if string(volume.Spec.SnapshotDataIntegrity) == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/snapshotDataIntegrity", "value": "%s"}`, longhorn.SnapshotDataIntegrityIgnored))
	}

	if string(volume.Spec.RestoreVolumeRecurringJob) == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/restoreVolumeRecurringJob", "value": "%s"}`, longhorn.RestoreVolumeRecurringJobDefault))
	}

	if volume.Spec.UnmapMarkSnapChainRemoved == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/unmapMarkSnapChainRemoved", "value": "%s"}`, longhorn.UnmapMarkSnapChainRemovedIgnored))
	}

	if string(volume.Spec.AccessMode) == "" {
		accessModeFromBackup := longhorn.AccessModeReadWriteOnce
		if volume.Spec.FromBackup != "" {
			bName, _, _, err := backupstore.DecodeBackupURL(volume.Spec.FromBackup)
			if err != nil {
				err := errors.Wrapf(err, "failed to decode backup url %v", volume.Spec.FromBackup)
				return nil, werror.NewInvalidError(err.Error(), "")
			}
			backup, err := v.ds.GetBackupRO(bName)
			if err != nil {
				err = errors.Wrapf(err, "failed to get backup %v", bName)
				return nil, werror.NewInvalidError(err.Error(), "")
			}
			if labelAccessMode, isExist := backup.Status.Labels[types.GetLonghornLabelKey(types.LonghornLabelVolumeAccessMode)]; isExist {
				accessModeFromBackup = longhorn.AccessMode(labelAccessMode)
			}
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/accessMode", "value": "%s"}`, string(accessModeFromBackup)))
	}

	labels := volume.Labels
	if labels == nil {
		labels = map[string]string{}
	}

	size := volume.Spec.Size
	if volume.Spec.FromBackup != "" {
		bName, bvName, _, err := backupstore.DecodeBackupURL(volume.Spec.FromBackup)
		if err != nil {
			return nil, werror.NewInvalidError(fmt.Sprintf("cannot get backup and volume name from backup URL %v: %v", volume.Spec.FromBackup, err), "")
		}

		bv, err := v.ds.GetBackupVolumeRO(bvName)
		if err != nil {
			return nil, werror.NewInvalidError(fmt.Sprintf("cannot get backup volume %s: %v", bvName, err), "")
		}
		if bv != nil && bv.Status.BackingImageName != "" {
			if volume.Spec.BackingImage == "" {
				volume.Spec.BackingImage = bv.Status.BackingImageName
				patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/backingImage", "value": "%s"}`, bv.Status.BackingImageName))
				logrus.Debugf("Since the backing image is not specified during the restore, "+
					"the previous backing image %v used by backup volume %v will be set for volume %v creation",
					bv.Status.BackingImageName, bvName, name)
			}
			bi, err := v.ds.GetBackingImage(volume.Spec.BackingImage)
			if err != nil {
				err = errors.Wrapf(err, "failed to get backing image %v", volume.Spec.BackingImage)
				return nil, werror.NewInvalidError(err.Error(), "")
			}
			// Validate the checksum only when the chosen backing image name is the same as the record in the backup volume.
			// If user picks up a backing image different from `backupVolume.BackingImageName`, there is no need to do verification.
			if volume.Spec.BackingImage == bv.Status.BackingImageName {
				if bv.Status.BackingImageChecksum != "" && bi.Status.Checksum != "" &&
					bv.Status.BackingImageChecksum != bi.Status.Checksum {
					return nil, werror.NewInvalidError(fmt.Sprintf("backing image %v current checksum doesn't match the recoreded checksum in backup volume", volume.Spec.BackingImage), "")
				}
			}
		}

		backup, err := v.ds.GetBackupRO(bName)
		if err != nil {
			return nil, werror.NewInvalidError(fmt.Sprintf("cannot get backup %s: %v", bName, err), "")
		}

		logrus.Infof("Override size of volume %v to %v because it's from backup", name, backup.Status.VolumeSize)
		// formalize the final size to the unit in bytes
		size, err = util.ConvertSize(backup.Status.VolumeSize)
		if err != nil {
			return nil, werror.NewInvalidError(fmt.Sprintf("get invalid size for volume %v: %v", backup.Status.VolumeSize, err), "")
		}

		labels[types.LonghornLabelBackupVolume] = bvName
	}

	labelsForVolumesFollowsGlobalSettings := datastore.GetLabelsForVolumesFollowsGlobalSettings(volume)
	for k, v := range labelsForVolumesFollowsGlobalSettings {
		labels[k] = v
	}

	patchOp, err := common.GetLonghornLabelsPatchOp(volume, labels, types.SettingsRelatedToVolume)
	if err != nil {
		err := errors.Wrapf(err, "failed to get label patch for volume %v", volume.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	patchOp, err = common.GetLonghornFinalizerPatchOp(volume)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for volume %v", volume.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	newSize := util.RoundUpSize(size)
	if newSize != size {
		logrus.Infof("Rounding up the volume spec size from %d to %d in the create mutator", size, newSize)
	}
	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/size", "value": "%v"}`, strconv.FormatInt(newSize, 10)))

	defaultEngineImage, _ := v.ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if defaultEngineImage == "" {
		return nil, werror.NewInvalidError("BUG: Invalid empty Setting.EngineImage", "")
	}
	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/engineImage", "value": "%s"}`, defaultEngineImage))

	if volume.Spec.BackupCompressionMethod == "" {
		defaultCompressionMethod, _ := v.ds.GetSettingValueExisted(types.SettingNameBackupCompressionMethod)
		if defaultCompressionMethod == "" {
			return nil, werror.NewInvalidError("BUG: Invalid empty Setting.BackupCompressionMethod", "")
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/backupCompressionMethod", "value": "%s"}`, defaultCompressionMethod))
	}

	return patchOps, nil
}

func (v *volumeMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	volume := newObj.(*longhorn.Volume)

	if volume.Spec.ReplicaAutoBalance == "" {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/replicaAutoBalance", "value": "ignored"}`)
	}
	if volume.Spec.AccessMode == "" {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/accessMode", "value": "rwo"}`)
	}
	if volume.Spec.DiskSelector == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/diskSelector", "value": []}`)
	}
	if volume.Spec.NodeSelector == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/nodeSelector", "value": []}`)
	}
	if volume.Spec.RecurringJobs == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/recurringJobs", "value": []}`)
	}
	for id, job := range volume.Spec.RecurringJobs {
		if job.Groups == nil {
			patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/recurringJobs/%d/groups", "value": []}`, id))
		}
		if job.Labels == nil {
			patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/recurringJobs/%d/labels", "value": {}}`, id))
		}
	}
	if volume.Spec.UnmapMarkSnapChainRemoved == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/unmapMarkSnapChainRemoved", "value": "%s"}`, longhorn.UnmapMarkSnapChainRemovedIgnored))
	}
	if string(volume.Spec.SnapshotDataIntegrity) == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/snapshotDataIntegrity", "value": "%s"}`, longhorn.SnapshotDataIntegrityIgnored))
	}
	if string(volume.Spec.RestoreVolumeRecurringJob) == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/restoreVolumeRecurringJob", "value": "%s"}`, longhorn.RestoreVolumeRecurringJobDefault))
	}

	size := util.RoundUpSize(volume.Spec.Size)
	if size != volume.Spec.Size {
		logrus.Infof("Rounding up the requested volume spec size from %d to %d in the update mutator", volume.Spec.Size, size)
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/size", "value": "%s"}`, strconv.FormatInt(size, 10)))
	}

	if volume.Spec.DataLocality == longhorn.DataLocalityStrictLocal {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/revisionCounterDisabled", "value": true}`)
	}

	labels := volume.Labels
	if labels == nil {
		labels = map[string]string{}
	}

	labelsForVolumesFollowsGlobalSettings := datastore.GetLabelsForVolumesFollowsGlobalSettings(volume)
	for k, v := range labelsForVolumesFollowsGlobalSettings {
		labels[k] = v
	}

	patchOp, err := common.GetLonghornLabelsPatchOp(volume, labels, types.SettingsRelatedToVolume)
	if err != nil {
		err := errors.Wrapf(err, "failed to get label patch for volume %v", volume.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	return patchOps, nil
}

func (v *volumeMutator) getDefaultReplicaCount() (int, error) {
	c, err := v.ds.GetSettingAsInt(types.SettingNameDefaultReplicaCount)
	if err != nil {
		return 0, err
	}
	return int(c), nil
}
