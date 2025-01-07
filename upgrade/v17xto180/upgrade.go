package v17xto180

import (
	"context"

	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.7.x to v1.8.0: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if resourceMaps == nil {
		return errors.New("resourceMaps cannot be nil")
	}

	if err := upgradeVolumes(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeBackupVolumes(namespace, lhClient, resourceMaps); err != nil {
		return err
	}
	if err := upgradeBackups(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeBackupBackingImages(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeBackingImageDataSources(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeSystemBackups(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return upgradeBackingImages(namespace, lhClient, resourceMaps)
}

func upgradeVolumes(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade volume failed")
	}()

	volumesMap, err := upgradeutil.ListAndUpdateVolumesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn volumes during the volume upgrade")
	}

	for _, v := range volumesMap {
		backupTargetName := setDefaultBackupTargetIfEmpty(v.Spec.BackupTargetName)
		v.Spec.BackupTargetName = backupTargetName
	}

	return nil
}

func setDefaultBackupTargetIfEmpty(backupTargetName string) string {
	if backupTargetName == "" {
		return types.DefaultBackupTargetName
	}
	return backupTargetName
}

func upgradeBackupVolumes(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backup volume failed")
	}()

	backupVolumeMap, err := upgradeutil.ListAndUpdateBackupVolumesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn BackupVolumes during the Longhorn BackupVolume upgrade")
	}

	for _, bv := range backupVolumeMap {
		backupTargetName := bv.Spec.BackupTargetName
		bv.Spec.BackupTargetName = setDefaultBackupTargetIfEmpty(backupTargetName)

		// volumeName is used to synchronize the backup volume between the cluster and remote backup target instead of the BackupVolume CR name.
		// It used the BackupVolume CR name as the volume name before, but now the BackupVolume CR name is a random name.
		if bv.Spec.VolumeName == "" {
			bv.Spec.VolumeName = bv.Name
		}
		bv.Labels = addLabel(bv.Labels, types.LonghornLabelBackupTarget, bv.Spec.BackupTargetName)
		bv.Labels = addLabel(bv.Labels, types.LonghornLabelBackupVolume, bv.Spec.VolumeName)
	}

	return nil
}

func addLabel(labels map[string]string, key, value string) map[string]string {
	if labels == nil {
		labels = make(map[string]string)
	}
	if _, exists := labels[key]; !exists {
		labels[key] = value
	}
	return labels
}

func upgradeBackups(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backup failed")
	}()

	backupMap, err := upgradeutil.ListAndUpdateBackupsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn Backups during the Longhorn Backup upgrade")
	}

	for _, b := range backupMap {
		backupTargetName := types.DefaultBackupTargetName
		vol, err := lhClient.LonghornV1beta2().Volumes(namespace).Get(context.TODO(), b.Status.VolumeName, metav1.GetOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return errors.Wrapf(err, "failed to get volume %v of backup %v", b.Status.VolumeName, b.Name)
			}
		} else {
			if vol.Spec.BackupTargetName != "" {
				backupTargetName = vol.Spec.BackupTargetName
			}
		}
		b.Labels = addLabel(b.Labels, types.LonghornLabelBackupTarget, backupTargetName)
	}

	return nil
}

func upgradeBackupBackingImages(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backup backing images failed")
	}()

	backupBackingImageMap, err := upgradeutil.ListAndUpdateBackupBackingImagesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn BackupBackingImages during the Longhorn Backup Backing Images upgrade")
	}

	for _, bbi := range backupBackingImageMap {
		backupTargetName := setDefaultBackupTargetIfEmpty(bbi.Spec.BackupTargetName)
		bbi.Spec.BackupTargetName = backupTargetName
		if bbi.Spec.BackingImage == "" {
			bbi.Spec.BackingImage = bbi.Status.BackingImage
		}
		bbi.Labels = addLabel(bbi.Labels, types.LonghornLabelBackupTarget, bbi.Spec.BackupTargetName)
		bbi.Labels = addLabel(bbi.Labels, types.LonghornLabelBackingImage, bbi.Spec.BackingImage)
	}

	return nil
}

func upgradeBackingImageDataSources(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backing image data source failed")
	}()

	backingImageDataSourcesMap, err := upgradeutil.ListAndUpdateBackingImageDataSourcesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn BackupImagesDataSources during the Longhorn Backup Images Data Sources upgrade")
	}

	for _, bids := range backingImageDataSourcesMap {
		if bids.Spec.Parameters == nil {
			bids.Spec.Parameters = map[string]string{longhorn.DataSourceTypeRestoreParameterBackupTargetName: types.DefaultBackupTargetName}
		} else {
			backupTargetName := bids.Spec.Parameters[longhorn.DataSourceTypeRestoreParameterBackupTargetName]
			bids.Spec.Parameters[longhorn.DataSourceTypeRestoreParameterBackupTargetName] = setDefaultBackupTargetIfEmpty(backupTargetName)
		}
	}
	return nil
}

func upgradeBackingImages(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backing image failed")
	}()

	backingImageMap, err := upgradeutil.ListAndUpdateBackingImagesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn backing images during the backing image upgrade")
	}

	for _, bi := range backingImageMap {
		if bi.Spec.MinNumberOfCopies == 0 {
			bi.Spec.MinNumberOfCopies = types.DefaultMinNumberOfCopies
		}
		if string(bi.Spec.DataEngine) == "" {
			bi.Spec.DataEngine = longhorn.DataEngineTypeV1
		}

		// before v1.8.0, there should not have any v2 data engine disk in the backing image.
		if bi.Spec.DiskFileSpecMap != nil {
			for diskUUID := range bi.Spec.DiskFileSpecMap {
				bi.Spec.DiskFileSpecMap[diskUUID].DataEngine = longhorn.DataEngineTypeV1
			}
		}
	}

	return nil
}

func upgradeSystemBackups(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade system backup failed")
	}()

	sbMap, err := upgradeutil.ListAndUpdateSystemBackupsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn system backups during the system backup upgrade")
	}

	defaultBackupTarget, err := lhClient.LonghornV1beta2().BackupTargets(namespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrapf(err, "failed to get default backup target")
	}

	for _, sb := range sbMap {
		if sb.OwnerReferences == nil {
			sb.OwnerReferences = datastore.GetOwnerReferencesForBackupTarget(defaultBackupTarget)
		}
	}

	return nil
}

func UpgradeResourcesStatus(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if err := upgradeBackingImageStatus(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return upgradeBackupStatus(namespace, lhClient, resourceMaps)
}

func upgradeBackupStatus(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backups failed")
	}()

	backupMap, err := upgradeutil.ListAndUpdateBackupsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn backups during the backup status upgrade")
	}

	for _, b := range backupMap {
		backupTargetName := types.DefaultBackupTargetName
		vol, err := lhClient.LonghornV1beta2().Volumes(namespace).Get(context.TODO(), b.Status.VolumeName, metav1.GetOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return errors.Wrapf(err, "failed to get volume %v of backup %v", b.Status.VolumeName, b.Name)
			}
		} else {
			if vol.Spec.BackupTargetName != "" {
				backupTargetName = vol.Spec.BackupTargetName
			}
		}

		b.Status.BackupTargetName = backupTargetName
	}
	return nil
}

func upgradeBackingImageStatus(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backing image failed")
	}()

	backingImageMap, err := upgradeutil.ListAndUpdateBackingImagesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn backing images during the backing image upgrade")
	}

	for _, bi := range backingImageMap {
		// before v1.8.0, there should not have any v2 data engine disk in the backing image.
		if bi.Status.DiskFileStatusMap != nil {
			for diskUUID := range bi.Status.DiskFileStatusMap {
				bi.Status.DiskFileStatusMap[diskUUID].DataEngine = longhorn.DataEngineTypeV1
			}
		}
	}

	return nil
}
