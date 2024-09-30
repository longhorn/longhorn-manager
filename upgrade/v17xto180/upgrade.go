package v17xto180

import (
	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.7.x to v1.8.0: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
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
		if v.Spec.BackupTargetName == "" {
			v.Spec.BackupTargetName = types.DefaultBackupTargetName
		}
	}

	return nil
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
		if bv.Spec.BackupTargetName == "" {
			bv.Spec.BackupTargetName = types.DefaultBackupTargetName
		}

		if bv.Spec.VolumeName == "" {
			bv.Spec.VolumeName = bv.Name
		}

		if bv.Labels == nil {
			bv.Labels = make(map[string]string)
		}

		_, exists := bv.Labels[types.LonghornLabelBackupVolume]
		if !exists {
			bv.Labels[types.LonghornLabelBackupVolume] = bv.Spec.VolumeName
		}

		_, exists = bv.Labels[types.LonghornLabelBackupTarget]
		if !exists {
			bv.Labels[types.LonghornLabelBackupTarget] = types.DefaultBackupTargetName
		}
	}

	return nil
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
		if b.Spec.BackupTargetName == "" {
			b.Spec.BackupTargetName = types.DefaultBackupTargetName
		}

		if b.Labels == nil {
			b.Labels = make(map[string]string)
		}

		_, exists := b.Labels[types.LonghornLabelBackupTarget]
		if !exists {
			b.Labels[types.LonghornLabelBackupTarget] = b.Spec.BackupTargetName
		}

		_, exists = b.Labels[types.LonghornLabelBackupVolume]
		if !exists {
			b.Labels[types.LonghornLabelBackupVolume] = b.Status.VolumeName
		}
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
		return errors.Wrapf(err, "failed to list all existing Longhorn Backups during the Longhorn Backup Backing Images upgrade")
	}

	for _, bbi := range backupBackingImageMap {
		if bbi.Spec.BackingImage == "" {
			bbi.Spec.BackingImage = bbi.Status.BackingImage
		}

		if bbi.Spec.BackupTargetName == "" {
			bbi.Spec.BackupTargetName = types.DefaultBackupTargetName
		}

		if bbi.Labels == nil {
			bbi.Labels = make(map[string]string)
		}

		_, exists := bbi.Labels[types.LonghornLabelBackupTarget]
		if !exists {
			bbi.Labels[types.LonghornLabelBackupTarget] = bbi.Spec.BackupTargetName
		}

		_, exists = bbi.Labels[types.LonghornLabelBackingImage]
		if !exists {
			bbi.Labels[types.LonghornLabelBackingImage] = bbi.Spec.BackingImage
		}
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
		return errors.Wrapf(err, "failed to list all existing Longhorn Backups during the Longhorn Backup Backing Images upgrade")
	}

	for _, bids := range backingImageDataSourcesMap {
		if _, exists := bids.Spec.Parameters[longhorn.DataSourceTypeRestoreParameterBackupTargetName]; !exists {
			bids.Spec.Parameters[longhorn.DataSourceTypeRestoreParameterBackupTargetName] = types.DefaultBackupTargetName
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
	}

	return nil
}

func UpgradeResourcesStatus(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	// Currently there are no statuses to upgrade. See UpgradeResources -> upgradeVolumes or previous Longhorn versions
	// for examples.
	return nil
}
