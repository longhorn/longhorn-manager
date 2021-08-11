package v111to120

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

const (
	upgradeLogPrefix = "upgrade from v1.1.1 to v1.2.0: "
)

func UpgradeCRs(namespace string, lhClient *lhclientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"UpgradeCRs failed")
	}()
	if err := upgradeBackingImages(namespace, lhClient); err != nil {
		return err
	}
	// TODO: Need to re-consider if this is required.
	if err := upgradeBackupTargets(namespace, lhClient); err != nil {
		return nil
	}
	if err := upgradeVolumes(namespace, lhClient); err != nil {
		return err
	}
	return nil
}

const (
	DeprecatedBackingImageStateDownloaded  = "downloaded"
	DeprecatedBackingImageStateDownloading = "downloading"
)

func upgradeBackingImages(namespace string, lhClient *lhclientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, "upgrade backing images failed")
	}()
	biList, err := lhClient.LonghornV1beta1().BackingImages(namespace).List(metav1.ListOptions{})
	if err != nil {
		return err
	}
	nodeList, err := lhClient.LonghornV1beta1().Nodes(namespace).List(metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, bi := range biList.Items {
		existingBI := bi.DeepCopy()
		if bi.Status.DiskFileStatusMap == nil {
			bi.Status.DiskFileStatusMap = map[string]*types.BackingImageDiskFileStatus{}
		}
		for diskUUID, state := range bi.Status.DiskDownloadStateMap {
			if _, exists := bi.Status.DiskFileStatusMap[diskUUID]; !exists {
				bi.Status.DiskFileStatusMap[diskUUID] = &types.BackingImageDiskFileStatus{}
			}
			switch string(state) {
			case DeprecatedBackingImageStateDownloaded:
				bi.Status.DiskFileStatusMap[diskUUID].State = types.BackingImageStateReady
			case DeprecatedBackingImageStateDownloading:
				bi.Status.DiskFileStatusMap[diskUUID].State = types.BackingImageStateInProgress
			default:
				bi.Status.DiskFileStatusMap[diskUUID].State = types.BackingImageState(state)
			}
		}
		bi.Status.DiskDownloadStateMap = map[string]types.BackingImageDownloadState{}

		for diskUUID, progress := range bi.Status.DiskDownloadProgressMap {
			if _, exists := bi.Status.DiskFileStatusMap[diskUUID]; !exists {
				continue
			}
			bi.Status.DiskFileStatusMap[diskUUID].Progress = progress
		}
		bi.Status.DiskDownloadProgressMap = map[string]int{}

		if !reflect.DeepEqual(bi.Status, existingBI.Status) {
			if _, err := lhClient.LonghornV1beta1().BackingImages(namespace).UpdateStatus(&bi); err != nil {
				return err
			}
		}

		if bi.Spec.ImageURL != "" || bi.Spec.SourceType == "" {
			bi, err := lhClient.LonghornV1beta1().BackingImages(namespace).Get(bi.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if err := checkAndCreateBackingImageDataSource(namespace, lhClient, bi, nodeList); err != nil {
				return err
			}
			bi.Spec.ImageURL = ""
			bi.Spec.SourceType = types.BackingImageDataSourceTypeDownload
			bi.Spec.SourceParameters = map[string]string{types.DataSourceTypeDownloadParameterURL: bi.Spec.ImageURL}
			if _, err := lhClient.LonghornV1beta1().BackingImages(namespace).Update(bi); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkAndCreateBackingImageDataSource(namespace string, lhClient *lhclientset.Clientset, bi *longhorn.BackingImage, nodeList *longhorn.NodeList) (err error) {
	bids, err := lhClient.LonghornV1beta1().BackingImageDataSources(namespace).Get(bi.Name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		// Pick up an random disk in the backing image spec to create the backing image data source
		var availableNode, availableDiskUUID, availableDiskPath string
		for _, node := range nodeList.Items {
			for diskName, status := range node.Status.DiskStatus {
				spec, exists := node.Spec.Disks[diskName]
				if !exists {
					continue
				}
				for diskUUID := range bi.Spec.Disks {
					if diskUUID != status.DiskUUID {
						continue
					}
					availableNode = node.Name
					availableDiskUUID = status.DiskUUID
					availableDiskPath = spec.Path
					break
				}
				if availableNode != "" {
					break
				}
			}
		}

		bids = &longhorn.BackingImageDataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name:            bi.Name,
				OwnerReferences: datastore.GetOwnerReferencesForBackingImage(bi),
			},
			Spec: types.BackingImageDataSourceSpec{
				NodeID:          availableNode,
				DiskUUID:        availableDiskUUID,
				DiskPath:        availableDiskPath,
				SourceType:      types.BackingImageDataSourceTypeDownload,
				Parameters:      map[string]string{types.DataSourceTypeDownloadParameterURL: bi.Spec.ImageURL},
				FileTransferred: true,
			},
		}
		if bids, err = lhClient.LonghornV1beta1().BackingImageDataSources(namespace).Create(bids); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}

	// Then blindly set the state as ready
	bids.Status.OwnerID = bids.Spec.NodeID
	bids.Status.Size = bi.Status.Size
	bids.Status.Progress = 100
	if bids, err = lhClient.LonghornV1beta1().BackingImageDataSources(namespace).UpdateStatus(bids); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}

	return nil
}

func upgradeBackupTargets(namespace string, lhClient *lhclientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, "upgrade backup target failed")
	}()

	const defaultBackupTargetName = "default"
	_, err = lhClient.LonghornV1beta1().BackupTargets(namespace).Get(defaultBackupTargetName, metav1.GetOptions{})
	if err != nil && !datastore.ErrorIsNotFound(err) {
		return err
	}
	if err == nil {
		return nil
	}

	// Get settings
	targetSetting, err := lhClient.LonghornV1beta1().Settings(namespace).Get(string(types.SettingNameBackupTarget), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	secretSetting, err := lhClient.LonghornV1beta1().Settings(namespace).Get(string(types.SettingNameBackupTargetCredentialSecret), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	interval, err := lhClient.LonghornV1beta1().Settings(namespace).Get(string(types.SettingNameBackupstorePollInterval), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	var pollInterval time.Duration
	definition, ok := types.SettingDefinitions[types.SettingNameBackupstorePollInterval]
	if !ok {
		return fmt.Errorf("setting %v is not supported", types.SettingNameBackupstorePollInterval)
	}
	if definition.Type != types.SettingTypeInt {
		return fmt.Errorf("The %v setting value couldn't change to integer, value is %v ", string(types.SettingNameBackupstorePollInterval), interval.Value)
	}
	result, err := strconv.ParseInt(interval.Value, 10, 64)
	if err != nil {
		return err
	}
	pollInterval = time.Duration(result) * time.Second

	// Create the default BackupTarget CR if not present
	_, err = lhClient.LonghornV1beta1().BackupTargets(namespace).Create(&longhorn.BackupTarget{
		ObjectMeta: metav1.ObjectMeta{
			Name:       defaultBackupTargetName,
			Finalizers: []string{longhorn.SchemeGroupVersion.Group},
		},
		Spec: types.BackupTargetSpec{
			BackupTargetURL:  targetSetting.Value,
			CredentialSecret: secretSetting.Value,
			PollInterval:     metav1.Duration{Duration: pollInterval},
			SyncRequestedAt:  &metav1.Time{Time: time.Now().Add(time.Second).UTC()},
		},
	})
	if err != nil {
		return err
	}
	return nil
}

func upgradeVolumes(namespace string, lhClient *lhclientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, "upgrade volume failed")
	}()

	volumeList, err := lhClient.LonghornV1beta1().Volumes(namespace).List(metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, volume := range volumeList.Items {
		if err := upgradeLabelsForVolume(&volume, lhClient, namespace); err != nil {
			return err
		}
	}
	return nil
}

func upgradeLabelsForVolume(v *longhorn.Volume, lhClient *lhclientset.Clientset, namespace string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "upgradeLabelsForVolume failed")
	}()

	// Add backup volume name label to the restore/DR volume
	if v.Spec.FromBackup == "" {
		return nil
	}
	_, backupVolumeName, _, err := backupstore.DecodeBackupURL(v.Spec.FromBackup)
	if err != nil {
		return fmt.Errorf("cannot decode backup URL %s for volume %s: %v", v.Spec.FromBackup, v.Name, err)
	}

	metadata, err := meta.Accessor(v)
	if err != nil {
		return err
	}

	labels := metadata.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[types.LonghornLabelBackupVolume] = backupVolumeName
	metadata.SetLabels(labels)

	if _, err := lhClient.LonghornV1beta1().Volumes(namespace).Update(v); err != nil {
		return errors.Wrapf(err, "failed to add label for volume %s during upgrade", v.Name)
	}
	return nil
}
