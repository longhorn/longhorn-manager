package v111to120

import (
	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.1.1 to v1.2.0: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if err := upgradeBackingImages(namespace, lhClient, resourceMaps); err != nil {
		return err
	}
	if err := upgradeVolumes(namespace, lhClient, resourceMaps); err != nil {
		return err
	}
	return nil
}

const (
	DeprecatedBackingImageStateDownloaded  = "downloaded"
	DeprecatedBackingImageStateDownloading = "downloading"
)

func upgradeBackingImages(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backing images failed")
	}()
	biMap, err := upgradeutil.ListAndUpdateBackingImagesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		return err
	}
	nodeMap, err := upgradeutil.ListAndUpdateNodesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		return err
	}
	for _, bi := range biMap {
		if bi.Status.DiskFileStatusMap == nil {
			bi.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{}
		}
		for diskUUID, state := range bi.Status.DiskDownloadStateMap {
			if _, exists := bi.Status.DiskFileStatusMap[diskUUID]; !exists {
				bi.Status.DiskFileStatusMap[diskUUID] = &longhorn.BackingImageDiskFileStatus{}
			}
			switch string(state) {
			case DeprecatedBackingImageStateDownloaded:
				bi.Status.DiskFileStatusMap[diskUUID].State = longhorn.BackingImageStateReady
			case DeprecatedBackingImageStateDownloading:
				bi.Status.DiskFileStatusMap[diskUUID].State = longhorn.BackingImageStateInProgress
			default:
				bi.Status.DiskFileStatusMap[diskUUID].State = longhorn.BackingImageState(state)
			}
		}
		bi.Status.DiskDownloadStateMap = map[string]longhorn.BackingImageDownloadState{}

		for diskUUID, progress := range bi.Status.DiskDownloadProgressMap {
			if _, exists := bi.Status.DiskFileStatusMap[diskUUID]; !exists {
				continue
			}
			bi.Status.DiskFileStatusMap[diskUUID].Progress = progress
		}
		bi.Status.DiskDownloadProgressMap = map[string]int{}

		if bi.Spec.ImageURL != "" || bi.Spec.SourceType == "" {
			bi, err := upgradeutil.GetBackingImageFromProvidedCache(namespace, lhClient, resourceMaps, bi.Name)
			if err != nil {
				return err
			}
			if err := checkAndCreateBackingImageDataSource(namespace, lhClient, bi, nodeMap, resourceMaps); err != nil {
				return err
			}
			bi.Spec.ImageURL = ""
			bi.Spec.SourceType = longhorn.BackingImageDataSourceTypeDownload
			bi.Spec.SourceParameters = map[string]string{longhorn.DataSourceTypeDownloadParameterURL: bi.Spec.ImageURL}
		}
	}
	return nil
}

func checkAndCreateBackingImageDataSource(namespace string, lhClient *lhclientset.Clientset, bi *longhorn.BackingImage, nodeMap map[string]*longhorn.Node, resourceMaps map[string]interface{}) (err error) {
	bids, err := upgradeutil.GetBackingImageDataSourceFromProvidedCache(namespace, lhClient, resourceMaps, bi.Name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		// Pick up an random disk in the backing image spec to create the backing image data source
		var availableNode, availableDiskUUID, availableDiskPath string
		for _, node := range nodeMap {
			for diskName, status := range node.Status.DiskStatus {
				spec, exists := node.Spec.Disks[diskName]
				if !exists {
					continue
				}
				if bi.Spec.Disks == nil {
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
			Spec: longhorn.BackingImageDataSourceSpec{
				NodeID:          availableNode,
				DiskUUID:        availableDiskUUID,
				DiskPath:        availableDiskPath,
				SourceType:      longhorn.BackingImageDataSourceTypeDownload,
				Parameters:      map[string]string{longhorn.DataSourceTypeDownloadParameterURL: bi.Spec.ImageURL},
				FileTransferred: true,
			},
		}
		// TODO
		if bids, err = upgradeutil.CreateAndUpdateBackingImageInProvidedCache(namespace, lhClient, resourceMaps, bids); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}

	// Then blindly set the state as ready
	bids.Status.OwnerID = bids.Spec.NodeID
	bids.Status.Size = bi.Status.Size
	bids.Status.Progress = 100

	return nil
}

func upgradeVolumes(namespace string, lhClient *lhclientset.Clientset, resources map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade volume failed")
	}()

	volumeMap, err := upgradeutil.ListAndUpdateVolumesInProvidedCache(namespace, lhClient, resources)
	if err != nil {
		return err
	}

	for _, volume := range volumeMap {
		if err := upgradeLabelsForVolume(volume, lhClient, namespace); err != nil {
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
		return errors.Wrapf(err, "cannot decode backup URL %s for volume %s", v.Spec.FromBackup, v.Name)
	}

	metadata, err := meta.Accessor(v)
	if err != nil {
		return err
	}

	labels := metadata.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	if _, exists := labels[types.LonghornLabelBackupVolume]; exists {
		return nil
	}
	labels[types.LonghornLabelBackupVolume] = backupVolumeName
	metadata.SetLabels(labels)

	return nil
}
