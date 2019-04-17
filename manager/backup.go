package manager

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/engineapi"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

var (
	ConflictRetryCount = 5
)

func UpdateVolumeLastBackup(volumeName string, backupTarget *engineapi.BackupTarget,
	getVolume func(name string) (*longhorn.Volume, error),
	updateVolume func(v *longhorn.Volume) (*longhorn.Volume, error)) (err error) {

	defer func() {
		err = errors.Wrapf(err, "failed to UpdateVolumeLastBackup for %v", volumeName)
	}()

	backupVolume, err := backupTarget.GetVolume(volumeName)
	if err != nil {
		return err
	}
	return SyncVolumeLastBackupWithBackupVolume(volumeName, backupVolume, getVolume, updateVolume)
}

func SyncVolumeLastBackupWithBackupVolume(volumeName string, backupVolume *engineapi.BackupVolume,
	getVolume func(name string) (*longhorn.Volume, error),
	updateVolume func(v *longhorn.Volume) (*longhorn.Volume, error)) (err error) {

	defer func() {
		err = errors.Wrapf(err, "failed to SyncVolumeWithBackupVolume for %v", volumeName)
	}()

	lastBackup := ""
	lastBackupAt := ""
	if backupVolume != nil {
		lastBackup = backupVolume.LastBackupName
		lastBackupAt = backupVolume.LastBackupAt
	}
	for i := 0; i < ConflictRetryCount; i++ {
		v, err := getVolume(volumeName)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				return nil
			}
			return err
		}
		if v.Status.LastBackup == lastBackup {
			return nil
		}
		v.Status.LastBackup = lastBackup
		v.Status.LastBackupAt = lastBackupAt
		v, err = updateVolume(v)
		if err == nil {
			logrus.Debugf("Volume %v LastBackup updated to %v at %v",
				v.Name, v.Status.LastBackup, v.Status.LastBackupAt)
			return nil
		}
		if !datastore.ErrorIsConflict(err) {
			return err
		}
		logrus.Debugf("Retrying updating LastBackup for volume %v due to conflict", v.Name)
	}
	return fmt.Errorf("Cannot update LastBackup for volume %v due to too many conflicts", volumeName)
}

func SyncVolumesLastBackupWithBackupVolumes(backupVolumes []*engineapi.BackupVolume,
	getVolume func(name string) (*longhorn.Volume, error),
	updateVolume func(v *longhorn.Volume) (*longhorn.Volume, error)) {
	for _, bv := range backupVolumes {
		if err := SyncVolumeLastBackupWithBackupVolume(bv.Name, bv, getVolume, updateVolume); err != nil {
			logrus.Errorf("backup store monitor: failed to update last backup for %+v: %v", bv, err)
		}
	}
}
