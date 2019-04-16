package manager

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

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
		err = errors.Wrapf(err, "failed to updateVolumeLastBackup for %v", volumeName)
	}()

	backupVolume, err := backupTarget.GetVolume(volumeName)
	if err != nil {
		return err
	}
	for i := 0; i < ConflictRetryCount; i++ {
		v, err := getVolume(volumeName)
		if err != nil {
			return err
		}
		if v.Status.LastBackup == backupVolume.LastBackupName {
			return nil
		}
		v.Status.LastBackup = backupVolume.LastBackupName
		v.Status.LastBackupAt = backupVolume.LastBackupAt
		v, err = updateVolume(v)
		if err == nil {
			logrus.Debugf("Volume %v LastBackup updated to %v at %v",
				v.Name, v.Status.LastBackup, v.Status.LastBackupAt)
			return nil
		}
		if !apierrors.IsConflict(err) {
			return err
		}
		logrus.Debugf("Retrying updating LastBackup for volume %v due to conflict", v.Name)
	}
	return fmt.Errorf("Cannot update LastBackup for volume %v due to too many conflicts", volumeName)
}
