package manager

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

const (
	BackupStatusQueryInterval = 2 * time.Second
)

func (m *VolumeManager) ListSnapshots(volumeName string) (map[string]*engineapi.Snapshot, error) {
	if volumeName == "" {
		return nil, fmt.Errorf("volume name required")
	}
	engine, err := m.GetEngineClient(volumeName)
	if err != nil {
		return nil, err
	}
	return engine.SnapshotList()
}

func (m *VolumeManager) GetSnapshot(snapshotName, volumeName string) (*engineapi.Snapshot, error) {
	if volumeName == "" || snapshotName == "" {
		return nil, fmt.Errorf("volume and snapshot name required")
	}
	engine, err := m.GetEngineClient(volumeName)
	if err != nil {
		return nil, err
	}
	snapshot, err := engine.SnapshotGet(snapshotName)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, fmt.Errorf("cannot find snapshot '%s' for volume '%s'", snapshotName, volumeName)
	}
	return snapshot, nil
}

func (m *VolumeManager) CreateSnapshot(snapshotName string, labels map[string]string, volumeName string) (*engineapi.Snapshot, error) {
	if volumeName == "" {
		return nil, fmt.Errorf("volume name required")
	}

	for k, v := range labels {
		if strings.Contains(k, "=") || strings.Contains(v, "=") {
			return nil, fmt.Errorf("labels cannot contain '='")
		}
	}

	if err := m.checkVolumeNotInMigration(volumeName); err != nil {
		return nil, err
	}
	engine, err := m.GetEngineClient(volumeName)
	if err != nil {
		return nil, err
	}
	snapshotName, err = engine.SnapshotCreate(snapshotName, labels)
	if err != nil {
		return nil, err
	}
	snap, err := engine.SnapshotGet(snapshotName)
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return nil, fmt.Errorf("cannot found just created snapshot '%s', for volume '%s'", snapshotName, volumeName)
	}
	logrus.Debugf("Created snapshot %v with labels %+v for volume %v", snapshotName, labels, volumeName)
	return snap, nil
}

func (m *VolumeManager) DeleteSnapshot(snapshotName, volumeName string) error {
	if volumeName == "" || snapshotName == "" {
		return fmt.Errorf("volume and snapshot name required")
	}

	if err := m.checkVolumeNotInMigration(volumeName); err != nil {
		return err
	}
	engine, err := m.GetEngineClient(volumeName)
	if err != nil {
		return err
	}
	if err := engine.SnapshotDelete(snapshotName); err != nil {
		return err
	}
	logrus.Debugf("Deleted snapshot %v for volume %v", snapshotName, volumeName)
	return nil
}

func (m *VolumeManager) RevertSnapshot(snapshotName, volumeName string) error {
	if volumeName == "" || snapshotName == "" {
		return fmt.Errorf("volume and snapshot name required")
	}

	if err := m.checkVolumeNotInMigration(volumeName); err != nil {
		return err
	}
	engine, err := m.GetEngineClient(volumeName)
	if err != nil {
		return err
	}
	if err := engine.SnapshotRevert(snapshotName); err != nil {
		return err
	}
	snapshot, err := engine.SnapshotGet(snapshotName)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return fmt.Errorf("not found snapshot '%s', for volume '%s'", snapshotName, volumeName)
	}
	logrus.Debugf("Revert to snapshot %v for volume %v", snapshotName, volumeName)
	return nil
}

func (m *VolumeManager) PurgeSnapshot(volumeName string) error {
	if volumeName == "" {
		return fmt.Errorf("volume name required")
	}

	if err := m.checkVolumeNotInMigration(volumeName); err != nil {
		return err
	}
	engine, err := m.GetEngineClient(volumeName)
	if err != nil {
		return err
	}

	if err := engine.SnapshotPurge(); err != nil {
		return err
	}
	logrus.Debugf("Started snapshot purge for volume %v", volumeName)
	return nil
}

func (m *VolumeManager) BackupSnapshot(snapshotName string, labels map[string]string, volumeName string) error {
	if volumeName == "" || snapshotName == "" {
		return fmt.Errorf("volume and snapshot name required")
	}

	if err := m.checkVolumeNotInMigration(volumeName); err != nil {
		return err
	}
	backupTarget, err := m.GetSettingValueExisted(types.SettingNameBackupTarget)
	if err != nil {
		return err
	}
	credential, err := GetBackupCredentialConfig(m.ds)
	if err != nil {
		return err
	}
	engine, err := m.GetEngineClient(volumeName)
	if err != nil {
		return err
	}
	go func() {
		if _, err := engine.SnapshotBackup(snapshotName, backupTarget, labels, credential); err != nil {
			logrus.Errorf("Failed to backup snapshot %v with label %v for volume %v: %v", snapshotName, labels, volumeName, err)
		}
		logrus.Debugf("Backup snapshot %v with label %v for volume %v", snapshotName, labels, volumeName)

		target, err := GenerateBackupTarget(m.ds)
		if err != nil {
			logrus.Warnf("Failed to update volume LastBackup for %v due to cannot get backup target: %v", volumeName, err)
		}

		bks := &types.BackupStatus{}
		for {
			engines, err := m.ds.ListVolumeEngines(volumeName)
			if err != nil {
				logrus.Errorf("fail to get engines for volume %v", volumeName)
				return
			}

			for _, e := range engines {
				backupStatusList := e.Status.BackupStatus
				for _, b := range backupStatusList {
					if b.SnapshotName == snapshotName {
						bks = b
						break
					}
				}
			}
			if bks.Error != "" {
				logrus.Errorf("Failed to updated volume LastBackup for %v due to backup error %v", volumeName, bks.Error)
				break
			}
			if bks.Progress == 100 {
				break
			}
			time.Sleep(BackupStatusQueryInterval)
		}

		if err := UpdateVolumeLastBackup(volumeName, target, m.ds.GetVolume, m.ds.UpdateVolume); err != nil {
			logrus.Warnf("Failed to update volume LastBackup for %v: %v", volumeName, err)
		}
	}()
	return nil
}

func (m *VolumeManager) GetEngineClient(volumeName string) (client engineapi.EngineClient, err error) {
	var e *longhorn.Engine

	defer func() {
		err = errors.Wrapf(err, "cannot get client for volume %v", volumeName)
	}()
	es, err := m.ds.ListVolumeEngines(volumeName)
	if err != nil {
		return nil, err
	}
	if len(es) == 0 {
		return nil, fmt.Errorf("cannot fine engine")
	}
	if len(es) != 1 {
		return nil, fmt.Errorf("more than one engine exists")
	}
	for _, e = range es {
		break
	}
	if e.Status.CurrentState != types.InstanceStateRunning {
		return nil, fmt.Errorf("engine is not running")
	}
	if err := m.CheckEngineImageReadiness(e.Status.CurrentImage); err != nil {
		return nil, errors.Wrapf(err, "cannot get engine client with image %v", e.Status.CurrentImage)
	}

	engineCollection := &engineapi.EngineCollection{}
	return engineCollection.NewEngineClient(&engineapi.EngineClientRequest{
		VolumeName:  e.Spec.VolumeName,
		EngineImage: e.Status.CurrentImage,
		IP:          e.Status.IP,
		Port:        e.Status.Port,
	})
}

func (m *VolumeManager) ListBackupVolumes() (map[string]*engineapi.BackupVolume, error) {
	backupTarget, err := GenerateBackupTarget(m.ds)
	if err != nil {
		return nil, err
	}

	backupVolumes, err := backupTarget.ListVolumes()
	if err != nil {
		return nil, err
	}
	// side effect, update known volumes
	SyncVolumesLastBackupWithBackupVolumes(backupVolumes, m.ds.ListVolumes, m.ds.GetVolume, m.ds.UpdateVolume)
	return backupVolumes, nil
}

func (m *VolumeManager) GetBackupVolume(volumeName string) (*engineapi.BackupVolume, error) {
	backupTarget, err := GenerateBackupTarget(m.ds)
	if err != nil {
		return nil, err
	}
	bv, err := backupTarget.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}
	// side effect, update known volumes
	SyncVolumeLastBackupWithBackupVolume(volumeName, bv, m.ds.GetVolume, m.ds.UpdateVolume)
	return bv, nil
}

func (m *VolumeManager) DeleteBackupVolume(volumeName string) error {
	backupTarget, err := GenerateBackupTarget(m.ds)
	if err != nil {
		return err
	}
	if err := backupTarget.DeleteVolume(volumeName); err != nil {
		return err
	}
	logrus.Debugf("Deleted backup volume %v", volumeName)
	return nil
}

func (m *VolumeManager) ListBackupsForVolume(volumeName string) ([]*engineapi.Backup, error) {
	backupTarget, err := GenerateBackupTarget(m.ds)
	if err != nil {
		return nil, err
	}

	return backupTarget.List(volumeName)
}

func (m *VolumeManager) GetBackup(backupName, volumeName string) (*engineapi.Backup, error) {
	backupTarget, err := GenerateBackupTarget(m.ds)
	if err != nil {
		return nil, err
	}

	url := engineapi.GetBackupURL(backupTarget.URL, backupName, volumeName)
	return backupTarget.GetBackup(url)
}

func (m *VolumeManager) DeleteBackup(backupName, volumeName string) error {
	backupTarget, err := GenerateBackupTarget(m.ds)
	if err != nil {
		return err
	}

	go func() {
		url := engineapi.GetBackupURL(backupTarget.URL, backupName, volumeName)
		if err := backupTarget.DeleteBackup(url); err != nil {
			logrus.Error(err)
			return
		}
		if err := UpdateVolumeLastBackup(volumeName, backupTarget, m.ds.GetVolume, m.ds.UpdateVolume); err != nil {
			logrus.Warnf("Failed to update volume LastBackup for %v for backup deletion: %v", volumeName, err)
		}
	}()
	return nil
}
