package manager

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
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
	return snap, nil
}

func (m *VolumeManager) DeleteSnapshot(snapshotName, volumeName string) error {
	if volumeName == "" || snapshotName == "" {
		return fmt.Errorf("volume and snapshot name required")
	}

	engine, err := m.GetEngineClient(volumeName)
	if err != nil {
		return err
	}
	return engine.SnapshotDelete(snapshotName)
}

func (m *VolumeManager) RevertSnapshot(snapshotName, volumeName string) error {
	if volumeName == "" || snapshotName == "" {
		return fmt.Errorf("volume and snapshot name required")
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
	return nil
}

func (m *VolumeManager) PurgeSnapshot(volumeName string) error {
	if volumeName == "" {
		return fmt.Errorf("volume name required")
	}

	engine, err := m.GetEngineClient(volumeName)
	if err != nil {
		return err
	}
	//TODO time consuming operation, move it out of API server path
	return engine.SnapshotPurge()
}

func (m *VolumeManager) BackupSnapshot(snapshotName string, labels map[string]string, volumeName string) error {
	if volumeName == "" || snapshotName == "" {
		return fmt.Errorf("volume and snapshot name required")
	}

	backupTarget, err := m.getBackupTargetURL()
	if err != nil {
		return err
	}
	credential, err := m.getBackupCredentialConfig()
	if err != nil {
		return err
	}
	engine, err := m.GetEngineClient(volumeName)
	if err != nil {
		return err
	}
	//TODO time consuming operation, move it out of API server path
	return engine.SnapshotBackup(snapshotName, backupTarget, nil, credential)
}

func (m *VolumeManager) GetEngineClient(volumeName string) (client engineapi.EngineClient, err error) {
	defer func() {
		err = errors.Wrapf(err, "cannot get client for volume %v", volumeName)
	}()
	e, err := m.ds.GetVolumeEngine(volumeName)
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, fmt.Errorf("cannot get engine for %v", volumeName)
	}
	if e.Status.CurrentState != types.InstanceStateRunning {
		return nil, fmt.Errorf("engine is not running")
	}
	engineCollection := &engineapi.EngineCollection{}
	return engineCollection.NewEngineClient(&engineapi.EngineClientRequest{
		VolumeName:        e.Spec.VolumeName,
		EngineImage:       e.Status.CurrentImage,
		ControllerURL:     engineapi.GetControllerDefaultURL(e.Status.IP),
		EngineLauncherURL: engineapi.GetEngineLauncherDefaultURL(e.Status.IP),
	})
}

func (m *VolumeManager) getBackupTarget() (*engineapi.BackupTarget, error) {
	targetURL, err := m.getBackupTargetURL()
	if err != nil {
		return nil, err
	}
	engineImage, err := m.GetDefaultEngineImage()
	if err != nil {
		return nil, err
	}
	credential, err := m.getBackupCredentialConfig()
	if err != nil {
		return nil, err
	}
	return engineapi.NewBackupTarget(targetURL, engineImage, credential), nil
}

func (m *VolumeManager) getBackupCredentialConfig() (map[string]string, error) {
	settings, err := m.ds.GetSetting()
	if err != nil || settings == nil {
		return nil, errors.New("cannot backup: unable to read settings")
	}
	backupType, err := util.CheckBackupType(settings.BackupTarget)
	if err != nil {
		return nil, err
	}
	if backupType == util.BackupStoreTypeS3 {
		secretName := settings.BackupTargetCredentialSecret
		if secretName == "" {
			return nil, errors.New("Could not backup for s3 without credential secret")
		}
		return m.ds.GetCredentialFromSecret(secretName)
	}
	return nil, nil
}

func (m *VolumeManager) ListBackupVolumes() ([]*engineapi.BackupVolume, error) {
	backupTarget, err := m.getBackupTarget()
	if err != nil {
		return nil, err
	}

	return backupTarget.ListVolumes()
}

func (m *VolumeManager) GetBackupVolume(volumeName string) (*engineapi.BackupVolume, error) {
	backupTarget, err := m.getBackupTarget()
	if err != nil {
		return nil, err
	}

	return backupTarget.GetVolume(volumeName)
}

func (m *VolumeManager) ListBackupsForVolume(volumeName string) ([]*engineapi.Backup, error) {
	backupTarget, err := m.getBackupTarget()
	if err != nil {
		return nil, err
	}

	return backupTarget.List(volumeName)
}

func (m *VolumeManager) GetBackup(backupName, volumeName string) (*engineapi.Backup, error) {
	backupTarget, err := m.getBackupTarget()
	if err != nil {
		return nil, err
	}

	url := engineapi.GetBackupURL(backupTarget.URL, backupName, volumeName)
	return backupTarget.GetBackup(url)
}

func (m *VolumeManager) DeleteBackup(backupName, volumeName string) error {
	backupTarget, err := m.getBackupTarget()
	if err != nil {
		return err
	}

	url := engineapi.GetBackupURL(backupTarget.URL, backupName, volumeName)
	return backupTarget.DeleteBackup(url)
}
