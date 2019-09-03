package engineapi

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

type BackupTarget struct {
	URL        string
	Image      string
	Credential map[string]string
}

type backupVolume struct {
	Name           string
	Size           string
	Created        string
	LastBackupName string
	LastBackupAt   string
	DataStored     string
	Messages       map[backupstore.MessageType]string
	Backups        map[string]interface{}
}

func NewBackupTarget(backupTarget, engineImage string, credential map[string]string) *BackupTarget {
	return &BackupTarget{
		URL:        backupTarget,
		Image:      engineImage,
		Credential: credential,
	}
}

func (b *BackupTarget) LonghornEngineBinary() string {
	return filepath.Join(types.GetEngineBinaryDirectoryOnHostForImage(b.Image), "longhorn")
}

func (b *BackupTarget) ExecuteEngineBinary(args ...string) (string, error) {
	err := util.ConfigBackupCredential(b.URL, b.Credential)
	if err != nil {
		return "", err
	}
	return util.Execute(b.LonghornEngineBinary(), args...)
}

func (b *BackupTarget) ExecuteEngineBinaryWithoutTimeout(args ...string) (string, error) {
	err := util.ConfigBackupCredential(b.URL, b.Credential)
	if err != nil {
		return "", err
	}
	return util.ExecuteWithoutTimeout(b.LonghornEngineBinary(), args...)
}

func parseBackup(v interface{}) (*Backup, error) {
	backup := new(Backup)
	if err := mapstructure.Decode(v, backup); err != nil {
		return nil, errors.Wrapf(err, "Error parsing backup info %+v", v)
	}
	return backup, nil
}

func parseBackupsList(output, volumeName string) ([]*Backup, error) {
	data := map[string]*backupVolume{}
	if err := json.Unmarshal([]byte(output), &data); err != nil {
		return nil, errors.Wrapf(err, "error parsing BackupsList: \n%s", output)
	}
	BackupTarget := []*Backup{}
	volume := data[volumeName]
	for _, v := range data[volumeName].Backups {
		backup, err := parseBackup(v)
		if err != nil {
			return nil, err
		}
		backup.VolumeName = volume.Name
		backup.VolumeSize = volume.Size
		backup.VolumeCreated = volume.Created
		BackupTarget = append(BackupTarget, backup)
	}

	return BackupTarget, nil
}

func parseBackupVolumesList(output string) (map[string]*BackupVolume, error) {
	data := map[string]*backupVolume{}
	if err := json.Unmarshal([]byte(output), &data); err != nil {
		return nil, errors.Wrapf(err, "error parsing BackupVolumesList: \n%s", output)
	}
	volumes := map[string]*BackupVolume{}

	for name, v := range data {
		if v.Messages != nil {
			for mType, mContent := range v.Messages {
				if mType == backupstore.MessageTypeError {
					logrus.Errorf("message from backupVolume[%v], type[%v], content[%v]",
						name, mType, mContent)
				} else {
					logrus.Warnf("message from backupVolume[%v], type[%v], content[%v]",
						name, mType, mContent)
				}
			}
		}
		volumes[name] = &BackupVolume{
			Name:           name,
			Size:           v.Size,
			Created:        v.Created,
			LastBackupName: v.LastBackupName,
			LastBackupAt:   v.LastBackupAt,
			DataStored:     v.DataStored,
			Messages:       v.Messages,
		}
	}

	return volumes, nil
}

func parseOneBackup(output string) (*Backup, error) {
	data := map[string]interface{}{}
	if err := json.Unmarshal([]byte(output), &data); err != nil {
		return nil, errors.Wrapf(err, "error parsing one backup: \n%s", output)
	}
	return parseBackup(data)
}

func (b *BackupTarget) ListVolumes() (map[string]*BackupVolume, error) {
	output, err := b.ExecuteEngineBinary("backup", "ls", "--volume-only", b.URL)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error listing backup volumes")
	}
	return parseBackupVolumesList(output)
}

func (b *BackupTarget) GetVolume(volumeName string) (*BackupVolume, error) {
	output, err := b.ExecuteEngineBinary("backup", "ls", "--volume", volumeName, "--volume-only", b.URL)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error getting backup volume")
	}
	list, err := parseBackupVolumesList(output)
	if err != nil {
		return nil, errors.Wrapf(err, "error getting backup volume")
	}
	return list[volumeName], nil
}

func (b *BackupTarget) DeleteVolume(volumeName string) error {
	_, err := b.ExecuteEngineBinaryWithoutTimeout("backup", "rm", "--volume", volumeName, b.URL)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			logrus.Warnf("delete: could not find the backup volume: '%s'", volumeName)
			return nil
		}
		return errors.Wrapf(err, "error deleting backup volume")
	}
	return nil
}
func (b *BackupTarget) List(volumeName string) ([]*Backup, error) {
	if volumeName == "" {
		return nil, nil
	}
	output, err := b.ExecuteEngineBinary("backup", "ls", "--volume", volumeName, b.URL)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error listing backups")
	}
	return parseBackupsList(output, volumeName)
}

func (b *BackupTarget) GetBackup(backupURL string) (*Backup, error) {
	output, err := b.ExecuteEngineBinary("backup", "inspect", backupURL)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error getting backup")
	}
	return parseOneBackup(output)
}

func (b *BackupTarget) DeleteBackup(backupURL string) error {
	logrus.Infof("Start Deleting backup %s", backupURL)
	_, err := b.ExecuteEngineBinaryWithoutTimeout("backup", "rm", backupURL)
	if err != nil {
		if types.ErrorIsNotFound(err) {
			logrus.Warnf("delete: could not find the backup: '%s'", backupURL)
			return nil
		}
		return errors.Wrapf(err, "error deleting backup %v", backupURL)
	}
	logrus.Infof("Complete deleting backup %s", backupURL)
	return nil
}

func GetBackupURL(backupTarget, backupName, volName string) string {
	return fmt.Sprintf("%s?backup=%s&volume=%s", backupTarget, backupName, volName)
}

func (e *Engine) SnapshotBackup(snapName, backupTarget string, labels map[string]string, credential map[string]string) (string, error) {
	if snapName == VolumeHeadName {
		return "", fmt.Errorf("invalid operation: cannot backup %v", VolumeHeadName)
	}
	snap, err := e.SnapshotGet(snapName)
	if err != nil {
		return "", errors.Wrapf(err, "error getting snapshot '%s', volume '%s'", snapName, e.name)
	}
	if snap == nil {
		return "", errors.Errorf("could not find snapshot '%s' to backup, volume '%s'", snapName, e.name)
	}
	args := []string{"backup", "create", "--dest", backupTarget}
	for k, v := range labels {
		args = append(args, "--label", k+"="+v)
	}
	args = append(args, snapName)
	// set credential if backup for s3
	err = util.ConfigBackupCredential(backupTarget, credential)
	if err != nil {
		return "", err
	}
	backup, err := e.ExecuteEngineBinaryWithoutTimeout(args...)
	if err != nil {
		return "", err
	}
	logrus.Debugf("Backup %v created for volume %v snapshot %v", backup, e.Name(), snapName)
	return strings.Trim(backup, "\n"), nil
}

func (e *Engine) SnapshotBackupStatus() (map[string]*types.BackupStatus, error) {
	args := []string{"backup", "status"}
	output, err := e.ExecuteEngineBinary(args...)
	if err != nil {
		return nil, err
	}
	backups := make(map[string]*types.BackupStatus, 0)
	if err := json.Unmarshal([]byte(output), &backups); err != nil {
		return nil, err
	}
	return backups, nil
}

func (e *Engine) BackupRestore(backupTarget, backupName, backupVolume, lastRestored string, credential map[string]string) error {
	backup := GetBackupURL(backupTarget, backupName, backupVolume)

	// set credential if backup for s3
	if err := util.ConfigBackupCredential(backupTarget, credential); err != nil {
		return err
	}

	args := []string{"backup", "restore", backup}
	if lastRestored != "" {
		args = append(args, "--incrementally", "--last-restored", lastRestored)
	}
	if _, err := e.ExecuteEngineBinaryWithoutTimeout(args...); err != nil {
		return errors.Wrapf(err, "error restoring backup '%s'", backup)
	}

	logrus.Debugf("Backup %v restored for volume %v", backup, e.Name())
	return nil
}

func (e *Engine) BackupRestoreStatus() (map[string]*types.RestoreStatus, error) {
	args := []string{"backup", "restore-status"}
	output, err := e.ExecuteEngineBinary(args...)
	if err != nil {
		return nil, err
	}
	replicaStatusMap := make(map[string]*types.RestoreStatus)
	if err := json.Unmarshal([]byte(output), &replicaStatusMap); err != nil {
		return nil, err
	}
	return replicaStatusMap, nil
}
