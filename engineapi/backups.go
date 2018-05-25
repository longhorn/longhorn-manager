package engineapi

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/util"
)

type BackupTarget struct {
	url string
}

type backupVolume struct {
	Name           string
	Size           string
	Created        string
	LastBackupName string
	SpaceUsage     string
	Backups        map[string]interface{}
}

func NewBackupTarget(backupTarget string) *BackupTarget {
	return &BackupTarget{
		url: backupTarget,
	}
}

func ExecuteDefaultEngineBinary(args ...string) (string, error) {
	return util.Execute(LonghornEngineBinary, args...)
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
		return nil, errors.Wrapf(err, "error parsing BackupTarget: \n%s", output)
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

func parseBackupVolumesList(output string) ([]*BackupVolume, error) {
	data := map[string]*backupVolume{}
	if err := json.Unmarshal([]byte(output), &data); err != nil {
		return nil, errors.Wrapf(err, "error parsing BackupTarget: \n%s", output)
	}
	volumes := []*BackupVolume{}

	for name, v := range data {
		volumes = append(volumes, &BackupVolume{
			Name:    name,
			Size:    v.Size,
			Created: v.Created,
		})
	}

	return volumes, nil
}

func parseOneBackup(output string) (*Backup, error) {
	data := map[string]interface{}{}
	if err := json.Unmarshal([]byte(output), &data); err != nil {
		return nil, errors.Wrapf(err, "error parsing BackupTarget: \n%s", output)
	}
	return parseBackup(data)
}

func (b *BackupTarget) ListVolumes() ([]*BackupVolume, error) {
	output, err := ExecuteDefaultEngineBinary("backup", "ls", "--volume-only", b.url)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error listing backup volumes")
	}
	return parseBackupVolumesList(output)
}

func (b *BackupTarget) GetVolume(volumeName string) (*BackupVolume, error) {
	output, err := ExecuteDefaultEngineBinary("backup", "ls", "--volume", volumeName, "--volume-only", b.url)
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
	return list[0], nil
}

func (b *BackupTarget) List(volumeName string) ([]*Backup, error) {
	if volumeName == "" {
		return nil, nil
	}
	output, err := ExecuteDefaultEngineBinary("backup", "ls", "--volume", volumeName, b.url)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error listing backups")
	}
	return parseBackupsList(output, volumeName)
}

func GetBackup(url string) (*Backup, error) {
	output, err := ExecuteDefaultEngineBinary("backup", "inspect", url)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error getting backup")
	}
	return parseOneBackup(output)
}

func DeleteBackup(url string) error {
	_, err := ExecuteDefaultEngineBinary("backup", "rm", url)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			logrus.Warnf("delete: could not find the backup: '%s'", url)
			return nil
		}
		return errors.Wrapf(err, "error deleting backup")
	}
	return nil
}

func GetBackupURL(backupTarget, backupName, volName string) string {
	return fmt.Sprintf("%s?backup=%s&volume=%s", backupTarget, backupName, volName)
}
