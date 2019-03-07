package engineapi

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/rancher/backupstore"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
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
	SpaceUsage     string
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
		volumes = append(volumes, &BackupVolume{
			Name:     name,
			Size:     v.Size,
			Created:  v.Created,
			Messages: v.Messages,
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
	return list[0], nil
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
	_, err := b.ExecuteEngineBinary("backup", "rm", backupURL)
	if err != nil {
		if strings.Contains(err.Error(), "msg=\"cannot find ") {
			logrus.Warnf("delete: could not find the backup: '%s'", backupURL)
			return nil
		}
		return errors.Wrapf(err, "error deleting backup")
	}
	return nil
}

func GetBackupURL(backupTarget, backupName, volName string) string {
	return fmt.Sprintf("%s?backup=%s&volume=%s", backupTarget, backupName, volName)
}
