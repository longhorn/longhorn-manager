package engineapi

import (
	"bytes"
	"encoding/json"
	"io"
	"os/exec"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
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

func parseBackup(v interface{}) (*Backup, error) {
	backup := new(Backup)
	if err := mapstructure.Decode(v, backup); err != nil {
		return nil, errors.Wrapf(err, "Error parsing backup info %+v", v)
	}
	return backup, nil
}

func parseBackupsList(stdout io.Reader, volumeName string) ([]*Backup, error) {
	buffer := new(bytes.Buffer)
	reader := io.TeeReader(stdout, buffer)
	data := map[string]*backupVolume{}
	if err := json.NewDecoder(reader).Decode(&data); err != nil {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(buffer.String())), "cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error parsing BackupTarget: \n%s", buffer)
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

func parseBackupVolumesList(stdout io.Reader) ([]*BackupVolume, error) {
	buffer := new(bytes.Buffer)
	reader := io.TeeReader(stdout, buffer)
	data := map[string]*backupVolume{}
	if err := json.NewDecoder(reader).Decode(&data); err != nil {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(buffer.String())), "cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error parsing BackupTarget: \n%s", buffer)
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

func parseOneBackup(stdout io.Reader) (*Backup, error) {
	buffer := new(bytes.Buffer)
	reader := io.TeeReader(stdout, buffer)
	data := map[string]interface{}{}
	if err := json.NewDecoder(reader).Decode(&data); err != nil {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(buffer.String())), "cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error parsing BackupTarget: \n%s", buffer)
	}
	return parseBackup(data)
}

func (b *BackupTarget) ListVolumes() ([]*BackupVolume, error) {
	cmd := exec.Command("longhorn", "backup", "ls", "--volume-only", b.url)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Wrapf(err, "error getting stdout from cmd '%v'", cmd)
	}
	if err := cmd.Start(); err != nil {
		return nil, errors.Wrapf(err, "error starting cmd '%v'", cmd)
	}
	defer func() {
		if err := cmd.Wait(); err != nil {
			logrus.Debugf("error waiting for cmd '%v' %v", cmd, err)
		}
	}()
	return parseBackupVolumesList(stdout)
}

func (b *BackupTarget) GetVolume(volumeName string) (*BackupVolume, error) {
	cmd := exec.Command("longhorn", "backup", "ls", "--volume", volumeName, "--volume-only", b.url)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Wrapf(err, "error getting stdout from cmd '%v'", cmd)
	}
	if err := cmd.Start(); err != nil {
		return nil, errors.Wrapf(err, "error starting cmd '%v'", cmd)
	}
	defer func() {
		if err := cmd.Wait(); err != nil {
			logrus.Debugf("error waiting for cmd '%v' %v", cmd, err)
		}
	}()
	list, err := parseBackupVolumesList(stdout)
	if err != nil {
		return nil, err
	}
	return list[0], nil
}

func (b *BackupTarget) List(volumeName string) ([]*Backup, error) {
	if volumeName == "" {
		return nil, nil
	}
	cmd := exec.Command("longhorn", "backup", "ls", "--volume", volumeName, b.url)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Wrapf(err, "error getting stdout from cmd '%v'", cmd)
	}
	if err := cmd.Start(); err != nil {
		return nil, errors.Wrapf(err, "error starting cmd '%v'", cmd)
	}
	defer func() {
		if err := cmd.Wait(); err != nil {
			logrus.Debugf("error waiting for cmd '%v' %v", cmd, err)
		}
	}()
	return parseBackupsList(stdout, volumeName)
}

func GetBackup(url string) (*Backup, error) {
	cmd := exec.Command("longhorn", "backup", "inspect", url)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Wrapf(err, "error getting stdout from cmd '%v'", cmd)
	}
	if err := cmd.Start(); err != nil {
		return nil, errors.Wrapf(err, "error starting cmd '%v'", cmd)
	}
	defer func() {
		if err := cmd.Wait(); err != nil {
			logrus.Debugf("error waiting for cmd '%v' %v", cmd, err)
		}
	}()
	return parseOneBackup(stdout)
}

func DeleteBackup(url string) error {
	cmd := exec.Command("longhorn", "backup", "rm", url)
	errBuff := new(bytes.Buffer)
	cmd.Stderr = errBuff
	out, err := cmd.Output()
	if err != nil {
		s := string(out)
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(s)), "cannot find ") {
			logrus.Warnf("delete: could not find the backup: '%s'", url)
			return nil
		}
		return errors.Wrapf(err, "Error deleting backup: %s", errBuff)
	}
	return nil
}
