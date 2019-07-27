package engineapi

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

const (
	VolumeHeadName = "volume-head"
)

func (e *Engine) SnapshotCreate(name string, labels map[string]string) (string, error) {
	args := []string{"snapshot", "create"}
	for k, v := range labels {
		args = append(args, "--label", k+"="+v)
	}
	args = append(args, name)

	output, err := e.ExecuteEngineBinary(args...)
	if err != nil {
		return "", errors.Wrapf(err, "error creating snapshot '%s'", name)
	}
	return strings.TrimSpace(output), nil
}

func (e *Engine) SnapshotList() (map[string]*Snapshot, error) {
	output, err := e.ExecuteEngineBinary("snapshot", "info")
	if err != nil {
		return nil, errors.Wrapf(err, "error listing snapshot")
	}
	data := map[string]*Snapshot{}
	if err := json.Unmarshal([]byte(output), &data); err != nil {
		return nil, errors.Wrapf(err, "error parsing snapshot list")
	}
	return data, nil
}

func (e *Engine) SnapshotGet(name string) (*Snapshot, error) {
	data, err := e.SnapshotList()
	if err != nil {
		return nil, err
	}
	return data[name], nil
}

func (e *Engine) SnapshotDelete(name string) error {
	if name == VolumeHeadName {
		return fmt.Errorf("invalid operation: cannot remove %v", VolumeHeadName)
	}
	if _, err := e.ExecuteEngineBinary("snapshot", "rm", name); err != nil {
		return errors.Wrapf(err, "error deleting snapshot '%s'", name)
	}
	return nil
}

func (e *Engine) SnapshotRevert(name string) error {
	if name == VolumeHeadName {
		return fmt.Errorf("invalid operation: cannot revert to %v", VolumeHeadName)
	}
	if _, err := e.ExecuteEngineBinary("snapshot", "revert", name); err != nil {
		return errors.Wrapf(err, "error reverting to snapshot '%s'", name)
	}
	return nil
}

func (e *Engine) SnapshotPurge() error {
	if _, err := e.ExecuteEngineBinaryWithoutTimeout("snapshot", "purge"); err != nil {
		return errors.Wrapf(err, "error purging snapshots")
	}
	logrus.Debugf("Volume %v snapshot purge completed", e.Name())
	return nil
}

func (e *Engine) SnapshotBackup(snapName, backupTarget string, labels map[string]string, credential map[string]string) error {
	if snapName == VolumeHeadName {
		return fmt.Errorf("invalid operation: cannot backup %v", VolumeHeadName)
	}
	snap, err := e.SnapshotGet(snapName)
	if err != nil {
		return errors.Wrapf(err, "error getting snapshot '%s', volume '%s'", snapName, e.name)
	}
	if snap == nil {
		return errors.Errorf("could not find snapshot '%s' to backup, volume '%s'", snapName, e.name)
	}
	args := []string{"backup", "create", "--dest", backupTarget}
	for k, v := range labels {
		args = append(args, "--label", k+"="+v)
	}
	args = append(args, snapName)
	// set credential if backup for s3
	err = util.ConfigBackupCredential(backupTarget, credential)
	if err != nil {
		return err
	}
	backup, err := e.ExecuteEngineBinaryWithoutTimeout(args...)
	if err != nil {
		return err
	}
	logrus.Debugf("Backup %v created for volume %v snapshot %v", backup, e.Name(), snapName)
	return nil
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
