package engineapi

import (
	"encoding/json"
	"os/exec"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/util"
)

const (
	VolumeHeadName = "volume-head"
	purgeTimeout   = 15 * time.Minute
	backupTimeout  = 360 * time.Minute
)

func (e *Engine) SnapshotCreate(name string, labels map[string]string) (string, error) {
	args := []string{"--url", e.cURL, "snapshot", "create"}
	for k, v := range labels {
		args = append(args, "--label", k+"="+v)
	}
	args = append(args, name)

	output, err := util.Execute("longhorn", args...)
	if err != nil {
		return "", errors.Wrapf(err, "error creating snapshot '%s'", name)
	}
	return strings.TrimSpace(output), nil
}

func (e *Engine) SnapshotList() (map[string]*Snapshot, error) {
	cmd := exec.Command("longhorn", "--url", e.cURL, "snapshot", "info")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Wrapf(err, "error getting stdout from cmd '%v'", cmd)
	}
	if err := cmd.Start(); err != nil {
		return nil, errors.Wrapf(err, "error starting cmd '%v'", cmd)
	}
	defer func() {
		if err := cmd.Wait(); err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error waiting for cmd '%v'", cmd))
		}
	}()
	data := map[string]*SnapshotFromEngine{}
	if err := json.NewDecoder(stdout).Decode(&data); err != nil {
		return nil, errors.Wrapf(err, "error parsing data from cmd '%v'", cmd)
	}
	delete(data, VolumeHeadName)
	ret := map[string]*Snapshot{}
	for k, v := range data {
		ret[k] = &Snapshot{
			Name:        v.Name,
			Parent:      v.Parent,
			Removed:     v.Removed,
			UserCreated: v.UserCreated,
			Created:     v.Created,
			Size:        v.Size,
			Labels:      v.Labels,
			Children:    []string{},
		}
		for child := range v.Children {
			ret[k].Children = append(ret[k].Children, child)
		}
	}
	return ret, nil
}

func (e *Engine) SnapshotGet(name string) (*Snapshot, error) {
	data, err := e.SnapshotList()
	if err != nil {
		return nil, err
	}
	return data[name], nil
}

func (e *Engine) SnapshotDelete(name string) error {
	if _, err := util.Execute("longhorn", "--url", e.cURL,
		"snapshot", "rm", name); err != nil {
		return errors.Wrapf(err, "error deleting snapshot '%s'", name)
	}
	return nil
}

func (e *Engine) SnapshotRevert(name string) error {
	if _, err := util.Execute("longhorn", "--url", e.cURL,
		"snapshot", "revert", name); err != nil {
		return errors.Wrapf(err, "error reverting to snapshot '%s'", name)
	}
	return nil
}

func (e *Engine) SnapshotPurge() error {
	if _, err := util.ExecuteWithTimeout(purgeTimeout, "longhorn", "--url", e.cURL,
		"snapshot", "purge"); err != nil {
		return errors.Wrapf(err, "error purging snapshots")
	}
	logrus.Debugf("Volume %v snapshot purge completed", e.Name())
	return nil
}

func (e *Engine) SnapshotBackup(snapName, backupTarget string, labels map[string]string) error {
	snap, err := e.SnapshotGet(snapName)
	if err != nil {
		return errors.Wrapf(err, "error getting snapshot '%s', volume '%s'", snapName, e.name)
	}
	if snap == nil {
		return errors.Errorf("could not find snapshot '%s' to backup, volume '%s'", snapName, e.name)
	}
	args := []string{"--url", e.cURL, "backup", "create", "--dest", backupTarget}
	for k, v := range labels {
		args = append(args, "--label", k+"="+v)
	}
	args = append(args, snapName)
	backup, err := util.ExecuteWithTimeout(backupTimeout, "longhorn", args...)
	if err != nil {
		return err
	}
	logrus.Debugf("Backup %v created for volume %v snapshot %v", backup, e.Name(), snapName)
	return nil
}
