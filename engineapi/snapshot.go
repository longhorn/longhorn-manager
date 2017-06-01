package engineapi

import (
	"encoding/json"
	"os/exec"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/yasker/lm-rewrite/util"
)

const (
	VolumeHeadName = "volume-head"
	purgeTimeout   = 15 * time.Minute
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
	data := map[string]*Snapshot{}
	if err := json.NewDecoder(stdout).Decode(&data); err != nil {
		return nil, errors.Wrapf(err, "error parsing data from cmd '%v'", cmd)
	}
	delete(data, VolumeHeadName)
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
	return nil
}
