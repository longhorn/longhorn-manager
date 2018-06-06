package engineapi

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

type EngineCollection struct{}

type Engine struct {
	name  string
	image string
	cURL  string
	lURL  string
}

const (
	rebuildTimeout = 180 * time.Minute
)

func (c *EngineCollection) NewEngineClient(request *EngineClientRequest) (EngineClient, error) {
	if request.EngineImage == "" {
		return nil, fmt.Errorf("Invalid empty engine image from request")
	}
	return &Engine{
		name:  request.VolumeName,
		image: request.EngineImage,
		cURL:  request.ControllerURL,
		lURL:  request.EngineLauncherURL,
	}, nil
}

func (e *Engine) Name() string {
	return e.name
}

func (e *Engine) LonghornEngineBinary() string {
	return filepath.Join(types.GetEngineBinaryDirectoryOnHostForImage(e.image), "longhorn")
}

func (e *Engine) LonghornEngineLauncherBinary() string {
	return filepath.Join(types.GetEngineBinaryDirectoryOnHostForImage(e.image), "longhorn-engine-launcher")
}

func (e *Engine) ExecuteEngineBinary(args ...string) (string, error) {
	args = append([]string{"--url", e.cURL}, args...)
	return util.Execute(e.LonghornEngineBinary(), args...)
}

func (e *Engine) ExecuteEngineBinaryWithTimeout(timeout time.Duration, args ...string) (string, error) {
	args = append([]string{"--url", e.cURL}, args...)
	return util.ExecuteWithTimeout(timeout, e.LonghornEngineBinary(), args...)
}

func (e *Engine) ExecuteEngineLauncherBinary(args ...string) (string, error) {
	args = append([]string{"--url", e.lURL}, args...)
	return util.Execute(e.LonghornEngineLauncherBinary(), args...)
}

func parseReplica(s string) (*Replica, error) {
	fields := strings.Fields(s)
	if len(fields) < 2 {
		return nil, errors.Errorf("cannot parse line `%s`", s)
	}
	url := fields[0]
	mode := types.ReplicaMode(fields[1])
	if mode != types.ReplicaModeRW && mode != types.ReplicaModeWO {
		mode = types.ReplicaModeERR
	}
	return &Replica{
		URL:  url,
		Mode: mode,
	}, nil
}

func (e *Engine) ReplicaList() (map[string]*Replica, error) {
	output, err := e.ExecuteEngineBinary("ls")
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list replicas from controller '%s'", e.name)
	}
	replicas := make(map[string]*Replica)
	lines := strings.Split(output, "\n")
	for _, l := range lines {
		if strings.HasPrefix(l, "ADDRESS") {
			continue
		}
		if l == "" {
			continue
		}
		replica, err := parseReplica(l)
		if err != nil {
			return nil, errors.Wrapf(err, "error parsing replica status from `%s`, output %v", l, output)
		}
		replicas[replica.URL] = replica
	}
	return replicas, nil
}

func (e *Engine) ReplicaAdd(url string) error {
	if err := ValidateReplicaURL(url); err != nil {
		return err
	}
	if _, err := e.ExecuteEngineBinaryWithTimeout(rebuildTimeout, "add", url); err != nil {
		return errors.Wrapf(err, "failed to add replica address='%s' to controller '%s'", url, e.name)
	}
	return nil
}

func (e *Engine) ReplicaRemove(url string) error {
	if err := ValidateReplicaURL(url); err != nil {
		return err
	}
	if _, err := e.ExecuteEngineBinary("rm", url); err != nil {
		return errors.Wrapf(err, "failed to rm replica address='%s' from controller '%s'", url, e.name)
	}
	return nil
}

func (e *Engine) Endpoint() string {
	info, err := e.launcherInfo()
	if err != nil {
		logrus.Warn("Fail to get frontend info: ", err)
		return ""
	}

	return info.Endpoint
}

func (e *Engine) launcherInfo() (*LauncherVolumeInfo, error) {
	output, err := e.ExecuteEngineLauncherBinary("info")
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get volume info")
	}

	info := &LauncherVolumeInfo{}
	if err := json.Unmarshal([]byte(output), info); err != nil {
		return nil, errors.Wrapf(err, "cannot decode volume info: %v", output)
	}
	return info, nil
}

func (e *Engine) info() (*Volume, error) {
	output, err := e.ExecuteEngineBinary("info")
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get volume info")
	}

	info := &Volume{}
	if err := json.Unmarshal([]byte(output), info); err != nil {
		return nil, errors.Wrapf(err, "cannot decode volume info: %v", output)
	}
	return info, nil
}

func (e *Engine) BackupRestore(backup string) error {
	if _, err := e.ExecuteEngineBinary("backup", "restore", backup); err != nil {
		return errors.Wrapf(err, "error restoring backup '%s'", backup)
	}
	logrus.Debugf("Backup %v restored for volume %v", backup, e.Name())
	return nil
}

func (e *Engine) Upgrade(binary string, replicaURLs []string) error {
	args := []string{
		"upgrade", "--longhorn-binary", binary,
	}
	for _, url := range replicaURLs {
		if err := ValidateReplicaURL(url); err != nil {
			return err
		}
		args = append(args, "--replica", url)
	}
	_, err := e.ExecuteEngineLauncherBinary(args...)
	if err != nil {
		return errors.Wrapf(err, "failed to upgrade Longhorn Engine")
	}
	return nil
}

func (e *Engine) Version() (*EngineVersion, error) {
	output, err := util.Execute(e.LonghornEngineBinary(), "version")
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get volume version")
	}

	version := &EngineVersion{}
	if err := json.Unmarshal([]byte(output), version); err != nil {
		return nil, errors.Wrapf(err, "cannot decode volume version: %v", output)
	}
	return version, nil
}
