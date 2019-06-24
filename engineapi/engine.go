package engineapi

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

type EngineCollection struct{}

type Engine struct {
	name  string
	image string
	ip    string
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
		ip:    request.IP,
		cURL:  GetControllerDefaultURL(request.IP),
		lURL:  GetEngineLauncherDefaultURL(request.IP),
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

func (e *Engine) Endpoint() (string, error) {
	info, err := e.launcherInfo()
	if err != nil {
		logrus.Warn("Fail to get frontend info: ", err)
		return "", err
	}

	switch info.Frontend {
	case string(FrontendISCSI):
		// it will looks like this in the end
		// iscsi://10.42.0.12:3260/iqn.2014-09.com.rancher:vol-name/1
		return "iscsi://" + e.ip + ":" + DefaultISCSIPort + "/" + info.Endpoint + "/" + DefaultISCSILUN, nil
	case string(FrontendBlockDev):
		return info.Endpoint, nil
	case "":
		return "", nil
	}
	return "", fmt.Errorf("Unknown frontend %v", info.Frontend)
}

func (e *Engine) launcherInfo() (*LauncherVolumeInfo, error) {
	output, err := e.ExecuteEngineLauncherBinary("info")
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get launcher info")
	}

	info := &LauncherVolumeInfo{}
	if err := json.Unmarshal([]byte(output), info); err != nil {
		return nil, errors.Wrapf(err, "cannot decode launcher info: %v", output)
	}
	return info, nil
}

func (e *Engine) Info() (*Volume, error) {
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

func (e *Engine) Version(clientOnly bool) (*EngineVersion, error) {
	cmdline := []string{"version"}
	if clientOnly {
		cmdline = append(cmdline, "--client-only")
	} else {
		cmdline = append([]string{"--url", e.cURL}, cmdline...)
	}
	output, err := util.Execute(e.LonghornEngineBinary(), cmdline...)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get volume version")
	}

	version := &EngineVersion{}
	if err := json.Unmarshal([]byte(output), version); err != nil {
		return nil, errors.Wrapf(err, "cannot decode volume version: %v", output)
	}
	return version, nil
}

func (e *Engine) BackupRestoreIncrementally(backupTarget, backupName, backupVolume, lastRestored string) error {
	backup := GetBackupURL(backupTarget, backupName, backupVolume)

	_, err := e.ExecuteEngineBinaryWithTimeout(backupTimeout, "backup", "restore", backup,
		"--incrementally", "--last-restored", lastRestored)
	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) BackupRestoreStatus() ([]*types.RestoreStatus, error) {
	args := []string{"backup", "restore-status"}
	output, err := e.ExecuteEngineBinary(args...)
	if err != nil {
		return nil, err
	}
	restoreStatusList := make([]*types.RestoreStatus, 0)
	if err := json.Unmarshal([]byte(output), &restoreStatusList); err != nil {
		return nil, err
	}
	return restoreStatusList, nil
}
