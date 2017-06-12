package engineapi

import (
	"encoding/json"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/util"
)

type EngineCollection struct{}

type Engine struct {
	name string
	cURL string
}

func (c *EngineCollection) NewEngineClient(request *EngineClientRequest) (EngineClient, error) {
	return &Engine{
		name: request.VolumeName,
		cURL: request.ControllerURL,
	}, nil
}

func (e *Engine) Name() string {
	return e.name
}

func parseReplica(s string) (*Replica, error) {
	fields := strings.Fields(s)
	if len(fields) < 2 {
		return nil, errors.Errorf("cannot parse line `%s`", s)
	}
	url := fields[0]
	mode := ReplicaMode(fields[1])
	if mode != ReplicaModeRW && mode != ReplicaModeWO {
		mode = ReplicaModeERR
	}
	return &Replica{
		URL:  url,
		Mode: mode,
	}, nil
}

func (e *Engine) ReplicaList() (map[string]*Replica, error) {
	output, err := util.Execute("longhorn", "--url", e.cURL, "ls")
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
	if _, err := util.Execute("longhorn", "--url", e.cURL, "add", url); err != nil {
		return errors.Wrapf(err, "failed to add replica address='%s' to controller '%s'", url, e.name)
	}
	return nil
}

func (e *Engine) ReplicaRemove(url string) error {
	if err := ValidateReplicaURL(url); err != nil {
		return err
	}
	if _, err := util.Execute("longhorn", "--url", e.cURL, "rm", url); err != nil {
		return errors.Wrapf(err, "failed to rm replica address='%s' from controller '%s'", url, e.name)
	}
	return nil
}

func (e *Engine) Endpoint() string {
	info, err := e.info()
	if err != nil {
		logrus.Warn("Fail to get frontend info: ", err)
		return ""
	}

	return info.Endpoint
}

func (e *Engine) info() (*Volume, error) {
	output, err := util.Execute("longhorn", "--url", e.cURL, "info")
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
	if _, err := util.Execute("longhorn", "--url", e.cURL, "backup", "restore", backup); err != nil {
		return errors.Wrapf(err, "error restoring backup '%s'", backup)
	}
	logrus.Debugf("Backup %v restored for volume %v", backup, e.Name())
	return nil
}
