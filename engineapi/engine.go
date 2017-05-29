package engineapi

import (
	"encoding/json"
	"net"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/yasker/lm-rewrite/util"
)

type EngineCollection struct{}

type Engine struct {
	name string
	cURL string
}

func (c *EngineCollection) NewEngineClient(request *EngineClientRequest) (EngineClient, error) {
	return &Engine{
		name: request.VolumeName,
		cURL: request.ControllerAddr,
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
	ip := GetIPFromURL(fields[0])
	if validIP := net.ParseIP(ip); validIP == nil {
		return nil, errors.Errorf("invalid ip parsed %v", ip)
	}
	mode := ReplicaMode(fields[1])
	if mode != ReplicaModeRW && mode != ReplicaModeWO {
		mode = ReplicaModeERR
	}
	return &Replica{
		Address: ip,
		Mode:    mode,
	}, nil
}

func (e *Engine) GetReplicaStates() (map[string]*Replica, error) {
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
		replica, err := parseReplica(l)
		if err != nil {
			return nil, errors.Wrapf(err, "error parsing replica status from `%s`", l)
		}
		replicas[replica.Address] = replica
	}
	return replicas, nil
}

func (e *Engine) AddReplica(replicaIP string) error {
	rURL := GetReplicaURL(replicaIP)
	if _, err := util.Execute("longhorn", "--url", e.cURL, "add", rURL); err != nil {
		return errors.Wrapf(err, "failed to add replica address='%s' to controller '%s'", rURL, e.name)
	}
	return nil
}

func (e *Engine) RemoveReplica(replicaIP string) error {
	rURL := GetReplicaURL(replicaIP)
	if _, err := util.Execute("longhorn", "--url", e.cURL, "rm", rURL); err != nil {
		return errors.Wrapf(err, "failed to rm replica address='%s' from controller '%s'", rURL, e.name)
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
