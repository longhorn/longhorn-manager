package engineapi

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	imutil "github.com/longhorn/longhorn-engine/pkg/instance-manager/util"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

type EngineCollection struct{}

type Engine struct {
	name  string
	image string
	ip    string
	port  int
	cURL  string
}

func (c *EngineCollection) NewEngineClient(request *EngineClientRequest) (EngineClient, error) {
	if request.EngineImage == "" {
		return nil, fmt.Errorf("Invalid empty engine image from request")
	}
	if request.IP != "" && request.Port == 0 {
		return nil, fmt.Errorf("Invalid empty port from request with valid IP")
	}
	return &Engine{
		name:  request.VolumeName,
		image: request.EngineImage,
		ip:    request.IP,
		port:  request.Port,
		cURL:  imutil.GetURL(request.IP, request.Port),
	}, nil
}

func (e *Engine) Name() string {
	return e.name
}

func (e *Engine) LonghornEngineBinary() string {
	return filepath.Join(types.GetEngineBinaryDirectoryOnHostForImage(e.image), "longhorn")
}

func (e *Engine) ExecuteEngineBinary(args ...string) (string, error) {
	args = append([]string{"--url", e.cURL}, args...)
	return util.Execute(e.LonghornEngineBinary(), args...)
}

func (e *Engine) ExecuteEngineBinaryWithTimeout(timeout time.Duration, args ...string) (string, error) {
	args = append([]string{"--url", e.cURL}, args...)
	return util.ExecuteWithTimeout(timeout, e.LonghornEngineBinary(), args...)
}

func (e *Engine) ExecuteEngineBinaryWithoutTimeout(args ...string) (string, error) {
	args = append([]string{"--url", e.cURL}, args...)
	return util.ExecuteWithoutTimeout(e.LonghornEngineBinary(), args...)
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
	if _, err := e.ExecuteEngineBinaryWithoutTimeout("add", url); err != nil {
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

func (e *Engine) Expand(size int64) error {
	if _, err := e.ExecuteEngineBinary("expand", "--size", strconv.FormatInt(size, 10)); err != nil {
		return errors.Wrapf(err, "cannot get expand volume engine to size %v", size)
	}

	return nil
}
