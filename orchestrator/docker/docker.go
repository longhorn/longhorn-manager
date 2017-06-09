package docker

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	dTypes "github.com/docker/docker/api/types"
	dContainer "github.com/docker/docker/api/types/container"
	dCli "github.com/docker/docker/client"

	"github.com/yasker/lm-rewrite/orchestrator"
	"github.com/yasker/lm-rewrite/types"
	"github.com/yasker/lm-rewrite/util"
)

const (
	cfgDirectory = "/var/lib/rancher/longhorn/"
	hostUUIDFile = cfgDirectory + ".physical_host_uuid"
)

var (
	ContainerStopTimeout = 1 * time.Minute
	WaitDeviceTimeout    = 30 //seconds
	WaitAPITimeout       = 30 //seconds

	GraceStopTimeout = 100 * time.Millisecond
)

type Docker struct {
	EngineImage string
	Network     string
	IP          string

	currentNode *types.NodeInfo

	cli *dCli.Client
}

type Config struct {
	EngineImage string
	Network     string
}

func NewDockerOrchestrator(cfg *Config) (*Docker, error) {
	var err error

	if cfg.EngineImage == "" {
		return nil, fmt.Errorf("missing required parameter EngineImage")
	}
	docker := &Docker{
		EngineImage: cfg.EngineImage,
	}

	os.Setenv("DOCKER_API_VERSION", "1.24")
	docker.cli, err = dCli.NewEnvClient()
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to docker")
	}

	if _, err := docker.cli.ContainerList(context.Background(), dTypes.ContainerListOptions{}); err != nil {
		return nil, errors.Wrap(err, "cannot pass test to get container list")
	}

	if err = docker.updateNetwork(cfg.Network); err != nil {
		return nil, errors.Wrapf(err, "fail to detect dedicated container network: %v", cfg.Network)
	}

	logrus.Infof("Detected network is %s, IP is %s", docker.Network, docker.IP)

	if err := docker.updateCurrentNode(); err != nil {
		return nil, err
	}

	logrus.Info("Docker orchestrator is ready")
	return docker, nil
}

func (d *Docker) updateNetwork(userSpecifiedNetwork string) error {
	containerID := os.Getenv("HOSTNAME")

	inspectJSON, err := d.cli.ContainerInspect(context.Background(), containerID)
	if err != nil {
		return errors.Errorf("cannot find manager container, may not be running inside container")
	}
	networks := inspectJSON.NetworkSettings.Networks
	if len(networks) == 0 {
		return errors.Errorf("cannot find manager container's network")
	}
	if userSpecifiedNetwork != "" {
		net := networks[userSpecifiedNetwork]
		if net == nil {
			return errors.Errorf("user specified network %v doesn't exist", userSpecifiedNetwork)
		}
		d.Network = userSpecifiedNetwork
		d.IP = net.IPAddress
		return nil
	}
	if len(networks) > 1 {
		return errors.Errorf("found multiple networks for container %v, "+
			"unable to decide which one to use, "+
			"please specify: %+v", containerID, networks)
	}
	// only one entry here
	for k, v := range networks {
		d.Network = k
		d.IP = v.IPAddress
	}
	return nil
}

func (d *Docker) updateCurrentNode() error {
	var err error

	node := &types.NodeInfo{
		IP:               d.IP,
		OrchestratorPort: types.DefaultOrchestratorPort,
	}
	node.Name, err = os.Hostname()
	if err != nil {
		return err
	}

	uuid, err := ioutil.ReadFile(hostUUIDFile)
	if err == nil {
		node.ID = string(uuid)
		d.currentNode = node
		return nil
	}

	// file doesn't exists, generate new UUID for the host
	node.ID = util.UUID()
	if err := os.MkdirAll(cfgDirectory, os.ModeDir|0600); err != nil {
		return fmt.Errorf("Fail to create configuration directory: %v", err)
	}
	if err := ioutil.WriteFile(hostUUIDFile, []byte(node.ID), 0600); err != nil {
		return fmt.Errorf("Fail to write host uuid file: %v", err)
	}
	d.currentNode = node
	return nil
}

func (d *Docker) GetCurrentNode() *types.NodeInfo {
	return d.currentNode
}

func (d *Docker) CreateController(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to create controller for %v", req.VolumeName)
	}()
	if err := orchestrator.ValidateRequestCreateController(req); err != nil {
		return nil, err
	}
	if req.NodeID != d.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			d.currentNode.ID)
	}

	cmd := []string{
		"launch", "controller",
		"--listen", "0.0.0.0:9501",
		"--frontend", "tgt",
	}
	for _, url := range req.ReplicaURLs {
		waitURL := strings.Replace(url, "tcp://", "http://", 1) + "/v1"
		if err := util.WaitForAPI(waitURL, WaitAPITimeout); err != nil {
			return nil, err
		}
		cmd = append(cmd, "--replica", url)
	}
	cmd = append(cmd, req.VolumeName)

	createBody, err := d.cli.ContainerCreate(context.Background(),
		&dContainer.Config{
			Image: d.EngineImage,
			Cmd:   cmd,
		},
		&dContainer.HostConfig{
			Binds: []string{
				"/dev:/host/dev",
				"/proc:/host/proc",
			},
			Privileged:  true,
			NetworkMode: dContainer.NetworkMode(d.Network),
		}, nil, req.InstanceName)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			logrus.Errorf("fail to start controller %v of %v, cleaning up: %v",
				req.InstanceName, req.VolumeName, err)
			d.StopInstance(req)
			d.DeleteInstance(req)
		}
	}()

	req.InstanceID = createBody.ID
	instance, err = d.StartInstance(req)
	if err != nil {
		return nil, err
	}

	url := "http://" + instance.IP + ":9501/v1"
	if err := util.WaitForAPI(url, WaitAPITimeout); err != nil {
		return nil, err
	}

	if err := util.WaitForDevice(d.getDeviceName(req.VolumeName), WaitDeviceTimeout); err != nil {
		return nil, err
	}

	return instance, nil
}

func (d *Docker) getDeviceName(volumeName string) string {
	return filepath.Join("/dev/longhorn/", volumeName)
}

func (d *Docker) CreateReplica(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to create replica %v for %v",
			req.InstanceName, req.VolumeName)
	}()

	if err := orchestrator.ValidateRequestCreateReplica(req); err != nil {
		return nil, err
	}
	if req.NodeID != d.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			d.currentNode.ID)
	}

	cmd := []string{
		"launch", "replica",
		"--listen", "0.0.0.0:9502",
		"--size", strconv.FormatInt(req.VolumeSize, 10),
	}
	if req.RestoreFrom != "" && req.RestoreName != "" {
		cmd = append(cmd, "--restore-from", req.RestoreFrom, "--restore-name", req.RestoreName)
	}
	cmd = append(cmd, "/volume")
	createBody, err := d.cli.ContainerCreate(context.Background(),
		&dContainer.Config{
			Image: d.EngineImage,
			Volumes: map[string]struct{}{
				"/volume": {},
			},
			Cmd: cmd,
		},
		&dContainer.HostConfig{
			Privileged:  true,
			NetworkMode: dContainer.NetworkMode(d.Network),
		}, nil, req.InstanceName)
	if err != nil {
		return nil, err
	}

	req.InstanceID = createBody.ID

	defer func() {
		if err != nil {
			d.StopInstance(req)
			d.DeleteInstance(req)
		}
	}()

	instance, err = d.InspectInstance(req)
	if err != nil {
		logrus.Errorf("fail to inspect when create replica %v of %v, cleaning up: %v", req.InstanceName, req.VolumeName, err)
		return nil, err
	}

	// make sure replica is initialized, especially for restoring backup
	instance, err = d.StartInstance(req)
	if err != nil {
		logrus.Errorf("fail to start when create replica %v of %v, cleaning up: %v", req.InstanceName, req.VolumeName, err)
		return nil, err
	}

	timeout := WaitAPITimeout
	// More time for backup restore, may need to customerize it
	if req.RestoreFrom != "" && req.RestoreName != "" {
		timeout = timeout * 10
	}
	url := "http://" + instance.IP + ":9502/v1"
	if err := util.WaitForAPI(url, timeout); err != nil {
		return nil, err
	}

	instance, err = d.StopInstance(req)
	if err != nil {
		logrus.Errorf("fail to stop when create replica %v of %v, cleaning up: %v", req.InstanceName, req.VolumeName, err)
		return nil, err
	}

	return instance, nil
}

func (d *Docker) InspectInstance(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to inspect instance %v(%v)", req.InstanceName, req.InstanceID)
	}()

	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return nil, err
	}
	if req.NodeID != d.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			d.currentNode.ID)
	}

	inspectJSON, err := d.cli.ContainerInspect(context.Background(), req.InstanceID)
	if err != nil {
		return nil, err
	}
	instance = &orchestrator.Instance{
		// It's weird that Docker put a forward slash to the container name
		// So it become "/replica-1"
		ID:      inspectJSON.ID,
		Name:    strings.TrimPrefix(inspectJSON.Name, "/"),
		Running: inspectJSON.State.Running,
		NodeID:  d.GetCurrentNode().ID,
	}
	if d.Network == "" {
		instance.IP = inspectJSON.NetworkSettings.IPAddress
	} else {
		instance.IP = inspectJSON.NetworkSettings.Networks[d.Network].IPAddress
	}
	if instance.Running && instance.IP == "" {
		msg := fmt.Sprintf("BUG: Cannot find IP address of %v", instance.ID)
		logrus.Errorf(msg)
		return nil, errors.Errorf(msg)
	}
	return instance, nil
}

func (d *Docker) StartInstance(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to start instance %v(%v)", req.InstanceName, req.InstanceID)
	}()

	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return nil, err
	}
	if req.NodeID != d.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			d.currentNode.ID)
	}

	if err := d.cli.ContainerStart(context.Background(), req.InstanceID, dTypes.ContainerStartOptions{}); err != nil {
		return nil, err
	}
	return d.InspectInstance(req)
}

func (d *Docker) StopInstance(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to stop instance %v(%v)", req.InstanceName, req.InstanceID)
	}()

	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return nil, err
	}
	if req.NodeID != d.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			d.currentNode.ID)
	}

	// Docker after v1.12 may lost SIGTERM to container if start/stop is too
	// quick, add a little delay to help
	time.Sleep(GraceStopTimeout)
	if err := d.cli.ContainerStop(context.Background(),
		req.InstanceID, &ContainerStopTimeout); err != nil {
		return nil, err
	}
	return d.InspectInstance(req)
}

func (d *Docker) DeleteInstance(req *orchestrator.Request) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to delete instance %v(%v)", req.InstanceName, req.InstanceID)
	}()

	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return err
	}
	if req.NodeID != d.currentNode.ID {
		return fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			d.currentNode.ID)
	}

	return d.cli.ContainerRemove(context.Background(), req.InstanceID, dTypes.ContainerRemoveOptions{
		RemoveVolumes: true,
	})
}
