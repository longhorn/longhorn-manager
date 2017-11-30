package kubernetes

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/k8s"
	"github.com/rancher/longhorn-manager/orchestrator"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

	apiv1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kCli "k8s.io/client-go/kubernetes"
)

const (
	cfgDirectory = "/var/lib/rancher/longhorn/"
	hostUUIDFile = cfgDirectory + ".physical_host_uuid"

	keyNodeName  = "NODE_NAME"
	keyNameSpace = "POD_NAMESPACE"
	keyNodeIP    = "POD_IP"
)

var (
	WaitDeviceTimeout = 30 //seconds
	WaitAPITimeout    = 30 //seconds
	WaitPodPeriod     = 5  //seconds
	WaitPodCounter    = 10
)

type Kubernetes struct {
	EngineImage string
	IP          string
	NodeName    string
	NameSpace   string
	currentNode *types.NodeInfo
	cli         *kCli.Clientset
}

type Config struct {
	EngineImage string
}

func NewOrchestrator(cfg *Config) (*Kubernetes, error) {
	if cfg.EngineImage == "" {
		return nil, fmt.Errorf("missing required parameter EngineImage")
	}

	config, err := k8s.GetClientConfig("")
	if err != nil {
		return nil, err
	}

	kubernetes := &Kubernetes{
		EngineImage: cfg.EngineImage,
	}
	kubernetes.cli, err = kCli.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to kubernetes")
	}

	if err = kubernetes.updateNetwork(); err != nil {
		return nil, errors.Wrapf(err, "fail to detect dedicated pod network")
	}

	if err = kubernetes.updateNameSpace(); err != nil {
		return nil, errors.Wrapf(err, "fail to detect namespace")
	}

	if err = kubernetes.updateNodeName(); err != nil {
		return nil, errors.Wrapf(err, "fail to detect nodename")
	}

	logrus.Infof("Detected IP is %s", kubernetes.IP)

	if err := kubernetes.updateCurrentNode(); err != nil {
		return nil, err
	}

	logrus.Info("Kubernetes orchestrator is ready")
	return kubernetes, nil
}

func (k *Kubernetes) updateNetwork() error {
	IP := os.Getenv(keyNodeIP)
	if IP == "" {
		return fmt.Errorf("can't get the Node IP")
	}

	k.IP = IP
	return nil
}

func (k *Kubernetes) updateNameSpace() error {
	nodeNameSpace := os.Getenv(keyNameSpace)
	if nodeNameSpace == "" {
		return fmt.Errorf("can't get the Node namespace")
	}

	k.NameSpace = nodeNameSpace
	return nil
}

func (k *Kubernetes) updateNodeName() error {
	nodeName := os.Getenv(keyNodeName)
	if nodeName == "" {
		return fmt.Errorf("can't get the Node name")
	}

	k.NodeName = nodeName
	return nil
}

func (k *Kubernetes) updateCurrentNode() error {
	node := &types.NodeInfo{
		IP: k.IP,
	}
	node.Name = k.NodeName
	node.ID = node.Name

	k.currentNode = node
	return nil
}

func (k *Kubernetes) GetCurrentNode() *types.NodeInfo {
	return k.currentNode
}

func (k *Kubernetes) CreateController(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to create controller for %v", req.VolumeName)
	}()

	if err := orchestrator.ValidateRequestCreateController(req); err != nil {
		return nil, err
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

	privilege := true
	pod := &apiv1.Pod{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: req.InstanceName,
		},
		Spec: apiv1.PodSpec{
			NodeSelector: map[string]string{
				"kubernetes.io/hostname": req.NodeID,
			},
			RestartPolicy: apiv1.RestartPolicyNever,
			Containers: []apiv1.Container{
				{
					Name:    req.InstanceName,
					Image:   k.EngineImage,
					Command: cmd,
					SecurityContext: &apiv1.SecurityContext{
						Privileged: &privilege,
					},
					VolumeMounts: []apiv1.VolumeMount{
						{
							Name:      "dev",
							MountPath: "/host/dev",
						},
						{
							Name:      "proc",
							MountPath: "/host/proc",
						},
					},
				},
			},
			Volumes: []apiv1.Volume{
				{
					Name: "dev",
					VolumeSource: apiv1.VolumeSource{
						HostPath: &apiv1.HostPathVolumeSource{
							Path: "/dev",
						},
					},
				},
				{
					Name: "proc",
					VolumeSource: apiv1.VolumeSource{
						HostPath: &apiv1.HostPathVolumeSource{
							Path: "/proc",
						},
					},
				},
			},
		},
	}

	podController, err := k.cli.CoreV1().Pods(k.NameSpace).Create(pod)
	if err != nil {
		return nil, err
	}

	req.InstanceID = podController.ObjectMeta.Name

	defer func() {
		if err != nil {
			logrus.Errorf("fail to start controller %v of %v, cleaning up: %v",
				req.InstanceName, req.VolumeName, err)
			k.StopInstance(req)
		}
	}()

	instance, err = k.InspectInstance(req)
	if err != nil {
		return nil, err
	}

	url := "http://" + instance.IP + ":9501/v1"
	if err := util.WaitForAPI(url, WaitAPITimeout); err != nil {
		return nil, err
	}

	if err := util.WaitForDevice(k.getDeviceName(req.VolumeName), WaitDeviceTimeout); err != nil {
		return nil, err
	}

	return instance, nil
}

func (k *Kubernetes) getDeviceName(volumeName string) string {
	return filepath.Join("/dev/longhorn/", volumeName)
}

func (k *Kubernetes) CreateReplica(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	if err := orchestrator.ValidateRequestCreateReplica(req); err != nil {
		return nil, err
	}
	instance = &orchestrator.Instance{
		ID:      req.InstanceName,
		Name:    req.InstanceName,
		Running: false,
		NodeID:  req.NodeID,
	}
	return instance, nil
}

func (k *Kubernetes) startReplica(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to create replica %v for %v",
			req.InstanceName, req.VolumeName)
	}()

	if err := orchestrator.ValidateRequestCreateReplica(req); err != nil {
		return nil, err
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

	privilege := true
	pod := &apiv1.Pod{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: req.InstanceName,
		},
		Spec: apiv1.PodSpec{
			NodeSelector: map[string]string{
				"kubernetes.io/hostname": req.NodeID,
			},
			RestartPolicy: apiv1.RestartPolicyNever,
			Containers: []apiv1.Container{
				{
					Name:    req.InstanceName,
					Image:   k.EngineImage,
					Command: cmd,
					SecurityContext: &apiv1.SecurityContext{
						Privileged: &privilege,
					},
					VolumeMounts: []apiv1.VolumeMount{
						{
							Name:      "volume",
							MountPath: "/volume",
						},
					},
				},
			},
			Volumes: []apiv1.Volume{
				{
					Name: "volume",
					VolumeSource: apiv1.VolumeSource{
						HostPath: &apiv1.HostPathVolumeSource{
							Path: "/var/longhorn/replica/" + req.InstanceName,
						},
					},
				},
			},
		},
	}

	podReplica, err := k.cli.CoreV1().Pods(k.NameSpace).Create(pod)

	if err != nil {
		return nil, err
	}

	req.InstanceID = podReplica.ObjectMeta.Name

	defer func() {
		if err != nil {
			k.StopInstance(req)
			k.DeleteInstance(req)
		}
	}()

	instance, err = k.InspectInstance(req)
	if err != nil {
		logrus.Errorf("fail to inspect when create replica %v of %v, cleaning up: %v", req.InstanceName, req.VolumeName, err)
		return nil, err
	}

	timeout := WaitAPITimeout
	// More time for backup restore, may need to customerize it
	if req.RestoreFrom != "" && req.RestoreName != "" {
		timeout = timeout * 10
	}
	url := "http://" + instance.IP + ":9502/v1"

	//this fuction maybe a risk, when the IP is unreachable, it will block forever
	if err := util.WaitForAPI(url, timeout); err != nil {
		return nil, err
	}

	return instance, nil
}

func (k *Kubernetes) waitForPodReady(podName string) (*apiv1.Pod, error) {
	for i := 0; i < WaitPodCounter; i++ {
		pod, err := k.cli.CoreV1().Pods(k.NameSpace).Get(podName, meta_v1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if pod.Status.PodIP != "" {
			return pod, nil
		}
		time.Sleep(time.Second * time.Duration(WaitPodPeriod))
	}

	return nil, fmt.Errorf("timeout: pod %v IP can't be acquired", podName)
}

func (k *Kubernetes) InspectInstance(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to inspect instance %v(%v)", req.InstanceName, req.InstanceID)
	}()

	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return nil, err
	}

	pod, err := k.waitForPodReady(req.InstanceName)
	if err != nil {
		return nil, err
	}

	if pod == nil {
		return nil, fmt.Errorf("incrediable error, can't get pod Instance %v", req.InstanceName)
	}

	instance = &orchestrator.Instance{
		ID:      pod.ObjectMeta.Name,
		Name:    pod.ObjectMeta.Name,
		Running: pod.Status.Phase == apiv1.PodPhase("Running"),
		NodeID:  req.NodeID,
	}

	instance.IP = pod.Status.PodIP

	if instance.Running && instance.IP == "" {
		msg := fmt.Sprintf("BUG: Cannot find IP address of %v", instance.ID)
		logrus.Errorf(msg)
		return nil, errors.Errorf(msg)
	}
	return instance, nil
}

func (k *Kubernetes) StartInstance(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	if strings.HasPrefix(req.InstanceName, req.VolumeName+"-replica") &&
		!strings.HasSuffix(req.InstanceName, "controller") {
		return k.startReplica(req)
	}

	return nil, fmt.Errorf("Bug: the name of instance %s is not known", req.InstanceName)
}

func (k *Kubernetes) StopInstance(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	instance, err = k.InspectInstance(req)
	if err != nil {
		instance = &orchestrator.Instance{
			ID:      req.InstanceName,
			Name:    req.InstanceName,
			Running: false,
			NodeID:  req.NodeID,
		}
		return instance, nil
	}

	defer func() {
		err = errors.Wrapf(err, "fail to delete instance %v(%v)", req.InstanceName, req.InstanceID)
	}()

	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return instance, err
	}

	return instance, k.cli.CoreV1().Pods(k.NameSpace).Delete(req.InstanceName, &meta_v1.DeleteOptions{})
}

func (k *Kubernetes) DeleteInstance(req *orchestrator.Request) (err error) {
	// TODO the Deleting for replica needs to clean the volume file
	return nil
}
