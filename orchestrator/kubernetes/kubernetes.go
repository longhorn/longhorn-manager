package kubernetes

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
	"github.com/rancher/longhorn-manager/orchestrator"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
	kCli "k8s.io/client-go/kubernetes"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiv1 "k8s.io/api/core/v1"
	"github.com/rancher/longhorn-manager/crdstore"
)

const (
	cfgDirectory = "/var/lib/rancher/longhorn/"
	hostUUIDFile = cfgDirectory + ".physical_host_uuid"
)

var (
	WaitDeviceTimeout    = 30 //seconds
	WaitAPITimeout       = 30 //seconds
	WaitPodTimeout		 = 2 //seconds
	WaitPodCounter		 = 3
)

type Kuber struct {
	EngineImage string
	IP          string
	currentNode *types.NodeInfo
	cli *kCli.Clientset
}

type Config struct {
	EngineImage string
}


func NewKuberOrchestrator(cfg *Config) (*Kuber, error) {

	if cfg.EngineImage == "" {
		return nil, fmt.Errorf("missing required parameter EngineImage")
	}

	config, err := crdstore.GetClientConfig("")
	if err != nil {
		panic(err.Error())
	}

	kube := &Kuber{
		EngineImage: cfg.EngineImage,
	}

	kube.cli, err = kCli.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to kubernetes")
	}

	if err = kube.updateNetwork(); err != nil {
		return nil, errors.Wrapf(err, "fail to detect dedicated pod network")
	}

	logrus.Infof("Detected  IP is %s",  kube.IP)

	if err := kube.updateCurrentNode(); err != nil {
		return nil, err
	}

	logrus.Info("Kubernetes orchestrator is ready")
	return kube, nil
}

func (k *Kuber) getCurrentNodePod() (*apiv1.Pod, error) {
	podName, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	pod, err := k.cli.CoreV1().Pods(apiv1.NamespaceDefault).Get(podName, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return pod, nil
}

func (k *Kuber) updateNetwork() error {
	pod, err := k.getCurrentNodePod()
	if err != nil {
		return err
	}
	k.IP = pod.Status.PodIP
	return nil
}

func (k *Kuber) updateCurrentNode() error {
	var err error
	node := &types.NodeInfo{
		IP:               k.IP,
		OrchestratorPort: types.DefaultOrchestratorPort,
	}
	node.Name, err = os.Hostname()
	if err != nil {
		return err
	}

	uuid, err := ioutil.ReadFile(hostUUIDFile)
	if err == nil {
		node.ID = string(uuid)
		k.currentNode = node
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

	k.currentNode = node
	return nil
}

func (k *Kuber) GetCurrentNode() *types.NodeInfo {
	return k.currentNode
}

func (k *Kuber) CreateController(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to create controller for %v", req.VolumeName)
	}()
	if err := orchestrator.ValidateRequestCreateController(req); err != nil {
		return nil, err
	}

	if req.NodeID != k.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			k.currentNode.ID)
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


	podNode, err := k.getCurrentNodePod()

	if err != nil {
		logrus.Errorf("fail to get daemonset pod info")
		return nil, err
	}

	privilege := true

	pod := &apiv1.Pod{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: req.InstanceName,
		},
		Spec: apiv1.PodSpec {
			NodeSelector:map[string]string{
				"kubernetes.io/hostname":podNode.Spec.NodeName,
				},
			Containers: []apiv1.Container{
				{
					Name:  req.InstanceName,
					Image: k.EngineImage,
					Command: cmd,
					SecurityContext: &apiv1.SecurityContext{
						Privileged: &privilege,
					},
					VolumeMounts: []apiv1.VolumeMount{
						{
							Name: "dev",
							MountPath: "/host/dev",
						},
						{
							Name: "proc",
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

	podController, err := k.cli.CoreV1().Pods(apiv1.NamespaceDefault).Create(pod)
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
		logrus.Errorf("fail to inspect when create controller %v of %v, cleaning up: %v", req.InstanceName, req.VolumeName, err)
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

func (k *Kuber) getDeviceName(volumeName string) string {
	return filepath.Join("/dev/longhorn/", volumeName)
}

func (k *Kuber) CreateReplica(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {

	if err := orchestrator.ValidateRequestCreateReplica(req); err != nil {
		return nil, err
	}
	if req.NodeID != k.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			k.currentNode.ID)
	}

	instance = &orchestrator.Instance{
		ID:      req.InstanceName,
		Name:    req.InstanceName,
		Running: false,
		NodeID:  k.GetCurrentNode().ID,
	}
	return instance, nil
}

func (k *Kuber) startReplica(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to create replica %v for %v",
			req.InstanceName, req.VolumeName)
	}()

	if err := orchestrator.ValidateRequestCreateReplica(req); err != nil {
		return nil, err
	}
	if req.NodeID != k.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			k.currentNode.ID)
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


	podNode, err := k.getCurrentNodePod()

	if err != nil {
		logrus.Errorf("fail to get daemonset pod info")
		return nil, err
	}

	privilege := true

	pod := &apiv1.Pod{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: req.InstanceName,
		},
		Spec: apiv1.PodSpec {
			NodeSelector:map[string]string{
				"kubernetes.io/hostname":podNode.Spec.NodeName,
			},
			Containers: []apiv1.Container{
				{
					Name:  req.InstanceName,
					Image: k.EngineImage,
					Command: cmd,
					SecurityContext: &apiv1.SecurityContext{
						Privileged: &privilege,
					},
					VolumeMounts: []apiv1.VolumeMount{
						{
							Name: "volume",
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
							Path: "/var/longhorn/replica/"+req.InstanceName,
						},
					},
				},
			},

		},

	}

	podReplica, err := k.cli.CoreV1().Pods(apiv1.NamespaceDefault).Create(pod)

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

func (k *Kuber) waitForPodReady(podName string)(*apiv1.Pod, error) {

	c := time.After(time.Second * time.Duration(WaitPodTimeout))
	for i := 0; i < WaitPodCounter; i++ {
		select {
		case <-c:
			pod, err := k.cli.CoreV1().Pods(apiv1.NamespaceDefault).Get(podName, meta_v1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("get pod fail %v", podName)
			}
			if pod.Status.PodIP == "" {
				c = time.After(time.Second)
			} else {
				return pod, err
			}
		}
	}

	return nil, fmt.Errorf("pod IP can't be acquired, timeout")
}

func (k *Kuber) InspectInstance(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to inspect instance %v(%v)", req.InstanceName, req.InstanceID)
	}()

	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return nil, err
	}
	if req.NodeID != k.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			k.currentNode.ID)
	}
	pod, err := k.waitForPodReady(req.InstanceName)

	if err != nil {
		return nil, err
	}

	if pod == nil {
		return nil, fmt.Errorf("incrediable error, can't get pod ip")
	}

	instance = &orchestrator.Instance{
		ID:      pod.ObjectMeta.Name,
		Name:    pod.ObjectMeta.Name,
		Running: pod.Status.Phase == apiv1.PodPhase("Running"),
		NodeID:  k.GetCurrentNode().ID,
	}

	instance.IP = pod.Status.PodIP

	if instance.Running && instance.IP == "" {
		msg := fmt.Sprintf("BUG: Cannot find IP address of %v", instance.ID)
		logrus.Errorf(msg)
		return nil, errors.Errorf(msg)
	}
	return instance, nil
}

func (k *Kuber) StartInstance(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	if strings.HasPrefix(req.InstanceName, req.VolumeName + "-replica") &&
	! strings.HasSuffix(req.InstanceName, "controller") {
		return k.startReplica(req)
	}

	return nil, fmt.Errorf("Bug: the name of instance %s is not known", req.InstanceName)
}

func (k *Kuber) StopInstance(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {

	instance, err = k.InspectInstance(req)
	if err != nil {
		return nil, fmt.Errorf("can't get instance")
	}
	defer func() {
		err = errors.Wrapf(err, "fail to delete instance %v(%v)", req.InstanceName, req.InstanceID)
	}()

	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return instance, err
	}

	if req.NodeID != k.currentNode.ID {
		return instance, fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			k.currentNode.ID)
	}

	return instance, k.cli.CoreV1().Pods(apiv1.NamespaceDefault).Delete(req.InstanceName, &meta_v1.DeleteOptions{})

}

func (k *Kuber) DeleteInstance(req *orchestrator.Request) (err error) {
	// TODO the Deleting for replica needs to clean the volume file
	return nil
}