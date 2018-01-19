package kubernetes

import (
	"fmt"
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

	batchv1 "k8s.io/api/batch/v1"
	apiv1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kCli "k8s.io/client-go/kubernetes"
)

const (
	longhornDirectory = "/var/lib/rancher/longhorn/"
)

var (
	WaitDeviceTimeout = 30 //seconds
	WaitAPITimeout    = 30 //seconds
	WaitPodPeriod     = 5  //seconds
	WaitPodCounter    = 10
	WaitJobPeriod     = 5 //seconds
	WaitJobCounter    = 20
)

type Kubernetes struct {
	EngineImage string
	IP          string
	NodeName    string
	Namespace   string
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

	kubernetes.IP, err = util.GetRequiredEnv(k8s.EnvPodIP)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to detect the pod IP")
	}

	kubernetes.Namespace, err = util.GetRequiredEnv(k8s.EnvPodNamespace)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to detect the pod namespace")
	}

	kubernetes.NodeName, err = util.GetRequiredEnv(k8s.EnvNodeName)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to detect the node name")
	}

	logrus.Infof("Detected IP is %s", kubernetes.IP)

	if err := kubernetes.updateCurrentNode(); err != nil {
		return nil, err
	}

	logrus.Info("Kubernetes orchestrator is ready")
	return kubernetes, nil
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

	logrus.Debugf("Starting controller %v for %v", req.Instance, req.VolumeName)
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
			Name: req.Instance,
		},
		Spec: apiv1.PodSpec{
			NodeSelector: map[string]string{
				"kubernetes.io/hostname": req.NodeID,
			},
			RestartPolicy: apiv1.RestartPolicyNever,
			Containers: []apiv1.Container{
				{
					Name:    req.Instance,
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

	if _, err := k.cli.CoreV1().Pods(k.Namespace).Create(pod); err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			logrus.Errorf("fail to start controller %v of %v, cleaning up: %v",
				req.Instance, req.VolumeName, err)
			k.StopInstance(req)
		}
	}()

	if err := k.waitForPodReady(req.Instance); err != nil {
		return nil, err
	}

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
	logrus.Debugf("Started controller %v for %v", req.Instance, req.VolumeName)

	return instance, nil
}

func (k *Kubernetes) getDeviceName(volumeName string) string {
	return filepath.Join("/dev/longhorn/", volumeName)
}

func (k *Kubernetes) getReplicaVolumeDirectory(replicaName string) string {
	return longhornDirectory + "/replicas/" + replicaName
}

func (k *Kubernetes) CreateReplica(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	if err := orchestrator.ValidateRequestCreateReplica(req); err != nil {
		return nil, err
	}
	instance = &orchestrator.Instance{
		Name:    req.Instance,
		Running: false,
		NodeID:  req.NodeID,
	}
	return instance, nil
}

func (k *Kubernetes) startReplica(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to create replica %v for %v",
			req.Instance, req.VolumeName)
	}()

	if err := orchestrator.ValidateRequestCreateReplica(req); err != nil {
		return nil, err
	}

	logrus.Debugf("Starting replica %v for %v", req.Instance, req.VolumeName)
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
			Name: req.Instance,
		},
		Spec: apiv1.PodSpec{
			NodeSelector: map[string]string{
				"kubernetes.io/hostname": req.NodeID,
			},
			RestartPolicy: apiv1.RestartPolicyNever,
			Containers: []apiv1.Container{
				{
					Name:    req.Instance,
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
							Path: k.getReplicaVolumeDirectory(req.Instance),
						},
					},
				},
			},
		},
	}

	if _, err := k.cli.CoreV1().Pods(k.Namespace).Create(pod); err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			k.StopInstance(req)
			k.DeleteInstance(req)
		}
	}()

	if err := k.waitForPodReady(req.Instance); err != nil {
		return nil, err
	}

	instance, err = k.InspectInstance(req)
	if err != nil {
		logrus.Errorf("fail to inspect when create replica %v of %v, cleaning up: %v", req.Instance, req.VolumeName, err)
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
	logrus.Debugf("Started replica %v for %v", req.Instance, req.VolumeName)

	return instance, nil
}

func (k *Kubernetes) getPod(podName string) (*apiv1.Pod, error) {
	return k.cli.CoreV1().Pods(k.Namespace).Get(podName, meta_v1.GetOptions{})
}

func (k *Kubernetes) waitForPodReady(podName string) error {
	for i := 0; i < WaitPodCounter; i++ {
		pod, err := k.getPod(podName)
		if err != nil {
			return fmt.Errorf("fail to acquire pod %v: %v", podName, err)
		}
		if pod.Status.PodIP != "" {
			return nil
		}
		time.Sleep(time.Second * time.Duration(WaitPodPeriod))
	}

	return fmt.Errorf("timeout: pod %v IP can't be acquired", podName)
}

func (k *Kubernetes) InspectInstance(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to inspect instance %v", req.Instance)
	}()

	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return nil, err
	}

	pod, err := k.getPod(req.Instance)
	if err != nil {
		return nil, err
	}

	instance = &orchestrator.Instance{
		Name:    pod.ObjectMeta.Name,
		Running: pod.Status.Phase == apiv1.PodPhase("Running"),
		NodeID:  req.NodeID,
		IP:      pod.Status.PodIP,
	}

	if instance.Running && instance.IP == "" {
		msg := fmt.Sprintf("BUG: Cannot find IP address of %v", instance.Name)
		logrus.Errorf(msg)
		return nil, errors.Errorf(msg)
	}
	return instance, nil
}

func (k *Kubernetes) StartInstance(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return nil, err
	}

	if strings.HasPrefix(req.Instance, req.VolumeName+"-replica") &&
		!strings.HasSuffix(req.Instance, "controller") {
		return k.startReplica(req)
	}

	return nil, fmt.Errorf("Bug: the name of instance %s is not known", req.Instance)
}

func (k *Kubernetes) StopInstance(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return nil, err
	}

	logrus.Debugf("Stopping instance %v for %v", req.Instance, req.VolumeName)
	instance, err = k.InspectInstance(req)
	if err != nil {
		logrus.Debugf("Cannot find instance %v, assume it's stopped. Error %v", req.Instance, err)
		instance = &orchestrator.Instance{
			Name:    req.Instance,
			Running: false,
			NodeID:  req.NodeID,
		}
		return instance, nil
	}

	defer func() {
		err = errors.Wrapf(err, "fail to delete instance %v", req.Instance)
	}()

	if err := k.cli.CoreV1().Pods(k.Namespace).Delete(req.Instance, &meta_v1.DeleteOptions{}); err != nil {
		return nil, err
	}
	logrus.Debugf("Stopped instance %v for %v", req.Instance, req.VolumeName)
	instance = &orchestrator.Instance{
		Name:    req.Instance,
		Running: false,
		NodeID:  req.NodeID,
	}
	return instance, nil
}

func (k *Kubernetes) DeleteInstance(req *orchestrator.Request) (err error) {
	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return err
	}

	if strings.HasPrefix(req.Instance, req.VolumeName+"-replica") &&
		!strings.HasSuffix(req.Instance, "controller") {
		return k.deleteReplica(req)
	}
	return nil
}

func (k *Kubernetes) deleteReplica(req *orchestrator.Request) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to delete replica %v for volume %v", req.Instance, req.VolumeName)
	}()

	logrus.Debugf("Deleting replica %v for %v", req.Instance, req.VolumeName)

	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return err
	}

	cmd := []string{"/bin/bash", "-c"}
	// There is a delay between starting pod and mount the volume, so
	// workaround it for now
	args := []string{"sleep 1 && rm -f /volume/*"}

	jobName := "cleanup-" + req.Instance
	backoffLimit := int32(1)
	job := &batchv1.Job{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: jobName,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: apiv1.PodTemplateSpec{
				ObjectMeta: meta_v1.ObjectMeta{
					Name: "cleanup-pod-" + req.Instance,
				},
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{
						"kubernetes.io/hostname": req.NodeID,
					},
					RestartPolicy: apiv1.RestartPolicyNever,
					Containers: []apiv1.Container{
						{
							Name:    "cleanup-" + req.Instance,
							Image:   k.EngineImage,
							Command: cmd,
							Args:    args,
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
									Path: k.getReplicaVolumeDirectory(req.Instance),
								},
							},
						},
					},
				},
			},
		},
	}
	job, err = k.cli.BatchV1().Jobs(k.Namespace).Create(job)
	if err != nil {
		return errors.Wrap(err, "failed to create cleanup job")
	}

	propagationPolicy := meta_v1.DeletePropagationBackground
	defer k.cli.BatchV1().Jobs(k.Namespace).Delete(jobName, &meta_v1.DeleteOptions{
		PropagationPolicy: &propagationPolicy,
	})

	for i := 0; i < WaitJobCounter; i++ {
		job, err = k.cli.BatchV1().Jobs(k.Namespace).Get(jobName, meta_v1.GetOptions{})
		if err != nil {
			return err
		}
		if job.Status.CompletionTime != nil {
			break
		}
		time.Sleep(time.Second * time.Duration(WaitJobPeriod))
	}
	if job.Status.CompletionTime == nil {
		return errors.Errorf("clean up job cannot finish")
	}
	if job.Status.Succeeded == 0 {
		return errors.Errorf("clean up job failed")
	}

	logrus.Debugf("Deleted replica %v for %v", req.Instance, req.VolumeName)
	return nil
}
