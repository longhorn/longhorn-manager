package app

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/rancher/longhorn-manager/types"
)

const (
	DetectPodMaxPolls = 120

	ArgFlexvolumePluginDir = "volume-plugin-dir"
	ArgKubeletRootDir      = "root-dir"

	DefaultFlexvolumeDir  = "/usr/libexec/kubernetes/kubelet-plugins/volume/exec/"
	DefaultKubeletRootDir = "/var/lib/kubelet"

	DetectFlexvolumeName     = "discover-flexvolume-dir"
	DetectKubeletRootDirName = "discover-kubelet-root-dir"

	DetectKubeletArgScript = `
    find_kubelet_proc() {
      for proc in $(find /proc -maxdepth 1 -type d 2>/dev/null); do
        if [ ! -f $proc/cmdline ]; then
          continue
        fi
        if [[ "$(cat $proc/cmdline | tr '\000' '\n' | head -n1 | tr '/' '\n' | tail -n1)" == "kubelet" ]]; then
          echo $proc
        fi
      done
    }
    get_kubelet_arg_dir() {
      proc=$(find_kubelet_proc)
      if [ "$proc" != "" ]; then
        path=$(cat $proc/cmdline | tr '\000' '\n' | grep %s | tr '=' '\n' | tail -n1)
        if [ "$path" == "" ]; then
          echo "No arg root-dir in proc kubelet. Will use default root-dir."
          echo %s
        else
          echo $path
        fi
      fi
    }
	get_kubelet_arg_dir
	`
	DetectK3SDataDirScript = `
    find_k3s_proc() {
      for proc in $(find /proc -maxdepth 1 -type d 2>/dev/null); do
        if [ ! -f $proc/cmdline ]; then
          continue
        fi
        if [[ $(cat $proc/cmdline | tr '\000' '\n' | sed -n '1p') == "k3s" ]]; then
          proc_name=$(cat $proc/cmdline | tr '\000' '\n' | sed -n '2p')
          if [[ "$proc_name"  == "server" || "$proc_name" == "agent" ]]; then
            echo $proc
          fi
        fi
      done
    }
    get_k3s_data_dir() {
      proc=$(find_k3s_proc)
      if [ "$proc" != "" ]; then
        path=$(cat $proc//cmdline | tr '\000' '\n' |  grep 'data-dir' -A 1 | tail -n1)
        if [ "$path" == "" ]; then
          path=$(cat $proc//cmdline | tr '\000' '\n' |  grep '\^-d$' -A 1 | tail -n1)
        fi
        if [ "$path" == "" ]; then
          echo "No arg data-dir in proc k3s. Will use default data-dir."
          echo '/var/lib/rancher/k3s'
        else
          echo $path
        fi
      fi
    }
    get_k3s_data_dir
	`
)

func getProcArg(kubeClient *clientset.Clientset, managerImage, name string) (string, error) {
	switch name {
	case ArgFlexvolumePluginDir:
		dir, err := discoverFlexvolumeDir(kubeClient, managerImage)
		if err != nil {
			return "", errors.Wrap(err, "Failed to get arg volume-plugin-dir")
		}
		return dir, nil
	case ArgKubeletRootDir:
		dir, err := discoverKubeletRootDir(kubeClient, managerImage)
		if err != nil {
			return "", errors.Wrap(err, "Failed to get arg root-dir")
		}
		return dir, nil
	}
	return "", fmt.Errorf("Getting arg %v is not supported", name)
}

func discoverFlexvolumeDir(kubeClient *clientset.Clientset, managerImage string) (string, error) {
	detectFlexVolumeScript := fmt.Sprintf(DetectKubeletArgScript, ArgFlexvolumePluginDir, DefaultFlexvolumeDir)
	dir, err := discoverProcArg(kubeClient, managerImage, DetectFlexvolumeName, detectFlexVolumeScript)
	if err != nil {
		return "", errors.Wrap(err, "Failed to get arg volume-plugin-dir in proc kubelet")
	}
	if dir == "" {
		return "", fmt.Errorf("Cannot get flexvolume dir, no related proc for volume-plugin-dir detection, error out")
	}
	return dir, nil
}

func discoverKubeletRootDir(kubeClient *clientset.Clientset, managerImage string) (string, error) {
	// try to detect root-dir in proc kubelet
	detectKubeletRootDirScript := fmt.Sprintf(DetectKubeletArgScript, ArgKubeletRootDir, DefaultKubeletRootDir)
	rootDir, err := discoverProcArg(kubeClient, managerImage, DetectKubeletRootDirName, detectKubeletRootDirScript)
	if err != nil {
		return "", errors.Wrap(err, "Failed to get arg root-dir in proc kubelet")
	}
	// no proc kubelet. then try to detect root-dir in proc k3s
	if rootDir == "" {
		rootDir, err = discoverProcArg(kubeClient, managerImage, DetectKubeletRootDirName, DetectK3SDataDirScript)
		if err != nil {
			return "", errors.Wrap(err, "Failed to get arg root-dir in proc k3s")
		}
		// proc k3s exists.
		if rootDir != "" {
			rootDir = filepath.Join(rootDir, "/agent/kubelet")
		}
	}
	// nothing found. error out
	if rootDir == "" {
		return "", fmt.Errorf("Cannot get kubelet root dir, no related proc for root-fir detection, error out")
	}
	return rootDir, nil
}

func discoverProcArg(kubeClient *clientset.Clientset, managerImage, name, script string) (string, error) {
	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return "", fmt.Errorf("Cannot detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}

	if err := deployDetectionPod(kubeClient, namespace, managerImage, name, script); err != nil {
		return "", errors.Wrapf(err, "Failed to deploy kubelet arg detection pod %v", name)
	}

	defer func() {
		if err := kubeClient.CoreV1().Pods(namespace).Delete(name, &metav1.DeleteOptions{}); err != nil {
			logrus.Warnf("Failed to delete kubelet arg detection pod %v: %v", name, err)
		}
	}()

	completed := false
	for i := 0; i < DetectPodMaxPolls; i++ {
		if pod, err := kubeClient.CoreV1().Pods(namespace).Get(name, metav1.GetOptions{}); err != nil {
			logrus.Warnf("Failed to get kubelet arg detection pod %v: %v", name, err)
		} else if pod.Status.Phase == v1.PodSucceeded {
			completed = true
			break
		} else {
			logrus.Debugf("kubelet arg detection pod %v in phase: %v", name, pod.Status.Phase)
		}
		time.Sleep(1 * time.Second)
	}
	if !completed {
		return "", fmt.Errorf("Kubelet arg detection pod %v didn't complete within %d seconds", name, DetectPodMaxPolls)
	}

	kubeletArg, err := getPodLogAsString(kubeClient, namespace, name)
	if err != nil {
		return "", err
	}
	return kubeletArg, nil
}

func deployDetectionPod(kubeClient *clientset.Clientset, namespace, managerImage, name, script string) error {
	privileged := true
	_, err := kubeClient.CoreV1().Pods(namespace).Create(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				v1.Container{
					Name:    name,
					Image:   managerImage,
					Command: []string{"/bin/bash"},
					Args:    []string{"-c", script},
					SecurityContext: &v1.SecurityContext{
						Privileged: &privileged,
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
			HostPID:       true,
		},
	})
	return err
}

func getPodLogAsString(kubeClient *clientset.Clientset, namespace, name string) (string, error) {
	req := kubeClient.CoreV1().Pods(namespace).GetLogs(name, &v1.PodLogOptions{})
	if req.URL().Path == "" {
		return "", fmt.Errorf("getPodLogAsString for %v/%v returns empty request path, may due to unit test run: %+v", namespace, name, req)
	}

	logs, err := req.DoRaw()
	if err != nil {
		return "", err
	}
	logsRes := strings.Split(string(logs), "\n")
	if strings.HasPrefix(logsRes[0], "No arg") {
		logrus.Warnf("%s If longhorn driver doesn't work, you may want to try to set argument manually.", logsRes[0])
		return logsRes[1], nil
	}
	return logsRes[0], nil
}
