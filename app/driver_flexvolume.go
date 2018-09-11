package app

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/rancher/longhorn-manager/types"
)

const (
	DetectFlexVolumeName   = "discover-flexvolume-dir"
	DetectFlexVolumeScript = `
    find_kubelet_proc() {
      for proc in $(find /proc -maxdepth 1 -type d 2>/dev/null); do
        if [ ! -f $proc/cmdline ]; then
          continue
        fi
        if [[ "$(cat $proc/cmdline | tr '\000' '\n' | head -n1 | tr '/' '\n' | tail -n1)" == "kubelet" ]]; then
          echo $proc
          return
        fi
      done
    }
    get_flexvolume_path() {
      proc=$(find_kubelet_proc)
      if [ "$proc" != "" ]; then
        path=$(cat $proc/cmdline | tr '\000' '\n' | grep volume-plugin-dir | tr '=' '\n' | tail -n1)
        if [ "$path" == "" ]; then
          echo '/usr/libexec/kubernetes/kubelet-plugins/volume/exec/'
        else
          echo $path
        fi
        return
      fi
      # no kubelet process found, echo nothing
    }
    get_flexvolume_path
  `
	DetectFlexVolumeMaxPolls = 120
)

func discoverFlexvolumeDir(kubeClient *clientset.Clientset, managerImage string) (string, error) {
	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return "", fmt.Errorf("Cannot detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}

	if err := deployPod(kubeClient, namespace, managerImage); err != nil {
		logrus.Warnf("Failed to deploy flexvolume detection pod: %v", err)
		return "", err
	}

	defer func() {
		if err := kubeClient.CoreV1().Pods(namespace).Delete(DetectFlexVolumeName, &metav1.DeleteOptions{}); err != nil {
			logrus.Warnf("Failed to delete flexvolume detection pod: %v", err)
		}
	}()

	completed := false
	for i := 0; i < DetectFlexVolumeMaxPolls; i++ {
		if pod, err := kubeClient.CoreV1().Pods(namespace).Get(DetectFlexVolumeName, metav1.GetOptions{}); err != nil {
			logrus.Warnf("Failed to get flexvolume detection pod: %v", err)
		} else if pod.Status.Phase == v1.PodSucceeded {
			completed = true
			break
		} else {
			logrus.Debugf("Flexvolume detection pod in phase: %v", pod.Status.Phase)
		}
		time.Sleep(1 * time.Second)
	}
	if !completed {
		return "", fmt.Errorf("Flexvolume detection pod didn't complete within %d seconds", DetectFlexVolumeMaxPolls)
	}

	flexvolumeDir, err := getPodLogAsString(kubeClient, namespace, DetectFlexVolumeName)
	if err != nil {
		return "", err
	}
	return flexvolumeDir, nil
}

func deployPod(kubeClient *clientset.Clientset, namespace, managerImage string) (err error) {
	privileged := true
	_, err = kubeClient.CoreV1().Pods(namespace).Create(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: DetectFlexVolumeName,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				v1.Container{
					Name:    DetectFlexVolumeName,
					Image:   managerImage,
					Command: []string{"/bin/bash"},
					Args:    []string{"-c", DetectFlexVolumeScript},
					SecurityContext: &v1.SecurityContext{
						Privileged: &privileged,
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
			HostPID:       true,
		},
	})
	return
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
	return strings.Trim(string(logs), "\n"), nil
}
