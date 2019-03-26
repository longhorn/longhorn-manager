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

	ArgNameFlexvolumePluginDir = "--volume-plugin-dir"
	ArgNameKubeletRootDir      = "--root-dir"
	ArgNameK3SDataDir          = "--data-dir"
	ArgShortNameK3SDataDir     = "-d"

	DefaultFlexvolumeDir  = "/usr/libexec/kubernetes/kubelet-plugins/volume/exec/"
	DefaultKubeletRootDir = "/var/lib/kubelet"
	DefaultK3SDataDir     = "/var/lib/rancher/k3s"

	KubeletDetectionPodName = "discover-proc-kubelet-cmdline"
	K3SDetectionPodName     = "discover-proc-k3s-cmdline"

	GetKubeletCmdlineScript = `
    find_kubelet_cmdline() {
      for proc in $(find /proc -maxdepth 1 -type d 2>/dev/null); do
        if [ ! -f $proc/cmdline ]; then
          continue
        fi
        if [[ "$(cat $proc/cmdline | tr '\000' '\n' | head -n1 | tr '/' '\n' | tail -n1)" == "kubelet" ]]; then
          echo "Proc found: kubelet"
          cat $proc/cmdline
          return
        fi
      done
      echo "Proc not found: kubelet"
    }
    find_kubelet_cmdline
	`
	GetK3SCmdlineScript = `
    find_k3s_cmdline() {
      for proc in $(find /proc -maxdepth 1 -type d 2>/dev/null); do
        if [ ! -f $proc/cmdline ]; then
          continue
        fi
        if [[ "$(cat $proc/cmdline | tr '\000' '\n' | head -n1 | tr '/' '\n' | tail -n1)" == "k3s" ]]; then
          proc_name=$(cat $proc/cmdline | tr '\000' '\n' | sed -n '2p')
          if [[ "$proc_name"  == "server" || "$proc_name" == "agent" ]]; then
            echo "Proc found: k3s"
            cat $proc/cmdline
            return
          fi
        fi
      done
      echo "Proc not found: k3s"
    }
    find_k3s_cmdline
	`
)

func getProcArg(kubeClient *clientset.Clientset, managerImage, name string) (string, error) {
	switch name {
	case ArgFlexvolumePluginDir:
		dir, err := detectFlexvolumeDir(kubeClient, managerImage)
		if err != nil {
			return "", errors.Wrap(err, `failed to get arg volume-plugin-dir. Need to specify "--flexvolume-dir" in your Longhorn deployment yaml.`)
		}
		return dir, nil
	case ArgKubeletRootDir:
		dir, err := detectKubeletRootDir(kubeClient, managerImage)
		if err != nil {
			return "", errors.Wrap(err, `failed to get arg root-dir. Need to specify "--kubelet-root-dir" in your Longhorn deployment yaml.`)
		}
		return dir, nil
	}
	return "", fmt.Errorf("getting arg %v is not supported", name)
}

func detectFlexvolumeDir(kubeClient *clientset.Clientset, managerImage string) (string, error) {
	kubeletCmdline, err := getProcCmdline(kubeClient, managerImage, KubeletDetectionPodName, GetKubeletCmdlineScript)
	if err != nil {
		return "", errors.Wrap(err, "failed to get cmdline of proc kubelet")
	}
	if kubeletCmdline == "" {
		return "", fmt.Errorf("failed to get flexvolume dir, no related proc for volume-plugin-dir detection, error out")
	}
	dir, err := getArgFromCmdline(kubeletCmdline, ArgNameFlexvolumePluginDir)
	if err != nil {
		return "", errors.Wrap(err, "failed to get arg volume-plugin-dir in cmdline of proc kubelet")
	}
	if dir == "" {
		logrus.Warnf(`Cmdline of proc kubelet found: "%s". But arg "%s" not found. Hence default value will be used: "%s"`, kubeletCmdline, ArgNameFlexvolumePluginDir, DefaultFlexvolumeDir)
		dir = DefaultFlexvolumeDir
	}
	return dir, nil
}

func detectKubeletRootDir(kubeClient *clientset.Clientset, managerImage string) (string, error) {
	// try to detect root-dir in proc kubelet
	kubeletCmdline, err := getProcCmdline(kubeClient, managerImage, KubeletDetectionPodName, GetKubeletCmdlineScript)
	if err != nil {
		return "", errors.Wrap(err, "failed to get cmdline of proc kubelet")
	}
	// proc kubelet exists.
	if kubeletCmdline != "" {
		rootDir, err := getArgFromCmdline(kubeletCmdline, ArgNameKubeletRootDir)
		if err != nil {
			return "", errors.Wrap(err, "failed to get arg root-dir in cmdline of proc kubelet")
		}
		if rootDir == "" {
			logrus.Warnf(`Cmdline of proc kubelet found: "%s". But arg "%s" not found. Hence default value will be used: "%s"`, kubeletCmdline, ArgNameKubeletRootDir, DefaultKubeletRootDir)
			rootDir = DefaultKubeletRootDir
		}
		return rootDir, nil
	}
	// no proc kubelet. then try to detect data-dir and get root-dir in proc k3s
	k3sCmdline, err := getProcCmdline(kubeClient, managerImage, K3SDetectionPodName, GetK3SCmdlineScript)
	if err != nil {
		return "", errors.Wrap(err, "failed to get cmdline of proc k3s")
	}
	// proc k3s exists.
	if k3sCmdline != "" {
		dataDir, err := getArgFromCmdline(k3sCmdline, ArgNameK3SDataDir, ArgShortNameK3SDataDir)
		if err != nil {
			return "", errors.Wrap(err, "failed to get arg data-dir in cmdline of proc k3s")
		}
		if dataDir == "" {
			logrus.Warnf(`Cmdline of proc k3s found: "%s". But arg "%s" or "%s" not found. Hence default value will be used: "%s"`, k3sCmdline, ArgNameK3SDataDir, ArgShortNameK3SDataDir, DefaultK3SDataDir)
			dataDir = DefaultK3SDataDir
		}
		rootDir := filepath.Join(dataDir, "/agent/kubelet")
		return rootDir, nil
	}
	// no related proc found. error out
	return "", fmt.Errorf("failed to get kubelet root dir, no related proc for root-dir detection, error out")
}

func getProcCmdline(kubeClient *clientset.Clientset, managerImage, name, script string) (string, error) {
	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return "", fmt.Errorf("failed to detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}

	if err := deployDetectionPod(kubeClient, namespace, managerImage, name, script); err != nil {
		return "", errors.Wrapf(err, "failed to deploy proc cmdline detection pod %v", name)
	}

	defer func() {
		if err := kubeClient.CoreV1().Pods(namespace).Delete(name, &metav1.DeleteOptions{}); err != nil {
			logrus.Warnf("failed to delete proc cmdline detection pod %v: %v", name, err)
		}
	}()

	completed := false
	for i := 0; i < DetectPodMaxPolls; i++ {
		if pod, err := kubeClient.CoreV1().Pods(namespace).Get(name, metav1.GetOptions{}); err != nil {
			logrus.Warnf("failed to get proc cmdline detection pod %v: %v", name, err)
		} else if pod.Status.Phase == v1.PodSucceeded {
			completed = true
			break
		} else {
			logrus.Debugf("proc cmdline detection pod %v in phase: %v", name, pod.Status.Phase)
		}
		time.Sleep(1 * time.Second)
	}
	if !completed {
		return "", fmt.Errorf("proc cmdline detection pod %v didn't complete within %d seconds", name, DetectPodMaxPolls)
	}

	procArg, err := getPodLogAsString(kubeClient, namespace, name)
	if err != nil {
		return "", err
	}
	return procArg, nil
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
	strLogs := strings.Split(string(logs), "\n")
	// proc not found
	if strings.HasPrefix(strLogs[0], "Proc not found") {
		logrus.Warn(strLogs[0])
		return "", nil
	} else if !strings.HasPrefix(strLogs[0], "Proc found") {
		// unexpected result
		return "", fmt.Errorf(string(logs))
	}
	logrus.Info(strLogs[0])
	return strLogs[1], nil
}

func getArgFromCmdline(cmdline string, argNames ...string) (string, error) {
	cmdList := strings.Split(cmdline, "\000")
	logrus.Infof("Try to find arg %v in cmdline: %v", argNames, cmdList)
	var arg string
	for idx, cmdStr := range cmdList {
		for _, argName := range argNames {
			if strings.HasPrefix(cmdStr, argName) {
				if cmdStr == argName {
					// the separator between <argName> and <argValue> is " "
					if idx < len(cmdList)-1 {
						arg = cmdList[idx+1]
					}
				} else if strings.Contains(cmdStr, "=") {
					// the separator between <argName> and <argValue> is "="
					argNameAndValue := strings.Split(cmdStr, "=")
					// the splitting result should be [<argName> <argValue>]
					if len(argNameAndValue) == 2 {
						arg = argNameAndValue[1]
					} else {
						return "", fmt.Errorf("unexpected argument: %s", cmdStr)
					}
				}
				return arg, nil
			}
		}
	}
	return "", nil
}
