package controller

import (
	"fmt"
	"reflect"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/types"
)

// These exceptions are exact native launches, not an allowance for arbitrary
// shell wrappers. The wait loop comes from chart/templates/deployment-driver.yaml;
// CSI socket cleanup comes from csi/deployment.go.
const archivedManagerWaitScript = `while [ $(curl -m 1 -s -o /dev/null -w "%{http_code}" http://longhorn-backend:9500/v1) != "200" ]; do echo waiting; sleep 2; done`

func isArchivedManagerContainer(container corev1.Container, image string) bool {
	if container.Image == image || strings.Contains(container.Image, "longhorn-manager") || strings.Contains(container.Name, "longhorn-manager") || container.Name == "longhorn-global-manager" || container.Name == types.CSIPluginName || container.Name == "longhorn-driver-deployer" || strings.Contains(strings.Join(append(append([]string{}, container.Command...), container.Args...), " "), "longhorn-manager") {
		return true
	}
	commands := [][]string{}
	for _, probe := range []*corev1.Probe{container.StartupProbe, container.LivenessProbe, container.ReadinessProbe} {
		if probe != nil && probe.Exec != nil {
			commands = append(commands, probe.Exec.Command)
		}
	}
	if container.Lifecycle != nil {
		for _, handler := range []*corev1.LifecycleHandler{container.Lifecycle.PostStart, container.Lifecycle.PreStop} {
			if handler != nil && handler.Exec != nil {
				commands = append(commands, handler.Exec.Command)
			}
		}
	}
	for _, command := range commands {
		if strings.Contains(strings.Join(command, " "), "longhorn-manager") {
			return true
		}
	}
	return false
}

func validateV2OnlyRestorePod(pod corev1.PodSpec, kind, name, image string) error {
	validate := func(container corev1.Container, init bool) error {
		if !isArchivedManagerContainer(container, image) {
			return nil
		}
		if container.Image != image {
			return fmt.Errorf("archived workload contains a different manager image")
		}
		command := ""
		switch {
		case !init && kind == "DaemonSet" && name == types.LonghornManagerDaemonSetName && container.Name == "longhorn-manager":
			command = "daemon"
		case !init && kind == "Deployment" && name == types.LonghornGlobalManagerName && container.Name == "longhorn-global-manager":
			command = "global"
		case !init && kind == "DaemonSet" && name == types.CSIPluginName && container.Name == types.CSIPluginName:
			command = "csi"
		case !init && kind == "Deployment" && name == "longhorn-driver-deployer" && container.Name == "longhorn-driver-deployer":
			command = "deploy-driver"
		case init && kind == "Deployment" && name == "longhorn-driver-deployer" && container.Name == "wait-longhorn-manager" && reflect.DeepEqual(container.Command, []string{"sh", "-c", archivedManagerWaitScript}) && len(container.Args) == 0:
			command = "wait"
		default:
			return fmt.Errorf("archived workload contains an unsupported manager launch")
		}
		if command != "wait" {
			if err := validateV2OnlyManagerLaunch(container, command, image); err != nil {
				return err
			}
		}
		for _, probe := range []*corev1.Probe{container.StartupProbe, container.LivenessProbe, container.ReadinessProbe} {
			if probe != nil && probe.Exec != nil {
				return fmt.Errorf("archived manager has an unsupported executable probe")
			}
		}
		if container.Lifecycle != nil {
			if handler := container.Lifecycle.PostStart; handler != nil && handler.Exec != nil {
				return fmt.Errorf("archived manager has an unsupported executable start hook")
			}
			if handler := container.Lifecycle.PreStop; handler != nil && handler.Exec != nil {
				if command != "csi" || !reflect.DeepEqual(handler.Exec.Command, []string{"/bin/sh", "-c", "rm -f /csi//*"}) {
					return fmt.Errorf("archived manager has an unsupported executable stop hook")
				}
			}
		}
		if command == "daemon" {
			flags, err := parseArchivedManagerLaunch(container, command, image)
			if err != nil || pod.ServiceAccountName == "" || flags["--service-account"] != pod.ServiceAccountName {
				return fmt.Errorf("archived manager service account is unresolved")
			}
		}
		if command == "global" {
			for envName, field := range map[string]string{"POD_NAME": "metadata.name", "POD_NAMESPACE": "metadata.namespace"} {
				count := 0
				for _, env := range container.Env {
					if env.Name == envName {
						count++
						if env.Value != "" || env.ValueFrom == nil || env.ValueFrom.FieldRef == nil || env.ValueFrom.FieldRef.FieldPath != field {
							return fmt.Errorf("archived global manager runtime identity is unresolved")
						}
					}
				}
				if count != 1 {
					return fmt.Errorf("archived global manager runtime identity is incomplete")
				}
			}
		}
		if command == "deploy-driver" {
			flags, err := parseArchivedManagerLaunch(container, command, image)
			if err != nil {
				return err
			}
			for _, suffix := range []string{"attacher", "provisioner", "resizer", "snapshotter", "node-driver-registrar", "liveness-probe"} {
				if flags["--csi-"+suffix+"-image"] != "" {
					continue
				}
				envName, count := "CSI_"+strings.ToUpper(strings.ReplaceAll(suffix, "-", "_"))+"_IMAGE", 0
				for _, env := range container.Env {
					if env.Name == envName {
						count++
						if env.Value == "" || env.ValueFrom != nil {
							return fmt.Errorf("archived CSI driver image is unresolved")
						}
					}
				}
				if count != 1 {
					return fmt.Errorf("archived CSI driver image is incomplete")
				}
			}
		}
		return nil
	}
	for _, container := range pod.Containers {
		if err := validate(container, false); err != nil {
			return err
		}
	}
	for _, container := range pod.InitContainers {
		if err := validate(container, true); err != nil {
			return err
		}
	}
	for _, container := range pod.EphemeralContainers {
		if isArchivedManagerContainer(corev1.Container{Name: container.Name, Image: container.Image, Command: container.Command, Args: container.Args}, image) {
			return fmt.Errorf("archived workload contains unsupported ephemeral manager execution")
		}
	}
	return nil
}
