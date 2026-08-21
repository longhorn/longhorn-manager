package csi

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestKataConfidentialDirectVolumeDeploymentInputs(t *testing.T) {
	provisioner := NewProvisionerDeployment(
		"longhorn-system", "longhorn-service-account", "provisioner:exact", "/var/lib/kubelet", 1,
		DefaultCSIPodAntiAffinityPreset, nil, "", "", "", corev1.PullIfNotPresent, nil, nil,
	)
	if !containsString(provisioner.deployment.Spec.Template.Spec.Containers[0].Args, "--extra-create-metadata=true") {
		t.Fatal("external-provisioner is missing --extra-create-metadata=true")
	}

	plugin := NewPluginDeployment(
		"longhorn-system", "longhorn-service-account", "registrar:exact", "liveness:exact", "manager:exact",
		"http://longhorn-backend:9500/v1", "/var/lib/kubelet", nil, "", "", "", corev1.PullIfNotPresent,
		nil, &longhorn.Setting{Value: string(types.CniNetworkNone)}, nil,
	)
	var pluginContainer *corev1.Container
	for i := range plugin.daemonSet.Spec.Template.Spec.Containers {
		container := &plugin.daemonSet.Spec.Template.Spec.Containers[i]
		if container.Name == types.CSIPluginName {
			pluginContainer = container
			break
		}
	}
	if pluginContainer == nil {
		t.Fatal("CSI plugin container missing")
	}
	foundMount := false
	for _, volumeMount := range pluginContainer.VolumeMounts {
		if volumeMount.Name == "kata-confidential-direct-volume-state" && volumeMount.MountPath == kataConfidentialDirectVolumeStateDir {
			foundMount = true
		}
	}
	if !foundMount {
		t.Fatal("CSI plugin is missing confidential direct-volume state mount")
	}
	foundVolume := false
	for _, volume := range plugin.daemonSet.Spec.Template.Spec.Volumes {
		if volume.Name == "kata-confidential-direct-volume-state" && volume.HostPath != nil && volume.HostPath.Path == kataConfidentialDirectVolumeStateDir && volume.HostPath.Type != nil && *volume.HostPath.Type == corev1.HostPathDirectoryOrCreate {
			foundVolume = true
		}
	}
	if !foundVolume {
		t.Fatal("CSI plugin is missing confidential direct-volume state hostPath")
	}
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
