package util

import (
	"testing"

	"github.com/stretchr/testify/assert"

	corev1 "k8s.io/api/core/v1"
)

func TestPodContainerIDsCountsEveryContainerKind(t *testing.T) {
	pod := corev1.Pod{}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{ContainerID: "c2"}, {ContainerID: "c1"}}
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{ContainerID: "init"}}
	pod.Status.EphemeralContainerStatuses = []corev1.ContainerStatus{{ContainerID: "debug"}, {ContainerID: ""}}

	assert.Equal(t, []string{"c1", "c2", "debug", "init"}, PodContainerIDs(&pod))
}

func TestPodUsesPVC(t *testing.T) {
	pod := corev1.Pod{
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc"},
				},
			}},
		},
	}
	assert.True(t, PodUsesPVC(&pod, "pvc"))
	assert.False(t, PodUsesPVC(&pod, "other"))
}

func TestPodUsesPVCGenericEphemeralVolume(t *testing.T) {
	pod := corev1.Pod{}
	pod.Name = "db-0"
	pod.Spec.Volumes = []corev1.Volume{{
		Name: "data",
		VolumeSource: corev1.VolumeSource{
			Ephemeral: &corev1.EphemeralVolumeSource{},
		},
	}}
	assert.True(t, PodUsesPVC(&pod, "db-0-data"))
	assert.False(t, PodUsesPVC(&pod, "db-0-other"))
	assert.False(t, PodUsesPVC(&pod, "data"))
}
