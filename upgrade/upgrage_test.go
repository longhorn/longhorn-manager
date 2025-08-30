package upgrade

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newDummyPod(name string, version string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"app.kubernetes.io/version": version,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: name,
				},
			},
		},
	}
}

func TestIsOldManagerPod(t *testing.T) {
	tests := []struct {
		runningPodVersionLabel string
		deployingManagerImage  string
		isOld                  bool
	}{
		{
			runningPodVersionLabel: "v1.8.0",
			deployingManagerImage:  "longhornio/longhorn-manager:v1.8.0",
			isOld:                  false,
		},
		{
			runningPodVersionLabel: "v1.8.0",
			deployingManagerImage:  "some.local.oci:5000/longhornio/longhorn-manager:v1.8.0",
			isOld:                  false,
		},
		{
			runningPodVersionLabel: "v1.8.0",
			deployingManagerImage:  "longhornio/longhorn-manager:v1.8.0-patched4.5.6.4",
			isOld:                  false,
		},
		{
			runningPodVersionLabel: "v1.8.0-dev-20250112",
			deployingManagerImage:  "longhornio/longhorn-manager:-v1.8.0-dev-20250112",
			isOld:                  false,
		},
		{
			runningPodVersionLabel: "v1.8.0",
			deployingManagerImage:  "longhornio/longhorn-manager:v1.9.0",
			isOld:                  true,
		},
		{
			runningPodVersionLabel: "",
			deployingManagerImage:  "some.local.oci:5000/longhornio/longhorn-manager:v1.8.0-dev-20250112",
			isOld:                  true,
		},
		{
			runningPodVersionLabel: "",
			deployingManagerImage:  "longhornio/longhorn-manager:some-non-semver",
			isOld:                  true,
		},
	}

	for _, tt := range tests {
		runningPods := newDummyPod("longhorn-manager", tt.runningPodVersionLabel)
		isOld, _ := isOldManagerPod(*runningPods, tt.deployingManagerImage)

		assert.Equal(t, tt.isOld, isOld,
			fmt.Sprintf("compering both Label: %s to Image %s",
				tt.runningPodVersionLabel, tt.deployingManagerImage,
			),
		)
	}
}
