package upgrade

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func newDummyPod(name string, image string) *corev1.Pod {
	return &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  name,
					Image: image,
				},
			},
		},
	}
}

func TestIsOldManagerPod(t *testing.T) {
	tests := []struct {
		runningManagerImage string
		deployManagerImage  string
		isOld               bool
	}{
		{
			runningManagerImage: "127.0.0.1:31999/longhornio/longhorn-manager:v1.8.0-zarf-4048341149",
			deployManagerImage:  "longhornio/longhorn-manager:v1.8.0",
			isOld:               false,
		},
		{
			runningManagerImage: "some.local.oci:5000/longhornio/longhorn-manager:v1.8.0-xyz423434.4.5.6.4-3-5",
			deployManagerImage:  "longhornio/longhorn-manager:v1.8.0",
			isOld:               false,
		},

		{
			runningManagerImage: "longhornio/longhorn-manager:v1.8.0",
			deployManagerImage:  "longhornio/longhorn-manager:v1.8.0",
			isOld:               false,
		},
		{
			runningManagerImage: "longhornio/longhorn-manager:v1.8.0",
			deployManagerImage:  "longhornio/longhorn-manager:v1.9.0",
			isOld:               true,
		},
		{
			runningManagerImage: "some.local.oci:5000/longhornio/longhorn-manager:v1.8.0-xyz423434.4.5.6.4",
			deployManagerImage:  "longhornio/longhorn-manager:v1.9.0",
			isOld:               true,
		},
	}

	for _, tt := range tests {
		runningPods := newDummyPod("longhorn-manager", tt.runningManagerImage)
		isOld, _ := isOldManagerPod(*runningPods, tt.deployManagerImage)
		assert.Equal(t, tt.isOld, isOld,
			fmt.Sprintf("compering both images version \n	-%s (%s)\n	-%s (%s)",
				tt.runningManagerImage, extractSemver(tt.runningManagerImage),
				tt.deployManagerImage, extractSemver(tt.deployManagerImage),
			),
		)
	}
}
