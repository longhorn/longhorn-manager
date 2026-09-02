package controller

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestGetBackingImageListenAddress(t *testing.T) {
	tests := []struct {
		name   string
		family string
		port   int
		want   string
	}{
		{
			name:   "blank uses legacy wildcard",
			family: "",
			port:   8500,
			want:   ":8500",
		},
		{
			name:   "ipv4 binds all IPv4 addresses",
			family: "ipv4",
			port:   8500,
			want:   "0.0.0.0:8500",
		},
		{
			name:   "ipv6 binds all IPv6 addresses",
			family: "ipv6",
			port:   8500,
			want:   "[::]:8500",
		},
		{
			name:   "malformed family uses legacy wildcard",
			family: "ipv7",
			port:   8500,
			want:   ":8500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getBackingImageListenAddress(tt.family, tt.port); got != tt.want {
				t.Fatalf("getBackingImageListenAddress(%q, %d) = %q, want %q", tt.family, tt.port, got, tt.want)
			}
		})
	}
}

func TestAppendBackingImageIPFamilyArgs(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		family string
		want   []string
	}{
		{
			name:   "blank leaves args unchanged",
			args:   []string{"daemon", "--listen", ":8500"},
			family: "",
			want:   []string{"daemon", "--listen", ":8500"},
		},
		{
			name:   "ipv4 appends family flag",
			args:   []string{"daemon", "--listen", "0.0.0.0:8500"},
			family: "ipv4",
			want:   []string{"daemon", "--listen", "0.0.0.0:8500", "--ip-family", "ipv4"},
		},
		{
			name:   "ipv6 appends family flag",
			args:   []string{"daemon", "--listen", "[::]:8500"},
			family: "ipv6",
			want:   []string{"daemon", "--listen", "[::]:8500", "--ip-family", "ipv6"},
		},
		{
			name:   "malformed family leaves args unchanged",
			args:   []string{"daemon", "--listen", ":8500"},
			family: "ipv7",
			want:   []string{"daemon", "--listen", ":8500"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := appendBackingImageIPFamilyArgs(tt.args, tt.family)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("appendBackingImageIPFamilyArgs(%#v, %q) = %#v, want %#v", tt.args, tt.family, got, tt.want)
			}
		})
	}
}

func TestIsBackingImagePodIPFamilySynced(t *testing.T) {
	tests := []struct {
		name          string
		containerName string
		containers    []corev1.Container
		desiredFamily string
		want          bool
	}{
		{
			name:          "absent flag matches blank setting",
			containerName: "backing-image-manager",
			containers: []corev1.Container{{
				Name:    "backing-image-manager",
				Command: []string{"daemon"},
			}},
			desiredFamily: "",
			want:          true,
		},
		{
			name:          "blank setting rejects explicit ipv4",
			containerName: "backing-image-manager",
			containers: []corev1.Container{{
				Name:    "backing-image-manager",
				Command: []string{"daemon", "--ip-family", "ipv4"},
			}},
			desiredFamily: "",
			want:          false,
		},
		{
			name:          "ipv4 matches authoritative container",
			containerName: "backing-image-manager",
			containers: []corev1.Container{{
				Name:    "backing-image-manager",
				Command: []string{"daemon", "--ip-family", "ipv4"},
			}},
			desiredFamily: "ipv4",
			want:          true,
		},
		{
			name:          "ipv6 matches authoritative container",
			containerName: "backing-image-manager",
			containers: []corev1.Container{{
				Name:    "backing-image-manager",
				Command: []string{"daemon", "--ip-family=ipv6"},
			}},
			desiredFamily: "ipv6",
			want:          true,
		},
		{
			name:          "ipv6 in args matches authoritative container",
			containerName: "backing-image-manager",
			containers: []corev1.Container{{
				Name:    "backing-image-manager",
				Command: []string{"daemon"},
				Args:    []string{"--ip-family", "ipv6"},
			}},
			desiredFamily: "ipv6",
			want:          true,
		},
		{
			name:          "malformed family is not synced",
			containerName: "backing-image-manager",
			containers: []corev1.Container{{
				Name:    "backing-image-manager",
				Command: []string{"daemon", "--ip-family", "ipv7"},
			}},
			desiredFamily: "ipv4",
			want:          false,
		},
		{
			name:          "duplicate family flags across command and args are not synced",
			containerName: "backing-image-manager",
			containers: []corev1.Container{{
				Name:    "backing-image-manager",
				Command: []string{"daemon", "--ip-family", "ipv4"},
				Args:    []string{"--ip-family=ipv4"},
			}},
			desiredFamily: "ipv4",
			want:          false,
		},
		{
			name:          "mismatched family is not synced",
			containerName: "backing-image-manager",
			containers: []corev1.Container{{
				Name:    "backing-image-manager",
				Command: []string{"daemon", "--ip-family", "ipv6"},
			}},
			desiredFamily: "ipv4",
			want:          false,
		},
		{
			name:          "sidecar-only family is ignored",
			containerName: "backing-image-manager",
			containers: []corev1.Container{{
				Name:    "sidecar",
				Command: []string{"--ip-family", "ipv4"},
			}},
			desiredFamily: "ipv4",
			want:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: tt.containers}}
			if got := isBackingImagePodIPFamilySynced(pod, tt.containerName, tt.desiredFamily); got != tt.want {
				t.Fatalf("isBackingImagePodIPFamilySynced(%#v, %q, %q) = %t, want %t", tt.containers, tt.containerName, tt.desiredFamily, got, tt.want)
			}
		})
	}
}
