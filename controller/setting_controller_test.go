package controller

import (
	"reflect"
	"testing"
)

func TestGetRegistry(t *testing.T) {
	tests := []struct {
		name  string
		image string
		want  string
	}{
		{
			name:  "ghcr.io with namespace",
			image: "ghcr.io/helloworld/longhorn-manager:master-head-135",
			want:  "ghcr.io/helloworld",
		},
		{
			name:  "docker.io default library",
			image: "nginx:latest",
			want:  "docker.io/library",
		},
		{
			name:  "docker.io with namespace",
			image: "library/ubuntu:20.04",
			want:  "docker.io/library",
		},
		{
			name:  "custom registry with port",
			image: "myregistry.local:5000/team/app:1.0",
			want:  "myregistry.local:5000/team",
		},
		{
			name:  "rancher registry",
			image: "abc.cde.test.io/containers/longhorn-instance-manager:1.10.0-rc1",
			want:  "abc.cde.test.io/containers",
		},
		{
			name:  "single word image",
			image: "busybox",
			want:  "docker.io/library",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getRegistry(tt.image); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getRegistry(%q) = %v, want %v", tt.image, got, tt.want)
			}
		})
	}
}
