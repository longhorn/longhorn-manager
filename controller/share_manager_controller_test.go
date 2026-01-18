package controller

import (
	"reflect"
	"testing"

	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestShareManagerController_splitFormatOptions(t *testing.T) {
	type args struct {
		sc *storagev1.StorageClass
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "mkfsParams with no mkfsParams",
			args: args{
				sc: &storagev1.StorageClass{},
			},
			want: nil,
		},
		{
			name: "mkfsParams with empty options",
			args: args{
				sc: &storagev1.StorageClass{
					Parameters: map[string]string{
						"mkfsParams": "",
					},
				},
			},
			want: nil,
		},
		{
			name: "mkfsParams with multiple options",
			args: args{
				sc: &storagev1.StorageClass{
					Parameters: map[string]string{
						"mkfsParams": "-O someopt -L label -n",
					},
				},
			},
			want: []string{"-O someopt", "-L label", "-n"},
		},
		{
			name: "mkfsParams with underscore options",
			args: args{
				sc: &storagev1.StorageClass{
					Parameters: map[string]string{
						"mkfsParams": "-O someopt -label_value test -L label -n",
					},
				},
			},
			want: []string{"-O someopt", "-label_value test", "-L label", "-n"},
		},
		{
			name: "mkfsParams with quoted options",
			args: args{
				sc: &storagev1.StorageClass{
					Parameters: map[string]string{
						"mkfsParams": "-O someopt -label_value \"test\" -L label -n",
					},
				},
			},
			want: []string{"-O someopt", "-label_value \"test\"", "-L label", "-n"},
		},
		{
			name: "mkfsParams with equal sign quoted options",
			args: args{
				sc: &storagev1.StorageClass{
					Parameters: map[string]string{
						"mkfsParams": "-O someopt -label_value=\"test\" -L label -n",
					},
				},
			},
			want: []string{"-O someopt", "-label_value=\"test\"", "-L label", "-n"},
		},
		{
			name: "mkfsParams with equal sign quoted options with spaces",
			args: args{
				sc: &storagev1.StorageClass{
					Parameters: map[string]string{
						"mkfsParams": "-O someopt -label_value=\"test label \" -L label -n",
					},
				},
			},
			want: []string{"-O someopt", "-label_value=\"test label \"", "-L label", "-n"},
		},
		{
			name: "mkfsParams with equal sign quoted options and different spacing",
			args: args{
				sc: &storagev1.StorageClass{
					Parameters: map[string]string{
						"mkfsParams": "-n -O someopt -label_value=\"test\" -Llabel",
					},
				},
			},
			want: []string{"-n", "-O someopt", "-label_value=\"test\"", "-Llabel"},
		},
		{
			name: "mkfsParams with special characters in options",
			args: args{
				sc: &storagev1.StorageClass{
					Parameters: map[string]string{
						"mkfsParams": "-I 256 -b 4096 -O ^metadata_csum,^64bit",
					},
				},
			},
			want: []string{"-I 256", "-b 4096", "-O ^metadata_csum,^64bit"},
		},
		{
			name: "mkfsParams with no spacing in options",
			args: args{
				sc: &storagev1.StorageClass{
					Parameters: map[string]string{
						"mkfsParams": "-Osomeopt -Llabel",
					},
				},
			},
			want: []string{"-Osomeopt", "-Llabel"},
		},
		{
			name: "mkfsParams with different spacing between options",
			args: args{
				sc: &storagev1.StorageClass{
					Parameters: map[string]string{
						"mkfsParams": "-Osomeopt -L label",
					},
				},
			},
			want: []string{"-Osomeopt", "-L label"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ShareManagerController{
				baseController: newBaseController("test-controller", logrus.StandardLogger()),
			}
			if got := c.splitFormatOptions(tt.args.sc); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("splitFormatOptions() = %v (len %d), want %v (len %d)",
					got, len(got), tt.want, len(tt.want))
			}
		})
	}
}

func TestSyncShareManagerCurrentImage(t *testing.T) {
	sm := &longhorn.ShareManager{
		Status: longhorn.ShareManagerStatus{
			CurrentImage: "previous-image",
		},
	}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "sidecar",
					Image: "sidecar:latest",
				},
				{
					Name:  types.LonghornLabelShareManager,
					Image: "share-manager:image",
				},
			},
		},
	}

	syncShareManagerCurrentImage(sm, pod)
	if sm.Status.CurrentImage != "share-manager:image" {
		t.Fatalf("expected current image to be %q, got %q", "share-manager:image", sm.Status.CurrentImage)
	}

	sm.Status.CurrentImage = "should-be-cleared"
	syncShareManagerCurrentImage(sm, nil)
	if sm.Status.CurrentImage != "" {
		t.Fatalf("expected current image to be cleared when pod is nil, got %q", sm.Status.CurrentImage)
	}
}
