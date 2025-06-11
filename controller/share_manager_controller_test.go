package controller

import (
	"github.com/sirupsen/logrus"
	storagev1 "k8s.io/api/storage/v1"
	"reflect"
	"testing"
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
