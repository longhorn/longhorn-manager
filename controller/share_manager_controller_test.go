package controller

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
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

func (s *TestSuite) TestSyncShareManagerVolumePodRecreateBackoff(c *C) {
	skipListerCheck := datastore.SkipListerCheck
	datastore.SkipListerCheck = true
	defer func() { datastore.SkipListerCheck = skipListerCheck }()

	testCases := []struct {
		name          string
		requiredByCSI bool
		faulted       bool
		state         longhorn.ShareManagerState
		expectedState longhorn.ShareManagerState
		expectBackoff bool
	}{
		{
			name:          "running share manager no longer required: regular stop clears the backoff",
			state:         longhorn.ShareManagerStateRunning,
			expectedState: longhorn.ShareManagerStateStopping,
			expectBackoff: false,
		},
		{
			name:          "failed share manager no longer required: the backoff is kept",
			state:         longhorn.ShareManagerStateError,
			expectedState: longhorn.ShareManagerStateStopping,
			expectBackoff: true,
		},
		{
			name:          "starting share manager no longer required: the backoff is kept",
			state:         longhorn.ShareManagerStateStarting,
			expectedState: longhorn.ShareManagerStateStopping,
			expectBackoff: true,
		},
		{
			name:          "running share manager of a faulted volume: the backoff is kept",
			requiredByCSI: true,
			faulted:       true,
			state:         longhorn.ShareManagerStateRunning,
			expectedState: longhorn.ShareManagerStateStopping,
			expectBackoff: true,
		},
		{
			name:          "failed share manager still required: restarting keeps the backoff",
			requiredByCSI: true,
			state:         longhorn.ShareManagerStateError,
			expectedState: longhorn.ShareManagerStateStarting,
			expectBackoff: true,
		},
	}

	for _, tc := range testCases {
		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)
		lhInformers := informerFactories.LhInformerFactory.Longhorn().V1beta2()
		ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

		smc, err := NewShareManagerController(logrus.StandardLogger(), ds, scheme.Scheme, kubeClient, TestNamespace, TestOwnerID1, "")
		c.Assert(err, IsNil)

		node, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(),
			newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, ""), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(lhInformers.Nodes().Informer().GetIndexer().Add(node), IsNil)

		v := newVolume(TestVolumeName, 1)
		v.Spec.AccessMode = longhorn.AccessModeReadWriteMany
		if tc.faulted {
			v.Status.Robustness = longhorn.VolumeRobustnessFaulted
		}
		v, err = lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(context.TODO(), v, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(lhInformers.Volumes().Informer().GetIndexer().Add(v), IsNil)

		va := newVolumeAttachment(TestVolumeName)
		if tc.requiredByCSI {
			va.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{
				"csi-ticket": {ID: "csi-ticket", Type: longhorn.AttacherTypeCSIAttacher, NodeID: TestNode1},
			}
		}
		va, err = lhClient.LonghornV1beta2().VolumeAttachments(TestNamespace).Create(context.TODO(), va, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(lhInformers.VolumeAttachments().Informer().GetIndexer().Add(va), IsNil)

		sm := &longhorn.ShareManager{
			ObjectMeta: metav1.ObjectMeta{Name: TestVolumeName, Namespace: TestNamespace},
			Status:     longhorn.ShareManagerStatus{OwnerID: TestOwnerID1, State: tc.state},
		}

		// a previous pod creation for this share manager started the backoff
		smc.backoff.Next(sm.Name, time.Now())

		err = smc.syncShareManagerVolume(sm)
		c.Assert(err, IsNil, Commentf(tc.name))
		c.Assert(sm.Status.State, Equals, tc.expectedState, Commentf(tc.name))
		c.Assert(smc.backoff.Get(sm.Name) > 0, Equals, tc.expectBackoff, Commentf(tc.name))
	}
}
