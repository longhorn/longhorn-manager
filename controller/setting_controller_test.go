package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakediscovery "k8s.io/client-go/discovery/fake"
	clienttest "k8s.io/client-go/testing"
	metricsfake "k8s.io/metrics/pkg/client/clientset/versioned/fake"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
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

func TestCountCPUCoresFromMask(t *testing.T) {
	tests := []struct {
		name string
		mask string
		want int
	}{
		{
			name: "empty string",
			mask: "",
			want: 0,
		},
		{
			name: "single core 0x1",
			mask: "0x1",
			want: 1,
		},
		{
			name: "two cores 0x3",
			mask: "0x3",
			want: 2,
		},
		{
			name: "four cores 0xf",
			mask: "0xf",
			want: 4,
		},
		{
			name: "eight cores 0xff",
			mask: "0xff",
			want: 8,
		},
		{
			name: "non-contiguous bits 0xa5",
			mask: "0xa5",
			want: 4,
		},
		{
			name: "uppercase prefix 0X0F",
			mask: "0X0F",
			want: 4,
		},
		{
			name: "no prefix plain hex ff",
			mask: "ff",
			want: 8,
		},
		{
			name: "large mask 0xffffffff",
			mask: "0xffffffff",
			want: 32,
		},
		{
			name: "whitespace around mask",
			mask: "  0xff  ",
			want: 8,
		},
		{
			name: "invalid hex string",
			mask: "xyz",
			want: 0,
		},
		{
			name: "single bit high position 0x100",
			mask: "0x100",
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := countCPUCoresFromMask(tt.mask); got != tt.want {
				t.Errorf("countCPUCoresFromMask(%q) = %v, want %v", tt.mask, got, tt.want)
			}
		})
	}
}

func TestGetVolumeSizeBucket(t *testing.T) {
	const (
		gib = int64(1 << 30)
		tib = int64(1 << 40)
	)

	tests := []struct {
		name string
		size int64
		want string
	}{
		{name: "less than 1GiB", size: gib - 1, want: "LessThan1GiB"},
		{name: "exactly 1GiB", size: gib, want: "1To2GiB"},
		{name: "just below 2GiB", size: 2*gib - 1, want: "1To2GiB"},
		{name: "exactly 2GiB", size: 2 * gib, want: "2To5GiB"},
		{name: "just below 5GiB", size: 5*gib - 1, want: "2To5GiB"},
		{name: "exactly 5GiB", size: 5 * gib, want: "5To10GiB"},
		{name: "just below 10GiB", size: 10*gib - 1, want: "5To10GiB"},
		{name: "exactly 10GiB", size: 10 * gib, want: "10To20GiB"},
		{name: "just below 20GiB", size: 20*gib - 1, want: "10To20GiB"},
		{name: "exactly 20GiB", size: 20 * gib, want: "20To50GiB"},
		{name: "just below 50GiB", size: 50*gib - 1, want: "20To50GiB"},
		{name: "exactly 50GiB", size: 50 * gib, want: "50To100GiB"},
		{name: "just below 100GiB", size: 100*gib - 1, want: "50To100GiB"},
		{name: "exactly 100GiB", size: 100 * gib, want: "100To200GiB"},
		{name: "just below 200GiB", size: 200*gib - 1, want: "100To200GiB"},
		{name: "exactly 200GiB", size: 200 * gib, want: "200To500GiB"},
		{name: "just below 500GiB", size: 500*gib - 1, want: "200To500GiB"},
		{name: "exactly 500GiB", size: 500 * gib, want: "500GiBTo1TiB"},
		{name: "just below 1TiB", size: tib - 1, want: "500GiBTo1TiB"},
		{name: "exactly 1TiB", size: tib, want: "1To2TiB"},
		{name: "just below 2TiB", size: 2*tib - 1, want: "1To2TiB"},
		{name: "exactly 2TiB", size: 2 * tib, want: "Gt2TiB"},
		{name: "greater than 2TiB", size: 2*tib + 1, want: "Gt2TiB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getVolumeSizeBucket(tt.size); got != tt.want {
				t.Errorf("getVolumeSizeBucket(%d) = %q, want %q", tt.size, got, tt.want)
			}
		})
	}
}

func TestUpdateEngineImagePodLivenessProbes(t *testing.T) {
	originalSkipListerCheck := datastore.SkipListerCheck
	datastore.SkipListerCheck = true
	defer func() {
		datastore.SkipListerCheck = originalSkipListerCheck
	}()

	kubeClient := fake.NewSimpleClientset()                   // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                   // nolint: staticcheck
	extensionClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
	settingIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	daemonSetIndexer := informerFactories.KubeNamespaceFilteredInformerFactory.Apps().V1().DaemonSets().Informer().GetIndexer()

	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionClient, informerFactories)
	sc := &SettingController{
		baseController: newBaseController("longhorn-setting", logrus.StandardLogger()),
		ds:             ds,
	}

	for _, setting := range []*longhorn.Setting{
		newSetting(string(types.SettingNameEngineImagePodLivenessProbePeriod), "30"),
		newSetting(string(types.SettingNameEngineImagePodLivenessProbeTimeout), "15"),
		newSetting(string(types.SettingNameEngineImagePodLivenessProbeFailureThreshold), "10"),
	} {
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create setting %s: %v", setting.Name, err)
		}
		if err := settingIndexer.Add(setting); err != nil {
			t.Fatalf("failed to index setting %s: %v", setting.Name, err)
		}
	}

	engineImageDaemonSet := newEngineImageDaemonSet()
	engineImageDaemonSet.Spec.Template.Spec.Containers[0].LivenessProbe = &corev1.Probe{
		PeriodSeconds:    datastore.PodProbePeriodSeconds,
		TimeoutSeconds:   datastore.PodProbeTimeoutSeconds,
		FailureThreshold: datastore.PodLivenessProbeFailureThreshold,
	}

	daemonSet, err := kubeClient.AppsV1().DaemonSets(TestNamespace).Create(context.TODO(), engineImageDaemonSet, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create daemonset: %v", err)
	}
	if err := daemonSetIndexer.Add(daemonSet); err != nil {
		t.Fatalf("failed to index daemonset: %v", err)
	}

	if err := sc.updateEngineImagePodLivenessProbes(); err != nil {
		t.Fatalf("failed to update engine image pod liveness probes: %v", err)
	}

	updatedDaemonSet, err := kubeClient.AppsV1().DaemonSets(TestNamespace).Get(context.TODO(), getTestEngineImageDaemonSetName(), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get updated daemonset: %v", err)
	}

	livenessProbe := updatedDaemonSet.Spec.Template.Spec.Containers[0].LivenessProbe
	if livenessProbe.PeriodSeconds != 30 {
		t.Fatalf("unexpected periodSeconds: got %d, want 30", livenessProbe.PeriodSeconds)
	}
	if livenessProbe.TimeoutSeconds != 15 {
		t.Fatalf("unexpected timeoutSeconds: got %d, want 15", livenessProbe.TimeoutSeconds)
	}
	if livenessProbe.FailureThreshold != 10 {
		t.Fatalf("unexpected failureThreshold: got %d, want 10", livenessProbe.FailureThreshold)
	}
}

func TestUpdateEngineImagePodLivenessProbesUsesDefaultValuesOnSettingError(t *testing.T) {
	originalSkipListerCheck := datastore.SkipListerCheck
	datastore.SkipListerCheck = true
	defer func() {
		datastore.SkipListerCheck = originalSkipListerCheck
	}()

	kubeClient := fake.NewSimpleClientset()                   // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                   // nolint: staticcheck
	extensionClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
	daemonSetIndexer := informerFactories.KubeNamespaceFilteredInformerFactory.Apps().V1().DaemonSets().Informer().GetIndexer()

	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionClient, informerFactories)
	sc := &SettingController{
		baseController: newBaseController("longhorn-setting", logrus.StandardLogger()),
		ds:             ds,
	}

	engineImageDaemonSet := newEngineImageDaemonSet()
	engineImageDaemonSet.Spec.Template.Spec.Containers[0].LivenessProbe = &corev1.Probe{
		PeriodSeconds:    30,
		TimeoutSeconds:   15,
		FailureThreshold: 10,
	}

	daemonSet, err := kubeClient.AppsV1().DaemonSets(TestNamespace).Create(context.TODO(), engineImageDaemonSet, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create daemonset: %v", err)
	}
	if err := daemonSetIndexer.Add(daemonSet); err != nil {
		t.Fatalf("failed to index daemonset: %v", err)
	}

	if err := sc.updateEngineImagePodLivenessProbes(); err != nil {
		t.Fatalf("failed to update engine image pod liveness probes: %v", err)
	}

	updatedDaemonSet, err := kubeClient.AppsV1().DaemonSets(TestNamespace).Get(context.TODO(), getTestEngineImageDaemonSetName(), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get updated daemonset: %v", err)
	}

	livenessProbe := updatedDaemonSet.Spec.Template.Spec.Containers[0].LivenessProbe
	if livenessProbe.PeriodSeconds != datastore.PodProbePeriodSeconds {
		t.Fatalf("unexpected periodSeconds: got %d, want %d", livenessProbe.PeriodSeconds, datastore.PodProbePeriodSeconds)
	}
	if livenessProbe.TimeoutSeconds != datastore.PodProbeTimeoutSeconds {
		t.Fatalf("unexpected timeoutSeconds: got %d, want %d", livenessProbe.TimeoutSeconds, datastore.PodProbeTimeoutSeconds)
	}
	if livenessProbe.FailureThreshold != datastore.PodLivenessProbeFailureThreshold {
		t.Fatalf("unexpected failureThreshold: got %d, want %d", livenessProbe.FailureThreshold, datastore.PodLivenessProbeFailureThreshold)
	}
}

func TestGetCheckUpgradeRequestExtraInfo(t *testing.T) {
	const (
		fakeGitVersion = "v1.30.0"
		controllerID   = "node1"
	)

	tests := []struct {
		name          string
		setup         func(t *testing.T, kubeClient *fake.Clientset, lhClient *lhfake.Clientset, ds *datastore.DataStore)
		wantTags      map[string]any
		wantFields    map[string]any
		wantErr       bool
		wantLogSubStr []string
	}{
		{
			name: "kube server version error - returns error",
			setup: func(t *testing.T, kubeClient *fake.Clientset, lhClient *lhfake.Clientset, ds *datastore.DataStore) {
				fd := kubeClient.Discovery().(*fakediscovery.FakeDiscovery)
				fd.PrependReactor("get", "version", func(action clienttest.Action) (bool, runtime.Object, error) {
					return true, nil, fmt.Errorf("api error")
				})
			},
			wantErr: true,
		},
		{
			name: "invalid usage setting value - logs warning and returns nil maps",
			setup: func(t *testing.T, kubeClient *fake.Clientset, lhClient *lhfake.Clientset, ds *datastore.DataStore) {
				createBoolSetting(t, lhClient, types.SettingNameAllowCollectingLonghornUsage, "invalid")
			},
			wantLogSubStr: []string{"Failed to get Setting", string(types.SettingNameAllowCollectingLonghornUsage)},
		},
		{
			name: "usage collection disabled - returns kubernetes version tag only",
			setup: func(t *testing.T, kubeClient *fake.Clientset, lhClient *lhfake.Clientset, ds *datastore.DataStore) {
				createBoolSetting(t, lhClient, types.SettingNameAllowCollectingLonghornUsage, "false")
			},
			wantTags: map[string]any{
				"kubernetesVersion": fakeGitVersion,
			},
			wantFields: nil,
		},
		{
			name: "responsible node lookup failure - logs warning and returns nil maps",
			setup: func(t *testing.T, kubeClient *fake.Clientset, lhClient *lhfake.Clientset, ds *datastore.DataStore) {
				createBoolSetting(t, lhClient, types.SettingNameAllowCollectingLonghornUsage, "true")
			},
			wantLogSubStr: []string{"Failed to get responsible Node"},
		},
		{
			name: "usage collection enabled and node is responsible - returns node and cluster info, no error",
			setup: func(t *testing.T, kubeClient *fake.Clientset, lhClient *lhfake.Clientset, ds *datastore.DataStore) {
				createBoolSetting(t, lhClient, types.SettingNameAllowCollectingLonghornUsage, "true")
				createLonghornNode(t, lhClient, controllerID)
			},
			wantTags: map[string]any{
				"kubernetesVersion": fakeGitVersion,
				"longhornDistro":    "unknown",
			},
			wantFields: map[string]any{
				"longhornNodeCount":              1,
				"longhornVolumeNumberOfReplicas": 0,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kubeClient := fake.NewSimpleClientset()
			lhClient := lhfake.NewClientset()
			extensionClient := apiextensionsfake.NewSimpleClientset()
			metricsClient := metricsfake.NewSimpleClientset()

			fd := kubeClient.Discovery().(*fakediscovery.FakeDiscovery)
			fd.FakedServerVersion = &version.Info{GitVersion: fakeGitVersion}

			informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
			ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionClient, informerFactories)

			logger, hook := test.NewNullLogger()
			sc := &SettingController{
				baseController: newBaseController("longhorn-setting", logger),
				ds:             ds,
				kubeClient:     kubeClient,
				metricsClient:  metricsClient,
				controllerID:   controllerID,
				namespace:      TestNamespace,
			}

			if tc.setup != nil {
				tc.setup(t, kubeClient, lhClient, ds)
			}

			stopCh := make(chan struct{})
			defer close(stopCh)
			informerFactories.Start(stopCh)
			if !cache.WaitForCacheSync(stopCh,
				informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().HasSynced,
				informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().HasSynced,
			) {
				t.Fatal("failed to sync Longhorn informer caches")
			}

			gotTags, gotFields, err := sc.GetCheckUpgradeRequestExtraInfo()

			if tc.wantErr && err == nil {
				t.Fatalf("expected an error, got nil")
			}

			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			if tc.wantTags == nil {
				if len(gotTags) != 0 {
					t.Fatalf("expected no tags, got %d", len(gotTags))
				}
			} else {
				for key, want := range tc.wantTags {
					if got, ok := gotTags[key]; !ok || !reflect.DeepEqual(got, want) {
						t.Errorf("unexpected tag %q: expected %v, got %v", key, want, got)
					}
				}
			}

			if tc.wantFields == nil {
				if len(gotFields) != 0 {
					t.Errorf("unexpected fields: expected empty map, got %v", gotFields)
				}
			} else {
				for key, want := range tc.wantFields {
					if got, ok := gotFields[key]; !ok || !reflect.DeepEqual(got, want) {
						t.Errorf("unexpected field %q: expected %v, got %v", key, want, got)
					}
				}
			}

			for _, msg := range tc.wantLogSubStr {
				found := false
				for _, entry := range hook.AllEntries() {
					if strings.Contains(entry.Message, msg) {
						found = true
						break
					}
				}
				if !found {
					var msgs []string
					for _, e := range hook.AllEntries() {
						msgs = append(msgs, fmt.Sprintf("[%s] %s", e.Level, e.Message))
					}
					t.Errorf("expected a log entry containing %q, got: %v", msg, msgs)
				}
			}
		})
	}
}

func createBoolSetting(t *testing.T, lhClient *lhfake.Clientset, name types.SettingName, value string) {
	t.Helper()
	setting := newSetting(string(name), value)
	if err := lhClient.Tracker().Add(setting); err != nil {
		t.Fatalf("failed to add setting %s: %v", name, err)
	}
}

func createLonghornNode(t *testing.T, lhClient *lhfake.Clientset, name string) {
	t.Helper()
	node := &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: TestNamespace},
		Spec:       longhorn.NodeSpec{Name: name, AllowScheduling: true},
		Status: longhorn.NodeStatus{
			Conditions: []longhorn.Condition{
				{Type: longhorn.NodeConditionTypeReady, Status: longhorn.ConditionStatusTrue},
			},
		},
	}
	if err := lhClient.Tracker().Add(node); err != nil {
		t.Fatalf("failed to add node %s: %v", name, err)
	}
}
