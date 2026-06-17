package controller

import (
	"context"
	"reflect"
	"testing"

	"github.com/sirupsen/logrus"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionClient, informerFactories)
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

	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionClient, informerFactories)
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
