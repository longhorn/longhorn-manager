package controller

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	k8scontroller "k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func TestPvcHasRecurringJobLabels(t *testing.T) {
	sourceKey := types.GetRecurringJobSourceLabelKey()
	defaultKey := types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJobGroup, longhorn.RecurringJobGroupDefault)
	rebuildableKey := types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJobGroup, "rebuildable")

	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
		sourceKey: types.LonghornLabelValueEnabled,
	}}}
	ok, err := pvcHasRecurringJobLabels(pvc)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("source label alone is not a RecurringJob assignment")
	}

	pvc.Labels[rebuildableKey] = types.LonghornLabelValueEnabled
	ok, err = pvcHasRecurringJobLabels(pvc)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("group label must count as a RecurringJob assignment")
	}

	pvc = &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
		defaultKey: types.LonghornLabelValueEnabled,
	}}}
	ok, err = pvcHasRecurringJobLabels(pvc)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("default group label must count as a RecurringJob assignment")
	}
}

func TestSyncRecurringJobLabelsKeepsMultipleGroups(t *testing.T) {
	defaultKey := types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJobGroup, longhorn.RecurringJobGroupDefault)
	rebuildableKey := types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJobGroup, "rebuildable")

	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "app",
		Labels: map[string]string{
			types.GetRecurringJobSourceLabelKey(): types.LonghornLabelValueEnabled,
			defaultKey:                            types.LonghornLabelValueEnabled,
			rebuildableKey:                        types.LonghornLabelValueEnabled,
		},
	}}
	vol := &longhorn.Volume{ObjectMeta: metav1.ObjectMeta{
		Name:   "pvc-app",
		Labels: map[string]string{rebuildableKey: types.LonghornLabelValueEnabled},
	}}
	if err := syncRecurringJobLabelsToTargetResource(types.LonghornKindVolume, vol, pvc, logrus.StandardLogger()); err != nil {
		t.Fatal(err)
	}
	if vol.Labels[defaultKey] != types.LonghornLabelValueEnabled {
		t.Fatalf("default group missing: %#v", vol.Labels)
	}
	if vol.Labels[rebuildableKey] != types.LonghornLabelValueEnabled {
		t.Fatalf("rebuildable group missing: %#v", vol.Labels)
	}
}

func TestHasRecurringJobSourceLabel(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
		types.GetRecurringJobSourceLabelKey(): types.LonghornLabelValueEnabled,
	}}}
	ok, err := hasRecurringJobSourceLabel(pvc)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected source label")
	}
}

func TestInferRecurringJobSourceIfNeeded(t *testing.T) {
	sourceKey := types.GetRecurringJobSourceLabelKey()
	defaultKey := types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJobGroup, longhorn.RecurringJobGroupDefault)
	rebuildableKey := types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJobGroup, "rebuildable")
	extraKey := types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJobGroup, "extra")

	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "app",
		Labels: map[string]string{
			defaultKey:     types.LonghornLabelValueEnabled,
			rebuildableKey: types.LonghornLabelValueEnabled,
		},
	}}
	inferred, err := inferRecurringJobSourceIfNeeded(pvc)
	if err != nil {
		t.Fatal(err)
	}
	if !inferred {
		t.Fatal("expected inference when source key is absent")
	}
	if pvc.Labels[sourceKey] != types.LonghornLabelValueEnabled {
		t.Fatalf("source not inferred: %#v", pvc.Labels)
	}

	vol := &longhorn.Volume{ObjectMeta: metav1.ObjectMeta{
		Name: "pvc-app",
		Labels: map[string]string{
			rebuildableKey: types.LonghornLabelValueEnabled,
			extraKey:       types.LonghornLabelValueEnabled,
		},
	}}
	if err := syncRecurringJobLabelsToTargetResource(types.LonghornKindVolume, vol, pvc, logrus.StandardLogger()); err != nil {
		t.Fatal(err)
	}
	if vol.Labels[defaultKey] != types.LonghornLabelValueEnabled {
		t.Fatalf("default group missing: %#v", vol.Labels)
	}
	if vol.Labels[rebuildableKey] != types.LonghornLabelValueEnabled {
		t.Fatalf("rebuildable group missing: %#v", vol.Labels)
	}
	if _, ok := vol.Labels[extraKey]; ok {
		t.Fatalf("volume-only extra group should be removed: %#v", vol.Labels)
	}
}

func TestInferRecurringJobSourceSkipsExplicitOptOut(t *testing.T) {
	sourceKey := types.GetRecurringJobSourceLabelKey()
	defaultKey := types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJobGroup, longhorn.RecurringJobGroupDefault)

	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Labels: map[string]string{
			sourceKey:  "ignored",
			defaultKey: types.LonghornLabelValueEnabled,
		},
	}}
	inferred, err := inferRecurringJobSourceIfNeeded(pvc)
	if err != nil {
		t.Fatal(err)
	}
	if inferred {
		t.Fatal("must not overwrite an explicit non-enabled source value")
	}
	if pvc.Labels[sourceKey] != "ignored" {
		t.Fatalf("opt-out overwritten: %#v", pvc.Labels)
	}
}

func TestSyncPVCRecurringJobLabelsInfersSourceWhenAbsent(t *testing.T) {
	sourceKey := types.GetRecurringJobSourceLabelKey()
	defaultKey := types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJobGroup, longhorn.RecurringJobGroupDefault)
	rebuildableKey := types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJobGroup, "rebuildable")
	volumeOnlyKey := types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJob, "volume-only")

	vc, kubeClient := setupVolumeControllerWithPVC(t, map[string]string{
		defaultKey:     types.LonghornLabelValueEnabled,
		rebuildableKey: types.LonghornLabelValueEnabled,
	})
	vol := boundVolumeWithRecurringJobLabels(map[string]string{
		volumeOnlyKey: types.LonghornLabelValueEnabled,
	})

	if err := vc.syncPVCRecurringJobLabels(vol); err != nil {
		t.Fatal(err)
	}

	pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(TestNamespace).Get(context.TODO(), TestPVCName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pvc.Labels[sourceKey] != types.LonghornLabelValueEnabled {
		t.Fatalf("expected persisted source=enabled, got %#v", pvc.Labels)
	}
	if vol.Labels[defaultKey] != types.LonghornLabelValueEnabled {
		t.Fatalf("default group missing on volume: %#v", vol.Labels)
	}
	if vol.Labels[rebuildableKey] != types.LonghornLabelValueEnabled {
		t.Fatalf("rebuildable group missing on volume: %#v", vol.Labels)
	}
	if _, exists := vol.Labels[volumeOnlyKey]; exists {
		t.Fatalf("volume-only RecurringJob label should be removed: %#v", vol.Labels)
	}
}

func TestSyncPVCRecurringJobLabelsPreservesNonEnabledSource(t *testing.T) {
	sourceKey := types.GetRecurringJobSourceLabelKey()
	defaultKey := types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJobGroup, longhorn.RecurringJobGroupDefault)
	volumeOnlyKey := types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJob, "volume-only")

	vc, kubeClient := setupVolumeControllerWithPVC(t, map[string]string{
		sourceKey:  "ignored",
		defaultKey: types.LonghornLabelValueEnabled,
	})
	vol := boundVolumeWithRecurringJobLabels(map[string]string{
		volumeOnlyKey: types.LonghornLabelValueEnabled,
	})

	if err := vc.syncPVCRecurringJobLabels(vol); err != nil {
		t.Fatal(err)
	}

	pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(TestNamespace).Get(context.TODO(), TestPVCName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pvc.Labels[sourceKey] != "ignored" {
		t.Fatalf("non-enabled source must not be overwritten, got %#v", pvc.Labels)
	}
	if vol.Labels[defaultKey] == types.LonghornLabelValueEnabled {
		t.Fatalf("should not sync PVC groups when source is ignored: %#v", vol.Labels)
	}
	if vol.Labels[volumeOnlyKey] != types.LonghornLabelValueEnabled {
		t.Fatalf("volume-only label must remain when source is ignored: %#v", vol.Labels)
	}
}

func setupVolumeControllerWithPVC(t *testing.T, labels map[string]string) (*VolumeController, *fake.Clientset) {
	t.Helper()

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, k8scontroller.NoResyncPeriodFunc())

	vc, err := newTestVolumeController(lhClient, kubeClient, extensionsClient, informerFactories, TestOwnerID1)
	if err != nil {
		t.Fatal(err)
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestPVCName,
			Namespace: TestNamespace,
			Labels:    labels,
		},
	}
	created, err := kubeClient.CoreV1().PersistentVolumeClaims(TestNamespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer().Add(created); err != nil {
		t.Fatal(err)
	}
	return vc, kubeClient
}

func boundVolumeWithRecurringJobLabels(labels map[string]string) *longhorn.Volume {
	return &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:   TestVolumeName,
			Labels: labels,
		},
		Status: longhorn.VolumeStatus{
			KubernetesStatus: longhorn.KubernetesStatus{
				Namespace: TestNamespace,
				PVCName:   TestPVCName,
			},
		},
	}
}
