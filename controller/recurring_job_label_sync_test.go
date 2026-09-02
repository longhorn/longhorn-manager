package controller

import (
	"testing"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
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
