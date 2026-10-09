package controller

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/kubernetes/pkg/controller"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	ctrlmetrics "github.com/longhorn/longhorn-manager/metrics_collector/controller_runtime"
)

// getMetricValue returns the value of the metric of the collector matching the labels.
// For histograms, the sample count is returned.
func getMetricValue(t *testing.T, collector prometheus.Collector, labels map[string]string) float64 {
	t.Helper()

	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(collector))

	mfs, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			matched := 0
			for _, l := range m.GetLabel() {
				if v, ok := labels[l.GetName()]; ok && v == l.GetValue() {
					matched++
				}
			}
			if matched != len(labels) || len(m.GetLabel()) != len(labels) {
				continue
			}
			switch {
			case m.GetCounter() != nil:
				return m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				return m.GetGauge().GetValue()
			case m.GetHistogram() != nil:
				return float64(m.GetHistogram().GetSampleCount())
			}
		}
	}
	require.FailNow(t, "metric not found", "labels: %v", labels)
	return 0
}

// resetReconcileMetrics drops the series of the controller since the metrics are process-global.
func resetReconcileMetrics(name string) {
	ctrlmetrics.WorkerCount.DeleteLabelValues(name)
	ctrlmetrics.ActiveWorkers.DeleteLabelValues(name)
	ctrlmetrics.ReconcileErrors.DeleteLabelValues(name)
	ctrlmetrics.ReconcilePanics.DeleteLabelValues(name)
	ctrlmetrics.ReconcileTime.DeleteLabelValues(name)
	for _, result := range []string{ctrlmetrics.LabelSuccess, ctrlmetrics.LabelRequeue, ctrlmetrics.LabelError} {
		ctrlmetrics.ReconcileTotal.DeleteLabelValues(name, result)
	}
}

func newTestBaseControllerWithMetrics(t *testing.T, name string, workers int) *baseController {
	resetReconcileMetrics(name)
	t.Cleanup(func() { resetReconcileMetrics(name) })

	c := newBaseController(name, logrus.StandardLogger())
	t.Cleanup(c.queue.ShutDown)
	c.initReconcileMetrics(workers)
	return c
}

func TestBaseControllerInitReconcileMetrics(t *testing.T) {
	name := "longhorn-test-init-reconcile-metrics"
	newTestBaseControllerWithMetrics(t, name, 5)

	controllerLabels := map[string]string{ctrlmetrics.LabelController: name}
	require.Equal(t, float64(5), getMetricValue(t, ctrlmetrics.WorkerCount, controllerLabels))
	require.Equal(t, float64(0), getMetricValue(t, ctrlmetrics.ActiveWorkers, controllerLabels))
	require.Equal(t, float64(0), getMetricValue(t, ctrlmetrics.ReconcileErrors, controllerLabels))
	require.Equal(t, float64(0), getMetricValue(t, ctrlmetrics.ReconcilePanics, controllerLabels))
	require.Equal(t, float64(0), getMetricValue(t, ctrlmetrics.ReconcileTime, controllerLabels))
	for _, result := range []string{ctrlmetrics.LabelSuccess, ctrlmetrics.LabelRequeue, ctrlmetrics.LabelError} {
		require.Equal(t, float64(0), getMetricValue(t, ctrlmetrics.ReconcileTotal,
			map[string]string{ctrlmetrics.LabelController: name, ctrlmetrics.LabelResult: result}))
	}
}

func TestBaseControllerSyncWithMetrics(t *testing.T) {
	conflictErr := apierrors.NewConflict(schema.GroupResource{Group: "longhorn.io", Resource: "volumes"}, "test", fmt.Errorf("conflict"))

	testCases := map[string]struct {
		syncErr        error
		expectedResult string
		expectedErrors float64
	}{
		"success": {
			syncErr:        nil,
			expectedResult: ctrlmetrics.LabelSuccess,
			expectedErrors: 0,
		},
		"conflict error is requeued": {
			syncErr:        conflictErr,
			expectedResult: ctrlmetrics.LabelRequeue,
			expectedErrors: 0,
		},
		"wrapped conflict error is requeued": {
			syncErr:        fmt.Errorf("failed to update: %w", conflictErr),
			expectedResult: ctrlmetrics.LabelRequeue,
			expectedErrors: 0,
		},
		"invalid state error is requeued": {
			syncErr:        &types.ErrorInvalidState{Reason: "waiting for state transition"},
			expectedResult: ctrlmetrics.LabelRequeue,
			expectedErrors: 0,
		},
		"generic error": {
			syncErr:        fmt.Errorf("failed to sync"),
			expectedResult: ctrlmetrics.LabelError,
			expectedErrors: 1,
		},
	}

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			name := "longhorn-test-" + testName
			c := newTestBaseControllerWithMetrics(t, name, 1)

			controllerLabels := map[string]string{ctrlmetrics.LabelController: name}

			called := 0
			err := c.syncWithMetrics(func() error {
				called++
				require.Equal(t, float64(1), getMetricValue(t, ctrlmetrics.ActiveWorkers, controllerLabels))
				return tc.syncErr
			})
			require.Equal(t, 1, called)
			require.Equal(t, tc.syncErr, err)

			require.Equal(t, float64(0), getMetricValue(t, ctrlmetrics.ActiveWorkers, controllerLabels))
			require.Equal(t, float64(1), getMetricValue(t, ctrlmetrics.ReconcileTime, controllerLabels))
			require.Equal(t, tc.expectedErrors, getMetricValue(t, ctrlmetrics.ReconcileErrors, controllerLabels))
			require.Equal(t, float64(0), getMetricValue(t, ctrlmetrics.ReconcilePanics, controllerLabels))

			for _, result := range []string{ctrlmetrics.LabelSuccess, ctrlmetrics.LabelRequeue, ctrlmetrics.LabelError} {
				expected := float64(0)
				if result == tc.expectedResult {
					expected = 1
				}
				require.Equal(t, expected, getMetricValue(t, ctrlmetrics.ReconcileTotal,
					map[string]string{ctrlmetrics.LabelController: name, ctrlmetrics.LabelResult: result}))
			}
		})
	}
}

func TestBaseControllerSyncWithMetricsPanic(t *testing.T) {
	name := "longhorn-test-panic"
	c := newTestBaseControllerWithMetrics(t, name, 1)

	require.PanicsWithValue(t, "boom", func() {
		_ = c.syncWithMetrics(func() error { panic("boom") })
	})

	controllerLabels := map[string]string{ctrlmetrics.LabelController: name}
	require.Equal(t, float64(0), getMetricValue(t, ctrlmetrics.ActiveWorkers, controllerLabels))
	require.Equal(t, float64(1), getMetricValue(t, ctrlmetrics.ReconcileTime, controllerLabels))
	require.Equal(t, float64(1), getMetricValue(t, ctrlmetrics.ReconcilePanics, controllerLabels))
	require.Equal(t, float64(1), getMetricValue(t, ctrlmetrics.ReconcileErrors, controllerLabels))
	require.Equal(t, float64(1), getMetricValue(t, ctrlmetrics.ReconcileTotal,
		map[string]string{ctrlmetrics.LabelController: name, ctrlmetrics.LabelResult: ctrlmetrics.LabelError}))
	require.Equal(t, float64(0), getMetricValue(t, ctrlmetrics.ReconcileTotal,
		map[string]string{ctrlmetrics.LabelController: name, ctrlmetrics.LabelResult: ctrlmetrics.LabelSuccess}))
}

// TestKubernetesConfigMapControllerRecordsReconcileMetrics drives a real controller
// worker loop to verify processNextWorkItem is wired through syncWithMetrics.
func TestKubernetesConfigMapControllerRecordsReconcileMetrics(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
	ds := datastore.NewDataStoreForNodeLocal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	kc, err := NewKubernetesConfigMapController(logrus.StandardLogger(), ds, scheme.Scheme, kubeClient, TestNode1, TestNamespace)
	require.NoError(t, err)
	t.Cleanup(kc.queue.ShutDown)

	resetReconcileMetrics(kc.name)
	t.Cleanup(func() { resetReconcileMetrics(kc.name) })
	kc.initReconcileMetrics(1)

	// A config map outside the Longhorn namespace is a no-op, while a malformed key fails the sync.
	kc.queue.Add("other-namespace/some-config-map")
	require.True(t, kc.processNextWorkItem())
	kc.queue.Add("malformed/config-map/key")
	require.True(t, kc.processNextWorkItem())

	resultLabels := func(result string) map[string]string {
		return map[string]string{ctrlmetrics.LabelController: kc.name, ctrlmetrics.LabelResult: result}
	}
	controllerLabels := map[string]string{ctrlmetrics.LabelController: kc.name}
	require.Equal(t, float64(1), getMetricValue(t, ctrlmetrics.ReconcileTotal, resultLabels(ctrlmetrics.LabelSuccess)))
	require.Equal(t, float64(1), getMetricValue(t, ctrlmetrics.ReconcileTotal, resultLabels(ctrlmetrics.LabelError)))
	require.Equal(t, float64(0), getMetricValue(t, ctrlmetrics.ReconcileTotal, resultLabels(ctrlmetrics.LabelRequeue)))
	require.Equal(t, float64(1), getMetricValue(t, ctrlmetrics.ReconcileErrors, controllerLabels))
	require.Equal(t, float64(2), getMetricValue(t, ctrlmetrics.ReconcileTime, controllerLabels))
	require.Equal(t, float64(0), getMetricValue(t, ctrlmetrics.ActiveWorkers, controllerLabels))
}
