package controller_runtime

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/longhorn/longhorn-manager/metrics_collector/registry"
)

func TestMetricsRegistered(t *testing.T) {
	controller := "longhorn-test-registered"
	deleteSeries := func() {
		ReconcileTotal.DeleteLabelValues(controller, LabelSuccess)
		ReconcileErrors.DeleteLabelValues(controller)
		ReconcilePanics.DeleteLabelValues(controller)
		ReconcileTime.DeleteLabelValues(controller)
		WorkerCount.DeleteLabelValues(controller)
		ActiveWorkers.DeleteLabelValues(controller)
	}
	deleteSeries()
	t.Cleanup(deleteSeries)

	ReconcileTotal.WithLabelValues(controller, LabelSuccess).Inc()
	ReconcileErrors.WithLabelValues(controller).Inc()
	ReconcilePanics.WithLabelValues(controller).Inc()
	ReconcileTime.WithLabelValues(controller).Observe(0.1)
	WorkerCount.WithLabelValues(controller).Set(5)
	ActiveWorkers.WithLabelValues(controller).Set(1)

	server := httptest.NewServer(registry.Handler())
	defer server.Close()

	resp, err := http.Get(server.URL)
	require.NoError(t, err)
	defer func() {
		_ = resp.Body.Close()
	}()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	for _, expected := range []string{
		`longhorn_controller_reconcile_total{controller="longhorn-test-registered",result="success"} 1`,
		`longhorn_controller_reconcile_errors_total{controller="longhorn-test-registered"} 1`,
		`longhorn_controller_reconcile_panics_total{controller="longhorn-test-registered"} 1`,
		`longhorn_controller_reconcile_time_seconds_count{controller="longhorn-test-registered"} 1`,
		`longhorn_controller_max_concurrent_reconciles{controller="longhorn-test-registered"} 5`,
		`longhorn_controller_active_workers{controller="longhorn-test-registered"} 1`,
	} {
		require.Contains(t, string(body), expected)
	}
}
