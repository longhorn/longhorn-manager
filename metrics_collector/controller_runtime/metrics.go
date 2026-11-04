// Package controller_runtime provides the controller related metrics, similar to the
// ones instrumented by default in sigs.k8s.io/controller-runtime, for the Longhorn
// controllers built on top of client-go.
package controller_runtime

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/metrics_collector/registry"
)

// Metrics subsystem, keys and label values used by the controllers.
const (
	LonghornName        = "longhorn"
	ControllerSubsystem = "controller"

	ReconcileTotalKey          = "reconcile_total"
	ReconcileErrorsKey         = "reconcile_errors_total"
	ReconcilePanicsKey         = "reconcile_panics_total"
	ReconcileTimeKey           = "reconcile_time_seconds"
	MaxConcurrentReconcilesKey = "max_concurrent_reconciles"
	ActiveWorkersKey           = "active_workers"

	LabelController = "controller"
	LabelResult     = "result"

	LabelError   = "error"
	LabelRequeue = "requeue"
	LabelSuccess = "success"
)

var (
	// ReconcileTotal is a prometheus counter metric which holds the total
	// number of reconciliations per controller. It has two labels. controller label refers
	// to the controller name and result label refers to the reconcile result i.e.
	// success, error, requeue.
	ReconcileTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: LonghornName,
		Subsystem: ControllerSubsystem,
		Name:      ReconcileTotalKey,
		Help: "Total number of reconciliations per controller. result=requeue counts reconciles that " +
			"failed with an expected, retried error (update conflict or invalid state); these are " +
			"not counted in reconcile_errors_total.",
	}, []string{LabelController, LabelResult})

	// ReconcileErrors is a prometheus counter metric which holds the total
	// number of errors from the reconciler.
	ReconcileErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: LonghornName,
		Subsystem: ControllerSubsystem,
		Name:      ReconcileErrorsKey,
		Help:      "Total number of reconciliation errors per controller, excluding conflict and invalid state errors",
	}, []string{LabelController})

	// ReconcilePanics is a prometheus counter metric which holds the total
	// number of panics from the reconciler.
	ReconcilePanics = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: LonghornName,
		Subsystem: ControllerSubsystem,
		Name:      ReconcilePanicsKey,
		Help:      "Total number of reconciliation panics per controller",
	}, []string{LabelController})

	// ReconcileTime is a prometheus metric which keeps track of the duration
	// of reconciliations.
	ReconcileTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: LonghornName,
		Subsystem: ControllerSubsystem,
		Name:      ReconcileTimeKey,
		Help:      "Length of time per reconciliation per controller",
		// Kept short because every longhorn-manager pod exports these series for every controller.
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
	}, []string{LabelController})

	// WorkerCount is a prometheus metric which holds the number of
	// concurrent reconciles per controller.
	WorkerCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: LonghornName,
		Subsystem: ControllerSubsystem,
		Name:      MaxConcurrentReconcilesKey,
		Help:      "Maximum number of concurrent reconciles per controller",
	}, []string{LabelController})

	// ActiveWorkers is a prometheus metric which holds the number
	// of active workers per controller.
	ActiveWorkers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: LonghornName,
		Subsystem: ControllerSubsystem,
		Name:      ActiveWorkersKey,
		Help:      "Number of currently used workers per controller",
	}, []string{LabelController})

	metrics = []prometheus.Collector{
		ReconcileTotal, ReconcileErrors, ReconcilePanics, ReconcileTime, WorkerCount, ActiveWorkers,
	}
)

func init() {
	for _, m := range metrics {
		if err := registry.Register(m); err != nil {
			logrus.WithError(err).WithField("metric", m).Error("Failed to register controller metrics")
		}
	}
}
