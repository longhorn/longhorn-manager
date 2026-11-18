package controller

import (
	"time"

	"github.com/sirupsen/logrus"

	"k8s.io/client-go/util/workqueue"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/longhorn/longhorn-manager/types"

	ctrlmetrics "github.com/longhorn/longhorn-manager/metrics_collector/controller_runtime"
)

var (
	// maxRetries is the number of times a deployment will be retried before it is dropped out of the queue.
	// With the current rate-limiter in use (5ms*2^(maxRetries-1)) the following numbers represent the times
	// a deployment is going to be requeued:
	//
	// 5ms, 10ms, 20ms
	maxRetries = 3
)

type baseController struct {
	name   string
	logger *logrus.Entry
	queue  workqueue.TypedRateLimitingInterface[any]
}

func newBaseController(name string, logger logrus.FieldLogger) *baseController {
	nameConfig := workqueue.TypedRateLimitingQueueConfig[any]{Name: name}
	return newBaseControllerWithQueue(name, logger,
		workqueue.NewTypedRateLimitingQueueWithConfig[any](EnhancedDefaultControllerRateLimiter(), nameConfig))
}

func newBaseControllerWithQueue(name string, logger logrus.FieldLogger,
	queue workqueue.TypedRateLimitingInterface[any]) *baseController {
	c := &baseController{
		name:   name,
		logger: logger.WithField("controller", name),
		queue:  queue,
	}

	return c
}

// initReconcileMetrics records the worker count of the controller and exports
// its reconcile series with zero values so they exist before the first reconcile.
func (c *baseController) initReconcileMetrics(workers int) {
	ctrlmetrics.WorkerCount.WithLabelValues(c.name).Set(float64(workers))
	ctrlmetrics.ActiveWorkers.WithLabelValues(c.name).Set(0)
	ctrlmetrics.ReconcileErrors.WithLabelValues(c.name)
	ctrlmetrics.ReconcilePanics.WithLabelValues(c.name)
	ctrlmetrics.ReconcileTime.WithLabelValues(c.name)
	for _, result := range []string{ctrlmetrics.LabelSuccess, ctrlmetrics.LabelRequeue, ctrlmetrics.LabelError} {
		ctrlmetrics.ReconcileTotal.WithLabelValues(c.name, result)
	}
}

// syncWithMetrics runs the given sync function and records the controller
// reconcile metrics, including the number of active workers, the reconcile
// duration and the reconcile result.
func (c *baseController) syncWithMetrics(sync func() error) (err error) {
	activeWorkers := ctrlmetrics.ActiveWorkers.WithLabelValues(c.name)
	activeWorkers.Inc()
	defer activeWorkers.Dec()

	startTime := time.Now()
	returned := false
	defer func() {
		ctrlmetrics.ReconcileTime.WithLabelValues(c.name).Observe(time.Since(startTime).Seconds())

		switch {
		case !returned:
			// sync is panicking; record it without recovering so the panic keeps propagating.
			ctrlmetrics.ReconcilePanics.WithLabelValues(c.name).Inc()
			ctrlmetrics.ReconcileTotal.WithLabelValues(c.name, ctrlmetrics.LabelError).Inc()
			ctrlmetrics.ReconcileErrors.WithLabelValues(c.name).Inc()
		case err == nil:
			ctrlmetrics.ReconcileTotal.WithLabelValues(c.name, ctrlmetrics.LabelSuccess).Inc()
		case apierrors.IsConflict(err):
			// Conflicts are expected and the object will be requeued and reconciled again.
			ctrlmetrics.ReconcileTotal.WithLabelValues(c.name, ctrlmetrics.LabelRequeue).Inc()
		case types.ErrorIsInvalidState(err):
			ctrlmetrics.ReconcileTotal.WithLabelValues(c.name, ctrlmetrics.LabelRequeue).Inc()
		default:
			ctrlmetrics.ReconcileTotal.WithLabelValues(c.name, ctrlmetrics.LabelError).Inc()
			ctrlmetrics.ReconcileErrors.WithLabelValues(c.name).Inc()
		}
	}()

	err = sync()
	returned = true
	return err
}
