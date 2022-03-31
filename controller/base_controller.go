package controller

import (
	"github.com/sirupsen/logrus"

	"k8s.io/client-go/util/workqueue"
)

var (
	// maxRetries is the number of times a deployment will be retried before it is dropped out of the queue.
	// With the current rate-limiter in use (5ms*2^(maxRetries-1)) the following numbers represent the times
	// a deployment is going to be requeued:
	//
	// 5ms, 10ms, 20ms, 40ms, 80ms, 160ms, 320ms, 640ms, 1.28s, 2.56s, 5.12s, 10.24s, ....
	maxRetries = 12
)

type baseController struct {
	name   string
	logger logrus.FieldLogger
	queue  workqueue.RateLimitingInterface
}

func newBaseController(name string, logger logrus.FieldLogger) *baseController {
	return newBaseControllerWithQueue(name, logger,
		workqueue.NewNamedRateLimitingQueue(EnhancedDefaultControllerRateLimiter(), name))
}

func newBaseControllerWithQueue(name string, logger logrus.FieldLogger,
	queue workqueue.RateLimitingInterface) *baseController {
	c := &baseController{
		name:   name,
		logger: logger.WithField("controller", name),
		queue:  queue,
	}

	return c
}
