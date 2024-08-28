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
