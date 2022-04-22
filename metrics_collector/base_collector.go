package metricscollector

import (
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"
)

type baseCollector struct {
	logger        logrus.FieldLogger
	currentNodeID string // the node where this collector running on
	ds            *datastore.DataStore
}

func newBaseCollector(name string, logger logrus.FieldLogger, nodeID string, ds *datastore.DataStore) *baseCollector {
	c := &baseCollector{
		logger:        logger.WithField("collector", name),
		currentNodeID: nodeID,
		ds:            ds,
	}
	return c
}
