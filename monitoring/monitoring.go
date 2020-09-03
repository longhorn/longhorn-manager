package monitoring

import (
	"net/http"

	"github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/longhorn/longhorn-manager/datastore"
)

// longhornCustomRegistry exposes Longhorn metrics only.
// We use longhornCustomRegistry to get rid of all default Prometheus go-client metrics
var longhornCustomRegistry = prometheus.NewRegistry()

// MustRegister registers the provided Collectors with the longhornCustomRegistry and
// panics if any error occurs.
func MustRegister(collector prometheus.Collector) {
	longhornCustomRegistry.MustRegister(collector)
}

// Handler returns an http.Handler for longhornCustomRegistry, using default HandlerOpts
func Handler() http.Handler {
	return promhttp.HandlerFor(longhornCustomRegistry, promhttp.HandlerOpts{})
}

func InitMonitoringSystem(logger logrus.FieldLogger, currentNodeID string, ds *datastore.DataStore) {
	vc := NewVolumeCollector(logger, currentNodeID, ds)
	nc := NewNodeCollector(logger, currentNodeID, ds)

	MustRegister(vc)
	MustRegister(nc)
}
