package registry

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// longhornCustomRegistry exposes Longhorn metrics only.
// We use longhornCustomRegistry to get rid of all default Prometheus go-client metrics
var longhornCustomRegistry = prometheus.NewRegistry()

// Register registers the provided Collector with the longhornCustomRegistry
func Register(collector prometheus.Collector) error {
	return longhornCustomRegistry.Register(collector)
}

// Handler returns an http.Handler for longhornCustomRegistry, using default HandlerOpts
func Handler() http.Handler {
	return promhttp.HandlerFor(longhornCustomRegistry, promhttp.HandlerOpts{})
}
