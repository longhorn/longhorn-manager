package registry

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// longhornCustomRegistry exposes Longhorn metrics plus the Go runtime and process metrics,
// keeping out whatever third-party libraries register on the Prometheus default registry.
var longhornCustomRegistry = newLonghornRegistry()

func newLonghornRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// Register registers the provided Collector with the longhornCustomRegistry
func Register(collector prometheus.Collector) error {
	return longhornCustomRegistry.Register(collector)
}

// Handler returns an http.Handler for longhornCustomRegistry, using default HandlerOpts
func Handler() http.Handler {
	return promhttp.HandlerFor(longhornCustomRegistry, promhttp.HandlerOpts{})
}
