package client_go_adaper

import (
	"context"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	clientmetrics "k8s.io/client-go/tools/metrics"

	"github.com/longhorn/longhorn-manager/metrics_collector/registry"
)

// this file contains setup logic to initialize the myriad of places
// that client-go registers metrics.  We copy the names and formats
// from Kubernetes so that we match the core controllers.
const (
	LonghornName = "longhorn"
)

var (
	// client metrics
	clientGoRestClientSubsystem = "rest_client"

	requestLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: LonghornName,
			Subsystem: clientGoRestClientSubsystem,
			Name:      "request_latency_seconds",
			Help:      "Request latency in seconds. Broken down by verb and URL.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 10),
		},
		[]string{"verb", "url"},
	)

	rateLimiterLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: LonghornName,
			Subsystem: clientGoRestClientSubsystem,
			Name:      "rate_limiter_latency_seconds",
			Help:      "Rate limiter latency in seconds. Broken down by verb and URL.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 10),
		},
		[]string{"verb", "url"},
	)

	requestResult = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: LonghornName,
			Subsystem: clientGoRestClientSubsystem,
			Name:      "requests_total",
			Help:      "Number of HTTP requests, partitioned by status code, method, and host.",
		},
		[]string{"code", "method", "host"},
	)
)

func init() {
	registerClientMetrics()
}

// registerClientMetrics sets up the client latency metrics from client-go
func registerClientMetrics() {
	// register the metrics with our registry
	registry.Register(requestLatency)
	registry.Register(requestResult)
	registry.Register(rateLimiterLatency)

	// register the metrics with client-go
	clientmetrics.Register(clientmetrics.RegisterOpts{
		RequestLatency:     &latencyAdapter{metric: requestLatency},
		RequestResult:      &resultAdapter{metric: requestResult},
		RateLimiterLatency: &latencyAdapter{metric: rateLimiterLatency},
	})
}

// this section contains adapters, implementations, and other sundry organic, artisanally
// hand-crafted syntax trees required to convince client-go that it actually wants to let
// someone use its metrics.

// Client metrics adapters (method #1 for client-go metrics),
// copied (more-or-less directly) from k8s.io/kubernetes setup code
// (which isn't anywhere in an easily-importable place).

type latencyAdapter struct {
	metric *prometheus.HistogramVec
}

func (l *latencyAdapter) Observe(ctx context.Context, verb string, u url.URL, latency time.Duration) {
	l.metric.WithLabelValues(verb, u.String()).Observe(latency.Seconds())
}

type resultAdapter struct {
	metric *prometheus.CounterVec
}

func (r *resultAdapter) Increment(ctx context.Context, code, method, host string) {
	r.metric.WithLabelValues(code, method, host).Inc()
}
