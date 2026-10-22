package runner

import "github.com/prometheus/client_golang/prometheus"

// Names follow the manager's longhorn_<crd>_<measure> convention, under
// the SnapshotGroup CRD that carries the hooks. The operation label
// (snapshot, backup) separates the two uses.
var (
	releasesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "longhorn_snapshot_group_hook_releases_total",
		Help: "Number of hooked snapshot group releases, by result (released or post_failed) and operation.",
	}, []string{"result", "operation"})

	deadlineExceededTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "longhorn_snapshot_group_hook_deadline_exceeded_total",
		Help: "Number of hooked snapshot groups released at their deadline, by operation.",
	}, []string{"operation"})

	commandFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "longhorn_snapshot_group_hook_command_failures_total",
		Help: "Number of hook command failures, by step, reason, and operation.",
	}, []string{"step", "reason", "operation"})

	windowSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longhorn_snapshot_group_hook_window_seconds",
		Help:    "Seconds from the first pre-command to the last post-command, by operation. Failed releases are not counted.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 10),
	}, []string{"operation"})
)

// RegisterMetrics registers this package's collectors with reg.
func RegisterMetrics(reg prometheus.Registerer) error {
	for _, collector := range []prometheus.Collector{releasesTotal, deadlineExceededTotal, commandFailures, windowSeconds} {
		if err := reg.Register(collector); err != nil {
			return err
		}
	}
	return nil
}
