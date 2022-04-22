package metricscollector

import (
	"context"
	"os"

	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/longhorn/longhorn-manager/datastore"
)

type ManagerCollector struct {
	*baseCollector

	kubeMetricsClient *metricsclientset.Clientset
	namespace         string

	cpuUsageMetric    metricInfo
	memoryUsageMetric metricInfo
}

func NewManagerCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore,
	kubeMetricsClient *metricsclientset.Clientset,
	namespace string) *ManagerCollector {

	mc := &ManagerCollector{
		baseCollector:     newBaseCollector(subsystemManager, logger, nodeID, ds),
		kubeMetricsClient: kubeMetricsClient,
		namespace:         namespace,
	}

	mc.cpuUsageMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemManager, "cpu_usage_millicpu"),
			"The cpu usage of this longhorn manager",
			[]string{nodeLabel, managerLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	mc.memoryUsageMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemManager, "memory_usage_bytes"),
			"The memory usage of this longhorn manager",
			[]string{nodeLabel, managerLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	return mc
}

func (mc *ManagerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- mc.cpuUsageMetric.Desc
	ch <- mc.memoryUsageMetric.Desc
}

func (mc *ManagerCollector) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			mc.logger.WithField("error", err).Warn("panic during collecting metrics")
		}
	}()

	// This code is running on the manager pod and os hostname always guarantees to be the manager pod name.
	managerPodName, err := os.Hostname()
	if err != nil {
		mc.logger.WithError(err).Warn("error during scrape")
		return
	}

	podMetrics, err := mc.kubeMetricsClient.MetricsV1beta1().PodMetricses(mc.namespace).List(context.TODO(), metav1.ListOptions{
		FieldSelector: "metadata.name=" + managerPodName,
	})
	if err != nil {
		mc.logger.WithError(err).Warn("error during scrape")
		return
	}

	for _, pm := range podMetrics.Items {
		var usageCPUCores, usageMemoryBytes float64
		for _, c := range pm.Containers {
			usageCPUCores += float64(c.Usage.Cpu().MilliValue())
			usageMemoryBytes += float64(c.Usage.Memory().Value())
		}
		ch <- prometheus.MustNewConstMetric(mc.cpuUsageMetric.Desc, mc.cpuUsageMetric.Type, usageCPUCores, mc.currentNodeID, managerPodName)
		ch <- prometheus.MustNewConstMetric(mc.memoryUsageMetric.Desc, mc.memoryUsageMetric.Type, usageMemoryBytes, mc.currentNodeID, managerPodName)
	}
}
