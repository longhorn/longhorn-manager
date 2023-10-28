package metricscollector

import (
	"context"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type InstanceManagerCollector struct {
	*baseCollector

	kubeMetricsClient *metricsclientset.Clientset
	namespace         string

	cpuUsageMetric      metricInfo
	cpuRequestMetric    metricInfo
	memoryUsageMetric   metricInfo
	memoryRequestMetric metricInfo

	proxyConnCounter util.Counter
	proxyConnMetric  metricInfo
}

func NewInstanceManagerCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore,
	proxyConnCounter util.Counter,
	kubeMetricsClient *metricsclientset.Clientset,
	namespace string) *InstanceManagerCollector {

	imc := &InstanceManagerCollector{
		baseCollector:     newBaseCollector(subsystemInstanceManager, logger, nodeID, ds),
		proxyConnCounter:  proxyConnCounter,
		kubeMetricsClient: kubeMetricsClient,
		namespace:         namespace,
	}

	imc.cpuUsageMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemInstanceManager, "cpu_usage_millicpu"),
			"The cpu usage of this longhorn instance manager",
			[]string{nodeLabel, instanceManagerLabel, instanceManagerType},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	imc.cpuRequestMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemInstanceManager, "cpu_requests_millicpu"),
			"Requested CPU resources in kubernetes of this Longhorn instance manager",
			[]string{nodeLabel, instanceManagerLabel, instanceManagerType},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	imc.memoryUsageMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemInstanceManager, "memory_usage_bytes"),
			"The memory usage of this longhorn instance manager",
			[]string{nodeLabel, instanceManagerLabel, instanceManagerType},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	imc.memoryRequestMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemInstanceManager, "memory_requests_bytes"),
			"Requested memory in Kubernetes of this longhorn instance manager",
			[]string{nodeLabel, instanceManagerLabel, instanceManagerType},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	imc.proxyConnMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemInstanceManager, "proxy_grpc_connection"),
			"The number of proxy gRPC connection of this longhorn instance manager",
			[]string{nodeLabel, instanceManagerLabel, instanceManagerType},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	return imc
}

func (imc *InstanceManagerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- imc.cpuUsageMetric.Desc
	ch <- imc.cpuRequestMetric.Desc
	ch <- imc.memoryUsageMetric.Desc
	ch <- imc.memoryRequestMetric.Desc
	ch <- imc.proxyConnMetric.Desc
}

func (imc *InstanceManagerCollector) Collect(ch chan<- prometheus.Metric) {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		imc.collectActualUsage(ch)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		imc.collectRequestValues(ch)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		imc.collectGrpcConnection(ch)
	}()

	wg.Wait()
}

func (imc *InstanceManagerCollector) collectActualUsage(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			imc.logger.WithField("error", err).Warn("Panic during collecting metrics")
		}
	}()

	podMetrics, err := imc.kubeMetricsClient.MetricsV1beta1().PodMetricses(imc.namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: makeInstanceManagerLabelSelector(imc.currentNodeID),
	})
	if err != nil {
		imc.logger.WithError(err).Warn("Error during scrape")
		return
	}

	for _, pm := range podMetrics.Items {
		var usageCPUCores, usageMemoryBytes float64
		for _, c := range pm.Containers {
			usageCPUCores += float64(c.Usage.Cpu().MilliValue())
			usageMemoryBytes += float64(c.Usage.Memory().Value())
		}

		instanceManagerType := getInstanceManagerTypeFromInstanceManagerName(pm.GetName())
		ch <- prometheus.MustNewConstMetric(imc.cpuUsageMetric.Desc, imc.cpuUsageMetric.Type, usageCPUCores, imc.currentNodeID, pm.GetName(), instanceManagerType)
		ch <- prometheus.MustNewConstMetric(imc.memoryUsageMetric.Desc, imc.memoryUsageMetric.Type, usageMemoryBytes, imc.currentNodeID, pm.GetName(), instanceManagerType)
	}
}

func makeInstanceManagerLabelSelector(nodeID string) string {
	componentLabel := types.GetLonghornLabelComponentKey() + "=" + types.LonghornLabelInstanceManager
	nodeLabel := types.GetLonghornLabelKey(types.LonghornLabelNode) + "=" + nodeID
	return componentLabel + "," + nodeLabel
}

func getInstanceManagerTypeFromInstanceManagerName(imName string) string {
	switch {
	case strings.HasPrefix(imName, types.GetInstanceManagerPrefix(longhorn.InstanceManagerTypeEngine)):
		return string(longhorn.InstanceManagerTypeEngine)
	case strings.HasPrefix(imName, types.GetInstanceManagerPrefix(longhorn.InstanceManagerTypeReplica)):
		return string(longhorn.InstanceManagerTypeReplica)
	case strings.HasPrefix(imName, types.GetInstanceManagerPrefix(longhorn.InstanceManagerTypeAllInOne)):
		return string(longhorn.InstanceManagerTypeAllInOne)
	}
	return ""
}

func (imc *InstanceManagerCollector) collectRequestValues(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			imc.logger.WithField("error", err).Warn("Panic during collecting metrics")
		}
	}()

	podList, err := imc.ds.ListPodsRO(imc.namespace)
	if err != nil {
		imc.logger.WithError(err).Warn("Error during scrape")
		return
	}

	for _, pod := range podList {
		podLabels := pod.GetLabels()
		componentLabel := podLabels[types.GetLonghornLabelComponentKey()]
		nodeLabel := podLabels[types.GetLonghornLabelKey(types.LonghornLabelNode)]

		if (componentLabel != types.LonghornLabelInstanceManager) || (nodeLabel != imc.currentNodeID) {
			continue
		}

		var requestCPUCores, requestMemoryBytes float64
		for _, container := range pod.Spec.Containers {
			requestCPUCores += float64(container.Resources.Requests.Cpu().MilliValue())
			requestMemoryBytes += float64(container.Resources.Requests.Memory().Value())
		}

		instanceManagerType := podLabels[types.GetLonghornLabelKey(types.LonghornLabelInstanceManagerType)]
		ch <- prometheus.MustNewConstMetric(imc.cpuRequestMetric.Desc, imc.cpuRequestMetric.Type, requestCPUCores, imc.currentNodeID, pod.GetName(), instanceManagerType)
		ch <- prometheus.MustNewConstMetric(imc.memoryRequestMetric.Desc, imc.memoryRequestMetric.Type, requestMemoryBytes, imc.currentNodeID, pod.GetName(), instanceManagerType)
	}
}

func (imc *InstanceManagerCollector) collectGrpcConnection(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			imc.logger.WithField("error", err).Warn("Panic during collecting metrics")
		}
	}()

	engineInstanceManagers, err := imc.ds.ListInstanceManagersByNodeRO(imc.currentNodeID, longhorn.InstanceManagerTypeEngine, "")
	if err != nil {
		imc.logger.WithError(err).Warn("Error during scrape")
		return
	}

	for _, im := range engineInstanceManagers {
		imPod, err := imc.ds.GetPod(im.Name)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				logrus.WithError(err).Infof("Resetting proxy gRPC connection counter for %v", im.Name)
				imc.proxyConnCounter.ResetCount()
				continue
			}

			logrus.WithError(err).Errorf("Failed to get instance manager pod from %v", im.Name)
			return
		}

		ch <- prometheus.MustNewConstMetric(
			imc.proxyConnMetric.Desc,
			imc.proxyConnMetric.Type,
			float64(imc.proxyConnCounter.GetCount()),
			imc.currentNodeID,
			imPod.Name,
			string(longhorn.InstanceManagerTypeEngine),
		)
	}
}
