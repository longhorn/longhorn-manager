package metricscollector

import (
	"context"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type NodeCollector struct {
	*baseCollector

	kubeMetricsClient *metricsclientset.Clientset

	statusMetric             metricInfo
	totalNumberOfNodesMetric metricInfo
	cpuUsageMetric           metricInfo
	cpuCapacityMetric        metricInfo
	memoryUsageMetric        metricInfo
	memoryCapacityMetric     metricInfo
	storageCapacityMetric    metricInfo
	storageUsageMetric       metricInfo
	storageReservationMetric metricInfo
}

func NewNodeCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore,
	kubeMetricsClient *metricsclientset.Clientset) *NodeCollector {

	nc := &NodeCollector{
		baseCollector:     newBaseCollector(subsystemNode, logger, nodeID, ds),
		kubeMetricsClient: kubeMetricsClient,
	}

	nc.statusMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemNode, "status"),
			"Status of this node",
			[]string{nodeLabel, conditionLabel, conditionReasonLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	nc.totalNumberOfNodesMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemNode, "count_total"),
			"Total number of nodes",
			[]string{},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	nc.cpuUsageMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemNode, "cpu_usage_millicpu"),
			"The cpu usage on this node",
			[]string{nodeLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	nc.cpuCapacityMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemNode, "cpu_capacity_millicpu"),
			"The maximum allocatable cpu on this node",
			[]string{nodeLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	nc.memoryUsageMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemNode, "memory_usage_bytes"),
			"The memory usage on this node",
			[]string{nodeLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	nc.memoryCapacityMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemNode, "memory_capacity_bytes"),
			"The maximum allocatable memory on this node",
			[]string{nodeLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	nc.storageCapacityMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemNode, "storage_capacity_bytes"),
			"The storage capacity of this node",
			[]string{nodeLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	nc.storageUsageMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemNode, "storage_usage_bytes"),
			"The used storage of this node",
			[]string{nodeLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	nc.storageReservationMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemNode, "storage_reservation_bytes"),
			"The reserved storage for other applications and system on this node",
			[]string{nodeLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	return nc
}

func (nc *NodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.statusMetric.Desc
	ch <- nc.totalNumberOfNodesMetric.Desc
	ch <- nc.cpuUsageMetric.Desc
	ch <- nc.cpuCapacityMetric.Desc
	ch <- nc.memoryUsageMetric.Desc
	ch <- nc.memoryCapacityMetric.Desc
	ch <- nc.storageCapacityMetric.Desc
	ch <- nc.storageUsageMetric.Desc
	ch <- nc.storageReservationMetric.Desc
}

func (nc *NodeCollector) Collect(ch chan<- prometheus.Metric) {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		nc.collectNodeStatus(ch)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		nc.collectTotalNumberOfNodes(ch)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		nc.collectNodeActualCPUMemoryUsage(ch)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		nc.collectNodeCPUMemoryCapacity(ch)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		nc.collectNodeStorage(ch)
	}()

	wg.Wait()
}

func (nc *NodeCollector) collectNodeStatus(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			nc.logger.WithField("error", err).Warn("panic during collecting metrics")
		}
	}()

	nodeList, err := nc.ds.ListNodesRO()
	if err != nil {
		nc.logger.WithError(err).Warn("error during scrape")
		return
	}

	for _, node := range nodeList {
		if node.Name != nc.currentNodeID {
			continue
		}

		for _, condition := range node.Status.Conditions {
			val := 0
			if condition.Status == longhorn.ConditionStatusTrue {
				val = 1
			}
			ch <- prometheus.MustNewConstMetric(nc.statusMetric.Desc, nc.statusMetric.Type, float64(val), nc.currentNodeID, strings.ToLower(condition.Type), condition.Reason)
		}

		// Get the allowScheduling value to determine whether this node is disabled by user
		allowSchedulingVal := 0
		if node.Spec.AllowScheduling {
			allowSchedulingVal = 1
		}
		ch <- prometheus.MustNewConstMetric(nc.statusMetric.Desc, nc.statusMetric.Type, float64(allowSchedulingVal), nc.currentNodeID, "allowScheduling", "")
	}
}

func (nc *NodeCollector) collectTotalNumberOfNodes(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			nc.logger.WithField("error", err).Warn("panic during collecting metrics")
		}
	}()

	nodeList, err := nc.ds.ListNodesRO()
	if err != nil {
		nc.logger.WithError(err).Warn("error during scrape")
		return
	}

	ch <- prometheus.MustNewConstMetric(nc.totalNumberOfNodesMetric.Desc, nc.totalNumberOfNodesMetric.Type, float64(len(nodeList)))
}

func (nc *NodeCollector) collectNodeActualCPUMemoryUsage(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			nc.logger.WithField("error", err).Warn("panic during collecting metrics")
		}
	}()

	nodeMetrics, err := nc.kubeMetricsClient.MetricsV1beta1().NodeMetricses().List(context.TODO(), metav1.ListOptions{
		FieldSelector: "metadata.name=" + nc.currentNodeID,
	})
	if err != nil {
		nc.logger.WithError(err).Warn("error during scrape")
		return
	}

	for _, nm := range nodeMetrics.Items {
		cpuUsageMilicpu := float64(nm.Usage.Cpu().MilliValue())
		memoryUsageBytes := float64(nm.Usage.Memory().Value())
		ch <- prometheus.MustNewConstMetric(nc.cpuUsageMetric.Desc, nc.cpuUsageMetric.Type, cpuUsageMilicpu, nc.currentNodeID)
		ch <- prometheus.MustNewConstMetric(nc.memoryUsageMetric.Desc, nc.memoryUsageMetric.Type, memoryUsageBytes, nc.currentNodeID)
	}
}

func (nc *NodeCollector) collectNodeCPUMemoryCapacity(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			nc.logger.WithField("error", err).Warn("panic during collecting metrics")
		}
	}()

	kubeNodeList, err := nc.ds.ListKubeNodesRO()
	if err != nil {
		nc.logger.WithError(err).Warn("error during scrape")
		return
	}

	for _, node := range kubeNodeList {
		if node.Name != nc.currentNodeID {
			continue
		}
		cpuCapacityMilicpu := float64(node.Status.Allocatable.Cpu().MilliValue())
		memoryCapacityBytes := float64(node.Status.Allocatable.Memory().Value())
		ch <- prometheus.MustNewConstMetric(nc.cpuCapacityMetric.Desc, nc.cpuCapacityMetric.Type, cpuCapacityMilicpu, nc.currentNodeID)
		ch <- prometheus.MustNewConstMetric(nc.memoryCapacityMetric.Desc, nc.memoryCapacityMetric.Type, memoryCapacityBytes, nc.currentNodeID)
	}
}

func (nc *NodeCollector) collectNodeStorage(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			nc.logger.WithField("error", err).Warn("panic during collecting metrics")
		}
	}()

	node, err := nc.ds.GetNodeRO(nc.currentNodeID)
	if err != nil {
		nc.logger.WithError(err).Warn("error during scrape")
		return
	}

	disks := getDiskListFromNode(node)
	var storageCapacity int64 = 0
	var storageUsage int64 = 0
	var storageReservation int64 = 0
	for _, disk := range disks {
		storageCapacity += disk.StorageMaximum
		storageUsage += disk.StorageMaximum - disk.StorageAvailable
		storageReservation += disk.StorageReserved
	}

	ch <- prometheus.MustNewConstMetric(nc.storageCapacityMetric.Desc, nc.storageCapacityMetric.Type, float64(storageCapacity), nc.currentNodeID)
	ch <- prometheus.MustNewConstMetric(nc.storageUsageMetric.Desc, nc.storageUsageMetric.Type, float64(storageUsage), nc.currentNodeID)
	ch <- prometheus.MustNewConstMetric(nc.storageReservationMetric.Desc, nc.storageReservationMetric.Type, float64(storageReservation), nc.currentNodeID)
}
