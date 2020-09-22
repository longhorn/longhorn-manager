package monitoring

import (
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

type NodeCollector struct {
	*baseCollector

	statusMetric             metricInfo
	totalNumberOfNodesMetric metricInfo
}

func NewNodeCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore) *NodeCollector {

	nc := &NodeCollector{
		baseCollector: newBaseCollector(subsystemNode, logger, nodeID, ds),
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

	return nc
}

func (nc *NodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.statusMetric.Desc
	ch <- nc.totalNumberOfNodesMetric.Desc
}

func (nc *NodeCollector) Collect(ch chan<- prometheus.Metric) {
	nodeList, err := nc.ds.ListNodesRO()
	if err != nil {
		nc.logger.WithError(err).Warn("error during scrape")
		return
	}

	nc.collectNodeStatus(ch, nodeList)
	nc.collectTotalNumberOfNodes(ch, nodeList)

}

func (nc *NodeCollector) collectNodeStatus(ch chan<- prometheus.Metric, nodeList []*longhorn.Node) {
	defer func() {
		if err := recover(); err != nil {
			nc.logger.WithField("error", err).Warn("panic during collecting metrics")
		}
	}()

	for _, node := range nodeList {
		if node.Name != nc.currentNodeID {
			continue
		}

		for _, condition := range node.Status.Conditions {
			val := 0
			if condition.Status == types.ConditionStatusTrue {
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

func (nc *NodeCollector) collectTotalNumberOfNodes(ch chan<- prometheus.Metric, nodeList []*longhorn.Node) {
	defer func() {
		if err := recover(); err != nil {
			nc.logger.WithField("error", err).Warn("panic during collecting metrics")
		}
	}()

	ch <- prometheus.MustNewConstMetric(nc.totalNumberOfNodesMetric.Desc, nc.totalNumberOfNodesMetric.Type, float64(len(nodeList)))
}
