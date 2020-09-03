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

	statusMetric metricInfo
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

	return nc
}

func (nc *NodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.statusMetric.Desc
}

func (nc *NodeCollector) Collect(ch chan<- prometheus.Metric) {
	nodeList, err := nc.ds.ListNodesRO()
	if err != nil {
		nc.logger.WithError(err).Warn("error during scrape")
		return
	}

	nc.collectNodeStatus(ch, nodeList)

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
	}
}
