package metricscollector

import (
	"regexp"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type EngineCollector struct {
	*baseCollector

	rebuildMetric         metricInfo
	pendingRebuildMetrics map[string]prometheus.Metric
	mutex                 sync.Mutex
}

func NewEngineCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore) *EngineCollector {

	ec := &EngineCollector{
		baseCollector:         newBaseCollector(subsystemEngine, logger, nodeID, ds),
		pendingRebuildMetrics: make(map[string]prometheus.Metric),
	}

	ec.rebuildMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemEngine, "rebuild_progress"),
			"Rebuild progress percentage of the engine (0-100)",
			[]string{nodeLabel, engineLabel, rebuildSrcLabel, rebuildDstLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	return ec
}

func (ec *EngineCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- ec.rebuildMetric.Desc
}

func (ec *EngineCollector) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			ec.logger.WithField("error", err).Warn("Panic during collecting metrics")
		}
	}()

	engineList, err := ec.ds.ListEnginesByNodeRO(ec.currentNodeID)
	if err != nil {
		ec.logger.WithError(err).Warn("Error during scrape")
		return
	}

	for _, e := range engineList {
		ec.collectRebuildProgress(ch, e)
	}

	ec.finalizeCompletedRebuilds(ch)
}

func (ec *EngineCollector) collectRebuildProgress(ch chan<- prometheus.Metric, e *longhorn.Engine) {
	defer func() {
		if err := recover(); err != nil {
			ec.logger.WithField("error", err).Warn("Panic during collecting metrics")
		}
	}()

	v, err := ec.ds.GetVolumeRO(e.Spec.VolumeName)
	if err != nil {
		if errors.IsNotFound(err) {
			return
		}
		ec.logger.WithError(err).Warnf("Failed to get volume for engine %v", e.Name)
		return
	}

	ec.mutex.Lock()
	defer ec.mutex.Unlock()

	for addr, rs := range e.Status.RebuildStatus {
		if rs.IsRebuilding {
			replicaAddress := strings.TrimPrefix(addr, "tcp://")
			srcAddress := strings.TrimPrefix(rs.FromReplicaAddress, "tcp://")

			replicaName := ec.getReplicaNameByAddress(e, replicaAddress)
			if replicaName == "" {
				continue
			}

			ch <- prometheus.MustNewConstMetric(
				ec.rebuildMetric.Desc,
				ec.rebuildMetric.Type,
				float64(rs.Progress),
				ec.currentNodeID,
				e.Name,
				srcAddress,
				replicaAddress,
				v.Status.KubernetesStatus.PVCName,
				v.Status.KubernetesStatus.Namespace,
			)

			ec.pendingRebuildMetrics[replicaName] = prometheus.MustNewConstMetric(
				ec.rebuildMetric.Desc,
				ec.rebuildMetric.Type,
				100.0,
				ec.currentNodeID,
				e.Name,
				srcAddress,
				replicaAddress,
				v.Status.KubernetesStatus.PVCName,
				v.Status.KubernetesStatus.Namespace,
			)
		}
	}
}

func (ec *EngineCollector) finalizeCompletedRebuilds(ch chan<- prometheus.Metric) {
	ec.mutex.Lock()
	defer ec.mutex.Unlock()

	engineMap := make(map[string]*longhorn.Engine)

	engineList, err := ec.ds.ListEnginesByNodeRO(ec.currentNodeID)
	if err != nil {
		ec.logger.WithError(err).Warn("Failed to list engines in cleanup")
		return
	}

	for _, e := range engineList {
		engineMap[e.Name] = e
	}

	var toDelete []string

	for replicaName, completeMetric := range ec.pendingRebuildMetrics {
		engineName := convertReplicaToEngine(replicaName)
		engine, exists := engineMap[engineName]
		if !exists {
			toDelete = append(toDelete, replicaName)
			continue
		}

		mode, exists := engine.Status.ReplicaModeMap[replicaName]
		if !exists {
			toDelete = append(toDelete, replicaName)
			continue
		}

		if mode == longhorn.ReplicaModeRW {
			ch <- completeMetric
			toDelete = append(toDelete, replicaName)
		}
	}

	for _, replicaName := range toDelete {
		delete(ec.pendingRebuildMetrics, replicaName)
	}
}

func (ec *EngineCollector) getReplicaNameByAddress(e *longhorn.Engine, addr string) string {
	for rName, rAddr := range e.Status.CurrentReplicaAddressMap {
		if rAddr == addr {
			return rName
		}
	}
	return ""
}

func convertReplicaToEngine(input string) string {
	re := regexp.MustCompile(`-r-[0-9a-f]{8}$`)
	return re.ReplaceAllString(input, "-e-0")
}
