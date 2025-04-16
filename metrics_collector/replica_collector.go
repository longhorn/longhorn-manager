package metricscollector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type ReplicaCollector struct {
	*baseCollector

	infoMetric  metricInfo
	stateMetric metricInfo
}

func NewReplicaCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore) *ReplicaCollector {

	rc := &ReplicaCollector{
		baseCollector: newBaseCollector(subsystemReplica, logger, nodeID, ds),
	}

	rc.infoMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemReplica, "info"),
			"Static information about this replica",
			[]string{replicaLabel, volumeLabel, nodeLabel, diskPathLabel, dataEngineLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	rc.stateMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemReplica, "state"),
			"The current state of this replica",
			[]string{replicaLabel, volumeLabel, nodeLabel, stateLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	return rc
}

func (rc *ReplicaCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- rc.infoMetric.Desc
	ch <- rc.stateMetric.Desc
}

func (rc *ReplicaCollector) Collect(ch chan<- prometheus.Metric) {
	replicas, err := rc.ds.ListReplicasByNodeRO(rc.currentNodeID)
	if err != nil {
		rc.logger.WithError(err).Warn("Failed to list replicas")
		return
	}

	// state metric: emit only the current state as 1, others as 0
	states := []longhorn.InstanceState{
		longhorn.InstanceStateRunning,
		longhorn.InstanceStateStopped,
		longhorn.InstanceStateError,
		longhorn.InstanceStateStarting,
		longhorn.InstanceStateStopping,
		longhorn.InstanceStateUnknown,
	}

	for _, r := range replicas {
		// info metric
		ch <- prometheus.MustNewConstMetric(
			rc.infoMetric.Desc,
			rc.infoMetric.Type,
			1,
			r.Name,
			r.Spec.VolumeName,
			r.Spec.NodeID,
			r.Spec.DiskPath,
			string(r.Spec.DataEngine),
		)

		for _, s := range states {
			val := 0.0
			if r.Status.CurrentState == s {
				val = 1.0
			}
			ch <- prometheus.MustNewConstMetric(
				rc.stateMetric.Desc,
				rc.stateMetric.Type,
				val,
				r.Name,
				r.Spec.VolumeName,
				r.Spec.NodeID,
				string(s),
			)
		}
	}
}
