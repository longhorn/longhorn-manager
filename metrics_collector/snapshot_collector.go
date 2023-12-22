package metricscollector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/labels"

	"github.com/longhorn/longhorn-manager/datastore"
)

type SnapshotCollector struct {
	*baseCollector

	sizeMetric metricInfo
}

func NewSnapshotCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore) *SnapshotCollector {

	snapshotCollector := &SnapshotCollector{
		baseCollector: newBaseCollector(subsystemSnapshot, logger, nodeID, ds),
	}

	snapshotCollector.sizeMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemSnapshot, "actual_size_bytes"),
			"Actual size of this snapshot",
			[]string{snapshotLabel, volumeLabel, userCreatedLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	return snapshotCollector
}

func (c *SnapshotCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.sizeMetric.Desc
}

func (c *SnapshotCollector) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			c.logger.WithField("error", err).Warn("Panic during collecting metrics")
		}
	}()

	snapshotLists, err := c.ds.ListSnapshotsRO(labels.Everything())
	if err != nil {
		c.logger.WithError(err).Warn("Error during scrape")
		return
	}

	for _, snapshot := range snapshotLists {
		volume, err := c.ds.GetVolumeRO(snapshot.Spec.Volume)
		if err != nil {
			logrus.WithError(err).Warnf("failed to get volume %v for snapshot %v", snapshot.Spec.Volume, snapshot.Name)
			continue
		}

		if volume.Status.OwnerID != c.currentNodeID {
			continue
		}

		// Skip volume-head because it is not a real snapshot.
		if snapshot.Name == "volume-head" {
			continue
		}

		isUserCreated := "false"
		if snapshot.Status.UserCreated {
			isUserCreated = "true"
		}

		ch <- prometheus.MustNewConstMetric(c.sizeMetric.Desc, c.sizeMetric.Type, float64(snapshot.Status.Size), snapshot.Name, volume.Name, isUserCreated)
	}
}
