package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type backupCollector struct {
	*baseCollector

	stateMetric *metricInfo
}

func NewBackupCollector(logger logrus.FieldLogger, nodeID string, ds *datastore.DataStore) prometheus.Collector {
	return &backupCollector{
		baseCollector: newBaseCollector(subsystemBackup, logger, nodeID, ds),
		stateMetric: &metricInfo{
			Desc: prometheus.NewDesc(
				prometheus.BuildFQName(longhornName, subsystemBackup, "state"),
				"State of this backup",
				[]string{nodeLabel, backupLabel},
				nil,
			),
			Type: prometheus.GaugeValue,
		},
	}
}

func (bc *backupCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- bc.stateMetric.Desc
}

func (bc *backupCollector) Collect(ch chan<- prometheus.Metric) {
	backups, err := bc.ds.ListBackups()
	if err != nil {
		bc.logger.WithError(err).Warn("error during scrape")
		return
	}

	for name, backup := range backups {
		ch <- prometheus.MustNewConstMetric(
			bc.stateMetric.Desc,
			bc.stateMetric.Type,
			float64(convertBackupState(backup.Status.State)),
			bc.currentNodeID,
			name,
		)
	}
}

func convertBackupState(state v1beta2.BackupState) int {
	switch state {
	case v1beta2.BackupStateInProgress:
		return 1
	case v1beta2.BackupStateCompleted:
		return 2
	case v1beta2.BackupStateError:
		return 3
	case v1beta2.BackupStateUnknown:
		return 4
	default:
		// case default == case v1beta2.BackupStateNew
		// because BackupStateNew == BackupState("")
		return 0
	}
}
