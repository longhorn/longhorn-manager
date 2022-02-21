package monitoring

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
)

type backupCollector struct {
	*baseCollector

	stateMetric metricInfo
	sizeMetric  metricInfo
}

func NewBackupCollector(logger logrus.FieldLogger, nodeID string, ds *datastore.DataStore) prometheus.Collector {
	return &backupCollector{
		baseCollector: newBaseCollector(subsystemBackup, logger, nodeID, ds),
		stateMetric: metricInfo{
			Desc: prometheus.NewDesc(
				prometheus.BuildFQName(longhornName, subsystemBackup, "state"),
				"State of this backup",
				[]string{backupLabel, backupVolumeLabel},
				nil,
			),
			Type: prometheus.GaugeValue,
		},
		sizeMetric: metricInfo{
			Desc: prometheus.NewDesc(
				prometheus.BuildFQName(longhornName, subsystemBackup, "actual_size_bytes"),
				"Actual size of this backup",
				[]string{backupLabel, backupVolumeLabel},
				nil,
			),
			Type: prometheus.GaugeValue,
		},
	}
}

func (bc *backupCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- bc.stateMetric.Desc
	ch <- bc.sizeMetric.Desc
}

func (bc *backupCollector) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			bc.logger.WithField("error", err).Warn("panic during collecting metrics")
		}
	}()

	backups, err := bc.ds.ListBackupsRO()
	if err != nil {
		bc.logger.WithError(err).Warn("error during scrape")
		return
	}

	for _, backup := range backups {
		backupVolName, ok := backup.Labels[types.LonghornLabelBackupVolume]
		if !ok {
			bc.logger.Warnf("error when getting %q backup volume name", backup.Name)
			continue
		}

		ch <- prometheus.MustNewConstMetric(
			bc.stateMetric.Desc,
			bc.stateMetric.Type,
			float64(convertBackupState(backup.Status.State)),
			backup.Name,
			backupVolName,
		)

		size, err := strconv.ParseFloat(backup.Status.Size, 64)
		if err != nil {
			bc.logger.WithError(err).Warnf("error when parsing %q backup size", backup.Name)
			continue
		}
		ch <- prometheus.MustNewConstMetric(
			bc.sizeMetric.Desc,
			bc.sizeMetric.Type,
			size,
			backup.Name,
			backupVolName,
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
