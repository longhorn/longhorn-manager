package metricscollector

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type BackupCollector struct {
	*baseCollector

	sizeMetric  metricInfo
	stateMetric metricInfo
}

func NewBackupCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore) *BackupCollector {

	bc := &BackupCollector{
		baseCollector: newBaseCollector(subsystemBackup, logger, nodeID, ds),
	}

	bc.sizeMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemBackup, "actual_size_bytes"),
			"Actual size of this backup",
			[]string{volumeLabel, backupLabel, recurringJobLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	bc.stateMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemBackup, "state"),
			"State of this backup",
			[]string{volumeLabel, backupLabel, recurringJobLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	return bc
}

func (bc *BackupCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- bc.sizeMetric.Desc
	ch <- bc.stateMetric.Desc
}

func (bc *BackupCollector) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			bc.logger.WithField("error", err).Warn("Panic during collecting metrics")
		}
	}()

	backupLists, err := bc.ds.ListBackupsRO()
	if err != nil {
		bc.logger.WithError(err).Warn("Error during scrape")
		return
	}

	for _, backup := range backupLists {
		if backup.Status.OwnerID == bc.currentNodeID {
			var size float64
			if size, err = strconv.ParseFloat(backup.Status.Size, 64); err != nil {
				bc.logger.WithError(err).Warn("Error get size")
			}
			backupVolumeName, ok := backup.Labels[types.LonghornLabelBackupVolume]
			if !ok {
				bc.logger.WithError(err).Warn("Error get backup volume label")
			}
			backupRecurringJobName := backup.Labels[types.RecurringJobLabel]
			ch <- prometheus.MustNewConstMetric(bc.sizeMetric.Desc, bc.sizeMetric.Type, size, backupVolumeName, backup.Name, backupRecurringJobName)
			ch <- prometheus.MustNewConstMetric(bc.stateMetric.Desc, bc.stateMetric.Type, float64(getBackupStateValue(backup)), backupVolumeName, backup.Name, backupRecurringJobName)
		}
	}
}

func getBackupStateValue(backup *longhorn.Backup) int {
	stateValue := 0
	switch backup.Status.State {
	case longhorn.BackupStateNew:
		stateValue = 0
	case longhorn.BackupStatePending:
		stateValue = 1
	case longhorn.BackupStateInProgress:
		stateValue = 2
	case longhorn.BackupStateCompleted:
		stateValue = 3
	case longhorn.BackupStateError:
		stateValue = 4
	case longhorn.BackupStateUnknown:
		stateValue = 5
	}
	return stateValue
}
