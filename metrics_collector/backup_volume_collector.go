package metricscollector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"
)

type BackupVolumeCollector struct {
	*baseCollector

	backupCountMetric metricInfo
}

func NewBackupVolumeCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore) *BackupVolumeCollector {

	bvc := &BackupVolumeCollector{
		baseCollector: newBaseCollector(subsystemBackupVolume, logger, nodeID, ds),
	}

	bvc.backupCountMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemBackupVolume, "backups_count"),
			"Number of backups belonging to this backup volume",
			[]string{backupVolumeLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	return bvc
}

func (bvc *BackupVolumeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- bvc.backupCountMetric.Desc
}

func (bvc *BackupVolumeCollector) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			bvc.logger.WithField("error", err).Warn("Panic during collecting metrics")
		}
	}()

	bvList, err := bvc.ds.ListBackupVolumes()
	if err != nil {
		bvc.logger.WithError(err).Warn("Error during scrape")
		return
	}

	for _, bv := range bvList {
		if bv.Status.OwnerID != bvc.currentNodeID {
			continue
		}

		bs, err := bvc.ds.ListBackupsWithBackupTargetAndBackupVolumeRO(bv.Spec.BackupTargetName, bv.Spec.VolumeName)
		if err != nil {
			bvc.logger.WithError(err).Warn("Error during scrape")
			continue
		}

		ch <- prometheus.MustNewConstMetric(bvc.backupCountMetric.Desc, bvc.backupCountMetric.Type, float64(len(bs)), bv.Name)
	}
}
