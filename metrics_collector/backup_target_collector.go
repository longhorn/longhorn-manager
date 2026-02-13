package metricscollector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"
)

type BackupTargetCollector struct {
	*baseCollector

	backupVolumeCountMetric metricInfo
}

func NewBackupTargetCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore) *BackupTargetCollector {

	btc := &BackupTargetCollector{
		baseCollector: newBaseCollector(subsystemBackupTarget, logger, nodeID, ds),
	}

	btc.backupVolumeCountMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemBackupTarget, "backup_volume_count"),
			"Number of backup volumes on this backup target",
			[]string{backupTargetLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	return btc
}

func (btc *BackupTargetCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- btc.backupVolumeCountMetric.Desc
}

func (btc *BackupTargetCollector) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			btc.logger.WithField("error", err).Warn("Panic during collecting metrics")
		}
	}()

	btList, err := btc.ds.ListBackupTargetsRO()
	if err != nil {
		btc.logger.WithError(err).Warn("Error during scrape")
		return
	}

	for _, bt := range btList {
		if bt.Status.OwnerID != btc.currentNodeID {
			continue
		}

		bvs, err := btc.ds.ListBackupVolumesWithBackupTargetNameRO(bt.Name)
		if err != nil {
			btc.logger.WithError(err).Warn("Error during scrape")
			continue
		}

		ch <- prometheus.MustNewConstMetric(btc.backupVolumeCountMetric.Desc, btc.backupVolumeCountMetric.Type, float64(len(bvs)), bt.Name)
	}
}
