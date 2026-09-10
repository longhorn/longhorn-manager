package metricscollector

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type BackupTargetCollector struct {
	*baseCollector

	backupVolumeCountMetric metricInfo
	statusMetric            metricInfo
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

	btc.statusMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemBackupTarget, "status"),
			"Status of the backup target",
			[]string{backupTargetLabel, conditionLabel, conditionReasonLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	return btc
}

func (btc *BackupTargetCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- btc.backupVolumeCountMetric.Desc
	ch <- btc.statusMetric.Desc
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

		for _, condition := range bt.Status.Conditions {
			val := 0
			if condition.Status == longhorn.ConditionStatusTrue {
				val = 1
			}

			ch <- prometheus.MustNewConstMetric(btc.statusMetric.Desc, btc.statusMetric.Type, float64(val), bt.Name, strings.ToLower(condition.Type), condition.Reason)
		}
	}
}
