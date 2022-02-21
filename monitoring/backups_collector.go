package monitoring

import (
	"strconv"

	"github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type BackupsCollector struct {
	*baseCollector

	sizeMetric  metricInfo
	stateMetric metricInfo
}

func NewBackupsCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore) *BackupsCollector {

	vc := &BackupsCollector{
		baseCollector: newBaseCollector(subsystemBackups, logger, nodeID, ds),
	}

	vc.sizeMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemBackups, "actual_size_bytes"),
			"Actual size of this backup",
			[]string{volumeLabel, backupsLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.stateMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemBackups, "state"),
			"State of this backup",
			[]string{volumeLabel, backupsLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	return vc
}

func (vc *BackupsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- vc.sizeMetric.Desc
	ch <- vc.stateMetric.Desc
}

func (vc *BackupsCollector) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			vc.logger.WithField("error", err).Warn("panic during collecting metrics")
		}
	}()

	backupsLists, err := vc.ds.ListBackupsRO()
	if err != nil {
		vc.logger.WithError(err).Warn("error during scrape ")
		return
	}

	for _, v := range backupsLists {
		if v.Status.OwnerID == vc.currentNodeID {
			var size float64
			if size, err = strconv.ParseFloat(v.Status.Size, 64); err != nil {
				vc.logger.WithError(err).Warn("error get size")
			}
			ch <- prometheus.MustNewConstMetric(vc.sizeMetric.Desc, vc.sizeMetric.Type, size, v.Status.VolumeName, v.Name)
			ch <- prometheus.MustNewConstMetric(vc.stateMetric.Desc, vc.stateMetric.Type, float64(getBackupsStateValue(v)), v.Status.VolumeName, v.Name)
		}
	}
}

func getBackupsStateValue(v *longhorn.Backup) int {
	stateValue := 0
	switch v.Status.State {
	case longhorn.BackupStateInProgress:
		stateValue = 0
	case longhorn.BackupStateCompleted:
		stateValue = 1
	case longhorn.BackupStateError:
		stateValue = 2
	case longhorn.BackupStateNew:
		stateValue = 3
	case longhorn.BackupStateUnknown:
		stateValue = 4
	}
	return stateValue
}
