package metricscollector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type BackupBackingImageCollector struct {
	*baseCollector

	sizeMetric  metricInfo
	stateMetric metricInfo
}

func NewBackupBackingImageCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore) *BackupBackingImageCollector {

	bc := &BackupBackingImageCollector{
		baseCollector: newBaseCollector(subsystemBackupBackingImage, logger, nodeID, ds),
	}

	bc.sizeMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemBackupBackingImage, "actual_size_bytes"),
			"Actual size of this backup backing image",
			[]string{backupBackingImageLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	bc.stateMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemBackupBackingImage, "state"),
			"State of this backup backing image",
			[]string{backupBackingImageLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	return bc
}

func (bc *BackupBackingImageCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- bc.sizeMetric.Desc
	ch <- bc.stateMetric.Desc
}

func (bc *BackupBackingImageCollector) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			bc.logger.WithField("error", err).Warn("Panic during collecting metrics")
		}
	}()

	backupBackingImageLists, err := bc.ds.ListBackupBackingImagesRO()
	if err != nil {
		bc.logger.WithError(err).Warn("Error during scrape")
		return
	}

	for _, backupBackingImage := range backupBackingImageLists {
		if backupBackingImage.Status.OwnerID == bc.currentNodeID {
			ch <- prometheus.MustNewConstMetric(bc.sizeMetric.Desc, bc.sizeMetric.Type, float64(backupBackingImage.Status.Size), backupBackingImage.Name)
			ch <- prometheus.MustNewConstMetric(bc.stateMetric.Desc, bc.stateMetric.Type, float64(getBackupBackingImageStateValue(backupBackingImage)), backupBackingImage.Name)

		}
	}
}

func getBackupBackingImageStateValue(backupBackingImage *longhorn.BackupBackingImage) int {
	stateValue := 0
	switch backupBackingImage.Status.State {
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
