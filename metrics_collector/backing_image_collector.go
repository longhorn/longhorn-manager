package metricscollector

import (
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type BackingImageCollector struct {
	*baseCollector

	sizeMetric  metricInfo
	stateMetric metricInfo
}

func NewBackingImageCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore) *BackingImageCollector {

	bc := &BackingImageCollector{
		baseCollector: newBaseCollector(subsystemBackingImage, logger, nodeID, ds),
	}

	bc.sizeMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemBackingImage, "actual_size_bytes"),
			"Actual size of this backing image",
			[]string{nodeLabel, diskLabel, backingImageLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	bc.stateMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemBackingImage, "state"),
			"State of this backing image",
			[]string{nodeLabel, diskLabel, backingImageLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	return bc
}

func (bc *BackingImageCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- bc.sizeMetric.Desc
	ch <- bc.stateMetric.Desc
}

func (bc *BackingImageCollector) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			bc.logger.WithField("error", err).Warn("Panic during collecting metrics")
		}
	}()

	diskNodeMap, err := bc.getDiskNodeMap()
	if err != nil {
		bc.logger.WithError(err).Warn("Error during scrape")
		return
	}

	backingImageLists, err := bc.ds.ListBackingImagesRO()
	if err != nil {
		bc.logger.WithError(err).Warn("Error during scrape")
		return
	}

	for _, backingImage := range backingImageLists {
		if backingImage.Status.OwnerID == bc.currentNodeID {
			for diskID, status := range backingImage.Status.DiskFileStatusMap {
				nodeID, ok := diskNodeMap[diskID]
				if !ok {
					bc.logger.Warnf("Failed to find nodeID with the given diskID %v of the BackingImage %v", diskID, backingImage.Name)
					continue
				}
				ch <- prometheus.MustNewConstMetric(bc.sizeMetric.Desc, bc.sizeMetric.Type, float64(backingImage.Status.Size), nodeID, diskID, backingImage.Name)
				ch <- prometheus.MustNewConstMetric(bc.stateMetric.Desc, bc.stateMetric.Type, float64(getBackingImageStateValue(status)), nodeID, diskID, backingImage.Name)
			}

		}
	}
}

func (bc *BackingImageCollector) getDiskNodeMap() (map[string]string, error) {
	diskNodeMap := make(map[string]string)
	nodeList, err := bc.ds.ListNodesRO()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get node list")
	}

	for _, node := range nodeList {
		for diskID := range node.Spec.Disks {
			diskNodeMap[diskID] = node.Name
		}
	}

	return diskNodeMap, nil
}

func getBackingImageStateValue(status *longhorn.BackingImageDiskFileStatus) int {
	stateValue := 0
	switch status.State {
	case longhorn.BackingImageStatePending:
		stateValue = 0
	case longhorn.BackingImageStateStarting:
		stateValue = 1
	case longhorn.BackingImageStateInProgress:
		stateValue = 2
	case longhorn.BackingImageStateReadyForTransfer:
		stateValue = 3
	case longhorn.BackingImageStateReady:
		stateValue = 4
	case longhorn.BackingImageStateFailed:
		stateValue = 5
	case longhorn.BackingImageStateFailedAndCleanUp:
		stateValue = 6
	case longhorn.BackingImageStateUnknown:
		stateValue = 7
	}
	return stateValue
}
