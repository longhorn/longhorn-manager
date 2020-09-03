package monitoring

import (
	"github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/longhorn/longhorn-manager/datastore"
)

type VolumeCollector struct {
	*baseCollector

	capacityMetric metricInfo
	sizeMetric     metricInfo
}

func NewVolumeCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore) *VolumeCollector {

	vc := &VolumeCollector{
		baseCollector: newBaseCollector(subsystemVolume, logger, nodeID, ds),
	}

	vc.capacityMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "capacity_bytes"),
			"Configured size in bytes for this volume",
			[]string{nodeLabel, volumeLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.sizeMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "usage_bytes"),
			"Used storage space in bytes for this volume",
			[]string{nodeLabel, volumeLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}
	return vc
}

func (vc *VolumeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- vc.capacityMetric.Desc
	ch <- vc.sizeMetric.Desc
}

func (vc *VolumeCollector) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			vc.logger.WithField("error", err).Warn("panic during collecting metrics")
		}
	}()

	volumeLists, err := vc.ds.ListVolumesRO()
	if err != nil {
		vc.logger.WithError(err).Warn("error during scrape ")
		return
	}

	for _, v := range volumeLists {
		if v.Status.OwnerID == vc.currentNodeID {
			ch <- prometheus.MustNewConstMetric(vc.capacityMetric.Desc, vc.capacityMetric.Type, float64(v.Spec.Size), vc.currentNodeID, v.Name)
			ch <- prometheus.MustNewConstMetric(vc.sizeMetric.Desc, vc.sizeMetric.Type, float64(v.Status.ActualSize), vc.currentNodeID, v.Name)
		}
	}
}
