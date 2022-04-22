package metricscollector

import (
	"github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/longhorn/longhorn-manager/datastore"
)

type DiskCollector struct {
	*baseCollector

	capacityMetric    metricInfo
	usageMetric       metricInfo
	reservationMetric metricInfo
}

func NewDiskCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore) *DiskCollector {

	dc := &DiskCollector{
		baseCollector: newBaseCollector(subsystemDisk, logger, nodeID, ds),
	}

	dc.capacityMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemDisk, "capacity_bytes"),
			"The storage capacity of this disk",
			[]string{nodeLabel, diskLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	dc.usageMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemDisk, "usage_bytes"),
			"The used storage of this disk",
			[]string{nodeLabel, diskLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	dc.reservationMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemDisk, "reservation_bytes"),
			"The reserved storage for other applications and system on this disk",
			[]string{nodeLabel, diskLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	return dc
}

func (dc *DiskCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- dc.capacityMetric.Desc
	ch <- dc.usageMetric.Desc
	ch <- dc.reservationMetric.Desc
}

func (dc *DiskCollector) Collect(ch chan<- prometheus.Metric) {
	dc.collectDiskStorage(ch)
}

func (dc *DiskCollector) collectDiskStorage(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			dc.logger.WithField("error", err).Warn("panic during collecting metrics")
		}
	}()

	node, err := dc.ds.GetNodeRO(dc.currentNodeID)
	if err != nil {
		dc.logger.WithError(err).Warn("error during scrape")
		return
	}

	disks := getDiskListFromNode(node)

	for diskName, disk := range disks {
		storageCapacity := disk.StorageMaximum
		storageUsage := disk.StorageMaximum - disk.StorageAvailable
		storageReservation := disk.StorageReserved
		ch <- prometheus.MustNewConstMetric(dc.capacityMetric.Desc, dc.capacityMetric.Type, float64(storageCapacity), dc.currentNodeID, diskName)
		ch <- prometheus.MustNewConstMetric(dc.usageMetric.Desc, dc.usageMetric.Type, float64(storageUsage), dc.currentNodeID, diskName)
		ch <- prometheus.MustNewConstMetric(dc.reservationMetric.Desc, dc.reservationMetric.Type, float64(storageReservation), dc.currentNodeID, diskName)
	}
}
