package metricscollector

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type DiskCollector struct {
	*baseCollector

	capacityMetric    metricInfo
	usageMetric       metricInfo
	reservationMetric metricInfo
	statusMetric      metricInfo
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

	dc.statusMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemDisk, "status"),
			"The status of this disk",
			[]string{nodeLabel, diskLabel, conditionLabel, conditionReasonLabel},
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
	ch <- dc.statusMetric.Desc
}

func (dc *DiskCollector) Collect(ch chan<- prometheus.Metric) {
	dc.collectDiskStorage(ch)
}

func (dc *DiskCollector) collectDiskStorage(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			dc.logger.WithField("error", err).Warn("Panic during collecting metrics")
		}
	}()

	node, err := dc.ds.GetNodeRO(dc.currentNodeID)
	if err != nil {
		dc.logger.WithError(err).Warn("Error during scrape")
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

		for _, condition := range disk.Conditions {
			val := 0
			if condition.Status == longhorn.ConditionStatusTrue {
				val = 1
			}
			ch <- prometheus.MustNewConstMetric(dc.statusMetric.Desc, dc.statusMetric.Type, float64(val), dc.currentNodeID, diskName, strings.ToLower(condition.Type), condition.Reason)
		}
	}
}
