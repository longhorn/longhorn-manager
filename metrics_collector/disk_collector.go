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

	// Performance metrics
	readThroughputMetric  metricInfo
	writeThroughputMetric metricInfo
	readIOPSMetric        metricInfo
	writeIOPSMetric       metricInfo
	readLatencyMetric     metricInfo
	writeLatencyMetric    metricInfo
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

	// Performance metrics
	dc.readThroughputMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemDisk, "read_throughput"),
			"Read throughput of this disk (Bytes/s)",
			[]string{nodeLabel, diskLabel, diskPathLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	dc.writeThroughputMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemDisk, "write_throughput"),
			"Write throughput of this disk (Bytes/s)",
			[]string{nodeLabel, diskLabel, diskPathLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	dc.readIOPSMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemDisk, "read_iops"),
			"Read IOPS of this disk",
			[]string{nodeLabel, diskLabel, diskPathLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	dc.writeIOPSMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemDisk, "write_iops"),
			"Write IOPS of this disk",
			[]string{nodeLabel, diskLabel, diskPathLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	dc.readLatencyMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemDisk, "read_latency"),
			"Read latency of this disk (ns)",
			[]string{nodeLabel, diskLabel, diskPathLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	dc.writeLatencyMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemDisk, "write_latency"),
			"Write latency of this disk (ns)",
			[]string{nodeLabel, diskLabel, diskPathLabel},
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
	ch <- dc.readThroughputMetric.Desc
	ch <- dc.writeThroughputMetric.Desc
	ch <- dc.readIOPSMetric.Desc
	ch <- dc.writeIOPSMetric.Desc
	ch <- dc.readLatencyMetric.Desc
	ch <- dc.writeLatencyMetric.Desc
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
		diskPath := disk.DiskPath
		storageCapacity := disk.StorageMaximum
		storageUsage := disk.StorageMaximum - disk.StorageAvailable
		storageReservation := disk.StorageReserved

		ch <- prometheus.MustNewConstMetric(dc.capacityMetric.Desc, dc.capacityMetric.Type, float64(storageCapacity), dc.currentNodeID, diskName)
		ch <- prometheus.MustNewConstMetric(dc.usageMetric.Desc, dc.usageMetric.Type, float64(storageUsage), dc.currentNodeID, diskName)
		ch <- prometheus.MustNewConstMetric(dc.reservationMetric.Desc, dc.reservationMetric.Type, float64(storageReservation), dc.currentNodeID, diskName)

		// Collect disk performance metrics if available
		if disk.DiskMetrics != nil {
			ch <- prometheus.MustNewConstMetric(dc.readThroughputMetric.Desc, dc.readThroughputMetric.Type, float64(disk.DiskMetrics.ReadThroughput), dc.currentNodeID, diskName, diskPath)
			ch <- prometheus.MustNewConstMetric(dc.writeThroughputMetric.Desc, dc.writeThroughputMetric.Type, float64(disk.DiskMetrics.WriteThroughput), dc.currentNodeID, diskName, diskPath)
			ch <- prometheus.MustNewConstMetric(dc.readIOPSMetric.Desc, dc.readIOPSMetric.Type, float64(disk.DiskMetrics.ReadIOPS), dc.currentNodeID, diskName, diskPath)
			ch <- prometheus.MustNewConstMetric(dc.writeIOPSMetric.Desc, dc.writeIOPSMetric.Type, float64(disk.DiskMetrics.WriteIOPS), dc.currentNodeID, diskName, diskPath)
			ch <- prometheus.MustNewConstMetric(dc.readLatencyMetric.Desc, dc.readLatencyMetric.Type, float64(disk.DiskMetrics.ReadLatency), dc.currentNodeID, diskName, diskPath)
			ch <- prometheus.MustNewConstMetric(dc.writeLatencyMetric.Desc, dc.writeLatencyMetric.Type, float64(disk.DiskMetrics.WriteLatency), dc.currentNodeID, diskName, diskPath)
		}

		for _, condition := range disk.Conditions {
			val := 0
			if condition.Status == longhorn.ConditionStatusTrue {
				val = 1
			}
			ch <- prometheus.MustNewConstMetric(dc.statusMetric.Desc, dc.statusMetric.Type, float64(val), dc.currentNodeID, diskName, strings.ToLower(condition.Type), condition.Reason)
		}
	}
}
