package metricscollector

import (
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

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

	// Health metrics
	healthMetric          metricInfo
	healthAttributeMetric metricInfo
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

	dc.healthMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemDisk, "health"),
			"Health status of this disk (1 = healthy, 0 = unhealthy)",
			[]string{nodeLabel, diskLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	dc.healthAttributeMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemDisk, "health_attribute_raw"),
			"Health attribute raw value for this disk",
			[]string{nodeLabel, diskLabel, "attribute", "attribute_id"},
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
	ch <- dc.healthMetric.Desc
	ch <- dc.healthAttributeMetric.Desc
}

func (dc *DiskCollector) Collect(ch chan<- prometheus.Metric) {
	dc.collectDiskStorage(ch)
}

func (dc *DiskCollector) getDiskServiceClient() (diskServiceClient *engineapi.DiskService, err error) {
	v2DataEngineEnabled, err := dc.ds.GetSettingAsBool(types.SettingNameV2DataEngine)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get setting %v for disk collector", types.SettingNameV2DataEngine)
	}

	if !v2DataEngineEnabled {
		return nil, nil
	}

	im, err := dc.ds.GetRunningInstanceManagerByNodeRO(dc.currentNodeID, longhorn.DataEngineTypeV2)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get running instance manager for node %v", dc.currentNodeID)
	}

	diskServiceClient, err = engineapi.NewDiskServiceClient(im, dc.logger)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create disk service client for instance manager %v", im.Name)
	}

	return diskServiceClient, nil
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

	diskServiceClient, err := dc.getDiskServiceClient()
	if err != nil {
		dc.logger.WithError(err).Warn("Failed to get disk service client")
	} else if diskServiceClient != nil {
		defer diskServiceClient.Close()
	}

	disks := getDiskListFromNode(node)

	for diskName, disk := range disks {
		diskPath := disk.Status.DiskPath
		diskDriver := string(disk.Status.DiskDriver)
		storageCapacity := disk.Status.StorageMaximum
		storageUsage := disk.Status.StorageMaximum - disk.Status.StorageAvailable
		storageReservation := disk.Spec.StorageReserved

		ch <- prometheus.MustNewConstMetric(dc.capacityMetric.Desc, dc.capacityMetric.Type, float64(storageCapacity), dc.currentNodeID, diskName)
		ch <- prometheus.MustNewConstMetric(dc.usageMetric.Desc, dc.usageMetric.Type, float64(storageUsage), dc.currentNodeID, diskName)
		ch <- prometheus.MustNewConstMetric(dc.reservationMetric.Desc, dc.reservationMetric.Type, float64(storageReservation), dc.currentNodeID, diskName)

		if diskServiceClient != nil && disk.Spec.Type == longhorn.DiskTypeBlock {
			diskMetrics, err := diskServiceClient.MetricsGet(string(disk.Spec.Type), diskName, diskPath, diskDriver)
			if err == nil {
				// Collect disk performance metrics if available
				if diskMetrics != nil {
					ch <- prometheus.MustNewConstMetric(dc.readThroughputMetric.Desc, dc.readThroughputMetric.Type, float64(diskMetrics.ReadThroughput), dc.currentNodeID, diskName, diskPath)
					ch <- prometheus.MustNewConstMetric(dc.writeThroughputMetric.Desc, dc.writeThroughputMetric.Type, float64(diskMetrics.WriteThroughput), dc.currentNodeID, diskName, diskPath)
					ch <- prometheus.MustNewConstMetric(dc.readIOPSMetric.Desc, dc.readIOPSMetric.Type, float64(diskMetrics.ReadIOPS), dc.currentNodeID, diskName, diskPath)
					ch <- prometheus.MustNewConstMetric(dc.writeIOPSMetric.Desc, dc.writeIOPSMetric.Type, float64(diskMetrics.WriteIOPS), dc.currentNodeID, diskName, diskPath)
					ch <- prometheus.MustNewConstMetric(dc.readLatencyMetric.Desc, dc.readLatencyMetric.Type, float64(diskMetrics.ReadLatency), dc.currentNodeID, diskName, diskPath)
					ch <- prometheus.MustNewConstMetric(dc.writeLatencyMetric.Desc, dc.writeLatencyMetric.Type, float64(diskMetrics.WriteLatency), dc.currentNodeID, diskName, diskPath)
				}
			} else {
				dc.logger.WithError(err).WithField("disk", diskName).Warn("Failed to get disk metrics")
			}
		}

		for _, condition := range disk.Status.Conditions {
			val := 0
			if condition.Status == longhorn.ConditionStatusTrue {
				val = 1
			}
			ch <- prometheus.MustNewConstMetric(dc.statusMetric.Desc, dc.statusMetric.Type, float64(val), dc.currentNodeID, diskName, strings.ToLower(condition.Type), condition.Reason)
		}

		// Health metrics (best-effort)
		if disk.Status.HealthData != nil {
			for _, healthData := range disk.Status.HealthData {
				if healthData.HealthStatus != "" {
					val := 0.0
					if healthData.HealthStatus == longhorn.HealthDataStatusPassed {
						val = 1.0
					}
					ch <- prometheus.MustNewConstMetric(dc.healthMetric.Desc, dc.healthMetric.Type, val, dc.currentNodeID, diskName)
				}

				// Export raw attribute values.
				for _, attr := range healthData.Attributes {
					if attr == nil {
						continue
					}

					// Some devices may expose multiple attributes with identical
					// names. To ensure label uniqueness in Prometheus, include
					// the attribute ID.
					attrIDStr := ""
					if attr.ID != 0 {
						attrIDStr = strconv.FormatUint(uint64(attr.ID), 10)
					}
					ch <- prometheus.MustNewConstMetric(dc.healthAttributeMetric.Desc, dc.healthAttributeMetric.Type, float64(attr.RawValue), dc.currentNodeID, diskName, attr.Name, attrIDStr)
				}
			}
		}
	}
}
