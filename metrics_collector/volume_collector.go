package metricscollector

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/longhorn/longhorn-manager/controller"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type VolumeCollector struct {
	*baseCollector

	proxyConnCounter util.Counter

	capacityMetric   metricInfo
	sizeMetric       metricInfo
	stateMetric      metricInfo
	robustnessMetric metricInfo

	volumePerfMetrics
}

type volumePerfMetrics struct {
	throughputMetrics rwMetrics
	iopsMetrics       rwMetrics
	latencyMetrics    rwMetrics
}

type rwMetrics struct {
	read  metricInfo
	write metricInfo
}

func NewVolumeCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore) *VolumeCollector {

	vc := &VolumeCollector{
		baseCollector:    newBaseCollector(subsystemVolume, logger, nodeID, ds),
		proxyConnCounter: util.NewAtomicCounter(),
	}

	vc.capacityMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "capacity_bytes"),
			"Configured size in bytes for this volume",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.sizeMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "actual_size_bytes"),
			"Actual space used by each replica of the volume on the corresponding node",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.stateMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "state"),
			"State of this volume",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.robustnessMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "robustness"),
			"Robustness of this volume",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.volumePerfMetrics.throughputMetrics.read = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "read_throughput"),
			"Read throughput of this volume (Bytes/s)",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.volumePerfMetrics.throughputMetrics.write = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "write_throughput"),
			"Write throughput of this volume (Bytes/s)",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.volumePerfMetrics.iopsMetrics.read = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "read_iops"),
			"Read IOPS of this volume",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.volumePerfMetrics.iopsMetrics.write = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "write_iops"),
			"Write IOPS of this volume",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.volumePerfMetrics.latencyMetrics.read = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "read_latency"),
			"Read latency of this volume (ns)",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.volumePerfMetrics.latencyMetrics.write = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "write_latency"),
			"Write latency of this volume (ns)",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	return vc
}

func (vc *VolumeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- vc.capacityMetric.Desc
	ch <- vc.sizeMetric.Desc
	ch <- vc.stateMetric.Desc
	ch <- vc.robustnessMetric.Desc
}

func (vc *VolumeCollector) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if err := recover(); err != nil {
			vc.logger.WithField("error", err).Warn("Panic during collecting metrics")
		}
	}()

	volumeLists, err := vc.ds.ListVolumesRO()
	if err != nil {
		vc.logger.WithError(err).Warn("Error during scrape")
		return
	}

	for _, v := range volumeLists {
		if v.Status.OwnerID == vc.currentNodeID {
			vc.collectMetrics(ch, v)
		}
	}
}

func (vc *VolumeCollector) collectMetrics(ch chan<- prometheus.Metric, v *longhorn.Volume) {
	defer func() {
		if err := recover(); err != nil {
			vc.logger.WithField("error", err).Warnf("Panic during collecting metrics for volume %v", v.Name)
		}
	}()

	e, err := vc.ds.GetVolumeCurrentEngine(v.Name)
	if err != nil {
		vc.logger.WithError(err).Warnf("Failed to get engine for volume %v", v.Name)
		return
	}

	engineClientProxy, err := vc.getEngineClientProxy(e)
	if err != nil {
		vc.logger.WithError(err).Warnf("Failed to get engine proxy of %v for volume %v", e.Name, v.Name)
		return
	}
	defer engineClientProxy.Close()

	metrics, err := engineClientProxy.MetricsGet(e)
	if err != nil {
		vc.logger.WithError(err).Warnf("Failed to get metrics from volume %v from engine %v", e.Spec.VolumeName, e.Name)
	}

	ch <- prometheus.MustNewConstMetric(vc.capacityMetric.Desc, vc.capacityMetric.Type, float64(v.Spec.Size), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
	ch <- prometheus.MustNewConstMetric(vc.sizeMetric.Desc, vc.sizeMetric.Type, float64(v.Status.ActualSize), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
	ch <- prometheus.MustNewConstMetric(vc.stateMetric.Desc, vc.stateMetric.Type, float64(getVolumeStateValue(v)), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
	ch <- prometheus.MustNewConstMetric(vc.robustnessMetric.Desc, vc.robustnessMetric.Type, float64(getVolumeRobustnessValue(v)), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
	ch <- prometheus.MustNewConstMetric(vc.volumePerfMetrics.throughputMetrics.read.Desc, vc.volumePerfMetrics.throughputMetrics.read.Type, float64(vc.getVolumeReadThroughput(metrics)), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
	ch <- prometheus.MustNewConstMetric(vc.volumePerfMetrics.throughputMetrics.write.Desc, vc.volumePerfMetrics.throughputMetrics.write.Type, float64(vc.getVolumeWriteThroughput(metrics)), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
	ch <- prometheus.MustNewConstMetric(vc.volumePerfMetrics.iopsMetrics.read.Desc, vc.volumePerfMetrics.iopsMetrics.read.Type, float64(vc.getVolumeReadIOPS(metrics)), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
	ch <- prometheus.MustNewConstMetric(vc.volumePerfMetrics.iopsMetrics.write.Desc, vc.volumePerfMetrics.iopsMetrics.write.Type, float64(vc.getVolumeWriteIOPS(metrics)), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
	ch <- prometheus.MustNewConstMetric(vc.volumePerfMetrics.latencyMetrics.read.Desc, vc.volumePerfMetrics.latencyMetrics.read.Type, float64(vc.getVolumeReadLatency(metrics)), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
	ch <- prometheus.MustNewConstMetric(vc.volumePerfMetrics.latencyMetrics.write.Desc, vc.volumePerfMetrics.latencyMetrics.write.Type, float64(vc.getVolumeWriteLatency(metrics)), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
}

func (vc *VolumeCollector) getEngineClientProxy(engine *longhorn.Engine) (c engineapi.EngineClientProxy, err error) {
	engineCliClient, err := controller.GetBinaryClientForEngine(engine, &engineapi.EngineCollection{}, engine.Status.CurrentImage)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get binary client for engine %v", engine.Name)
	}

	return engineapi.GetCompatibleClient(engine, engineCliClient, vc.ds, nil, vc.proxyConnCounter)
}

func getVolumeStateValue(v *longhorn.Volume) int {
	stateValue := 0
	switch v.Status.State {
	case longhorn.VolumeStateCreating:
		stateValue = 1
	case longhorn.VolumeStateAttached:
		stateValue = 2
	case longhorn.VolumeStateDetached:
		stateValue = 3
	case longhorn.VolumeStateAttaching:
		stateValue = 4
	case longhorn.VolumeStateDetaching:
		stateValue = 5
	case longhorn.VolumeStateDeleting:
		stateValue = 6
	}
	return stateValue
}

func getVolumeRobustnessValue(v *longhorn.Volume) int {
	robustnessValue := 0
	switch v.Status.Robustness {
	case longhorn.VolumeRobustnessUnknown:
		robustnessValue = 0
	case longhorn.VolumeRobustnessHealthy:
		robustnessValue = 1
	case longhorn.VolumeRobustnessDegraded:
		robustnessValue = 2
	case longhorn.VolumeRobustnessFaulted:
		robustnessValue = 3
	}
	return robustnessValue
}

func (vc *VolumeCollector) getVolumeReadThroughput(metrics *engineapi.Metrics) int64 {
	if metrics == nil {
		return 0
	}
	return int64(metrics.ReadThroughput)
}

func (vc *VolumeCollector) getVolumeWriteThroughput(metrics *engineapi.Metrics) int64 {
	if metrics == nil {
		return 0
	}
	return int64(metrics.WriteThroughput)
}

func (vc *VolumeCollector) getVolumeReadIOPS(metrics *engineapi.Metrics) int64 {
	if metrics == nil {
		return 0
	}
	return int64(metrics.ReadIOPS)
}

func (vc *VolumeCollector) getVolumeWriteIOPS(metrics *engineapi.Metrics) int64 {
	if metrics == nil {
		return 0
	}
	return int64(metrics.WriteIOPS)
}

func (vc *VolumeCollector) getVolumeReadLatency(metrics *engineapi.Metrics) int64 {
	if metrics == nil {
		return 0
	}
	return int64(metrics.ReadLatency)
}

func (vc *VolumeCollector) getVolumeWriteLatency(metrics *engineapi.Metrics) int64 {
	if metrics == nil {
		return 0
	}
	return int64(metrics.WriteLatency)
}
