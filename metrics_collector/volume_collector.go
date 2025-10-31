package metricscollector

import (
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	imtypes "github.com/longhorn/longhorn-instance-manager/pkg/types"

	"github.com/longhorn/longhorn-manager/controller"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type VolumeCollector struct {
	*baseCollector

	proxyConnCounter util.Counter

	capacityMetric           metricInfo
	sizeMetric               metricInfo
	stateMetric              metricInfo
	robustnessMetric         metricInfo
	fileSystemReadOnlyMetric metricInfo

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

	vc.fileSystemReadOnlyMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "file_system_read_only"),
			"Volume whose mount point is in read-only mode",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
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
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel, stateLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.robustnessMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "robustness"),
			"Robustness of this volume",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel, stateLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.throughputMetrics.read = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "read_throughput"),
			"Read throughput of this volume (Bytes/s)",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.throughputMetrics.write = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "write_throughput"),
			"Write throughput of this volume (Bytes/s)",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.iopsMetrics.read = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "read_iops"),
			"Read IOPS of this volume",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.iopsMetrics.write = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "write_iops"),
			"Write IOPS of this volume",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.latencyMetrics.read = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "read_latency"),
			"Read latency of this volume (ns)",
			[]string{nodeLabel, volumeLabel, pvcLabel, pvcNamespaceLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.latencyMetrics.write = metricInfo{
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
	ch <- vc.fileSystemReadOnlyMetric.Desc
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

	ch <- prometheus.MustNewConstMetric(vc.capacityMetric.Desc, vc.capacityMetric.Type, float64(v.Spec.Size), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
	ch <- prometheus.MustNewConstMetric(vc.sizeMetric.Desc, vc.sizeMetric.Type, float64(v.Status.ActualSize), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)

	vc.collectVolumeState(ch, v)
	vc.collectVolumeRobustness(ch, v)

	e, err := vc.ds.GetVolumeCurrentEngine(v.Name)
	if err != nil {
		vc.logger.WithError(err).Debugf("Failed to get engine for volume %v", v.Name)
		return
	}

	engineClientProxy, err := vc.getEngineClientProxy(e)
	if err != nil {
		vc.logger.WithError(err).Debugf("Failed to get engine proxy of %v for volume %v", e.Name, v.Name)
		return
	}
	defer engineClientProxy.Close()

	metrics, err := engineClientProxy.MetricsGet(e)
	if err != nil {
		vc.logger.WithError(err).Debugf("Failed to get metrics from volume %v from engine %v", e.Spec.VolumeName, e.Name)
	}

	ch <- prometheus.MustNewConstMetric(vc.throughputMetrics.read.Desc, vc.throughputMetrics.read.Type, float64(vc.getVolumeReadThroughput(metrics)), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
	ch <- prometheus.MustNewConstMetric(vc.throughputMetrics.write.Desc, vc.throughputMetrics.write.Type, float64(vc.getVolumeWriteThroughput(metrics)), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
	ch <- prometheus.MustNewConstMetric(vc.iopsMetrics.read.Desc, vc.iopsMetrics.read.Type, float64(vc.getVolumeReadIOPS(metrics)), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
	ch <- prometheus.MustNewConstMetric(vc.iopsMetrics.write.Desc, vc.iopsMetrics.write.Type, float64(vc.getVolumeWriteIOPS(metrics)), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
	ch <- prometheus.MustNewConstMetric(vc.latencyMetrics.read.Desc, vc.latencyMetrics.read.Type, float64(vc.getVolumeReadLatency(metrics)), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
	ch <- prometheus.MustNewConstMetric(vc.latencyMetrics.write.Desc, vc.latencyMetrics.write.Type, float64(vc.getVolumeWriteLatency(metrics)), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)

	fileSystemReadOnlyCondition := types.GetCondition(e.Status.Conditions, imtypes.EngineConditionFilesystemReadOnly)
	isPVMountOptionReadOnly, err := vc.ds.IsPVMountOptionReadOnly(v)
	if err != nil {
		vc.logger.WithError(err).Warn("Failed to check if volume's PV mount option is read only during metric collection")
		return
	}

	if fileSystemReadOnlyCondition.Status == longhorn.ConditionStatusTrue && !isPVMountOptionReadOnly {
		ch <- prometheus.MustNewConstMetric(vc.fileSystemReadOnlyMetric.Desc, vc.fileSystemReadOnlyMetric.Type, float64(1), vc.currentNodeID, v.Name, v.Status.KubernetesStatus.PVCName, v.Status.KubernetesStatus.Namespace)
	}

}

func (vc *VolumeCollector) getEngineClientProxy(engine *longhorn.Engine) (c engineapi.EngineClientProxy, err error) {
	engineCliClient, err := controller.GetBinaryClientForEngine(engine, &engineapi.EngineCollection{}, engine.Status.CurrentImage)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get binary client for engine %v", engine.Name)
	}

	return engineapi.GetCompatibleClient(engine, engineCliClient, vc.ds, nil, vc.proxyConnCounter)
}

// collectVolumeState emits label-based state metrics - one metric per state with value 1 for current state, 0 for others
func (vc *VolumeCollector) collectVolumeState(ch chan<- prometheus.Metric, v *longhorn.Volume) {
	states := getAllVolumeStates()
	for _, s := range states {
		val := 0.0
		if v.Status.State == s {
			val = 1.0
		}
		ch <- prometheus.MustNewConstMetric(
			vc.stateMetric.Desc,
			vc.stateMetric.Type,
			val,
			vc.currentNodeID,
			v.Name,
			v.Status.KubernetesStatus.PVCName,
			v.Status.KubernetesStatus.Namespace,
			string(s),
		)
	}
}

// collectVolumeRobustness emits label-based robustness metrics - one metric per robustness state with value 1 for current state, 0 for others
func (vc *VolumeCollector) collectVolumeRobustness(ch chan<- prometheus.Metric, v *longhorn.Volume) {
	robustnessStates := getAllVolumeRobustnessStates()
	for _, r := range robustnessStates {
		val := 0.0
		if v.Status.Robustness == r {
			val = 1.0
		}
		ch <- prometheus.MustNewConstMetric(
			vc.robustnessMetric.Desc,
			vc.robustnessMetric.Type,
			val,
			vc.currentNodeID,
			v.Name,
			v.Status.KubernetesStatus.PVCName,
			v.Status.KubernetesStatus.Namespace,
			string(r),
		)
	}
}

func getAllVolumeStates() []longhorn.VolumeState {
	return []longhorn.VolumeState{
		longhorn.VolumeStateCreating,
		longhorn.VolumeStateAttached,
		longhorn.VolumeStateDetached,
		longhorn.VolumeStateAttaching,
		longhorn.VolumeStateDetaching,
		longhorn.VolumeStateDeleting,
	}
}

func getAllVolumeRobustnessStates() []longhorn.VolumeRobustness {
	return []longhorn.VolumeRobustness{
		longhorn.VolumeRobustnessUnknown,
		longhorn.VolumeRobustnessHealthy,
		longhorn.VolumeRobustnessDegraded,
		longhorn.VolumeRobustnessFaulted,
	}
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
