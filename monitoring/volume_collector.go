package monitoring

import (
	"github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type VolumeCollector struct {
	*baseCollector

	capacityMetric   metricInfo
	sizeMetric       metricInfo
	stateMetric      metricInfo
	robustnessMetric metricInfo
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
			prometheus.BuildFQName(longhornName, subsystemVolume, "actual_size_bytes"),
			"Actual space used by each replica of the volume on the corresponding node",
			[]string{nodeLabel, volumeLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.stateMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "state"),
			"State of this volume",
			[]string{nodeLabel, volumeLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	vc.robustnessMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemVolume, "robustness"),
			"Robustness of this volume",
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
	ch <- vc.stateMetric.Desc
	ch <- vc.robustnessMetric.Desc
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
			ch <- prometheus.MustNewConstMetric(vc.stateMetric.Desc, vc.stateMetric.Type, float64(getVolumeStateValue(v)), vc.currentNodeID, v.Name)
			ch <- prometheus.MustNewConstMetric(vc.robustnessMetric.Desc, vc.robustnessMetric.Type, float64(getVolumeRobustnessValue(v)), vc.currentNodeID, v.Name)
		}
	}
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
