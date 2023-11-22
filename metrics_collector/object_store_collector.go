package metricscollector

import (
	"fmt"
	"strconv"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	prometheus "github.com/prometheus/client_golang/prometheus"
	logrus "github.com/sirupsen/logrus"
)

type ObjectStoreCollector struct {
	*baseCollector

	sizeMetric metricInfo
}

func NewObjectStoreCollector(
	logger logrus.FieldLogger,
	nodeID string,
	ds *datastore.DataStore,
) *ObjectStoreCollector {
	osc := &ObjectStoreCollector{
		baseCollector: newBaseCollector(subsystemObjectStorage, logger, nodeID, ds),
	}

	osc.sizeMetric = metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(longhornName, subsystemObjectStorage, "size_bytes"),
			"Allocated size of this object store",
			[]string{volumeLabel, objectStoreLabel},
			nil,
		),
		Type: prometheus.GaugeValue,
	}

	return osc
}

func (osc *ObjectStoreCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- osc.sizeMetric.Desc
}

func (osc *ObjectStoreCollector) Collect(ch chan<- prometheus.Metric) {
	osList, err := osc.ds.ListObjectStoresRO()
	if err != nil {
		osc.logger.WithError(err).Warn("error during scrape")
	}

	for _, store := range osList {
		vol, err := osc.ds.GetVolume(store.Name)
		if err != nil {
			osc.logger.WithError(err).Warnf("could not find volume for %v", store.Name)
		}

		if vol.Status.OwnerID == osc.currentNodeID {
			var size float64
			if size, err = strconv.ParseFloat(fmt.Sprintf("%v", store.Spec.Size.Value()), 64); err != nil {
				osc.logger.WithError(err).Warnf("error getting size for %v", store.Name)
			}
			objectStoreName, ok := store.Labels[types.LonghornLabelObjectStore]
			if !ok {
				osc.logger.WithError(err).Warnf("error getting object store label %v", store.Name)
			}
			ch <- prometheus.MustNewConstMetric(
				osc.sizeMetric.Desc,
				osc.sizeMetric.Type,
				size,
				objectStoreName,
				store.Name,
			)
		}
	}
}
