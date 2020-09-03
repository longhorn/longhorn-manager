package monitoring

import (
	"net/http"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
)

// longhornCustomRegistry exposes Longhorn metrics only.
// We use longhornCustomRegistry to get rid of all default Prometheus go-client metrics
var longhornCustomRegistry = prometheus.NewRegistry()

// Register registers the provided Collector with the longhornCustomRegistry
func Register(collector prometheus.Collector) error {
	return longhornCustomRegistry.Register(collector)
}

// Handler returns an http.Handler for longhornCustomRegistry, using default HandlerOpts
func Handler() http.Handler {
	return promhttp.HandlerFor(longhornCustomRegistry, promhttp.HandlerOpts{})
}

func InitMonitoringSystem(logger logrus.FieldLogger, currentNodeID string, ds *datastore.DataStore, kubeconfigPath string) {
	vc := NewVolumeCollector(logger, currentNodeID, ds)
	nc := NewNodeCollector(logger, currentNodeID, ds)

	if err := Register(vc); err != nil {
		logger.WithField("collector", subsystemVolume).WithError(err).Warn("failed to register collector")
	}
	if err := Register(nc); err != nil {
		logger.WithField("collector", subsystemNode).WithError(err).Warn("failed to register collector")
	}

	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		logger.Warnf("Cannot detect pod namespace, environment variable %v is missing, "+
			"using default namespace", types.EnvPodNamespace)
		namespace = corev1.NamespaceDefault
	}

	if kubeMetricsClient, err := buildMetricClientFromConfigPath(kubeconfigPath); err != nil {
		logger.WithError(err).Warn("skip instantiating InstanceManagerCollector and ManagerCollector")
	} else {
		imc := NewInstanceManagerCollector(logger, currentNodeID, ds, kubeMetricsClient, namespace)
		mc := NewManagerCollector(logger, currentNodeID, ds, kubeMetricsClient, namespace)

		if err := Register(imc); err != nil {
			logger.WithField("collector", subsystemInstanceManager).WithError(err).Warn("failed to register collector")
		}
		if err := Register(mc); err != nil {
			logger.WithField("collector", subsystemManager).WithError(err).Warn("failed to register collector")
		}
	}

	return
}

func buildMetricClientFromConfigPath(kubeconfigPath string) (*metricsclientset.Clientset, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get client config")
	}

	kubeMetricsClient, err := metricsclientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get k8s metrics client")
	}

	return kubeMetricsClient, nil
}
