package metricscollector

import (
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/metrics_collector/registry"
	_ "github.com/longhorn/longhorn-manager/metrics_collector/workqueue" // load the workqueue metrics
	"github.com/longhorn/longhorn-manager/types"
)

func InitMetricsCollectorSystem(logger logrus.FieldLogger, currentNodeID string, ds *datastore.DataStore, kubeconfigPath string) {
	vc := NewVolumeCollector(logger, currentNodeID, ds)
	dc := NewDiskCollector(logger, currentNodeID, ds)
	bc := NewBackupCollector(logger, currentNodeID, ds)

	if err := registry.Register(vc); err != nil {
		logger.WithField("collector", subsystemVolume).WithError(err).Warn("failed to register collector")
	}

	if err := registry.Register(dc); err != nil {
		logger.WithField("collector", subsystemDisk).WithError(err).Warn("failed to register collector")
	}

	if err := registry.Register(bc); err != nil {
		logger.WithField("collector", subsystemBackup).WithError(err).Warn("failed to register collector")
	}

	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		logger.Warnf("Cannot detect pod namespace, environment variable %v is missing, "+
			"using default namespace", types.EnvPodNamespace)
		namespace = corev1.NamespaceDefault
	}

	if kubeMetricsClient, err := buildMetricClientFromConfigPath(kubeconfigPath); err != nil {
		logger.WithError(err).Warn("skip instantiating InstanceManagerCollector, ManagerCollector, and NodeCollector")
	} else {
		imc := NewInstanceManagerCollector(logger, currentNodeID, ds, kubeMetricsClient, namespace)
		nc := NewNodeCollector(logger, currentNodeID, ds, kubeMetricsClient)
		mc := NewManagerCollector(logger, currentNodeID, ds, kubeMetricsClient, namespace)

		if err := registry.Register(imc); err != nil {
			logger.WithField("collector", subsystemInstanceManager).WithError(err).Warn("failed to register collector")
		}
		if err := registry.Register(nc); err != nil {
			logger.WithField("collector", subsystemNode).WithError(err).Warn("failed to register collector")
		}
		if err := registry.Register(mc); err != nil {
			logger.WithField("collector", subsystemManager).WithError(err).Warn("failed to register collector")
		}
	}

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
