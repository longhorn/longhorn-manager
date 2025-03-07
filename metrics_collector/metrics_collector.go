package metricscollector

import (
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/client-go/tools/clientcmd"

	corev1 "k8s.io/api/core/v1"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/metrics_collector/registry"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	_ "github.com/longhorn/longhorn-manager/metrics_collector/client_go_adaper" // load the client-go metrics
	_ "github.com/longhorn/longhorn-manager/metrics_collector/workqueue"        // load the workqueue metrics
)

func InitMetricsCollectorSystem(logger logrus.FieldLogger, currentNodeID string, ds *datastore.DataStore, kubeconfigPath string, proxyConnCounter util.Counter) {
	logger.Info("Initializing metrics collector system")

	volumeCollector := NewVolumeCollector(logger, currentNodeID, ds)
	diskCollector := NewDiskCollector(logger, currentNodeID, ds)
	backupCollector := NewBackupCollector(logger, currentNodeID, ds)
	snapshotController := NewSnapshotCollector(logger, currentNodeID, ds)
	backingImageCollector := NewBackingImageCollector(logger, currentNodeID, ds)
	backupBackingImageCollector := NewBackupBackingImageCollector(logger, currentNodeID, ds)
	engineCollector := NewEngineCollector(logger, currentNodeID, ds)

	if err := registry.Register(volumeCollector); err != nil {
		logger.WithField("collector", subsystemVolume).WithError(err).Warn("Failed to register collector")
	}

	if err := registry.Register(diskCollector); err != nil {
		logger.WithField("collector", subsystemDisk).WithError(err).Warn("Failed to register collector")
	}

	if err := registry.Register(backupCollector); err != nil {
		logger.WithField("collector", subsystemBackup).WithError(err).Warn("Failed to register collector")
	}

	if err := registry.Register(snapshotController); err != nil {
		logger.WithField("collector", subsystemSnapshot).WithError(err).Warn("Failed to register collector")
	}

	if err := registry.Register(backingImageCollector); err != nil {
		logger.WithField("collector", subsystemBackingImage).WithError(err).Warn("Failed to register collector")
	}

	if err := registry.Register(backupBackingImageCollector); err != nil {
		logger.WithField("collector", subsystemBackupBackingImage).WithError(err).Warn("Failed to register collector")
	}

	if err := registry.Register(engineCollector); err != nil {
		logger.WithField("collector", subsystemEngine).WithError(err).Warn("Failed to register collector")
	}

	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		logger.Warnf("Cannot detect pod namespace, environment variable %v is missing, "+
			"using default namespace", types.EnvPodNamespace)
		namespace = corev1.NamespaceDefault
	}

	if kubeMetricsClient, err := buildMetricClientFromConfigPath(kubeconfigPath); err != nil {
		logger.WithError(err).Warn("Skipped instantiating InstanceManagerCollector, ManagerCollector, and NodeCollector")
	} else {
		instanceManagerCollector := NewInstanceManagerCollector(logger, currentNodeID, ds, proxyConnCounter, kubeMetricsClient, namespace)
		nodeCollector := NewNodeCollector(logger, currentNodeID, ds, kubeMetricsClient)
		managerCollector := NewManagerCollector(logger, currentNodeID, ds, kubeMetricsClient, namespace)

		if err := registry.Register(instanceManagerCollector); err != nil {
			logger.WithField("collector", subsystemInstanceManager).WithError(err).Warn("Failed to register collector")
		}
		if err := registry.Register(nodeCollector); err != nil {
			logger.WithField("collector", subsystemNode).WithError(err).Warn("Failed to register collector")
		}
		if err := registry.Register(managerCollector); err != nil {
			logger.WithField("collector", subsystemManager).WithError(err).Warn("Failed to register collector")
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
