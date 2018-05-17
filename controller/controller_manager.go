package controller

import (
	"fmt"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	pvController "github.com/kubernetes-incubator/external-storage/lib/controller"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/manager"
	"github.com/rancher/longhorn-manager/types"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhclientset "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhinformers "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions"
)

var (
	Workers              = 5
	longhornFinalizerKey = longhorn.SchemeGroupVersion.Group
)

func StartControllers(stopCh chan struct{}, controllerID, serviceAccount, engineImage, managerImage string) (*datastore.DataStore, error) {
	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		logrus.Warnf("Cannot detect pod namespace, environment variable %v is missing, " +
			"using default namespace")
		namespace = corev1.NamespaceDefault
	}

	// Only supports in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get k8s client")
	}

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get clientset")
	}

	scheme := runtime.NewScheme()
	if err := longhorn.SchemeBuilder.AddToScheme(scheme); err != nil {
		return nil, errors.Wrap(err, "unable to create scheme")
	}

	kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, time.Second*30)
	lhInformerFactory := lhinformers.NewSharedInformerFactory(lhClient, time.Second*30)

	replicaInformer := lhInformerFactory.Longhorn().V1alpha1().Replicas()
	engineInformer := lhInformerFactory.Longhorn().V1alpha1().Engines()
	volumeInformer := lhInformerFactory.Longhorn().V1alpha1().Volumes()

	podInformer := kubeInformerFactory.Core().V1().Pods()
	jobInformer := kubeInformerFactory.Batch().V1().Jobs()
	cronJobInformer := kubeInformerFactory.Batch().V1beta1().CronJobs()
	daemonSetInformer := kubeInformerFactory.Apps().V1beta2().DaemonSets()

	ds := datastore.NewDataStore(volumeInformer, engineInformer, replicaInformer, lhClient,
		podInformer, cronJobInformer, daemonSetInformer, kubeClient, namespace)
	rc := NewReplicaController(ds, scheme, replicaInformer, podInformer, jobInformer, kubeClient,
		namespace, controllerID)
	ec := NewEngineController(ds, scheme, engineInformer, podInformer, kubeClient,
		&engineapi.EngineCollection{}, namespace, controllerID)
	vc := NewVolumeController(ds, scheme, volumeInformer, engineInformer, replicaInformer, kubeClient,
		namespace, controllerID, serviceAccount, engineImage, managerImage)

	go kubeInformerFactory.Start(stopCh)
	go lhInformerFactory.Start(stopCh)
	if !ds.Sync(stopCh) {
		return nil, fmt.Errorf("datastore cache sync up failed")
	}
	go rc.Run(Workers, stopCh)
	go ec.Run(Workers, stopCh)
	go vc.Run(Workers, stopCh)

	return ds, nil
}

func StartProvisioner(m *manager.VolumeManager) error {
	// Only supports in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		return errors.Wrap(err, "unable to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get k8s client")
	}

	serverVersion, err := kubeClient.Discovery().ServerVersion()
	if err != nil {
		return err
	}

	provisioner := NewProvisioner(m)
	pc := pvController.NewProvisionController(
		kubeClient,
		LonghornProvisionerName,
		provisioner,
		serverVersion.GitVersion,
	)

	//FIXME stopch should be exposed
	stopCh := make(chan struct{})
	go pc.Run(stopCh)
	return nil
}
