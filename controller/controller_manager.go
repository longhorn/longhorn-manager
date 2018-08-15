package controller

import (
	"fmt"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/types"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhclientset "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhinformers "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions"
)

var (
	Workers              = 5
	longhornFinalizerKey = longhorn.SchemeGroupVersion.Group
)

func StartControllers(stopCh chan struct{}, controllerID, serviceAccount, managerImage, kubeconfigPath string) (*datastore.DataStore, *WebsocketController, error) {
	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		logrus.Warnf("Cannot detect pod namespace, environment variable %v is missing, "+
			"using default namespace", types.EnvPodNamespace)
		namespace = corev1.NamespaceDefault
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to get k8s client")
	}

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to get clientset")
	}

	scheme := runtime.NewScheme()
	if err := longhorn.SchemeBuilder.AddToScheme(scheme); err != nil {
		return nil, nil, errors.Wrap(err, "unable to create scheme")
	}

	kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, time.Second*30)
	lhInformerFactory := lhinformers.NewSharedInformerFactory(lhClient, time.Second*30)

	replicaInformer := lhInformerFactory.Longhorn().V1alpha1().Replicas()
	engineInformer := lhInformerFactory.Longhorn().V1alpha1().Engines()
	volumeInformer := lhInformerFactory.Longhorn().V1alpha1().Volumes()
	engineImageInformer := lhInformerFactory.Longhorn().V1alpha1().EngineImages()
	nodeInformer := lhInformerFactory.Longhorn().V1alpha1().Nodes()
	settingInformer := lhInformerFactory.Longhorn().V1alpha1().Settings()

	podInformer := kubeInformerFactory.Core().V1().Pods()
	jobInformer := kubeInformerFactory.Batch().V1().Jobs()
	cronJobInformer := kubeInformerFactory.Batch().V1beta1().CronJobs()
	daemonSetInformer := kubeInformerFactory.Apps().V1beta2().DaemonSets()

	ds := datastore.NewDataStore(
		volumeInformer, engineInformer, replicaInformer,
		engineImageInformer, nodeInformer, settingInformer,
		lhClient,
		podInformer, cronJobInformer, daemonSetInformer,
		kubeClient, namespace)
	rc := NewReplicaController(ds, scheme,
		replicaInformer, podInformer, jobInformer,
		kubeClient, namespace, controllerID)
	ec := NewEngineController(ds, scheme,
		engineInformer, podInformer,
		kubeClient, &engineapi.EngineCollection{}, namespace, controllerID)
	vc := NewVolumeController(ds, scheme,
		volumeInformer, engineInformer, replicaInformer,
		kubeClient, namespace, controllerID,
		serviceAccount, managerImage)
	ic := NewEngineImageController(ds, scheme,
		engineImageInformer, volumeInformer, daemonSetInformer,
		kubeClient, namespace, controllerID)
	nc := NewNodeController(ds, scheme,
		nodeInformer, settingInformer, podInformer, replicaInformer,
		kubeClient, namespace, controllerID)
	ws := NewWebsocketController(volumeInformer, engineInformer, replicaInformer,
		settingInformer, engineImageInformer, nodeInformer)

	go kubeInformerFactory.Start(stopCh)
	go lhInformerFactory.Start(stopCh)
	if !ds.Sync(stopCh) {
		return nil, nil, fmt.Errorf("datastore cache sync up failed")
	}
	go rc.Run(Workers, stopCh)
	go ec.Run(Workers, stopCh)
	go vc.Run(Workers, stopCh)
	go ic.Run(Workers, stopCh)
	go nc.Run(Workers, stopCh)
	go ws.Run(stopCh)

	return ds, ws, nil
}
