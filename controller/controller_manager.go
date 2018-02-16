package controller

import (
	"fmt"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

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

func StartControllers(controllerID, engineImage string) (*datastore.DataStore, error) {
	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		logrus.Warnf("Cannot detect pod namespace, environment variable %v is missing, " +
			"using default namespace")
		namespace = corev1.NamespaceDefault
	}

	// Only supports in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get k8s client")
	}

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get clientset")
	}

	kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, time.Second*30)
	lhInformerFactory := lhinformers.NewSharedInformerFactory(lhClient, time.Second*30)

	replicaInformer := lhInformerFactory.Longhorn().V1alpha1().Replicas()
	engineInformer := lhInformerFactory.Longhorn().V1alpha1().Controllers()
	volumeInformer := lhInformerFactory.Longhorn().V1alpha1().Volumes()
	podInformer := kubeInformerFactory.Core().V1().Pods()
	jobInformer := kubeInformerFactory.Batch().V1().Jobs()

	ds := datastore.NewDataStore(volumeInformer, engineInformer, replicaInformer, lhClient,
		podInformer, kubeClient, namespace)
	rc := NewReplicaController(ds, replicaInformer, podInformer, jobInformer, kubeClient,
		namespace, controllerID)
	ec := NewEngineController(ds, engineInformer, podInformer, kubeClient,
		&engineapi.EngineCollection{}, namespace, controllerID)
	vc := NewVolumeController(ds, volumeInformer, engineInformer, replicaInformer, kubeClient,
		namespace, controllerID, engineImage)

	//FIXME stopch should be exposed
	stopCh := make(chan struct{})
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
