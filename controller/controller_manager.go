package controller

import (
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/rancher/longhorn-manager/k8s"
	lhclientset "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhinformers "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions"
)

var (
	Workers = 5
)

func StartControllers() error {
	namespace := os.Getenv(k8s.EnvPodNamespace)
	if namespace == "" {
		logrus.Warnf("Cannot detect pod namespace, environment variable %v is missing, " +
			"using default namespace")
		namespace = apiv1.NamespaceDefault
	}

	config, err := k8s.GetClientConfig("")
	if err != nil {
		return errors.Wrapf(err, "unable to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return errors.Wrapf(err, "unable to get k8s client")
	}

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return errors.Wrapf(err, "unable to get clientset")
	}

	kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, time.Second*30)
	lhInformerFactory := lhinformers.NewSharedInformerFactory(lhClient, time.Second*30)

	replicaInformer := lhInformerFactory.Longhorn().V1alpha1().Replicas()
	podInformer := kubeInformerFactory.Core().V1().Pods()
	jobInformer := kubeInformerFactory.Batch().V1().Jobs()

	rc := NewReplicaController(replicaInformer, podInformer, jobInformer, lhClient, kubeClient, namespace)

	//FIXME
	stopCh := make(chan struct{})
	go kubeInformerFactory.Start(stopCh)
	go lhInformerFactory.Start(stopCh)
	go rc.Run(Workers, stopCh)

	return nil
}
