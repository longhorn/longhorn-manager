package app

import (
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/rancher/longhorn-manager/controller"
	"github.com/rancher/longhorn-manager/datastore"
	lhclientset "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhinformers "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions"
)

const (
	FlagForce     = "force"
	FlagNamespace = "namespace"

	EnvLonghornNamespace = "LONGHORN_NAMESPACE"
)

func UninstallCmd() cli.Command {
	return cli.Command{
		Name: "uninstall",
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name:  FlagForce,
				Usage: "uninstall even if volumes are in use",
			},
			cli.StringFlag{
				Name:  FlagKubeConfig,
				Usage: "Specify path to kube config (optional)",
			},
			cli.StringFlag{
				Name:   FlagNamespace,
				EnvVar: EnvLonghornNamespace,
			},
		},
		Action: func(c *cli.Context) {
			if err := uninstall(c); err != nil {
				logrus.Fatalln(err)
			}
		},
	}
}

func uninstall(c *cli.Context) error {
	namespace := c.String(FlagNamespace)
	if namespace == "" {
		return errors.New("namespace is required")
	}

	config, err := clientcmd.BuildConfigFromFlags("", c.String(FlagKubeConfig))
	if err != nil {
		return errors.Wrap(err, "unable to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get k8s client")
	}

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get lh client")
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
	cronJobInformer := kubeInformerFactory.Batch().V1beta1().CronJobs()
	daemonSetInformer := kubeInformerFactory.Apps().V1beta2().DaemonSets()
	persistentVolumeInformer := kubeInformerFactory.Core().V1().PersistentVolumes()
	persistentVolumeClaimInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()

	ds := datastore.NewDataStore(
		volumeInformer, engineInformer, replicaInformer,
		engineImageInformer, nodeInformer, settingInformer,
		lhClient,
		podInformer, cronJobInformer, daemonSetInformer,
		persistentVolumeInformer, persistentVolumeClaimInformer,
		kubeClient, namespace)

	doneCh := make(chan struct{})
	ctrl := controller.NewUninstallController(
		namespace,
		c.Bool(FlagForce),
		ds,
		doneCh,
		volumeInformer,
		engineInformer,
		replicaInformer,
		engineImageInformer,
		nodeInformer,
		daemonSetInformer,
	)
	go lhInformerFactory.Start(doneCh)
	go kubeInformerFactory.Start(doneCh)
	return ctrl.Run()
}
