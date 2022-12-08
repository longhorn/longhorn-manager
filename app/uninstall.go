package app

import (
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/longhorn/longhorn-manager/controller"
	"github.com/longhorn/longhorn-manager/datastore"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
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

	extensionsClient, err := apiextensionsclientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get k8s extension client")
	}

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get lh client")
	}

	kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, time.Second*30)
	lhInformerFactory := lhinformers.NewSharedInformerFactory(lhClient, time.Second*30)

	ds := datastore.NewDataStore(lhInformerFactory, lhClient, kubeInformerFactory, kubeClient, extensionsClient, namespace)

	logger := logrus.StandardLogger()

	doneCh := make(chan struct{})
	ctrl := controller.NewUninstallController(
		logger,
		namespace,
		c.Bool(FlagForce),
		ds,
		doneCh,
		kubeClient,
		extensionsClient,
	)
	go lhInformerFactory.Start(doneCh)
	go kubeInformerFactory.Start(doneCh)
	return ctrl.Run()
}
