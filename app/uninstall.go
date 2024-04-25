package app

import (
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"k8s.io/client-go/tools/clientcmd"

	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/controller"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/util"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
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
		return errors.Wrap(err, "failed to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "failed to get k8s client")
	}

	extensionsClient, err := apiextensionsclientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "failed to get k8s extension client")
	}

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "failed to get lh client")
	}

	informerFactories := util.NewInformerFactories(namespace, kubeClient, lhClient, 30*time.Second)
	ds := datastore.NewDataStore(namespace, lhClient, kubeClient, extensionsClient, informerFactories)

	logger := logrus.StandardLogger()

	doneCh := make(chan struct{})
	ctrl, err := controller.NewUninstallController(
		logger,
		namespace,
		c.Bool(FlagForce),
		ds,
		doneCh,
		kubeClient,
		extensionsClient,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create uninstall controller")
	}

	informerFactories.Start(doneCh)

	return ctrl.Run()
}
