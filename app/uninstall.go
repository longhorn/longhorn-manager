package app

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

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

func UninstallCmd() *cli.Command {
	return &cli.Command{
		Name: "uninstall",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  FlagForce,
				Usage: "uninstall even if volumes are in use",
			},
			&cli.StringFlag{
				Name:  FlagKubeConfig,
				Usage: "Specify path to kube config (optional)",
			},
			&cli.StringFlag{
				Name:    FlagNamespace,
				Sources: cli.EnvVars(EnvLonghornNamespace),
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if err := uninstall(cmd); err != nil {
				logrus.Fatalln(err)
			}
			return nil
		},
	}
}

func uninstall(cmd *cli.Command) error {
	namespace := cmd.String(FlagNamespace)
	if namespace == "" {
		return errors.New("namespace is required")
	}

	config, err := clientcmd.BuildConfigFromFlags("", cmd.String(FlagKubeConfig))
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
		cmd.Bool(FlagForce),
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
