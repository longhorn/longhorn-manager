package app

import (
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/clientcmd"

	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/controller"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
)

func SystemRolloutCmd() cli.Command {
	return cli.Command{
		Name: "system-rollout",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  FlagKubeConfig,
				Usage: "Specify path to kube config (optional)",
			},
			cli.StringFlag{
				Name:   FlagNamespace,
				EnvVar: types.EnvPodNamespace,
			},
		},
		Action: func(c *cli.Context) {
			if err := systemRollout(c); err != nil {
				logrus.Fatalln(err)
			}
		},
	}
}

func systemRollout(c *cli.Context) error {
	if c.NArg() != 1 {
		return errors.New("require SystemRestore name")
	}

	systemRestoreName := c.Args()[0]

	namespace := c.String(FlagNamespace)
	if namespace == "" {
		return errors.New("namespace is required")
	}

	currentNodeID, err := util.GetRequiredEnv(types.EnvNodeName)
	if err != nil {
		return errors.New("fail to detect the node name")
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

	scheme := runtime.NewScheme()
	if err := longhorn.SchemeBuilder.AddToScheme(scheme); err != nil {
		return errors.Wrap(err, "unable to create scheme")
	}

	kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, time.Second*30)
	lhInformerFactory := lhinformers.NewSharedInformerFactory(lhClient, time.Second*30)

	ds := datastore.NewDataStore(lhInformerFactory, lhClient, kubeInformerFactory, kubeClient, extensionsClient, namespace)

	logger := logrus.StandardLogger()
	logrus.SetLevel(logrus.DebugLevel)

	doneCh := make(chan struct{})
	go lhInformerFactory.Start(doneCh)
	go kubeInformerFactory.Start(doneCh)

	ctrl := controller.NewSystemRolloutController(
		systemRestoreName,
		logger,
		currentNodeID,
		ds,
		scheme,
		doneCh,
		kubeClient,
		extensionsClient,
	)
	return ctrl.Run()
}
