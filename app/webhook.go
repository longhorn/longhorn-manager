package app

import (
	"context"
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/rancher/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/server"
)

func WebhookServerCommand() cli.Command {
	return cli.Command{
		Name: "webhook",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  FlagServiceAccount,
				Usage: "Specify service account for webhook",
			},
			cli.StringFlag{
				Name:  FlagKubeConfig,
				Usage: "Specify path to kube config (optional)",
			},
		},
		Action: func(c *cli.Context) {
			if err := runWebhookServer(c); err != nil {
				logrus.Fatalf("Error starting longhorn webhook server: %v", err)
			}
		},
	}
}

func runWebhookServer(c *cli.Context) error {
	logrus.Info("Starting longhorn webhook server")

	serviceAccount := c.String(FlagServiceAccount)
	if serviceAccount == "" {
		return fmt.Errorf("require %v", FlagServiceAccount)
	}
	kubeconfigPath := c.String(FlagKubeConfig)

	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		logrus.Warnf("Cannot detect pod namespace, environment variable %v is missing, "+
			"using default namespace", types.EnvPodNamespace)
		namespace = corev1.NamespaceDefault
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return errors.Wrap(err, "unable to get client config")
	}

	s := server.New(context.Background(), cfg, namespace)
	if err := s.ListenAndServe(); err != nil {
		return err
	}

	stopCh := signals.SetupSignalHandler()
	<-stopCh
	return nil
}
