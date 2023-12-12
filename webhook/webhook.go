package webhook

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/rancher/wrangler/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"k8s.io/client-go/tools/clientcmd"

	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/server"
)

func AdmissionWebhookServerCommand() cli.Command {
	return webhookServerCommand(types.WebhookTypeAdmission)
}

<<<<<<< HEAD:app/webhook.go
func ConversionWebhookServerCommand() cli.Command {
	return webhookServerCommand(types.WebhookTypeConversion)
}
=======
func StartWebhook(ctx context.Context, webhookType string, clients *client.Clients) error {
	logrus.Infof("Starting longhorn %s webhook server", webhookType)
>>>>>>> ac3a1301 (refactor: move non-entry app functions away from app pkg):webhook/webhook.go

func webhookServerCommand(webhookType string) cli.Command {
	return cli.Command{
		Name: fmt.Sprintf("%v-webhook", webhookType),
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  FlagServiceAccount,
				Usage: fmt.Sprintf("Specify service account for %v webhook", webhookType),
			},
			cli.StringFlag{
				Name:  FlagKubeConfig,
				Usage: "Specify path to kube config (optional)",
			},
		},
		Action: func(c *cli.Context) {
			if err := runWebhookServer(c, webhookType); err != nil {
				logrus.Fatalf("Error starting longhorn %v webhook server: %v", webhookType, err)
			}
		},
	}
}

func runWebhookServer(c *cli.Context, webhookType string) error {
	logrus.Infof("Starting longhorn %s webhook server", webhookType)

	ctx := signals.SetupSignalContext()

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

	s := server.New(ctx, cfg, namespace, webhookType)
	if err := s.ListenAndServe(); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}
