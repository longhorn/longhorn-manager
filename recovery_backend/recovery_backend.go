package recoverybackend

import (
	"fmt"
	"net/http"
	"os"

	"github.com/pkg/errors"
	"github.com/rancher/wrangler/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"k8s.io/client-go/tools/clientcmd"

	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/recovery_backend/server"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util/client"
)

<<<<<<< HEAD:app/recovery_backend.go
func RecoveryBackendServiceCommand() cli.Command {
	return cli.Command{
		Name: "recovery-backend",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  FlagServiceAccount,
				Usage: "Specify service account for recovery-backend service",
			},
			cli.StringFlag{
				Name:  FlagKubeConfig,
				Usage: "Specify path to kube config (optional)",
			},
		},
		Action: func(c *cli.Context) {
			if err := runRecoveryBackendServer(c); err != nil {
				logrus.Fatalf("Failed to start longhorn recovery-backend server: %v", err)
			}
		},
	}
}
=======
func StartRecoveryBackend(clients *client.Clients) error {
	logrus.Info("Starting longhorn recovery-backend server")
>>>>>>> ac3a1301 (refactor: move non-entry app functions away from app pkg):recovery_backend/recovery_backend.go

func runRecoveryBackendServer(c *cli.Context) error {
	logrus.Infof("Starting longhorn recovery-backend server")

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

	client, err := client.NewClient(ctx, cfg, namespace, true)
	if err != nil {
		return err
	}

	if err := client.Start(ctx); err != nil {
		return err
	}

	srv := server.New(namespace, client.Datastore)
	router := http.Handler(server.NewRouter(srv))

	if err := srv.ListenAndServe(router); err != nil {
		return err
	}

	<-ctx.Done()
	return nil
}
