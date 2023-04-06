package app

import (
	"context"
	"fmt"
	"net/http"

	"github.com/longhorn/longhorn-manager/recovery_backend/server"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/util/client"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/tools/clientcmd"
)

func startRecoveryBackend(ctx context.Context, serviceAccount, kubeconfigPath string) error {
	logrus.Info("Starting longhorn recovery-backend server")

	namespace := util.GetNamespace(types.EnvPodNamespace)

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return fmt.Errorf("unable to get client config: %v", err)
	}

	client, err := client.NewClient(ctx, cfg, namespace, true)
	if err != nil {
		return err
	}

	if err := client.Start(ctx); err != nil {
		return err
	}

	s := server.New(namespace, client.Datastore)
	router := http.Handler(server.NewRouter(s))
	go func() {
		if err := s.ListenAndServe(router); err != nil {
			logrus.Fatalf("Error recovery backend server failed: %v", err)
		}
	}()
	logrus.Info("Started longhorn recovery-backend server")

	return nil
}
