package app

import (
	"net/http"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/recovery_backend/server"
	"github.com/longhorn/longhorn-manager/util/client"
)

func startRecoveryBackend(clients *client.Clients) error {
	logrus.Info("Starting longhorn recovery-backend server")

	s := server.New(clients.Namespace, clients.Datastore)
	router := http.Handler(server.NewRouter(s))
	go func() {
		if err := s.ListenAndServe(router); err != nil {
			logrus.Fatalf("Error recovery backend server failed: %v", err)
		}
	}()
	logrus.Info("Started longhorn recovery-backend server")

	return nil
}
