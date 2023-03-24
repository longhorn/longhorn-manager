package server

import (
	"fmt"
	"net/http"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/recovery_backend/backend"
	"github.com/longhorn/longhorn-manager/types"
)

type RecoveryBackendServer struct {
	rb *backend.RecoveryBackend
}

func New(namespace string, ds *datastore.DataStore) *RecoveryBackendServer {
	return &RecoveryBackendServer{
		rb: &backend.RecoveryBackend{
			Namespace: namespace,
			Datastore: ds,
			Logger:    logrus.StandardLogger().WithField("service", "recoveryBackend"),
		},
	}
}

func (s *RecoveryBackendServer) ListenAndServe(handler http.Handler) error {
	port := int32(types.DefaultRecoveryBackendServerPort)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%v", port),
		Handler: handler,
	}

	logrus.Infof("Recovery-backend server is running at :%v", port)

	return server.ListenAndServe()
}
