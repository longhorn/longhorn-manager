package api

import (
	"net/http"

	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	corev1 "k8s.io/api/core/v1"
)

func (s *Server) TLSSecretList(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	col, err := s.tlsSecretList(apiContext)
	if err != nil {
		return err
	}

	apiContext.Write(col)
	return nil
}

func (s *Server) tlsSecretList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	list, err := s.m.ListSecrets()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list secrets")
	}

	tlsSecrets := []*corev1.Secret{}
	for _, s := range list {
		if s.Type == corev1.SecretTypeTLS {
			tlsSecrets = append(tlsSecrets, s)
		}
	}

	return toSecretRefCollection(tlsSecrets, apiContext), nil
}
