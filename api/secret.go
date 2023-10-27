package api

import (
	"net/http"

	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

func (s *Server) SecretList(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	col, err := s.secretList(apiContext)
	if err != nil {
		return err
	}

	apiContext.Write(col)
	return nil
}

func (s *Server) secretList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	list, err := s.m.ListSecrets()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list secrets")
	}
	return toSecretRefCollection(list, apiContext), nil
}
