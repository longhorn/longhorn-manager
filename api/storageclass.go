package api

import (
	"net/http"

	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

func (s *Server) StorageClassList(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)
	col, err := s.storageClassList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(col)
	return nil
}

func (s *Server) storageClassList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	list, err := s.m.ListStorageClassesSorted()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list storage classes")
	}
	return toStorageClassCollection(list, apiContext), nil
}
