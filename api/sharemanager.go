package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

func (s *Server) ShareManagerList(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	sml, err := s.shareManagerList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(sml)
	return nil
}

func (s *Server) shareManagerList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	smList, err := s.m.ListShareManagersSorted()
	if err != nil {
		return nil, errors.Wrap(err, "error listing share manager")
	}
	return toShareManagerCollection(smList, apiContext), nil
}

func (s *Server) ShareManagerGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	id := mux.Vars(req)["name"]

	sm, err := s.m.GetShareManager(id)
	if err != nil {
		return errors.Wrapf(err, "error get share manager '%s'", id)
	}

	apiContext.Write(toShareManagerResource(sm, apiContext))
	return nil
}
