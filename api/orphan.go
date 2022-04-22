package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

func (s *Server) OrphanList(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	orphans, err := s.m.ListOrphans()
	if err != nil {
		return errors.Wrap(err, "failed to list instance managers")
	}

	apiContext.Write(toOrphanCollection(orphans))
	return nil
}

func (s *Server) orphanList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	list, err := s.m.ListOrphans()
	if err != nil {
		return nil, errors.Wrap(err, "error listing orhpan")
	}
	return toOrphanCollection(list), nil
}

func (s *Server) OrphanGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	id := mux.Vars(req)["name"]

	orphan, err := s.m.GetOrphan(id)
	if err != nil {
		return errors.Wrapf(err, "error get orphan '%s'", id)
	}
	apiContext.Write(toOrphanResource(orphan))
	return nil
}

func (s *Server) OrphanDelete(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	if err := s.m.DeleteOrphan(id); err != nil {
		return errors.Wrapf(err, "unable to delete orphan %v", id)
	}

	return nil
}
