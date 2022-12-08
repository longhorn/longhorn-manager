package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

func (s *Server) SystemRestoreCreate(w http.ResponseWriter, req *http.Request) error {
	var input SystemRestoreInput
	var err error

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	systemRestore, err := s.m.CreateSystemRestore(input.Name, input.SystemBackup)
	if err != nil {
		return errors.Wrapf(err, "failed to create SystemRestore %v", input.Name)
	}

	apiContext.Write(toSystemRestoreResource(systemRestore))
	return nil
}

func (s *Server) SystemRestoreDelete(w http.ResponseWriter, req *http.Request) error {
	name := mux.Vars(req)["name"]

	err := s.m.DeleteSystemRestore(name)
	if err != nil {
		return errors.Wrapf(err, "failed to delete SystemRestore %v", name)
	}
	return nil
}

func (s *Server) SystemRestoreGet(rw http.ResponseWriter, req *http.Request) error {
	name := mux.Vars(req)["name"]
	systemRestore, err := s.m.GetSystemRestore(name)
	if err != nil {
		return errors.Wrapf(err, "error get SystemRestore '%s'", name)
	}

	apiContext := api.GetApiContext(req)
	apiContext.Write(toSystemRestoreResource(systemRestore))
	return nil
}

func (s *Server) SystemRestoreList(w http.ResponseWriter, req *http.Request) error {
	systemRestores, err := s.m.ListSystemRestoresSorted()
	if err != nil {
		return errors.Wrap(err, "failed to list SystemRestores")
	}

	apiContext := api.GetApiContext(req)
	apiContext.Write(toSystemRestoreCollection(systemRestores))
	return nil
}

func (s *Server) systemRestoreList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	systemRestores, err := s.m.ListSystemRestoresSorted()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list SystemRestores")
	}
	return toSystemRestoreCollection(systemRestores), nil
}
