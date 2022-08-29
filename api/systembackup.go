package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"

	"github.com/rancher/go-rancher/client"
)

func (s *Server) SystemBackupCreate(w http.ResponseWriter, req *http.Request) error {
	var input SystemBackupInput
	var err error

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	systemBackup, err := s.m.CreateSystemBackup(input.Name)
	if err != nil {
		return errors.Wrapf(err, "failed to create SystemBackup")
	}

	apiContext.Write(toSystemBackupResource(systemBackup))
	return nil
}

func (s *Server) SystemBackupDelete(w http.ResponseWriter, req *http.Request) error {
	name := mux.Vars(req)["name"]

	err := s.m.DeleteSystemBackup(name)
	if err != nil {
		return errors.Wrap(err, "failed to delete SystemBackup")
	}
	return nil
}

func (s *Server) SystemBackupGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	name := mux.Vars(req)["name"]

	systemBackup, err := s.m.GetSystemBackup(name)
	if err != nil {
		return errors.Wrapf(err, "error get SystemBackup '%s'", name)
	}
	apiContext.Write(toSystemBackupResource(systemBackup))
	return nil
}

func (s *Server) SystemBackupList(w http.ResponseWriter, req *http.Request) error {
	systemBackups, err := s.m.ListSystemBackupsSorted()
	if err != nil {
		return errors.Wrapf(err, "failed to list SystemBackups")
	}

	apiContext := api.GetApiContext(req)
	apiContext.Write(toSystemBackupCollection(systemBackups))
	return nil
}

func (s *Server) systemBackupList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	systemBackups, err := s.m.ListSystemBackupsSorted()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list SystemBackups")
	}
	return toSystemBackupCollection(systemBackups), nil
}
