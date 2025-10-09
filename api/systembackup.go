package api

import (
	"net/http"

	"github.com/cockroachdb/errors"
	"github.com/gorilla/mux"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (s *Server) SystemBackupCreate(w http.ResponseWriter, req *http.Request) error {
	var input SystemBackupInput
	var err error

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	obj := &longhorn.SystemBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: input.Name,
		},
		Spec: longhorn.SystemBackupSpec{
			VolumeBackupPolicy: input.VolumeBackupPolicy,
		},
	}
	systemBackup, err := s.m.CreateSystemBackup(obj)
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
		return errors.Wrapf(err, "failed to get SystemBackup '%s'", name)
	}
	apiContext.Write(toSystemBackupResource(systemBackup))
	return nil
}

func (s *Server) SystemBackupList(w http.ResponseWriter, req *http.Request) error {
	systemBackups, err := s.m.ListSystemBackupsSorted()
	if err != nil {
		return errors.Wrap(err, "failed to list SystemBackups")
	}

	apiContext := api.GetApiContext(req)
	apiContext.Write(toSystemBackupCollection(systemBackups))
	return nil
}

func (s *Server) systemBackupList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	systemBackups, err := s.m.ListSystemBackupsSorted()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list SystemBackups")
	}
	return toSystemBackupCollection(systemBackups), nil
}
