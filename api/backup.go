package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/sirupsen/logrus"
)

func (s *Server) BackupVolumeList(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	volumes, err := s.m.ListBackupVolumes()
	if err != nil {
		return errors.Wrapf(err, "error listing backups")
	}
	apiContext.Write(toBackupVolumeCollection(volumes, apiContext))
	return nil
}

func (s *Server) BackupVolumeGet(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	volName := mux.Vars(req)["volName"]

	bv, err := s.m.GetBackupVolume(volName)
	if err != nil {
		return errors.Wrapf(err, "error get backup volume '%s'", volName)
	}
	apiContext.Write(toBackupVolumeResource(bv, apiContext))
	return nil
}

func (s *Server) BackupList(w http.ResponseWriter, req *http.Request) error {
	volName := mux.Vars(req)["volName"]

	bs, err := s.m.ListBackupsForVolume(volName)
	if err != nil {
		return errors.Wrapf(err, "error listing backups for volume '%s'", volName)
	}
	api.GetApiContext(req).Write(toBackupCollection(bs))
	return nil
}

func (s *Server) BackupGet(w http.ResponseWriter, req *http.Request) error {
	var input BackupInput

	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&input); err != nil {
		return err
	}
	if input.Name == "" {
		return errors.Errorf("empty backup name is not allowed")
	}
	volName := mux.Vars(req)["volName"]

	backup, err := s.m.GetBackup(input.Name, volName)
	if err != nil {
		return errors.Wrapf(err, "error getting backup %v of volume %v", input.Name, volName)
	}
	if backup == nil {
		logrus.Warnf("cannot find backup %v of volume %v", input.Name, volName)
		w.WriteHeader(http.StatusNotFound)
		return nil
	}
	apiContext.Write(toBackupResource(backup))
	return nil
}

func (s *Server) BackupDelete(w http.ResponseWriter, req *http.Request) error {
	var input BackupInput

	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&input); err != nil {
		return err
	}
	if input.Name == "" {
		return errors.Errorf("empty backup name is not allowed")
	}

	volName := mux.Vars(req)["volName"]

	if err := s.m.DeleteBackup(input.Name, volName); err != nil {
		return errors.Wrapf(err, "error deleting backup %v of volume %v", input.Name, volName)
	}
	logrus.Debugf("Removed backup %v of volume %v", input.Name, volName)
	return nil
}
