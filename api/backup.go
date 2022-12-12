package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"github.com/sirupsen/logrus"
)

func (s *Server) BackupTargetList(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	backupTargets, err := s.m.ListBackupTargetsSorted()
	if err != nil {
		return errors.Wrap(err, "failed to list backup targets")
	}
	apiContext.Write(toBackupTargetCollection(backupTargets))
	return nil
}

func (s *Server) BackupVolumeList(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	bvs, err := s.backupVolumeList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(bvs)
	return nil
}

func (s *Server) backupVolumeList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	bvs, err := s.m.ListBackupVolumesSorted()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list backup volume")
	}
	return toBackupVolumeCollection(bvs, apiContext), nil
}

func (s *Server) BackupVolumeGet(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	volName := mux.Vars(req)["volName"]

	bv, err := s.m.GetBackupVolume(volName)
	if err != nil {
		return errors.Wrapf(err, "failed to get backup volume '%s'", volName)
	}
	apiContext.Write(toBackupVolumeResource(bv, apiContext))
	return nil
}

func (s *Server) BackupVolumeDelete(w http.ResponseWriter, req *http.Request) error {
	volName := mux.Vars(req)["volName"]
	if err := s.m.DeleteBackupVolume(volName); err != nil {
		return errors.Wrapf(err, "failed to delete backup volume '%s'", volName)
	}
	return nil
}

func (s *Server) BackupList(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	volName := mux.Vars(req)["volName"]

	bs, err := s.m.ListBackupsForVolumeSorted(volName)
	if err != nil {
		return errors.Wrapf(err, "failed to list backups for volume '%s'", volName)
	}
	apiContext.Write(toBackupCollection(bs))
	return nil
}

func (s *Server) backupListAll(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	bs, err := s.m.ListAllBackupsSorted()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list all backups")
	}
	return toBackupCollection(bs), nil
}

func (s *Server) BackupGet(w http.ResponseWriter, req *http.Request) error {
	var input BackupInput

	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&input); err != nil {
		return err
	}
	if input.Name == "" {
		return errors.New("empty backup name is not allowed")
	}
	volName := mux.Vars(req)["volName"]

	backup, err := s.m.GetBackup(input.Name, volName)
	if err != nil {
		return errors.Wrapf(err, "failed to get backup '%v' of volume '%v'", input.Name, volName)
	}
	if backup == nil {
		logrus.Warnf("cannot find backup '%v' of volume '%v'", input.Name, volName)
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
		return errors.New("empty backup name is not allowed")
	}

	volName := mux.Vars(req)["volName"]

	backup, err := s.m.GetBackup(input.Name, volName)
	if err != nil {
		logrus.WithError(err).Warnf("failed to get backup '%v' of volume '%v'", input.Name, volName)
	}

	if backup != nil {
		if err := s.m.DeleteBackup(input.Name, volName); err != nil {
			return errors.Wrapf(err, "failed to delete backup '%v' of volume '%v'", input.Name, volName)
		}
	}

	bv, err := s.m.GetBackupVolume(volName)
	if err != nil {
		return errors.Wrapf(err, "failed to get backup volume '%s'", volName)
	}
	apiContext.Write(toBackupVolumeResource(bv, apiContext))
	return nil
}
