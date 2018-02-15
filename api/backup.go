package api

import (
	"fmt"
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"

	"github.com/rancher/longhorn-manager/engineapi"
)

func (s *Server) BackupVolumeList(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	settings, err := s.ds.GetSetting()
	if err != nil || settings == nil {
		return errors.New("cannot backup: unable to read settings")
	}
	backupTarget := settings.BackupTarget
	if backupTarget == "" {
		return errors.New("cannot backup: backupTarget not set")
	}

	backups := engineapi.NewBackupTarget(backupTarget)

	volumes, err := backups.ListVolumes()
	if err != nil {
		return errors.Wrapf(err, "error listing backups, backupTarget '%s'", backupTarget)
	}
	apiContext.Write(toBackupVolumeCollection(volumes, apiContext))
	return nil
}

func (s *Server) BackupVolumeGet(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	volName := mux.Vars(req)["volName"]

	settings, err := s.ds.GetSetting()
	if err != nil || settings == nil {
		return errors.New("cannot backup: unable to read settings")
	}
	backupTarget := settings.BackupTarget
	if backupTarget == "" {
		return errors.New("cannot backup: backupTarget not set")
	}

	backups := engineapi.NewBackupTarget(backupTarget)

	bv, err := backups.GetVolume(volName)
	if err != nil {
		return errors.Wrapf(err, "error get backup volume, backupTarget '%s', volume '%s'", backupTarget, volName)
	}
	apiContext.Write(toBackupVolumeResource(bv, apiContext))
	return nil
}

func (s *Server) BackupList(w http.ResponseWriter, req *http.Request) error {
	volName := mux.Vars(req)["volName"]

	settings, err := s.ds.GetSetting()
	if err != nil || settings == nil {
		return errors.New("cannot backup: unable to read settings")
	}
	backupTarget := settings.BackupTarget
	if backupTarget == "" {
		return errors.New("cannot backup: backupTarget not set")
	}

	backups := engineapi.NewBackupTarget(backupTarget)

	bs, err := backups.List(volName)
	if err != nil {
		return errors.Wrapf(err, "error listing backups, backupTarget '%s', volume '%s'", backupTarget, volName)
	}
	api.GetApiContext(req).Write(toBackupCollection(bs))
	return nil
}

func backupURL(backupTarget, backupName, volName string) string {
	return fmt.Sprintf("%s?backup=%s&volume=%s", backupTarget, backupName, volName)
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

	settings, err := s.ds.GetSetting()
	if err != nil || settings == nil {
		return errors.New("cannot backup: unable to read settings")
	}
	backupTarget := settings.BackupTarget
	if backupTarget == "" {
		return errors.New("cannot backup: backupTarget not set")
	}

	url := backupURL(backupTarget, input.Name, volName)
	backup, err := engineapi.GetBackup(url)
	if err != nil {
		return errors.Wrapf(err, "error getting backup '%s'", url)
	}
	if backup == nil {
		logrus.Warnf("not found: backup '%s'", url)
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

	settings, err := s.ds.GetSetting()
	if err != nil || settings == nil {
		return errors.New("cannot backup: unable to read settings")
	}
	backupTarget := settings.BackupTarget
	if backupTarget == "" {
		return errors.New("cannot backup: backupTarget not set")
	}

	url := backupURL(backupTarget, input.Name, volName)
	if err := engineapi.DeleteBackup(url); err != nil {
		return errors.Wrapf(err, "error deleting backup '%s'", url)
	}
	logrus.Debugf("success: removed backup '%s'", url)
	return nil
}
