package api

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

func (s *Server) BackupTargetList(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	backupTargets, err := s.m.ListBackupTargetsSorted()
	if err != nil {
		return errors.Wrap(err, "failed to list backup targets")
	}
	apiContext.Write(toBackupTargetCollection(backupTargets, apiContext))
	return nil
}

func (s *Server) BackupTargetSyncAll(w http.ResponseWriter, req *http.Request) error {
	var input SyncBackupResource

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	bts, err := s.m.ListBackupTargetsSorted()
	if err != nil {
		return errors.Wrap(err, "failed to list backup targets")
	}

	if input.SyncAllBackupTargets {
		for _, bt := range bts {
			if _, err := s.m.SyncBackupTarget(bt); err != nil {
				logrus.WithError(err).Warnf("Failed to synchronize backup target %v", bt.Name)
			}
		}
	}

	apiContext.Write(toBackupTargetCollection(bts, apiContext))
	return nil
}

func (s *Server) BackupTargetSync(w http.ResponseWriter, req *http.Request) error {
	var input SyncBackupResource

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	backupTargetName := mux.Vars(req)["backupTargetName"]
	if backupTargetName == "" {
		return fmt.Errorf("backup target name is required")
	}

	bt, err := s.m.GetBackupTarget(backupTargetName)
	if err != nil {
		return errors.Wrapf(err, "failed to get backup target %v", backupTargetName)
	}

	if input.SyncBackupTarget {
		bt, err = s.m.SyncBackupTarget(bt)
		if err != nil {
			return errors.Wrapf(err, "failed to synchronize backup target %v", backupTargetName)
		}
	}

	apiContext.Write(toBackupTargetResource(bt, apiContext))
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

func (s *Server) BackupVolumeSyncAll(w http.ResponseWriter, req *http.Request) error {
	var input SyncBackupResource

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	bvs, err := s.m.ListBackupVolumesSorted()
	if err != nil {
		return errors.Wrap(err, "failed to list backup volumes")
	}

	if input.SyncAllBackupVolumes {
		for _, bv := range bvs {
			if _, err := s.m.SyncBackupVolume(bv); err != nil {
				logrus.WithError(err).Warnf("Failed to synchronize backup volume %v", bv.Name)
			}
		}
	}

	apiContext.Write(toBackupVolumeCollection(bvs, apiContext))
	return nil
}

func (s *Server) SyncBackupVolume(w http.ResponseWriter, req *http.Request) error {
	var input SyncBackupResource

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	volName := mux.Vars(req)["volName"]
	if volName == "" {
		return fmt.Errorf("backup volume name is required")
	}

	bv, err := s.m.GetBackupVolume(volName)
	if err != nil {
		return errors.Wrapf(err, "failed to get backup volume '%s'", volName)
	}

	if input.SyncBackupVolume {
		bv, err = s.m.SyncBackupVolume(bv)
		if err != nil {
			return errors.Wrapf(err, "failed to synchronize backup volume %v", volName)
		}
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
