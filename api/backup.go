package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (s *Server) BackupTargetGet(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	backupTargetName := mux.Vars(req)["backupTargetName"]
	backupTarget, err := s.m.GetBackupTarget(backupTargetName)
	if err != nil {
		return errors.Wrapf(err, "failed to get backup target %v", backupTargetName)
	}
	apiContext.Write(toBackupTargetResource(backupTarget, apiContext))
	return nil
}

func (s *Server) BackupTargetList(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	bts, err := s.backupTargetList(apiContext)
	if err != nil {
		return err
	}

	apiContext.Write(bts)
	return nil
}

func (s *Server) backupTargetList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	bts, err := s.m.ListBackupTargetsSorted()
	if err != nil {
		return nil, err
	}
	return toBackupTargetCollection(bts, apiContext), nil
}

func (s *Server) BackupTargetCreate(rw http.ResponseWriter, req *http.Request) error {
	var input BackupTarget
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&input); err != nil {
		return err
	}

	backupTargetSpec, err := newBackupTarget(input)
	if err != nil {
		return err
	}

	obj, err := s.m.CreateBackupTarget(input.Name, backupTargetSpec)
	if err != nil {
		return errors.Wrapf(err, "failed to create backup target %v", input.Name)
	}
	apiContext.Write(toBackupTargetResource(obj, apiContext))
	return nil
}

func newBackupTarget(input BackupTarget) (*longhorn.BackupTargetSpec, error) {
	pollInterval, err := strconv.ParseInt(input.PollInterval, 10, 64)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid backup target polling interval '%s'", input.PollInterval)
	}

	return &longhorn.BackupTargetSpec{
		BackupTargetURL:  input.BackupTargetURL,
		CredentialSecret: input.CredentialSecret,
		PollInterval:     metav1.Duration{Duration: time.Duration(pollInterval) * time.Second}}, nil
}

func (s *Server) BackupTargetUpdate(rw http.ResponseWriter, req *http.Request) error {
	var input BackupTarget

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	name := input.Name
	backupTargetSpec, err := newBackupTarget(input)
	if err != nil {
		return err
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateBackupTarget(name, backupTargetSpec)
	})
	if err != nil {
		return errors.Wrapf(err, "failed to update backup target %v", name)
	}

	backupTarget, ok := obj.(*longhorn.BackupTarget)
	if !ok {
		return fmt.Errorf("failed to convert %v to backup target", name)
	}

	apiContext.Write(toBackupTargetResource(backupTarget, apiContext))
	return nil
}

func (s *Server) BackupTargetDelete(rw http.ResponseWriter, req *http.Request) error {
	backupTargetName := mux.Vars(req)["backupTargetName"]
	if err := s.m.DeleteBackupTarget(backupTargetName); err != nil {
		return errors.Wrapf(err, "failed to delete backup target %v", backupTargetName)
	}

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

	backupVolumeName := mux.Vars(req)["backupVolumeName"]

	bv, err := s.m.GetBackupVolume(backupVolumeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get backup volume '%s'", backupVolumeName)
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

	backupVolumeName := mux.Vars(req)["backupVolumeName"]
	if backupVolumeName == "" {
		return fmt.Errorf("backup volume name is required")
	}

	bv, err := s.m.GetBackupVolume(backupVolumeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get backup volume '%s'", backupVolumeName)
	}

	if input.SyncBackupVolume {
		bv, err = s.m.SyncBackupVolume(bv)
		if err != nil {
			return errors.Wrapf(err, "failed to synchronize backup volume %v", backupVolumeName)
		}
	}

	apiContext.Write(toBackupVolumeResource(bv, apiContext))
	return nil
}

func (s *Server) BackupVolumeDelete(w http.ResponseWriter, req *http.Request) error {
	backupVolumeName := mux.Vars(req)["backupVolumeName"]
	if err := s.m.DeleteBackupVolume(backupVolumeName); err != nil {
		return errors.Wrapf(err, "failed to delete backup volume '%s'", backupVolumeName)
	}
	return nil
}

func (s *Server) BackupList(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	bs, err := s.m.ListAllBackupsSorted()
	if err != nil {
		return errors.Wrapf(err, "failed to list all backups")
	}
	apiContext.Write(toBackupCollection(bs))
	return nil
}

func (s *Server) BackupListByBackupVolume(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	backupVolumeName := mux.Vars(req)["backupVolumeName"]

	bs, err := s.m.ListBackupsForBackupVolumeSorted(backupVolumeName)
	if err != nil {
		return errors.Wrapf(err, "failed to list backups for volume '%s'", backupVolumeName)
	}
	apiContext.Write(toBackupCollection(bs))
	return nil
}

func (s *Server) BackupListByVolume(w http.ResponseWriter, req *http.Request) error {
	var input Volume
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	bs, err := s.m.ListBackupsForVolumeNameSorted(input.Name)
	if err != nil {
		return errors.Wrapf(err, "failed to list backups for volume '%s'", input.Name)
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
	backupVolumeName := mux.Vars(req)["backupVolumeName"]

	backup, err := s.m.GetBackup(input.Name)
	if err != nil {
		return errors.Wrapf(err, "failed to get backup '%v' of volume '%v'", input.Name, backupVolumeName)
	}
	if backup == nil {
		logrus.Warnf("cannot find backup '%v' of volume '%v'", input.Name, backupVolumeName)
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

	backupVolumeName := mux.Vars(req)["backupVolumeName"]

	backup, err := s.m.GetBackup(input.Name)
	if err != nil {
		logrus.WithError(err).Warnf("failed to get backup '%v' of volume '%v'", input.Name, backupVolumeName)
	}

	if backup != nil {
		if err := s.m.DeleteBackup(input.Name); err != nil {
			return errors.Wrapf(err, "failed to delete backup '%v' of volume '%v'", input.Name, backupVolumeName)
		}
	}

	bv, err := s.m.GetBackupVolume(backupVolumeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get backup volume '%s'", backupVolumeName)
	}
	apiContext.Write(toBackupVolumeResource(bv, apiContext))
	return nil
}
