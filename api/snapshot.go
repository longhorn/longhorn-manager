package api

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"

	"github.com/longhorn/longhorn-manager/types"
)

func (s *Server) SnapshotCreate(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "fail to create snapshot")
	}()
	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	volName := mux.Vars(req)["name"]

	vol, err := s.m.Get(volName)
	if err != nil {
		return errors.Wrap(err, "unable to get volume")
	}

	if vol.Spec.Standby {
		return fmt.Errorf("cannot create snapshot for standby volume %v", vol.Name)
	}

	snapshot, err := s.m.CreateSnapshot(input.Name, input.Labels, volName)
	if err != nil {
		return err
	}
	apiContext.Write(toSnapshotResource(snapshot))
	return nil
}

func (s *Server) SnapshotList(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "fail to list snapshot")
	}()

	volName := mux.Vars(req)["name"]

	snapList, err := s.m.ListSnapshots(volName)
	if err != nil {
		return err
	}
	api.GetApiContext(req).Write(toSnapshotCollection(snapList))
	return nil
}

func (s *Server) SnapshotGet(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "fail to get snapshot")
	}()

	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}
	volName := mux.Vars(req)["name"]

	snap, err := s.m.GetSnapshot(input.Name, volName)
	if err != nil {
		return err
	}
	api.GetApiContext(req).Write(toSnapshotResource(snap))
	return nil
}

func (s *Server) SnapshotDelete(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "fail to delete snapshot")
	}()

	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	volName := mux.Vars(req)["name"]

	vol, err := s.m.Get(volName)
	if err != nil {
		return errors.Wrap(err, "unable to get volume")
	}

	if vol.Spec.Standby {
		return fmt.Errorf("cannot delete snapshot for standby volume %v", vol.Name)
	}

	if err := s.m.DeleteSnapshot(input.Name, volName); err != nil {
		return err
	}
	return nil
}

func (s *Server) SnapshotRevert(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "fail to revert snapshot")
	}()

	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}
	volName := mux.Vars(req)["name"]

	vol, err := s.m.Get(volName)
	if err != nil {
		return errors.Wrap(err, "unable to get volume")
	}

	if vol.Spec.Standby {
		return fmt.Errorf("cannot revert snapshot for standby volume %v", vol.Name)
	}

	if err := s.m.RevertSnapshot(input.Name, volName); err != nil {
		return err
	}

	return nil
}

func (s *Server) SnapshotBackup(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "fail to backup snapshot")
	}()

	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	volName := mux.Vars(req)["name"]

	vol, err := s.m.Get(volName)
	if err != nil {
		return errors.Wrap(err, "unable to get volume")
	}

	if vol.Spec.Standby {
		return fmt.Errorf("cannot create backup for standby volume %v", vol.Name)
	}

	labels := make(map[string]string)
	if vol.Spec.BaseImage != "" {
		labels[types.BaseImageLabel] = vol.Spec.BaseImage
	}

	return s.m.BackupSnapshot(input.Name, labels, volName)
}

func (s *Server) SnapshotPurge(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "fail to purge snapshot")
	}()

	volName := mux.Vars(req)["name"]

	return s.m.PurgeSnapshot(volName)
}
