package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
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

	return s.m.BackupSnapshot(input.Name, nil, volName)
}

func (s *Server) SnapshotPurge(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "fail to purge snapshot")
	}()

	volName := mux.Vars(req)["name"]

	return s.m.PurgeSnapshot(volName)
}
