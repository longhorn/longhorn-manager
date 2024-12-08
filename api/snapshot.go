package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/rancher/go-rancher/api"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	bsutil "github.com/longhorn/backupstore/util"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (s *Server) SnapshotCreate(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to create snapshot")
	}()
	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	volName := mux.Vars(req)["name"]

	vol, err := s.m.Get(volName)
	if err != nil {
		return errors.Wrap(err, "failed to get volume")
	}

	if vol.Status.IsStandby {
		return fmt.Errorf("failed to create snapshot for standby volume %v", vol.Name)
	}

	snapshot, err := s.m.CreateSnapshot(input.Name, input.Labels, volName)
	if err != nil {
		return err
	}
	apiContext.Write(toSnapshotResource(snapshot, ""))
	return nil
}

func (s *Server) SnapshotList(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to list snapshot")
	}()

	volName := mux.Vars(req)["name"]

	snapList, err := s.m.ListSnapshotInfos(volName)
	if err != nil {
		return err
	}

	snapListRO, _ := s.m.ListSnapshotsCR(volName)
	api.GetApiContext(req).Write(toSnapshotCollection(snapList, snapListRO))

	return nil
}

func (s *Server) SnapshotGet(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to get snapshot")
	}()

	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}
	volName := mux.Vars(req)["name"]

	snap, err := s.m.GetSnapshotInfo(input.Name, volName)
	if err != nil {
		return err
	}

	checksum := ""
	if snapRO, err := s.m.GetSnapshotCR(snap.Name); err == nil {
		checksum = snapRO.Status.Checksum
	}

	api.GetApiContext(req).Write(toSnapshotResource(snap, checksum))
	return nil
}

func (s *Server) SnapshotDelete(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to delete snapshot")
	}()

	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	volName := mux.Vars(req)["name"]

	vol, err := s.m.Get(volName)
	if err != nil {
		return errors.Wrap(err, "failed to get volume")
	}

	if vol.Status.IsStandby {
		return fmt.Errorf("failed to delete snapshot for standby volume %v", vol.Name)
	}

	if err := s.m.DeleteSnapshot(input.Name, volName); err != nil {
		return err
	}
	return s.responseWithVolume(w, req, volName, nil)
}

func (s *Server) SnapshotRevert(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to revert snapshot")
	}()

	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}
	volName := mux.Vars(req)["name"]

	vol, err := s.m.Get(volName)
	if err != nil {
		return errors.Wrap(err, "failed to get volume")
	}

	if vol.Status.IsStandby {
		return fmt.Errorf("failed to revert snapshot for standby volume %v", vol.Name)
	}

	if vol.Spec.Frontend != "" && !vol.Spec.DisableFrontend {
		return fmt.Errorf("failed to revert snapshot for volume %v with frontend enabled", vol.Name)
	}

	if err := s.m.RevertSnapshot(input.Name, volName); err != nil {
		return err
	}

	return nil
}

func (s *Server) SnapshotBackup(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to backup snapshot")
	}()

	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	volName := mux.Vars(req)["name"]

	vol, err := s.m.Get(volName)
	if err != nil {
		return errors.Wrap(err, "failed to get volume")
	}

	if vol.Status.IsStandby {
		return fmt.Errorf("failed to create backup for standby volume %v", vol.Name)
	}

	labels, err := util.ValidateSnapshotLabels(input.Labels)
	if err != nil {
		return err
	}

	// Cannot directly compare the structs since KubernetesStatus contains a slice which cannot be compared.
	if !reflect.DeepEqual(vol.Status.KubernetesStatus, longhorn.KubernetesStatus{}) {
		kubeStatus, err := json.Marshal(vol.Status.KubernetesStatus)
		if err != nil {
			return errors.Wrapf(err, "failed to convert volume %v's KubernetesStatus to json", volName)
		}
		labels[types.KubernetesStatusLabel] = string(kubeStatus)
	}

	if err := s.m.BackupSnapshot(bsutil.GenerateName("backup"), vol.Spec.BackupTargetName, volName, input.Name, labels, input.BackupMode); err != nil {
		return err
	}

	return s.responseWithVolume(w, req, volName, nil)
}

func (s *Server) SnapshotPurge(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to purge snapshot")
	}()

	volName := mux.Vars(req)["name"]
	if err := s.m.PurgeSnapshot(volName); err != nil {
		return err
	}

	return s.responseWithVolume(w, req, volName, nil)
}

func (s *Server) SnapshotCRCreate(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to create snapshot CR")
	}()
	var input SnapshotCRInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	volName := mux.Vars(req)["name"]

	vol, err := s.m.Get(volName)
	if err != nil {
		return errors.Wrap(err, "failed to get volume")
	}

	if vol.Status.IsStandby {
		return fmt.Errorf("failed to create snapshot for standby volume %v", vol.Name)
	}

	snapshot, err := s.m.CreateSnapshotCR(input.Name, input.Labels, volName)
	if err != nil {
		return err
	}
	apiContext.Write(toSnapshotCRResource(snapshot))
	return nil
}

func (s *Server) SnapshotCRList(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to list snapshot CRs")
	}()

	volName := mux.Vars(req)["name"]

	snapCRsRO, err := s.m.ListSnapshotsCR(volName)
	if err != nil {
		return err
	}
	api.GetApiContext(req).Write(toSnapshotCRCollection(snapCRsRO))

	return nil
}

func (s *Server) SnapshotCRGet(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to get snapshot CR")
	}()

	var input SnapshotCRInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	snapRO, err := s.m.GetSnapshotCR(input.Name)
	if err != nil {
		return err
	}

	api.GetApiContext(req).Write(toSnapshotCRResource(snapRO))
	return nil
}

func (s *Server) SnapshotCRDelete(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to delete snapshot CR")
	}()

	var input SnapshotCRInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	volName := mux.Vars(req)["name"]

	vol, err := s.m.Get(volName)
	if err != nil {
		return errors.Wrap(err, "failed to get volume")
	}

	if vol.Status.IsStandby {
		return fmt.Errorf("failed to delete snapshot for standby volume %v", vol.Name)
	}

	err = s.m.DeleteSnapshotCR(input.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	api.GetApiContext(req).Write(toEmptyResource())
	return nil
}
