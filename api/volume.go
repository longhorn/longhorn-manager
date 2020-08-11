package api

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

func (s *Server) VolumeList(rw http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "unable to list")
	}()

	apiContext := api.GetApiContext(req)

	resp, err := s.volumeList(apiContext)
	if err != nil {
		return err
	}

	apiContext.Write(resp)

	return nil
}

func (s *Server) volumeList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	resp := &client.GenericCollection{}

	volumes, err := s.m.ListSorted()
	if err != nil {
		return nil, err
	}

	for _, v := range volumes {
		controllers, err := s.m.GetEnginesSorted(v.Name)
		if err != nil {
			return nil, err
		}
		replicas, err := s.m.GetReplicasSorted(v.Name)
		if err != nil {
			return nil, err
		}

		resp.Data = append(resp.Data, toVolumeResource(v, controllers, replicas, apiContext))
	}
	resp.ResourceType = "volume"
	resp.CreateTypes = map[string]string{
		"volume": apiContext.UrlBuilder.Collection("volume"),
	}

	return resp, nil
}

func (s *Server) VolumeGet(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	return s.responseWithVolume(rw, req, id, nil)
}

func (s *Server) responseWithVolume(rw http.ResponseWriter, req *http.Request, id string, v *longhorn.Volume) error {
	var err error
	apiContext := api.GetApiContext(req)

	if v == nil {
		if id == "" {
			rw.WriteHeader(http.StatusNotFound)
			return nil
		}
		v, err = s.m.Get(id)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				rw.WriteHeader(http.StatusNotFound)
				return nil
			}
			return errors.Wrap(err, "unable to get volume")
		}
	}

	controllers, err := s.m.GetEnginesSorted(id)
	if err != nil {
		return err
	}
	replicas, err := s.m.GetReplicasSorted(id)
	if err != nil {
		return err
	}

	apiContext.Write(toVolumeResource(v, controllers, replicas, apiContext))
	return nil
}

func (s *Server) VolumeCreate(rw http.ResponseWriter, req *http.Request) error {
	var volume Volume
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&volume); err != nil {
		return err
	}

	if volume.BaseImage != "" {
		return fmt.Errorf("BaseImage feature is currently unsupported")
	}

	if volume.Standby {
		if volume.Frontend != "" {
			return fmt.Errorf("cannot set frontend for standby volume: %v", volume.Name)
		}
		if volume.FromBackup == "" {
			return fmt.Errorf("cannot create standby volume %v without field FromBackup", volume.Name)
		}
	} else {
		if volume.Frontend == "" {
			volume.Frontend = types.VolumeFrontendBlockDev
		}
	}

	size, err := util.ConvertSize(volume.Size)
	if err != nil {
		return fmt.Errorf("fail to parse size %v", err)
	}

	// Check DiskSelector.
	diskTags, err := s.m.GetDiskTags()
	if err != nil {
		return errors.Wrapf(err, "failed to get all disk tags")
	}
	sort.Strings(diskTags)
	for _, selector := range volume.DiskSelector {
		if index := sort.SearchStrings(diskTags, selector); index >= len(diskTags) || diskTags[index] != selector {
			return fmt.Errorf("specified disk tag %v does not exist", selector)
		}
	}

	// Check NodeSelector.
	nodeTags, err := s.m.GetNodeTags()
	if err != nil {
		return errors.Wrapf(err, "failed to get all node tags")
	}
	sort.Strings(nodeTags)
	for _, selector := range volume.NodeSelector {
		if index := sort.SearchStrings(nodeTags, selector); index >= len(nodeTags) || nodeTags[index] != selector {
			return fmt.Errorf("specified node tag %v does not exist", selector)
		}
	}

	v, err := s.m.Create(volume.Name, &types.VolumeSpec{
		Size:                size,
		Frontend:            volume.Frontend,
		FromBackup:          volume.FromBackup,
		NumberOfReplicas:    volume.NumberOfReplicas,
		DataLocality:        volume.DataLocality,
		StaleReplicaTimeout: volume.StaleReplicaTimeout,
		BaseImage:           volume.BaseImage,
		RecurringJobs:       volume.RecurringJobs,
		Standby:             volume.Standby,
		DiskSelector:        volume.DiskSelector,
		NodeSelector:        volume.NodeSelector,
	})
	if err != nil {
		return errors.Wrap(err, "unable to create volume")
	}
	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeDelete(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	if err := s.m.Delete(id); err != nil {
		return errors.Wrap(err, "unable to delete volume")
	}

	return nil
}

func (s *Server) VolumeAttach(rw http.ResponseWriter, req *http.Request) error {
	var input AttachInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	id := mux.Vars(req)["name"]
	// check attach node state
	node, err := s.m.GetNode(input.HostID)
	if err != nil {
		return err
	}
	readyCondition := types.GetCondition(node.Status.Conditions, types.NodeConditionTypeReady)
	if readyCondition.Status != types.ConditionStatusTrue {
		return fmt.Errorf("Node %v is not ready, couldn't attach volume %v to it", node.Name, id)
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.Attach(id, input.HostID, input.DisableFrontend)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("BUG: cannot convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeDetach(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	vol, err := s.m.Get(id)
	if err != nil {
		return errors.Wrap(err, "unable to get volume")
	}
	if vol.Status.IsStandby {
		return fmt.Errorf("cannot detach standby volume %v", vol.Name)
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.Detach(id)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("BUG: cannot convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeSalvage(rw http.ResponseWriter, req *http.Request) error {
	var input SalvageInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error read salvageInput")
	}

	id := mux.Vars(req)["name"]

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.Salvage(id, input.Names)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("BUG: cannot convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeRecurringUpdate(rw http.ResponseWriter, req *http.Request) error {
	var input RecurringInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error reading recurringInput")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateRecurringJobs(id, input.Jobs)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("BUG: cannot convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeUpdateReplicaCount(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateReplicaCountInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error reading recurringInput")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateReplicaCount(id, input.ReplicaCount)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("BUG: cannot convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeUpdateDataLocality(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateDataLocalityInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error reading dataLocality")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateDataLocality(id, types.DataLocality(input.DataLocality))
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("BUG: cannot convert to volume %v object", id)
	}
	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeActivate(rw http.ResponseWriter, req *http.Request) error {
	var input ActivateInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	id := mux.Vars(req)["name"]

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.Activate(id, input.Frontend)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("BUG: cannot convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeExpand(rw http.ResponseWriter, req *http.Request) error {
	var input ExpandInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	size, err := util.ConvertSize(input.Size)
	if err != nil {
		return fmt.Errorf("fail to parse size %v", err)
	}

	id := mux.Vars(req)["name"]

	vol, err := s.m.Get(id)
	if err != nil {
		return errors.Wrap(err, "unable to get volume")
	}
	if vol.Status.IsStandby {
		return fmt.Errorf("cannot manually expand standby volume %v", vol.Name)
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.Expand(id, size)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("BUG: cannot convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeCancelExpansion(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.CancelExpansion(id)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("BUG: cannot convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) PVCreate(rw http.ResponseWriter, req *http.Request) error {
	var input PVCreateInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error reading pvCreateInput")
	}

	vol, err := s.m.Get(id)
	if err != nil {
		return errors.Wrap(err, "unable to get volume")
	}

	if vol.Status.IsStandby {
		return fmt.Errorf("cannot create PV for standby volume %v", vol.Name)
	}

	_, err = util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.PVCreate(id, input.PVName, input.FSType)
	})
	if err != nil {
		return err
	}
	return s.responseWithVolume(rw, req, id, nil)
}

func (s *Server) PVCCreate(rw http.ResponseWriter, req *http.Request) error {
	var input PVCCreateInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error reading pvcCreateInput")
	}

	vol, err := s.m.Get(id)
	if err != nil {
		return errors.Wrap(err, "unable to get volume")
	}

	if vol.Status.IsStandby {
		return fmt.Errorf("cannot create PVC for standby volume %v", vol.Name)
	}

	_, err = util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.PVCCreate(id, input.Namespace, input.PVCName)
	})
	if err != nil {
		return err
	}
	return s.responseWithVolume(rw, req, id, nil)
}

func (s *Server) ReplicaRemove(rw http.ResponseWriter, req *http.Request) error {
	var input ReplicaRemoveInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error read replicaRemoveInput")
	}

	id := mux.Vars(req)["name"]

	if err := s.m.DeleteReplica(id, input.Name); err != nil {
		return errors.Wrap(err, "unable to remove replica")
	}

	return s.responseWithVolume(rw, req, id, nil)
}

func (s *Server) EngineUpgrade(rw http.ResponseWriter, req *http.Request) error {
	var input EngineUpgradeInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error read engineUpgradeInput")
	}

	id := mux.Vars(req)["name"]

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.EngineUpgrade(id, input.Image)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("BUG: cannot convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, id, v)
}
