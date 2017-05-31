package api

import (
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/yasker/lm-rewrite/manager"
	"github.com/yasker/lm-rewrite/util"
)

func (s *Server) VolumeList(rw http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "unable to list")
	}()

	apiContext := api.GetApiContext(req)

	resp := &client.GenericCollection{}

	volumes, err := s.m.VolumeList()
	if err != nil {
		return err
	}

	for _, v := range volumes {
		controller, err := s.m.VolumeControllerInfo(v.Name)
		if err != nil {
			return err
		}
		replicas, err := s.m.VolumeReplicaList(v.Name)
		if err != nil {
			return err
		}
		resp.Data = append(resp.Data, toVolumeResource(v, controller, replicas, apiContext))
	}
	resp.ResourceType = "volume"
	resp.CreateTypes = map[string]string{
		"volume": apiContext.UrlBuilder.Collection("volume"),
	}
	apiContext.Write(resp)

	return nil
}

func (s *Server) VolumeGet(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	return s.responseWithVolume(rw, req, id)
}

func (s *Server) responseWithVolume(rw http.ResponseWriter, req *http.Request, id string) error {
	apiContext := api.GetApiContext(req)

	v, err := s.m.VolumeInfo(id)
	if err != nil {
		return errors.Wrap(err, "unable to get volume")
	}

	if v == nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}
	controller, err := s.m.VolumeControllerInfo(id)
	if err != nil {
		return err
	}
	replicas, err := s.m.VolumeReplicaList(id)
	if err != nil {
		return err
	}

	apiContext.Write(toVolumeResource(v, controller, replicas, apiContext))
	return nil
}

func (s *Server) VolumeCreate(rw http.ResponseWriter, req *http.Request) error {
	var v Volume
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&v); err != nil {
		return err
	}

	request, err := generateVolumeCreateRequest(&v)
	if err != nil {
		return errors.Wrap(err, "unable to filter create volume input")
	}

	if err := s.m.VolumeCreate(request); err != nil {
		return errors.Wrap(err, "unable to create volume")
	}
	return s.responseWithVolume(rw, req, v.Name)
}

func (s *Server) VolumeDelete(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	if err := s.m.VolumeDelete(&manager.VolumeDeleteRequest{
		Name: id,
	}); err != nil {
		return errors.Wrap(err, "unable to delete volume")
	}

	return nil
}

func generateVolumeCreateRequest(v *Volume) (*manager.VolumeCreateRequest, error) {
	size, err := util.ConvertSize(v.Size)
	if err != nil {
		return nil, errors.Wrapf(err, "error converting size '%s'", v.Size)
	}
	return &manager.VolumeCreateRequest{
		Name:                v.Name,
		Size:                strconv.FormatInt(util.RoundUpSize(size), 10),
		BaseImage:           v.BaseImage,
		FromBackup:          v.FromBackup,
		NumberOfReplicas:    v.NumberOfReplicas,
		StaleReplicaTimeout: v.StaleReplicaTimeout,
	}, nil
}

func (s *Server) VolumeAttach(rw http.ResponseWriter, req *http.Request) error {
	var input AttachInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	id := mux.Vars(req)["name"]

	if err := s.m.VolumeAttach(&manager.VolumeAttachRequest{
		Name:   id,
		NodeID: input.HostID,
	}); err != nil {
		return errors.Wrap(err, "unable to attach volume")
	}

	return s.responseWithVolume(rw, req, id)
}

func (s *Server) VolumeDetach(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	if err := s.m.VolumeDetach(&manager.VolumeDetachRequest{
		Name: id,
	}); err != nil {
		return errors.Wrap(err, "unable to detach volume")
	}

	return s.responseWithVolume(rw, req, id)
}
