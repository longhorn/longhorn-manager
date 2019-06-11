package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-manager/types"
)

func (s *Server) EngineImageList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	eil, err := s.engineImageList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(eil)
	return nil
}

func (s *Server) engineImageList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	eis, err := s.m.ListEngineImagesSorted()
	if err != nil {
		return nil, errors.Wrap(err, "error listing engine image")
	}
	defaultImage, err := s.m.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if err != nil {
		return nil, errors.Wrap(err, "error listing engine image")
	}
	return toEngineImageCollection(eis, defaultImage), nil
}

func (s *Server) EngineImageGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	id := mux.Vars(req)["name"]

	ei, err := s.m.GetEngineImageByName(id)
	if err != nil {
		return errors.Wrapf(err, "error get engine image '%s'", id)
	}
	defaultImage, err := s.m.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if err != nil {
		return errors.Wrapf(err, "error get engine image '%s'", id)
	}
	apiContext.Write(toEngineImageResource(ei, ei.Spec.Image == defaultImage))
	return nil
}

func (s *Server) EngineImageCreate(rw http.ResponseWriter, req *http.Request) error {
	var img EngineImage
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&img); err != nil {
		return err
	}

	ei, err := s.m.CreateEngineImage(img.Image)
	if err != nil {
		return errors.Wrapf(err, "unable to create engine image %v", img.Image)
	}
	defaultImage, err := s.m.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if err != nil {
		return errors.Wrap(err, "unable to create engine image")
	}
	apiContext.Write(toEngineImageResource(ei, ei.Spec.Image == defaultImage))
	return nil
}

func (s *Server) EngineImageDelete(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	if err := s.m.DeleteEngineImageByName(id); err != nil {
		return errors.Wrap(err, "unable to delete engine image")
	}

	return nil
}
