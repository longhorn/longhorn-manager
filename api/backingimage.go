package api

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

func (s *Server) BackingImageList(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	bil, err := s.backingImageList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(bil)
	return nil
}

func (s *Server) backingImageList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	biList, err := s.m.ListBackingImagesSorted()
	if err != nil {
		return nil, errors.Wrap(err, "error listing backing image")
	}
	return toBackingImageCollection(biList, apiContext), nil
}

func (s *Server) BackingImageGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	id := mux.Vars(req)["name"]

	bi, err := s.m.GetBackingImage(id)
	if err != nil {
		return errors.Wrapf(err, "error get backing image '%s'", id)
	}

	apiContext.Write(toBackingImageResource(bi, apiContext))
	return nil
}

func (s *Server) BackingImageCreate(rw http.ResponseWriter, req *http.Request) error {
	var input BackingImage
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&input); err != nil {
		return err
	}

	bi, err := s.m.CreateBackingImage(input.Name, input.ExpectedChecksum, input.SourceType, input.Parameters)
	if err != nil {
		return errors.Wrapf(err, "unable to create backing image %v from source type %v with parameters %+v", input.Name, input.SourceType, input.Parameters)
	}
	apiContext.Write(toBackingImageResource(bi, apiContext))
	return nil
}

func (s *Server) BackingImageDelete(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	if err := s.m.DeleteBackingImage(id); err != nil {
		return errors.Wrap(err, "unable to delete backing image")
	}

	return nil
}

func (s *Server) BackingImageCleanup(rw http.ResponseWriter, req *http.Request) error {
	var input BackingImageCleanupInput
	apiContext := api.GetApiContext(req)

	id := mux.Vars(req)["name"]
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	bi, err := s.m.CleanUpBackingImageDiskFiles(id, input.Disks)
	if err != nil {
		return errors.Wrapf(err, "unable to cleanup backing image %v for disk %+v", id, input.Disks)
	}
	apiContext.Write(toBackingImageResource(bi, apiContext))
	return nil
}

func (s *Server) BackingImageProxyFallback(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	bi, err := s.m.GetBackingImage(id)
	if err != nil {
		return errors.Wrapf(err, "error get backing image '%s'", id)
	}

	return fmt.Errorf("cannot proxy the request to other servers for backing image %v(%v)", bi.Name, bi.Status.UUID)
}
