package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
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
	bidsMap, err := s.m.ListBackingImageDataSources()
	if err != nil {
		return nil, errors.Wrap(err, "error listing backing image")
	}
	for _, bi := range biList {
		if _, exists := bidsMap[bi.Name]; exists {
			continue
		}
		bidsMap[bi.Name] = s.prepareBackingImageDataSource(bi)
	}
	return toBackingImageCollection(biList, bidsMap, apiContext), nil
}

func (s *Server) BackingImageGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	id := mux.Vars(req)["name"]

	bi, err := s.m.GetBackingImage(id)
	if err != nil {
		return errors.Wrapf(err, "error get backing image '%s'", id)
	}
	bids := s.prepareBackingImageDataSource(bi)

	apiContext.Write(toBackingImageResource(bi, bids, apiContext))
	return nil
}

func (s *Server) BackingImageCreate(rw http.ResponseWriter, req *http.Request) error {
	var input BackingImage
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&input); err != nil {
		return err
	}

	bi, bids, err := s.m.CreateBackingImage(input.Name, input.ExpectedChecksum, input.SourceType, input.Parameters)
	if err != nil {
		return errors.Wrapf(err, "unable to create backing image %v from source type %v with parameters %+v", input.Name, input.SourceType, input.Parameters)
	}
	apiContext.Write(toBackingImageResource(bi, bids, apiContext))
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

	bi, err := s.m.CleanUpBackingImageInDisks(id, input.Disks)
	if err != nil {
		return errors.Wrapf(err, "unable to cleanup backing image %v for disk %+v", id, input.Disks)
	}
	bids := s.prepareBackingImageDataSource(bi)
	apiContext.Write(toBackingImageResource(bi, bids, apiContext))
	return nil
}

func (s *Server) prepareBackingImageDataSource(bi *longhorn.BackingImage) *longhorn.BackingImageDataSource {
	// For deleting backing image, the related data source object may be cleaned up.
	if bi.DeletionTimestamp != nil {
		return nil
	}
	obj, err := util.RetryOnNotFoundCause(func() (interface{}, error) {
		return s.m.GetBackingImageDataSource(bi.Name)
	})
	if err != nil {
		logrus.Errorf("failed to prepare backing image data source for backing image %v: %v", bi.Name, err)
		return nil
	}
	bids, ok := obj.(*longhorn.BackingImageDataSource)
	if !ok {
		logrus.Errorf("BUG: cannot convert to backing image data source %v object", bi.Name)
		return nil
	}
	return bids
}
