package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

func (s *Server) VolumeAttachmentGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	id := mux.Vars(req)["name"]

	volumeAttachment, err := s.m.GetVolumeAttachment(id)
	if err != nil {
		return errors.Wrapf(err, "failed to get volume attachment  '%s'", id)
	}
	apiContext.Write(toVolumeAttachmentResource(volumeAttachment))
	return nil
}

func (s *Server) VolumeAttachmentList(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	volumeAttachmentList, err := s.m.ListVolumeAttachment()
	if err != nil {
		return errors.Wrap(err, "failed to list volume attachments")
	}
	apiContext.Write(toVolumeAttachmentCollection(volumeAttachmentList, apiContext))
	return nil
}

func (s *Server) volumeAttachmentList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	volumeAttachmentList, err := s.m.ListVolumeAttachment()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list volume attachments")
	}
	return toVolumeAttachmentCollection(volumeAttachmentList, apiContext), nil
}
