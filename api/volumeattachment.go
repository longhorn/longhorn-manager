package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/rancher/go-rancher/api"
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
