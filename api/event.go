package api

import (
	"io"
	"net/http"
	"strconv"

	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

func (s *Server) EventList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	eventList, err := s.eventList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(eventList)
	return nil
}

func (s *Server) eventList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	eventList, err := s.m.ListEvent()
	if err != nil {
		return nil, errors.Wrap(err, "fail to list events")
	}
	return toEventCollection(eventList), nil
}

func (s *Server) GenerateSupportBundle(w http.ResponseWriter, req *http.Request) error {
	bundleFile, size, err := s.m.GenerateSupportBundle()
	if err != nil {
		return err
	}
	defer bundleFile.Close()

	w.Header().Set("Content-Disposition", "attachment; filename=longhorn-support-bundle.zip")
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	if _, err = io.Copy(w, bundleFile); err != nil {
		return err
	}
	return nil
}
