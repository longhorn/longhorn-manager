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
	eventList, err := s.m.GetLonghornEventList()
	if err != nil {
		return nil, errors.Wrap(err, "fail to list events")
	}
	return toEventCollection(eventList), nil
}

func (s *Server) InitiateSupportBundle(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	nodeID, fileName, err := s.m.InitSupportBundle()
	if err != nil {
		return errors.Wrap(err, "Unable to initiate Support Bundle Download")
	}
	apiContext.Write(toSBResource(nodeID, fileName))
	return nil
}

func (s *Server) CheckSupportBundle(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	if err := s.m.CheckSBError(); err != nil {
		s.m.SetBundleStateToReady()
		return errors.Wrap(err, "Download failed. Please try again")
	}
	if err := s.m.SBDownloadComplete(); err != nil {
		return errors.Wrap(err, "Please wait for download to complete")
	}
	apiContext.Write(toSBResource(s.m.GetCurrentNodeID(), s.m.GetSBFileName()))
	return nil
}

func (s *Server) GetSupportBundle(w http.ResponseWriter, req *http.Request) error {
	if err := s.m.SBDownloadComplete(); err != nil {
		return errors.Wrap(err, "Please wait for download to complete")
	}
	bundleFile := s.m.GetBundleFile()
	defer bundleFile.Close()
	s.m.SetBundleStateToReady()
	fileName := s.m.GetSBFileName()
	w.Header().Set("Content-Disposition", "attachment; filename="+fileName)
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Length", strconv.FormatInt(s.m.GetSupportBundleSize(), 10))
	if _, err := io.Copy(w, bundleFile); err != nil {
		return err
	}
	return nil
}
