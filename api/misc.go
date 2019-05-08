package api

import (
	"io"
	"net/http"
	"strconv"

	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/rancher/longhorn-manager/manager"
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
	var sb *manager.SupportBundle
	apiContext := api.GetApiContext(req)
	sb, err := s.m.InitSupportBundle()
	if err != nil {
		return errors.Errorf("unable to initiate Support Bundle Download:%v", err)
	}
	if sb != nil {
		apiContext.Write(toSupportBundleResource(s.m.GetCurrentNodeID(), sb))
	}
	return nil
}

func (s *Server) QuerySupportBundle(w http.ResponseWriter, req *http.Request) error {
	var query SupportBundleQueryInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&query); err != nil {
		return err
	}
	sb, err := s.m.GetSupportBundle(query.Name)
	if err != nil {
		return err
	}
	apiContext.Write(toSupportBundleResource(s.m.GetCurrentNodeID(), sb))

	// we reset the bundle as a side affect of query
	// since we don't have a separate API to do it
	if sb.State == manager.BundleStateError {
		s.m.DeleteSupportBundle()
	}
	return nil
}

func (s *Server) DownloadSupportBundle(w http.ResponseWriter, req *http.Request) error {
	var query SupportBundleQueryInput
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&query); err != nil {
		return err
	}

	sb, err := s.m.GetSupportBundle(query.Name)
	if err != nil {
		return err
	}

	// we reset the bundle as a side affect of query
	// since we don't have a separate API to do it
	if sb.State == manager.BundleStateError {
		err := errors.Errorf("support bundle creation failed:%s", sb.Error)
		s.m.DeleteSupportBundle()
		return err
	}
	if sb.State == manager.BundleStateInProgress {
		return errors.Errorf("support bundle creation is still in progress")
	}

	file, err := s.m.GetBundleFileHandler()
	if err != nil {
		return err
	}
	defer file.Close()

	w.Header().Set("Content-Disposition", "attachment; filename="+sb.Filename)
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Length", strconv.FormatInt(sb.Size, 10))
	if _, err := io.Copy(w, file); err != nil {
		return err
	}
	// we reset the bundle as a side affect after the download
	// since we don't have a separate API to do it
	s.m.DeleteSupportBundle()
	return nil
}
