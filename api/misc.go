package api

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-manager/manager"
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
		return nil, errors.Wrap(err, "failed to list events")
	}
	return toEventCollection(eventList), nil
}

func (s *Server) InitiateSupportBundle(w http.ResponseWriter, req *http.Request) error {
	var sb *manager.SupportBundle
	var supportBundleInput SupportBundleInitateInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&supportBundleInput); err != nil {
		return err
	}
	sb, err := s.m.InitSupportBundle(supportBundleInput.IssueURL, supportBundleInput.Description)
	if err != nil {
		return fmt.Errorf("unable to initiate Support Bundle Download:%v", err)
	}
	apiContext.Write(toSupportBundleResource(s.m.GetCurrentNodeID(), sb))
	return nil
}

func (s *Server) QuerySupportBundle(w http.ResponseWriter, req *http.Request) error {
	bundleName := mux.Vars(req)["bundleName"]
	apiContext := api.GetApiContext(req)
	sb, err := s.m.GetSupportBundle(bundleName)
	if err != nil {
		return errors.Wrap(err, "failed to get support bundle")
	}
	apiContext.Write(toSupportBundleResource(s.m.GetCurrentNodeID(), sb))
	return nil
}

func (s *Server) DownloadSupportBundle(w http.ResponseWriter, req *http.Request) error {
	bundleName := mux.Vars(req)["bundleName"]
	sb, err := s.m.GetSupportBundle(bundleName)
	if err != nil {
		return err
	}

	if sb.State == manager.BundleStateError {
		return errors.Wrap(err, "support bundle creation failed")
	}
	if sb.State == manager.BundleStateInProgress {
		return fmt.Errorf("support bundle creation is still in progress")
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
	return nil
}

func (s *Server) DiskTagList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	tags, err := s.m.GetDiskTags()
	if err != nil {
		return errors.Wrap(err, "failed to get all tags")
	}

	apiContext.Write(toTagCollection(tags, "disk", apiContext))
	return nil
}

func (s *Server) NodeTagList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	tags, err := s.m.GetNodeTags()
	if err != nil {
		return errors.Wrap(err, "failed to get all tags")
	}

	apiContext.Write(toTagCollection(tags, "node", apiContext))
	return nil
}

func (s *Server) InstanceManagerGet(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	apiContext := api.GetApiContext(req)

	im, err := s.m.GetInstanceManager(id)
	if err != nil {
		return err
	}

	apiContext.Write(toInstanceManagerResource(im))
	return nil
}

func (s *Server) InstanceManagerList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	instanceManagers, err := s.m.ListInstanceManagers()
	if err != nil {
		return errors.Wrap(err, "failed to list instance managers")
	}

	apiContext.Write(toInstanceManagerCollection(instanceManagers))
	return nil
}
