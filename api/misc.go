package api

import (
	"net/http"

	"github.com/gorilla/mux"
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
		return nil, errors.Wrap(err, "failed to list events")
	}
	return toEventCollection(eventList), nil
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
