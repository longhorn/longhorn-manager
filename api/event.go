package api

import (
	"net/http"

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
