package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
)

func (s *Server) NodeList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return errors.Wrap(err, "fail to list host")
	}
	apiContext.Write(toHostCollection(nodeIPMap))
	return nil
}

func (s *Server) NodeGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return errors.Wrap(err, "fail to list node")
	}
	ip, exists := nodeIPMap[id]
	if !exists {
		return errors.Wrapf(err, "fail to find node %v", id)
	}
	apiContext.Write(toHostResource(id, ip))
	return nil
}
