package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
)

func (s *Server) NodeList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	nodes, err := s.m.ListNodes()
	if err != nil {
		return errors.Wrap(err, "fail to list host")
	}
	apiContext.Write(toHostCollection(nodes))
	return nil
}

func (s *Server) NodeGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	node, err := s.m.GetNode(id)
	if err != nil {
		return errors.Wrap(err, "fail to get host")
	}
	apiContext.Write(toHostResource(&node.NodeInfo))
	return nil
}
