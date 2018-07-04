package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
)

func (s *Server) NodeList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	nodeList, err := s.m.GetManagerNode()
	if err != nil {
		return errors.Wrap(err, "fail to list nodes")
	}
	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return errors.Wrap(err, "fail to get node ip")
	}
	apiContext.Write(toNodeCollection(nodeList, nodeIPMap))
	return nil
}

func (s *Server) NodeGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	node, err := s.m.GetNode(id)
	if err != nil {
		return errors.Wrap(err, "fail to get node")
	}
	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return errors.Wrap(err, "fail to get node ip")
	}
	apiContext.Write(toNodeResource(node, nodeIPMap[node.Name]))
	return nil
}

func (s *Server) NodeUpdate(rw http.ResponseWriter, req *http.Request) error {
	var n Node
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&n); err != nil {
		return err
	}

	id := mux.Vars(req)["id"]
	node, err := s.m.GetNode(id)
	if err != nil {
		return errors.Wrap(err, "fail to get node")
	}
	node.Spec.AllowScheduling = n.AllowScheduling

	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return errors.Wrap(err, "fail to get node ip")
	}

	unode, err := s.m.UpdateNode(node)
	apiContext.Write(toNodeResource(unode, nodeIPMap[node.Name]))
	return nil
}
