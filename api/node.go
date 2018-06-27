package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
)

func (s *Server) HostList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return errors.Wrap(err, "fail to list host")
	}
	apiContext.Write(toHostCollection(nodeIPMap))
	return nil
}

func (s *Server) HostGet(rw http.ResponseWriter, req *http.Request) error {
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

func (s *Server) NodeList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	nodeList, err := s.m.GetManagerNode()
	if err != nil {
		return errors.Wrap(err, "fail to list nodes")
	}
	apiContext.Write(toNodeCollection(nodeList))
	return nil
}

func (s *Server) NodeGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	node, err := s.m.GetNode(id)
	if err != nil {
		return errors.Wrap(err, "fail to get node")
	}
	apiContext.Write(toNodeResource(node))
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

	unode, err := s.m.UpdateNode(node)
	apiContext.Write(toNodeResource(unode))
	return nil
}
