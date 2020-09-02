package api

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

func (s *Server) NodeList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	nodeList, err := s.nodeList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(nodeList)
	return nil
}

func (s *Server) nodeList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	nodeList, err := s.m.ListNodesSorted()
	if err != nil {
		return nil, errors.Wrap(err, "fail to list nodes")
	}
	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return nil, errors.Wrap(err, "fail to get node ip")
	}
	return toNodeCollection(nodeList, nodeIPMap, apiContext), nil
}

func (s *Server) NodeGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["name"]

	node, err := s.m.GetNode(id)
	if err != nil {
		return errors.Wrapf(err, "fail to get node %v", id)
	}
	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return errors.Wrap(err, "fail to get node ip")
	}
	apiContext.Write(toNodeResource(node, nodeIPMap[node.Name], apiContext))
	return nil
}

func (s *Server) NodeUpdate(rw http.ResponseWriter, req *http.Request) error {
	var n Node
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&n); err != nil {
		return err
	}

	id := mux.Vars(req)["name"]

	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return errors.Wrap(err, "fail to get node ip")
	}

	// Only scheduling disabled node can be evicted
	// Can not enable scheduling on an evicting node
	if n.EvictionRequested == true && n.AllowScheduling != false {
		return fmt.Errorf("need to disable scheduling on node %v for node eviction, or cancel eviction to enable scheduling on this node", n.Name)
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		node, err := s.m.GetNode(id)
		if err != nil {
			return nil, err
		}
		node.Spec.AllowScheduling = n.AllowScheduling
		node.Spec.EvictionRequested = n.EvictionRequested
		node.Spec.Tags = n.Tags

		return s.m.UpdateNode(node)
	})
	if err != nil {
		return err
	}
	unode, ok := obj.(*longhorn.Node)
	if !ok {
		return fmt.Errorf("BUG: cannot convert to node %v object", id)
	}

	apiContext.Write(toNodeResource(unode, nodeIPMap[id], apiContext))
	return nil
}

func (s *Server) NodeDelete(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	if err := s.m.DeleteNode(id); err != nil {
		return errors.Wrap(err, "unable to delete node")
	}

	return nil
}
