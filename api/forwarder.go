package api

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/types"
)

type OwnerIDFunc func(req *http.Request) (string, error)

func OwnerIDFromVolume(ds *datastore.KDataStore) func(req *http.Request) (string, error) {
	return func(req *http.Request) (string, error) {
		name := mux.Vars(req)["name"]
		volume, err := ds.GetVolume(name)
		if err != nil {
			return "", errors.Wrapf(err, "error getting volume '%s'", name)
		}
		if volume == nil {
			return "", nil
		}
		return volume.Spec.OwnerID, nil
	}
}

type NodeLocator interface {
	GetCurrentNodeID() string
	Node2APIAddress(nodeID string) (string, error)
}

type Fwd struct {
	locator NodeLocator
	proxy   http.Handler
}

func NewFwd(locator NodeLocator) *Fwd {
	return &Fwd{
		locator: locator,
		proxy:   &httputil.ReverseProxy{Director: func(r *http.Request) {}},
	}
}

func (f *Fwd) Handler(getNodeID OwnerIDFunc, h HandleFuncWithError) HandleFuncWithError {
	return func(w http.ResponseWriter, req *http.Request) error {
		nodeID, err := getNodeID(req)
		if err != nil {
			return errors.Wrap(err, "fail to get node ID")
		}
		if nodeID != "" && nodeID != f.locator.GetCurrentNodeID() {
			targetNode, err := f.locator.Node2APIAddress(nodeID)
			if err != nil {
				return errors.Wrapf(err, "cannot find node %v", nodeID)
			}
			if targetNode != req.Host {
				req.Host = targetNode
				req.URL.Host = targetNode
				req.URL.Scheme = "http"
				logrus.Debugf("Forwarding request to %v", targetNode)
				f.proxy.ServeHTTP(w, req)
				return nil
			}
		}
		return h(w, req)
	}
}

func (s *Server) GetCurrentNodeID() string {
	return s.CurrentNodeID
}

func (s *Server) Node2APIAddress(nodeID string) (string, error) {
	nodeIPMap, err := s.ds.GetManagerNodeIPMap()
	if err != nil {
		return "", err
	}
	ip, exists := nodeIPMap[nodeID]
	if !exists {
		return "", fmt.Errorf("cannot find longhorn manager on node %v", nodeID)
	}
	return ip + ":" + strconv.Itoa(types.DefaultAPIPort), nil
}

func (s *Server) GetCurrentIP() (string, error) {
	currentNode := s.GetCurrentNodeID()
	if currentNode == "" {
		return "", fmt.Errorf("cannot detect current node")
	}
	return s.Node2APIAddress(currentNode)
}
