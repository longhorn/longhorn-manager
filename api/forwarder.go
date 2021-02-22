package api

import (
	"fmt"
	"net/http"
	"net/http/httputil"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"
)

type OwnerIDFunc func(req *http.Request) (string, error)

func OwnerIDFromVolume(m *manager.VolumeManager) func(req *http.Request) (string, error) {
	return func(req *http.Request) (string, error) {
		name := mux.Vars(req)["name"]
		volume, err := m.Get(name)
		if err != nil {
			return "", errors.Wrapf(err, "error getting volume '%s'", name)
		}
		if volume == nil {
			return "", nil
		}
		return volume.Status.OwnerID, nil
	}
}

// NodeHasDefaultEngineImage picks a node that is ready and has default engine image deployed.
// To prevent the repeatedly forwarding the request around, prioritize the current node if it meets the requirement.
func NodeHasDefaultEngineImage(m *manager.VolumeManager) func(req *http.Request) (string, error) {
	return func(req *http.Request) (string, error) {
		engineImage, err := m.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
		if err != nil {
			return "", err
		}
		nodes, err := m.ListReadyNodesWithEngineImage(engineImage)
		if err != nil {
			return "", err
		}
		if _, ok := nodes[m.GetCurrentNodeID()]; ok {
			return m.GetCurrentNodeID(), nil
		}

		for nodeID := range nodes {
			return nodeID, nil
		}
		return "", fmt.Errorf("cannot find a node that is ready and has the default engine image %v deployed", engineImage)
	}
}

func OwnerIDFromNode(m *manager.VolumeManager) func(req *http.Request) (string, error) {
	return func(req *http.Request) (string, error) {
		id := mux.Vars(req)["name"]
		return id, nil
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
