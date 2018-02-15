package api

import (
	//"bytes"
	//"encoding/json"
	//"io/ioutil"
	"net/http"
	"net/http/httputil"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/datastore"
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
		//nodeID, err := getNodeID(copyReq(req))
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

//func copyReq(req *http.Request) *http.Request {
//	r := *req
//	buf, _ := ioutil.ReadAll(r.Body)
//	req.Body = ioutil.NopCloser(bytes.NewBuffer(buf))
//	r.Body = ioutil.NopCloser(bytes.NewBuffer(buf))
//	return &r
//}
