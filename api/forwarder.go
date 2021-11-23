package api

import (
	"fmt"
	"net/http"
	"net/http/httputil"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/engineapi"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"
)

type OwnerIDFunc func(req *http.Request) (string, error)
type BackingImageUploadServerAddressFunc func(req *http.Request) (string, error)
type GetTargetAddressFunc func(req *http.Request) (string, error)
type ProxyRequestHandler func(address string, req *http.Request) (proxyRequired bool, err error)

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

func (f *Fwd) Handler(proxyHandler ProxyRequestHandler, getTargetAddressFunc GetTargetAddressFunc, h HandleFuncWithError) HandleFuncWithError {
	return func(w http.ResponseWriter, req *http.Request) error {
		var requireProxy bool
		if proxyHandler != nil {
			if getTargetAddressFunc == nil {
				return fmt.Errorf("nil HTTP target address get function")
			}
			targetAddress, err := getTargetAddressFunc(req)
			if err != nil {
				return errors.Wrap(err, "fail to get the target address")
			}
			requireProxy, err = proxyHandler(targetAddress, req)
			if err != nil {
				return errors.Wrap(err, "fail to verify if the proxy is required")
			}
		}
		if requireProxy {
			f.proxy.ServeHTTP(w, req)
			return nil
		}
		return h(w, req)
	}
}

func (f *Fwd) HandleProxyRequestByNodeID(targetAddress string, req *http.Request) (proxyRequired bool, err error) {
	if targetAddress == req.Host {
		return false, nil
	}

	req.Host = targetAddress
	req.URL.Host = targetAddress
	req.URL.Scheme = "http"
	h := req.Header
	if h.Get("X-Forwarded-Proto") == "https" {
		h.Set("X-Forwarded-Proto", "http")
	}
	if h.Get("X-Forwarded-Host") != "" {
		h.Del("X-Forwarded-Host")
	}
	req.Header = h
	logrus.Debugf("Forwarding request to %v", targetAddress)

	return true, nil
}

func (f *Fwd) GetHTTPAddressByNodeID(getNodeID OwnerIDFunc) GetTargetAddressFunc {
	return func(req *http.Request) (string, error) {
		nodeID, err := getNodeID(req)
		if err != nil {
			return "", errors.Wrap(err, "fail to get target node ID")
		}
		return f.locator.Node2APIAddress(nodeID)
	}
}

const (
	BackingImageUpload = "upload"
)

func (f *Fwd) HandleProxyRequestForBackingImageUpload(targetAddress string, req *http.Request) (proxyRequired bool, err error) {
	if targetAddress == req.Host {
		return false, fmt.Errorf("backing image upload request should not be handled by current longhorn manager service")
	}

	req.Host = targetAddress
	req.URL.Host = targetAddress
	req.URL.Scheme = "http"
	if req.URL.Query().Get("action") != BackingImageUpload {
		return false, fmt.Errorf("unknown action for backing image upload related request: %v", req.URL.Query().Get("action"))
	}
	req.URL.Path = "/v1/file"
	logrus.Infof("Forwarding backing image upload request to URL %v", req.URL.String())

	return true, nil
}

func (f *Fwd) GetHTTPAddressForBackingImageUpload(getBackingImageUploadServerAddress BackingImageUploadServerAddressFunc) GetTargetAddressFunc {
	return func(req *http.Request) (string, error) {
		return getBackingImageUploadServerAddress(req)
	}
}

func UploadServerAddressFromBackingImage(m *manager.VolumeManager) func(req *http.Request) (string, error) {
	return func(req *http.Request) (string, error) {
		name := mux.Vars(req)["name"]
		pod, err := m.GetBackingImageDataSourcePod(name)
		if err != nil {
			return "", err
		}
		if pod.Status.Phase != v1.PodRunning || pod.Status.PodIP == "" {
			return "", fmt.Errorf("backing image data source pod is not ready for uploading")
		}
		bids, err := m.GetBackingImageDataSource(name)
		if err != nil {
			return "", errors.Wrapf(err, "error getting backing image %s", name)
		}
		if bids.Status.CurrentState != longhorn.BackingImageStateStarting {
			return "", fmt.Errorf("upload server for backing image %s has not been initiated", name)
		}
		return fmt.Sprintf("%s:%d", pod.Status.PodIP, engineapi.BackingImageDataSourceDefaultPort), nil
	}
}
