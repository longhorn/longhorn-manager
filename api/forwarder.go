package api

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	ParameterKeyAddress  = "address"
	ParameterKeyFilePath = "filePath"
)

type OwnerIDFunc func(req *http.Request) (string, error)
type ParametersGetFunc func(req *http.Request) (map[string]string, error)
type ProxyRequestHandler func(parameters map[string]string, req *http.Request) (proxyRequired bool, err error)

func OwnerIDFromVolume(m *manager.VolumeManager) func(req *http.Request) (string, error) {
	return func(req *http.Request) (string, error) {
		name := mux.Vars(req)["name"]
		volume, err := m.Get(name)
		if err != nil {
			return "", errors.Wrapf(err, "failed to get volume '%s'", name)
		}
		if volume == nil {
			return "", nil
		}
		return volume.Status.OwnerID, nil
	}
}

func OwnerIDFromBackupTarget(m *manager.VolumeManager) func(req *http.Request) (string, error) {
	return func(req *http.Request) (string, error) {
		backupTargetName := mux.Vars(req)["backupTargetName"]
		backupTarget, err := m.GetBackupTarget(backupTargetName)
		if err != nil {
			return "", errors.Wrapf(err, "failed to get backup target '%s'", backupTargetName)
		}
		if backupTarget == nil {
			return "", nil
		}
		return backupTarget.Status.OwnerID, nil
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
		nodes, err := m.ListReadyNodesContainingEngineImageRO(engineImage)
		if err != nil {
			return "", err
		}
		if _, ok := nodes[m.GetCurrentNodeID()]; ok {
			return m.GetCurrentNodeID(), nil
		}

		for nodeID := range nodes {
			return nodeID, nil
		}
		return "", fmt.Errorf("failed to find a node that is ready and has the default engine image %v deployed", engineImage)
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

func (f *Fwd) Handler(proxyHandler ProxyRequestHandler, parametersGetFunc ParametersGetFunc, h HandleFuncWithError) HandleFuncWithError {
	return func(w http.ResponseWriter, req *http.Request) error {
		var requireProxy bool
		if proxyHandler != nil {
			if parametersGetFunc == nil {
				return fmt.Errorf("nil HTTP parameters get function")
			}
			parameters, err := parametersGetFunc(req)
			if err != nil {
				return errors.Wrap(err, "failed to get the parameters")
			}
			if parameters == nil {
				return errors.Wrap(err, "nil parameter")
			}
			requireProxy, err = proxyHandler(parameters, req)
			if err != nil {
				return errors.Wrap(err, "failed to verify if the proxy is required")
			}
		}
		if requireProxy {
			f.proxy.ServeHTTP(w, req)
			return nil
		}
		return h(w, req)
	}
}

func (f *Fwd) HandleProxyRequestByNodeID(parameters map[string]string, req *http.Request) (proxyRequired bool, err error) {
	targetAddress := parameters[ParameterKeyAddress]
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
	logrus.Infof("Forwarding request to %v", targetAddress)

	return true, nil
}

func (f *Fwd) GetHTTPAddressByNodeID(getNodeID OwnerIDFunc) ParametersGetFunc {
	return func(req *http.Request) (map[string]string, error) {
		nodeID, err := getNodeID(req)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get target node ID")
		}
		address, err := f.locator.Node2APIAddress(nodeID)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get the address from node ID")
		}
		return map[string]string{ParameterKeyAddress: address}, nil
	}
}

const (
	BackingImageUpload = "upload"
)

func (f *Fwd) HandleProxyRequestForBackingImageUpload(parameters map[string]string, req *http.Request) (proxyRequired bool, err error) {
	targetAddress := parameters[ParameterKeyAddress]
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

func UploadParametersForBackingImage(m *manager.VolumeManager) func(req *http.Request) (map[string]string, error) {
	return func(req *http.Request) (map[string]string, error) {
		name := mux.Vars(req)["name"]
		pod, err := m.GetBackingImageDataSourcePod(name)
		if err != nil {
			return nil, err
		}
		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			return nil, fmt.Errorf("backing image data source pod is not ready for uploading")
		}
		bids, err := m.GetBackingImageDataSource(name)
		if err != nil {
			return nil, errors.Wrapf(err, "error getting backing image %s", name)
		}
		if bids.Status.CurrentState != longhorn.BackingImageStatePending {
			return nil, fmt.Errorf("upload server for backing image %s has not been initiated", name)
		}
		return map[string]string{ParameterKeyAddress: net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(engineapi.BackingImageDataSourceDefaultPort))}, nil
	}
}

func (f *Fwd) HandleProxyRequestForBackingImageDownload(parameters map[string]string, req *http.Request) (proxyRequired bool, err error) {
	targetAddress := parameters[ParameterKeyAddress]
	filePath := parameters[ParameterKeyFilePath]

	if targetAddress == req.Host {
		return false, fmt.Errorf("backing image download request should not be handled by current longhorn manager service")
	}

	h := req.Header
	if h.Get("X-Forwarded-Proto") == "https" {
		h.Set("X-Forwarded-Proto", "http")
	}
	if h.Get("X-Forwarded-Host") != "" {
		h.Del("X-Forwarded-Host")
	}
	req.Header = h

	req.URL.Path = fmt.Sprintf("/v1/files/%s/download", filePath)
	req.URL.RawPath = fmt.Sprintf("/v1/files/%s/download", url.PathEscape(filePath))
	req.URL.Host = targetAddress
	req.URL.Scheme = "http"
	req.Host = targetAddress

	logrus.Infof("Forwarding backing image download request to URL %v", req.URL.String())

	return true, nil
}

func DownloadParametersFromBackingImage(m *manager.VolumeManager) func(req *http.Request) (map[string]string, error) {
	return func(req *http.Request) (map[string]string, error) {
		name := mux.Vars(req)["name"]
		bi, err := m.GetBackingImage(name)
		if err != nil {
			return nil, err
		}

		var targetBIM *longhorn.BackingImageManager
		for diskUUID, fStatus := range bi.Status.DiskFileStatusMap {
			if fStatus.State != longhorn.BackingImageStateReady {
				continue
			}
			bim, err := m.GetDefaultBackingImageManagersByDiskUUID(diskUUID)
			if err != nil {
				return nil, err
			}
			targetBIM = bim
			break
		}
		if targetBIM == nil {
			return nil, fmt.Errorf("failed to find a default backing image manager for backing image %v download", name)
		}

		cli, err := engineapi.NewBackingImageManagerClient(targetBIM)
		if err != nil {
			return nil, err
		}
		filePath, address, err := cli.PrepareDownload(name, bi.Status.UUID)
		if err != nil {
			return nil, err
		}
		return map[string]string{
			ParameterKeyFilePath: filePath,
			ParameterKeyAddress:  address,
		}, nil
	}
}
