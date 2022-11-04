package api

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (s *Server) SupportBundleCreate(w http.ResponseWriter, req *http.Request) error {
	var supportBundleInput SupportBundleInitateInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&supportBundleInput); err != nil {
		return err
	}
	supportBundle, err := s.m.CreateSupportBundle(supportBundleInput.IssueURL, supportBundleInput.Description)
	if err != nil {
		return errors.Wrap(err, "failed to create SupportBundle")
	}
	apiContext.Write(toSupportBundleResource(s.m.GetCurrentNodeID(), supportBundle))
	return nil
}

func (s *Server) SupportBundleDelete(w http.ResponseWriter, req *http.Request) error {
	name := mux.Vars(req)["bundleName"]
	if err := s.m.DeleteSupportBundle(name); err != nil {
		return errors.Wrapf(err, "unable to delete SupportBundle %v", name)
	}

	return nil
}

func (s *Server) SupportBundleDownload(w http.ResponseWriter, req *http.Request) error {
	bundleName := mux.Vars(req)["bundleName"]
	supportBundle, supportBundleIP, err := s.m.GetSupportBundle(bundleName)
	if err != nil {
		return err
	}

	if supportBundleIP == "" {
		return errors.New("failed to get SupportBundle IP")
	}

	if supportBundle.State == longhorn.SupportBundleStateError {
		return errors.Wrap(err, "failed to create support bundle")
	}
	if supportBundle.State != longhorn.SupportBundleStateReady {
		return fmt.Errorf("support bundle not ready")
	}

	sourceURL := fmt.Sprintf(types.SupportBundleURLDownloadFmt, supportBundleIP, types.SupportBundleURLPort)
	newReq, err := http.NewRequestWithContext(req.Context(), http.MethodGet, sourceURL, nil)
	if err != nil {
		return err
	}

	httpClient := http.Client{
		Timeout: types.SupportBundleDownloadTimeout,
	}

	resp, err := httpClient.Do(newReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return err
	}

	w.Header().Set("Content-Disposition", "attachment; filename="+supportBundle.Filename)
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Length", strconv.FormatInt(supportBundle.Size, 10))
	if _, err := io.Copy(w, resp.Body); err != nil {
		return err
	}

	return s.m.DeleteSupportBundle(bundleName)
}

func (s *Server) SupportBundleGet(w http.ResponseWriter, req *http.Request) error {
	bundleName := mux.Vars(req)["bundleName"]
	apiContext := api.GetApiContext(req)
	sb, _, err := s.m.GetSupportBundle(bundleName)
	if err != nil {
		return errors.Wrap(err, "failed to get support bundle")
	}
	apiContext.Write(toSupportBundleResource(s.m.GetCurrentNodeID(), sb))
	return nil
}

func (s *Server) SupportBundleList(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	supportBundleList, err := s.supportBundleList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(supportBundleList)
	return nil
}

func (s *Server) supportBundleList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	list, err := s.m.ListSupportBundlesSorted()
	if err != nil {
		return nil, errors.Wrap(err, "error listing SupportBundles")
	}
	return toSupportBundleCollection(list, apiContext), nil
}
