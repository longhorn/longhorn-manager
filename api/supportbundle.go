package api

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/sirupsen/logrus"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

func (s *Server) InitiateSupportBundle(w http.ResponseWriter, req *http.Request) error {
	var sb *longhorn.SupportBundle
	var supportBundleInput SupportBundleInitateInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&supportBundleInput); err != nil {
		return err
	}

	sb, err := s.m.InitSupportBundle(supportBundleInput.IssueURL, supportBundleInput.Description)
	if err != nil {
		return fmt.Errorf("unable to initiate Support Bundle Download:%v", err)
	}

	apiContext.Write(toSupportBundleResource(s.m.GetCurrentNodeID(), sb))
	return nil
}

func (s *Server) QuerySupportBundle(w http.ResponseWriter, req *http.Request) error {
	bundleName := mux.Vars(req)["bundleName"]
	apiContext := api.GetApiContext(req)
	sb, err := s.m.GetSupportBundle(bundleName)
	if err != nil {
		return errors.Wrap(err, "failed to get support bundle")
	}

	apiContext.Write(toSupportBundleResource(s.m.GetCurrentNodeID(), sb))
	return nil
}

func (s *Server) DownloadSupportBundle(rw http.ResponseWriter, r *http.Request) error {
	bundleName := mux.Vars(r)["bundleName"]

	retainSb := false
	if retain, err := strconv.ParseBool(r.URL.Query().Get("retain")); err == nil {
		retainSb = retain
	}

	sb, err := s.m.GetSupportBundle(bundleName)
	if err != nil {
		util.ResponseError(rw, http.StatusBadRequest, errors.Wrap(err, "fail to get support bundle resource"))
		return err
	}

	if sb.Status.State != types.SupportBundleStateReadyForDownload || sb.Status.FileName == "" || sb.Status.FileSize == 0 {
		util.ResponseError(rw, http.StatusBadRequest, errors.New("support bundle is not ready"))
		return err
	}

	managerPodIP, err := s.m.GetManagerPodIP()
	if err != nil {
		util.ResponseError(rw, http.StatusBadRequest, errors.Wrap(err, "fail to get support bundle manager pod IP"))
		return err
	}

	url := fmt.Sprintf("http://%s:8080/bundle", managerPodIP)
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil)
	if err != nil {
		util.ResponseError(rw, http.StatusInternalServerError, err)
		return err
	}

	httpClient := http.Client{
		Timeout: 1 * time.Hour,
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		util.ResponseError(rw, http.StatusInternalServerError, err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		util.ResponseErrorMsg(rw, http.StatusInternalServerError, fmt.Sprintf("unexpected status code %d", resp.StatusCode))
		return err
	}

	rw.Header().Set("Content-Length", fmt.Sprint(sb.Status.FileSize))
	rw.Header().Set("Content-Disposition", "attachment; filename="+sb.Status.FileName)
	contentType := resp.Header.Get("Content-Type")
	if contentType != "" {
		rw.Header().Set("Content-Type", contentType)
	}

	if _, err := io.Copy(rw, resp.Body); err != nil {
		util.ResponseError(rw, http.StatusInternalServerError, err)
		return err
	}

	if retainSb {
		return err
	}

	logrus.Infof("delete support bundle %s", sb.Name)
	err = s.m.DeleteSupportBundle(bundleName)
	if err != nil {
		logrus.Errorf("fail to delete support bundle %s: %s", sb.Name, err)
		return err
	}
	return nil
}
