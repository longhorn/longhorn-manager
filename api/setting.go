package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
)

func (s *Server) SettingsList(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	si, err := s.ds.GetSetting()
	if err != nil || si == nil {
		return errors.Wrap(err, "fail to read settings")
	}
	apiContext.Write(toSettingCollection(&si.SettingsInfo))
	return nil
}

func (s *Server) SettingsGet(w http.ResponseWriter, req *http.Request) error {
	name := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	si, err := s.ds.GetSetting()
	if err != nil || si == nil {
		return errors.Wrap(err, "fail to read settings")
	}
	var value string
	switch name {
	case "backupTarget":
		value = si.BackupTarget
	default:
		return errors.Errorf("invalid setting name %v", name)
	}
	apiContext.Write(toSettingResource(name, value))
	return nil
}

func (s *Server) SettingsSet(w http.ResponseWriter, req *http.Request) error {
	var setting Setting

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&setting); err != nil {
		return err
	}

	name := mux.Vars(req)["name"]

	si, err := s.ds.GetSetting()
	if err != nil || si == nil {
		return errors.Wrap(err, "fail to read settings")
	}

	switch name {
	case "backupTarget":
		si.BackupTarget = setting.Value
	default:
		return errors.Wrapf(err, "invalid setting name %v", name)
	}
	if _, err := s.ds.UpdateSetting(si); err != nil {
		return errors.Wrapf(err, "fail to set settings %v", si)
	}

	apiContext.Write(toSettingResource(name, setting.Value))
	return nil
}
