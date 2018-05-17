package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"

	"github.com/rancher/longhorn-manager/types"
)

func (s *Server) SettingsList(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	si, err := s.m.GetSetting()
	if err != nil || si == nil {
		return errors.Wrap(err, "fail to read settings")
	}
	apiContext.Write(toSettingCollection(&si.SettingsInfo))
	return nil
}

func (s *Server) SettingsGet(w http.ResponseWriter, req *http.Request) error {
	name := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	si, err := s.m.GetSetting()
	if err != nil || si == nil {
		return errors.Wrap(err, "fail to read settings")
	}
	var value string
	switch name {
	case types.SettingBackupTarget:
		value = si.BackupTarget
	case types.SettingEngineUpgradeImage:
		value = si.EngineUpgradeImage
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

	si, err := s.m.GetSetting()
	if err != nil || si == nil {
		return errors.Wrap(err, "fail to read settings")
	}

	switch name {
	case types.SettingBackupTarget:
		si.BackupTarget = setting.Value
	case types.SettingEngineUpgradeImage:
		si.EngineUpgradeImage = setting.Value
	default:
		return errors.Wrapf(err, "invalid setting name %v", name)
	}
	if _, err := s.m.UpdateSetting(si); err != nil {
		return errors.Wrapf(err, "fail to set settings %v", si)
	}

	apiContext.Write(toSettingResource(name, setting.Value))
	return nil
}
