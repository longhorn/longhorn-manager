package api

import (
	"net/http"
	"regexp"
	"strings"

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
	case types.SettingDefaultEngineImage:
		value = si.DefaultEngineImage
	case types.SettingBackupTargetCredentialSecret:
		value = si.BackupTargetCredentialSecret
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
		// additional check whether have $ or , have been set in BackupTarget
		regStr := `[\$\,]`
		reg := regexp.MustCompile(regStr)
		findStr := reg.FindAllString(setting.Value, -1)
		if len(findStr) != 0 {
			return errors.Wrapf(err, "fail to set settings with invalid BackupTarget %s, contains %v", setting.Value, strings.Join(findStr, " or "))
		}
		si.BackupTarget = setting.Value
	case types.SettingDefaultEngineImage:
		si.DefaultEngineImage = setting.Value
	case types.SettingBackupTargetCredentialSecret:
		si.BackupTargetCredentialSecret = setting.Value
	default:
		return errors.Wrapf(err, "invalid setting name %v", name)
	}
	if _, err := s.m.UpdateSetting(si); err != nil {
		return errors.Wrapf(err, "fail to set settings %v", si)
	}

	apiContext.Write(toSettingResource(name, setting.Value))
	return nil
}
