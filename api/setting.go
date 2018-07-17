package api

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/rancher/longhorn-manager/types"
)

func (s *Server) SettingList(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	sl, err := s.settingList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(sl)
	return nil
}

func (s *Server) settingList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	sList, err := s.m.ListSettingsSorted()
	if err != nil || sList == nil {
		return nil, errors.Wrap(err, "fail to list settings")
	}
	return toSettingCollection(sList), nil
}

func (s *Server) SettingGet(w http.ResponseWriter, req *http.Request) error {
	name := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	si, err := s.m.GetSetting(types.SettingName(name))
	if err != nil {
		return errors.Wrapf(err, "fail get setting %v", name)
	}
	apiContext.Write(toSettingResource(si))
	return nil
}

func (s *Server) SettingSet(w http.ResponseWriter, req *http.Request) error {
	var setting Setting

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&setting); err != nil {
		return err
	}

	name := mux.Vars(req)["name"]
	sName := types.SettingName(name)

	if sName == types.SettingNameBackupTarget {
		// additional check whether have $ or , have been set in BackupTarget
		regStr := `[\$\,]`
		reg := regexp.MustCompile(regStr)
		findStr := reg.FindAllString(setting.Value, -1)
		if len(findStr) != 0 {
			return fmt.Errorf("fail to set settings with invalid BackupTarget %s, contains %v", setting.Value, strings.Join(findStr, " or "))
		}
	}

	si, err := s.m.GetSetting(sName)
	if err != nil {
		return err
	}
	si.Value = strings.TrimSpace(setting.Value)
	si, err = s.m.CreateOrUpdateSetting(si)
	if err != nil {
		return errors.Wrapf(err, "fail to set settings %v", si.Name)
	}

	apiContext.Write(toSettingResource(si))
	return nil
}
