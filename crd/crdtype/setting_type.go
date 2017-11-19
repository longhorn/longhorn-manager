package crdtype

import (
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/rancher/longhorn-manager/crd/tools/crdcopy"
	"github.com/rancher/longhorn-manager/types"
)

const (
	SettingPlural      string = "crdsettings"
	SettingFullName    string = SettingPlural + "." + CRDGroup
	SettingShortname	   string = "crs"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Crdsetting struct {
	meta_v1.TypeMeta   `json:",inline"`
	meta_v1.ObjectMeta `json:"metadata"`
	Spec               CrdSettingSpec   `json:"spec"`
}



type CrdSettingSpec struct {
	BackupTarget string `json:"backupTarget"`

	types.KVMetadata

}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CrdsettingList struct {
	meta_v1.TypeMeta             `json:",inline"`
	meta_v1.ListMeta             `json:"metadata"`
	Items            []Crdsetting `json:"items"`
}

func LhSetting2CRDSetting(sinfo *types.SettingsInfo, crdsetting *Crdsetting, key string) {
	crdsetting.ObjectMeta.Name = key
	crdcopy.CRDDeepCopy(&crdsetting.Spec, sinfo)
}

func CRDSetting2LhSetting(crdsetting *Crdsetting, sinfo *types.SettingsInfo) {
	crdcopy.CRDDeepCopy(sinfo, &crdsetting.Spec)
}