package stype

import (
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/rancher/longhorn-manager/crd/tools/crdcopy"
	"github.com/rancher/longhorn-manager/types"
)

// Definition of our CRD LongHorn Volume class
type Crdsetting struct {
	meta_v1.TypeMeta   `json:",inline"`
	meta_v1.ObjectMeta `json:"metadata"`
	Spec               CrdSettingSpec   `json:"spec"`
	Status             CrdSettingStatus `json:"status,omitempty"`
}



type CrdSettingSpec struct {
	BackupTarget string `json:"backupTarget"`

	types.KVMetadata

}

type CrdSettingStatus struct {
	State   string `json:"state,omitempty"`
	Message string `json:"message,omitempty"`
}

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