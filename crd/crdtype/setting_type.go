package crdtype

import (
	"github.com/rancher/longhorn-manager/crd/tools/crdcopy"
	"github.com/rancher/longhorn-manager/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CrdSetting struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec               SettingSpec `json:"spec"`
}

type SettingSpec struct {
	BackupTarget string `json:"backupTarget"`

	types.KVMetadata
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CrdSettingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items            []CrdSetting `json:"items"`
}

func LhSetting2CRDSetting(sinfo *types.SettingsInfo, crdsetting *CrdSetting, key string) {
	crdsetting.ObjectMeta.Name = key
	crdcopy.CRDDeepCopy(&crdsetting.Spec, sinfo)
}

func CRDSetting2LhSetting(crdsetting *CrdSetting, sinfo *types.SettingsInfo) {
	crdcopy.CRDDeepCopy(sinfo, &crdsetting.Spec)
}
