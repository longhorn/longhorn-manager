package crdtype

import (
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/rancher/longhorn-manager/crd/tools/crdcopy"
	"github.com/rancher/longhorn-manager/types"
)

const (
	ControllerPlural      string = "crdcontrollers"
	ControllerFullName    string = ControllerPlural + "." + CRDGroup
	ControllerShortname	   string = "cc"
)
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Crdcontroller struct {
	meta_v1.TypeMeta   `json:",inline"`
	meta_v1.ObjectMeta `json:"metadata"`
	Spec               CrdControllerSpec   `json:"spec"`
}

type CrdControllerSpec struct {
	types.InstanceInfo

}
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CrdcontrollerList struct {
	meta_v1.TypeMeta             `json:",inline"`
	meta_v1.ListMeta             `json:"metadata"`
	Items            []Crdcontroller `json:"items"`
}

func LhController2CRDController(cinfo *types.ControllerInfo, crdcontroller *Crdcontroller, key string) {
	crdcontroller.ObjectMeta.Name = key
	crdcopy.CRDDeepCopy(&crdcontroller.Spec, cinfo)
}

func CRDController2LhController(crdcontroller *Crdcontroller, cinfo *types.ControllerInfo) {
	crdcopy.CRDDeepCopy(cinfo, &crdcontroller.Spec)
}