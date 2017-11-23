package crdtype

import (
	"github.com/rancher/longhorn-manager/crd/tools/crdcopy"
	"github.com/rancher/longhorn-manager/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type Controller struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ControllerSpec `json:"spec"`
}

type ControllerSpec struct {
	types.InstanceInfo
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ControllerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Controller `json:"items"`
}

func LhController2CRDController(cinfo *types.ControllerInfo, crdcontroller *Controller, key string) {
	crdcontroller.ObjectMeta.Name = key
	crdcopy.CRDDeepCopy(&crdcontroller.Spec, cinfo)
}

func CRDController2LhController(crdcontroller *Controller, cinfo *types.ControllerInfo) {
	crdcopy.CRDDeepCopy(cinfo, &crdcontroller.Spec)
}
