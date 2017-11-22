package crdtype

import (
	"github.com/rancher/longhorn-manager/crd/tools/crdcopy"
	"github.com/rancher/longhorn-manager/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CrdNode struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              NodeSpec `json:"spec"`
}

type NodeSpec struct {
	ID               string          `json:"id"`
	Name             string          `json:"name"`
	IP               string          `json:"ip"`
	ManagerPort      int             `json:"managerPort"`
	OrchestratorPort int             `json:"orchestratorPort"`
	State            types.NodeState `json:"state"`
	LastCheckin      string          `json:"lastCheckin"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CrdNodeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []CrdNode `json:"items"`
}

func LhNode2CRDNode(vinfo *types.NodeInfo, crdnode *CrdNode, key string) {
	crdnode.ObjectMeta.Name = key
	crdcopy.CRDDeepCopy(&crdnode.Spec, vinfo)
}

func CRDNode2LhNode(crdnode *CrdNode, vinfo *types.NodeInfo) {
	crdcopy.CRDDeepCopy(vinfo, &crdnode.Spec)
}
