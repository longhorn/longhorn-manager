package crdtype

import (
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/rancher/longhorn-manager/crd/tools/crdcopy"
	"github.com/rancher/longhorn-manager/types"
)


const (
	NodePlural      string = "crdnodes"
	NodeFullName    string = NodePlural + "." + CRDGroup
	NodeShortname	   string = "cn"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Crdnode struct {
	meta_v1.TypeMeta   `json:",inline"`
	meta_v1.ObjectMeta `json:"metadata"`
	Spec               CrdNodeSpec   `json:"spec"`
}

type CrdNodeSpec struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	IP               string    `json:"ip"`
	ManagerPort      int       `json:"managerPort"`
	OrchestratorPort int       `json:"orchestratorPort"`
	State            types.NodeState `json:"state"`
	LastCheckin      string    `json:"lastCheckin"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CrdnodeList struct {
	meta_v1.TypeMeta             `json:",inline"`
	meta_v1.ListMeta             `json:"metadata"`
	Items            []Crdnode `json:"items"`
}

func LhNode2CRDNode(vinfo *types.NodeInfo, crdnode *Crdnode, key string){
	crdnode.ObjectMeta.Name = key
	crdcopy.CRDDeepCopy(&crdnode.Spec, vinfo)
}

func CRDNode2LhNode(crdnode *Crdnode, vinfo *types.NodeInfo)  {
	crdcopy.CRDDeepCopy(vinfo, &crdnode.Spec)
}
