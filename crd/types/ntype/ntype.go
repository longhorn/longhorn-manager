
package ntype

import (
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/rancher/longhorn-manager/crd/tools/crdcopy"
	"github.com/rancher/longhorn-manager/types"
)

// Definition of our CRD LongHorn Volume class
type Crdnode struct {
	meta_v1.TypeMeta   `json:",inline"`
	meta_v1.ObjectMeta `json:"metadata"`
	Spec               CrdNodeSpec   `json:"spec"`
	Status             CrdNodeStatus `json:"status,omitempty"`
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

type CrdNodeStatus struct {
	State   string `json:"state,omitempty"`
	Message string `json:"message,omitempty"`
}

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
