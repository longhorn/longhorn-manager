package crdtype

import (
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/rancher/longhorn-manager/crd/tools/crdcopy"
	"github.com/rancher/longhorn-manager/types"
)

const (
	ReplicaPlural      string = "crdreplicas"
	ReplicaFullName    string = ReplicaPlural + "." + CRDGroup
	ReplicaShortname	   string = "cr"
)
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Crdreplica struct {
	meta_v1.TypeMeta   `json:",inline"`
	meta_v1.ObjectMeta `json:"metadata"`
	Spec               CrdReplicasSpec   `json:"spec"`
}

type CrdReplicasSpec struct {
	types.InstanceInfo

	FailedAt string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CrdreplicaList struct {
	meta_v1.TypeMeta             `json:",inline"`
	meta_v1.ListMeta             `json:"metadata"`
	Items            []Crdreplica `json:"items"`
}

func LhReplicas2CRDReplicas(rinfo *types.ReplicaInfo, crdreplica *Crdreplica, key string) {
	crdreplica.ObjectMeta.Name = key
	crdcopy.CRDDeepCopy(&crdreplica.Spec, rinfo)
}

func CRDReplicas2LhReplicas(crdreplicas *Crdreplica, rinfo *types.ReplicaInfo) {
	crdcopy.CRDDeepCopy(rinfo, &crdreplicas.Spec)
}