package crdtype

import (
	"github.com/rancher/longhorn-manager/crd/tools/crdcopy"
	"github.com/rancher/longhorn-manager/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Replica struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              ReplicasSpec `json:"spec"`
}

type ReplicasSpec struct {
	types.InstanceInfo

	FailedAt string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ReplicaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []Replica `json:"items"`
}

func LhReplicas2CRDReplicas(rinfo *types.ReplicaInfo, crdreplica *Replica, key string) {
	crdreplica.ObjectMeta.Name = key
	crdcopy.CRDDeepCopy(&crdreplica.Spec, rinfo)
}

func CRDReplicas2LhReplicas(crdreplicas *Replica, rinfo *types.ReplicaInfo) {
	crdcopy.CRDDeepCopy(rinfo, &crdreplicas.Spec)
}
