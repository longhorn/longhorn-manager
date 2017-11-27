package crdtype

import (
	"github.com/rancher/longhorn-manager/crd/tools/crdcopy"
	"github.com/rancher/longhorn-manager/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Volume struct {
	metav1.TypeMeta    `json:",inline"`
	metav1.ObjectMeta  `json:"metadata"`
	Spec               VolumeSpec `json:"spec"`
	OperateFromKubectl bool       `json:"operateFromKubectl, bool, omitempty"`
}

type VolumeSpec struct {
	// Attributes
	Name                string `json:"name"`
	Size                int64  `json:"size, int64"`
	BaseImage           string `json:"baseImage,omitempty"`
	FromBackup          string `json:"fromBackup,omitempty"`
	NumberOfReplicas    int    `json:"numReplicas, int"`
	StaleReplicaTimeout int    `json:"staleReplicaTimeout, int"`

	// Running spec
	TargetNodeID  string            `json:"targetNodeID,omitempty"`
	DesireState   types.VolumeState `json:"desireState,omitempty"`
	RecurringJobs []types.RecurringJob

	// Running state
	Created  string
	NodeID   string `json:"nodeid,omitempty"`
	State    types.VolumeState
	Endpoint string

	types.KVMetadata
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type VolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []Volume `json:"items"`
}

func LhVolume2CRDVolume(vinfo *types.VolumeInfo, crdvolume *Volume) {
	crdvolume.ObjectMeta.Name = vinfo.Name
	crdcopy.CRDDeepCopy(&crdvolume.Spec, vinfo)
}

func CRDVolume2LhVolume(crdvolume *Volume, vinfo *types.VolumeInfo) {
	crdcopy.CRDDeepCopy(vinfo, &crdvolume.Spec)
}
