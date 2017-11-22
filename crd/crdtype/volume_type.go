package crdtype

import (
	"github.com/rancher/longhorn-manager/crd/tools/crdcopy"
	"github.com/rancher/longhorn-manager/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CrdVolume struct {
	metav1.TypeMeta    `json:",inline"`
	metav1.ObjectMeta  `json:"metadata"`
	Spec               VolumeSpec `json:"spec"`
	OperateFromKubectl bool       `json:"operatefromkubectl, bool, omitempyt"`
}

type VolumeSpec struct {
	// Attributes
	Name                string `json:"name"`
	Size                int64  `json:"size, int64"`
	BaseImage           string `json:"baseimage,omitempty"`
	FromBackup          string `json:"frombackup,omitempty"`
	NumberOfReplicas    int    `json:"numreplicas, int"`
	StaleReplicaTimeout int    `json:"stalereplicatimeout, int"`

	// Running spec
	TargetNodeID  string            `json:"targetnodeid,omitempty"`
	DesireState   types.VolumeState `json:"desirestate,omitempty"`
	RecurringJobs []types.RecurringJob

	// Running state
	Created  string
	NodeID   string `json:"nodeid,omitempty"`
	State    types.VolumeState
	Endpoint string

	types.KVMetadata
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CrdVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []CrdVolume `json:"items"`
}

func LhVoulme2CRDVolume(vinfo *types.VolumeInfo, crdvolume *CrdVolume) {
	crdvolume.ObjectMeta.Name = vinfo.Name
	crdcopy.CRDDeepCopy(&crdvolume.Spec, vinfo)
}

func CRDVolume2LhVolume(crdvolume *CrdVolume, vinfo *types.VolumeInfo) {
	crdcopy.CRDDeepCopy(vinfo, &crdvolume.Spec)
}
