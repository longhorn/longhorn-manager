package crdtype

import (
meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/rancher/longhorn-manager/crd/tools/crdcopy"
	"github.com/rancher/longhorn-manager/types"
)

const (
	CRDVersion     string = "v1"
	CRDGroup       string = "rancher.io"
	VolumePlural      string = "crdvolumes"
	VolumeFullName    string = VolumePlural + "." + CRDGroup
	VolumeShortname	   string = "cv"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Crdvolume struct {
	meta_v1.TypeMeta   `json:",inline"`
	meta_v1.ObjectMeta `json:"metadata"`
	Spec               CrdVolumeSpec   `json:"spec"`
	OperateFromKubectl 	bool   `json:"operatefromkubectl, bool, omitempyt"`
}


type CrdVolumeSpec struct {
	// Attributes
	Name                string 	`json:"name"`
	Size                int64  	`json:"size, int64"`
	BaseImage           string 	`json:"baseimage,omitempty"`
	FromBackup          string 	`json:"frombackup,omitempty"`
	NumberOfReplicas    int    	`json:"numreplicas, int"`
	StaleReplicaTimeout int	   	`json:"stalereplicatimeout, int"`

	// Running spec
	TargetNodeID  string		`json:"targetnodeid,omitempty"`
	DesireState   types.VolumeState	`json:"desirestate,omitempty"`
	RecurringJobs []types.RecurringJob

	// Running state
	Created  string
	NodeID   string 			`json:"nodeid,omitempty"`
	State    types.VolumeState
	Endpoint string

	types.KVMetadata
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CrdvolumeList struct {
	meta_v1.TypeMeta             `json:",inline"`
	meta_v1.ListMeta             `json:"metadata"`
	Items            []Crdvolume `json:"items"`
}

func LhVoulme2CRDVolume(vinfo *types.VolumeInfo, crdvolume *Crdvolume){
	crdvolume.ObjectMeta.Name = vinfo.Name
	crdcopy.CRDDeepCopy(&crdvolume.Spec, vinfo)
}

func CRDVolume2LhVoulme(crdvolume *Crdvolume, vinfo *types.VolumeInfo)  {
	crdcopy.CRDDeepCopy(vinfo, &crdvolume.Spec)
}
