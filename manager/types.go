package manager

import (
	"github.com/yasker/lm-rewrite/types"
)

type EventType string

const (
	EventTypeNotify = EventType("notify")
)

type Event struct {
	Type       EventType
	VolumeName string
}

type VolumeChan struct {
	Notify chan struct{}
}

type VolumeCreateRequest struct {
	Name                string `json:"name"`
	Size                string `json:"size"`
	BaseImage           string `json:"baseImage"`
	FromBackup          string `json:"fromBackup"`
	NumberOfReplicas    int    `json:"numberOfReplicas"`
	StaleReplicaTimeout int    `json:"staleReplicaTimeout"`
}

type VolumeAttachRequest struct {
	Name   string `json:"name"`
	NodeID string `json:"nodeId"`
}

type VolumeDetachRequest struct {
	Name string `json:"name"`
}

type VolumeDeleteRequest struct {
	Name string `json:"name"`
}

type VolumeSalvageRequest struct {
	Name                string   `json:"name"`
	SalvageReplicaNames []string `json:"salvageReplicaNames"`
}

type Volume struct {
	types.VolumeInfo

	Controller  *types.ControllerInfo
	Replicas    map[string]*types.ReplicaInfo
	BadReplicas map[string]*types.ReplicaInfo

	RebuildingReplica *types.ReplicaInfo

	m *VolumeManager
	//done chan struct{}
}

type Node struct {
	types.NodeInfo

	m *VolumeManager
}

type RPCManager interface {
	StartServer(address string) error

	SetCallbackChan(ch chan Event)
	NodeNotify(address string, event *Event) error
}
