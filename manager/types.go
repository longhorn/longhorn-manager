package manager

import (
	"github.com/yasker/lm-rewrite/types"
	"sync"
)

type EventType string

const (
	EventTypeNotify = EventType("notify")
)

type Event struct {
	Type       EventType
	VolumeName string
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
	Controller *types.ControllerInfo
	Replicas   map[string]*types.ReplicaInfo
}

type ManagedVolume struct {
	Volume
	Jobs map[string]*Job

	// mutex protects above
	mutex *sync.RWMutex

	Notify chan struct{}

	m *VolumeManager
}

type Node struct {
	types.NodeInfo

	m *VolumeManager
}

type RPCManager interface {
	StartServer(address string, ch chan Event) error
	StopServer()

	NodeNotify(address string, event *Event) error
}

type JobState string

const (
	JobStateOngoing = JobState("ongoing")
	JobStateSucceed = JobState("succeed")
	JobStateFailed  = JobState("failed")
)

type JobType string

const (
	// JobTypeReplicaCreate associateID = replicaName
	JobTypeReplicaCreate = JobType("replica-create")

	// JobTypeReplicaRebuild associateID = replicaName
	JobTypeReplicaRebuild = JobType("replica-rebuild")

	// JobTypeSnapshotBackup associatedID = snapshotName
	JobTypeSnapshotBackup = JobType("snapshot-backup")

	// JobTypeSnapshotPurge associatedID (one per volume)
	JobTypeSnapshotPurge = JobType("snapshot-purge")
)

type Job struct {
	ID          string
	Type        JobType
	AssoicateID string
	CreatedAt   string
	CompletedAt string
	State       JobState
	Error       error
	Data        map[string]string
}
