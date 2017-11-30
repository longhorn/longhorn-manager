package manager

import (
	"sync"

	"github.com/robfig/cron"

	"github.com/rancher/longhorn-manager/types"
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

type VolumeRecurringUpdateRequest struct {
	Name          string               `json:"name"`
	RecurringJobs []types.RecurringJob `json:"recurringJobs"`
}

type Volume struct {
	types.VolumeInfo
	Controller *types.ControllerInfo
	Replicas   map[string]*types.ReplicaInfo
}

type ManagedVolume struct {
	Volume
	mutex *sync.RWMutex

	Jobs      map[string]*Job
	jobsMutex *sync.RWMutex

	recurringJobScheduled []types.RecurringJob
	recurringCron         *cron.Cron

	Notify chan struct{}

	m *VolumeManager
}

type Node struct {
	types.NodeInfo

	m *VolumeManager
}

type RPCManager interface {
	Start(chan Event) error
	Stop()

	GetPort() int
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
	ID          string            `json:"id"`
	Type        JobType           `json:"type"`
	AssoicateID string            `json:"assoicateID"`
	CreatedAt   string            `json:"createdAt"`
	CompletedAt string            `json:"completedAt"`
	State       JobState          `json:"state"`
	Error       error             `json:"error"`
	Data        map[string]string `json:"data"`
}

type CronJob struct {
	types.RecurringJob

	volumeName string
	m          *VolumeManager
}
