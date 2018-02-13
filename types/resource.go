package types

type VolumeState string

const (
	VolumeStateDetached = VolumeState("detached")
	VolumeStateHealthy  = VolumeState("healthy")
	VolumeStateDeleted  = VolumeState("deleted")

	VolumeStateCreated  = VolumeState("created")
	VolumeStateFault    = VolumeState("fault")
	VolumeStateDegraded = VolumeState("degraded")
)

type Metadata struct {
	Name            string `json:"name"`
	ResourceVersion string `json:"-"`
	DeletionPending bool   `json:"-"`
}

type VolumeInfo struct {
	VolumeSpec
	VolumeStatus

	Metadata
}

type VolumeSpec struct {
	Size                string         `json:"size"`
	BaseImage           string         `json:"baseImage"`
	FromBackup          string         `json:"fromBackup"`
	NumberOfReplicas    int            `json:"numberOfReplicas"`
	StaleReplicaTimeout int            `json:"staleReplicaTimeout"`
	TargetNodeID        string         `json:"targetNodeID"`
	RecurringJobs       []RecurringJob `json:"recurringJobs"`
	DesireState         VolumeState    `json:"desireState"`
	DesireOwnerID       string         `json:"desireOwnerID"`
}

type VolumeStatus struct {
	Created        string      `json:"created"`
	NodeID         string      `json:"nodeID"`
	State          VolumeState `json:"state"`
	Endpoint       string      `json:"endpoint"`
	CurrentOwnerID string      `json:"currentOwnerID"`
}

type RecurringJobType string

const (
	RecurringJobTypeSnapshot = RecurringJobType("snapshot")
	RecurringJobTypeBackup   = RecurringJobType("backup")
)

type RecurringJob struct {
	Name   string           `json:"name"`
	Type   RecurringJobType `json:"task"`
	Cron   string           `json:"cron"`
	Retain int              `json:"retain"`
}

type InstanceState string

const (
	InstanceStateRunning = InstanceState("running")
	InstanceStateStopped = InstanceState("stopped")
	InstanceStateError   = InstanceState("error")
)

type InstanceSpec struct {
	VolumeName    string        `json:"volumeName"`
	NodeID        string        `json:"nodeID"`
	EngineImage   string        `json:"engineImage"`
	DesireState   InstanceState `json:"desireState"`
	DesireOwnerID string        `json:"desireOwnerID"`
}

type InstanceStatus struct {
	CurrentOwnerID string        `json:"currentOwnerID"`
	State          InstanceState `json:"state"`
	IP             string        `json:"ip"`
}

func (i *InstanceStatus) Running() bool {
	return i.State == InstanceStateRunning
}

type EngineSpec struct {
	InstanceSpec
	ReplicaAddressMap map[string]string `json:"replicaAddressMap"`
}

type EngineStatus struct {
	InstanceStatus
	ReplicaModeMap map[string]ReplicaMode `json:"replicaModeMap"`
	Endpoint       string                 `json:"endpoint"`
}

type ControllerInfo struct {
	EngineSpec
	EngineStatus

	Metadata
}

type ReplicaSpec struct {
	InstanceSpec
	VolumeSize  string `json:"volumeSize"`
	RestoreFrom string `json:"restoreFrom"`
	RestoreName string `json:"restoreName"`
	FailedAt    string `json:"failedAt"`
}

type ReplicaStatus struct {
	InstanceStatus
}

type ReplicaInfo struct {
	ReplicaSpec
	ReplicaStatus

	Metadata
}

type NodeState string

const (
	NodeStateUp   = NodeState("up")
	NodeStateDown = NodeState("down")
)

type NodeInfo struct {
	ID          string    `json:"id"`
	IP          string    `json:"ip"`
	State       NodeState `json:"state"`
	LastCheckin string    `json:"lastCheckin"`

	Metadata
}

type SettingsInfo struct {
	BackupTarget string `json:"backupTarget"`

	Metadata
}
