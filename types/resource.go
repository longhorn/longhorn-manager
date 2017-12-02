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
}

type VolumeInfo struct {
	VolumeSpec
	VolumeStatus

	Metadata
}

type VolumeSpec struct {
	Size                int64          `json:"size, string"`
	BaseImage           string         `json:"baseImage"`
	FromBackup          string         `json:"fromBackup"`
	NumberOfReplicas    int            `json:"numberOfReplicas"`
	StaleReplicaTimeout int            `json:"staleReplicaTimeout"`
	TargetNodeID        string         `json:"targetNodeID"`
	RecurringJobs       []RecurringJob `json:"recurringJobs"`
	DesireState         VolumeState    `json:"desireState"`
}

type VolumeStatus struct {
	Created  string      `json:"created"`
	NodeID   string      `json:"nodeID"`
	State    VolumeState `json:"state"`
	Endpoint string      `json:"endpoint"`
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

type InstanceType string

const (
	InstanceTypeNone       = InstanceType("")
	InstanceTypeController = InstanceType("controller")
	InstanceTypeReplica    = InstanceType("replica")
)

type InstanceInfo struct {
	ID         string       `json:"id"`
	Type       InstanceType `json:"type"`
	NodeID     string       `json:"nodeID"`
	IP         string       `json:"ip"`
	Running    bool         `json:"running"`
	VolumeName string       `json:"volumeName"`

	Metadata
}

type ControllerInfo struct {
	InstanceInfo
}

type ReplicaInfo struct {
	InstanceInfo

	FailedAt string `json:"failedAt"`
}

type NodeState string

const (
	NodeStateUp   = NodeState("up")
	NodeStateDown = NodeState("down")
)

type NodeInfo struct {
	ID               string    `json:"id"`
	IP               string    `json:"ip"`
	ManagerPort      int       `json:"managerPort"`
	OrchestratorPort int       `json:"orchestratorPort"`
	State            NodeState `json:"state"`
	LastCheckin      string    `json:"lastCheckin"`

	Metadata
}

type SettingsInfo struct {
	BackupTarget string `json:"backupTarget"`

	Metadata
}
