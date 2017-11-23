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
	ResourceVersion string `json:"-"`
}

type VolumeInfo struct {
	// Attributes
	Name                string
	Size                int64 `json:",string"`
	BaseImage           string
	FromBackup          string
	NumberOfReplicas    int
	StaleReplicaTimeout int

	// Running spec
	TargetNodeID  string
	DesireState   VolumeState
	RecurringJobs []RecurringJob

	// Running state
	Created  string
	NodeID   string
	State    VolumeState
	Endpoint string

	Metadata
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
	ID         string
	Type       InstanceType
	Name       string
	NodeID     string
	IP         string
	Running    bool
	VolumeName string

	Metadata
}

type ControllerInfo struct {
	InstanceInfo
}

type ReplicaInfo struct {
	InstanceInfo

	FailedAt string
}

type NodeState string

const (
	NodeStateUp   = NodeState("up")
	NodeStateDown = NodeState("down")
)

type NodeInfo struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
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
