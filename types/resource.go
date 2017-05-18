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

type KVMetadata struct {
	KVIndex uint64 `json:"-"`
}

type VolumeInfo struct {
	// Attributes
	Name                string
	Size                int64 `json:",string"`
	BaseImage           string
	FromBackup          string
	NumberOfReplicas    int
	StaleReplicaTimeout int

	// Running state
	Created      string
	TargetNodeID string
	NodeID       string
	State        VolumeState
	DesireState  VolumeState
	Endpoint     string

	KVMetadata
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
	Address    string
	Running    bool
	VolumeName string

	KVMetadata
}

type ControllerInfo struct {
	InstanceInfo
}

type ReplicaInfo struct {
	InstanceInfo

	Mode         ReplicaMode
	BadTimestamp string
}

type NodeState string

const (
	NodeStateUp   = NodeState("up")
	NodeStateDown = NodeState("down")
)

type NodeInfo struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Address     string    `json:"address"`
	State       NodeState `json:"state"`
	LastCheckin string    `json:"lastCheckin"`

	KVMetadata
}

type SettingsInfo struct {
	BackupTarget string `json:"backupTarget"`
	EngineImage  string `json:"engineImage"`

	KVMetadata
}
