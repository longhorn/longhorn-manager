package types

type VolumeState string

const (
	VolumeStateCreated  = VolumeState("created")
	VolumeStateDetached = VolumeState("detached")
	VolumeStateFault    = VolumeState("fault")
	VolumeStateHealthy  = VolumeState("healthy")
	VolumeStateDegraded = VolumeState("degraded")
)

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
	TargetHostID string
	HostID       string
	State        VolumeState
	DesireState  VolumeState
	Endpoint     string

	KVIndex int64 `json:"-"`
}

type InstanceInfo struct {
	ID         string
	Type       InstanceType
	Name       string
	HostID     string
	Address    string
	Running    bool
	VolumeName string

	KVIndex int64 `json:"-"`
}

type ControllerInfo struct {
	InstanceInfo
}

type ReplicaInfo struct {
	InstanceInfo

	Mode         ReplicaMode
	BadTimestamp string
}

type HostInfo struct {
	UUID    string `json:"uuid"`
	Name    string `json:"name"`
	Address string `json:"address"`

	KVIndex int64 `json:"-"`
}

type SettingsInfo struct {
	BackupTarget string `json:"backupTarget"`
	EngineImage  string `json:"engineImage"`

	KVIndex int64 `json:"-"`
}
