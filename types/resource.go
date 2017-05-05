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
	Name                string
	Size                int64 `json:",string"`
	BaseImage           string
	FromBackup          string
	NumberOfReplicas    int
	StaleReplicaTimeout int
	Created             string

	TargetHostID string
	HostID       string
	State        VolumeState
	DesireState  VolumeState
	Endpoint     string

	KVIndex int64 `json:"-"`
	//EngineImage string
	//Controller          *ControllerInfo
	//Replicas            map[string]*ReplicaInfo //key is replicaName
	//RecurringJobs       []*RecurringJob
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
	BadTimestamp time.Time
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
