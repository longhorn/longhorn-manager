package types

type VolumeState string

const (
	VolumeStateAttached = VolumeState("attached")
	VolumeStateDetached = VolumeState("detached")
	VolumeStateFaulted  = VolumeState("faulted")

	VolumeStateAttaching = VolumeState("attaching")
	VolumeStateDetaching = VolumeState("detaching")
	VolumeStateDeleting  = VolumeState("deleting")
)

type VolumeSpec struct {
	OwnerID             string         `json:"ownerID"`
	Size                string         `json:"size"`
	FromBackup          string         `json:"fromBackup"`
	NumberOfReplicas    int            `json:"numberOfReplicas"`
	StaleReplicaTimeout int            `json:"staleReplicaTimeout"`
	NodeID              string         `json:"nodeID"`
	RecurringJobs       []RecurringJob `json:"recurringJobs"`
}

type VolumeStatus struct {
	Created             string      `json:"created"`
	State               VolumeState `json:"state"`
	HealthyReplicaCount int         `json:"healthyReplicaCount"`
	Endpoint            string      `json:"endpoint"`
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
	InstanceStateRunning  = InstanceState("running")
	InstanceStateStopped  = InstanceState("stopped")
	InstanceStateError    = InstanceState("error")
	InstanceStateStarting = InstanceState("starting")
	InstanceStateStopping = InstanceState("stopping")
)

type InstanceSpec struct {
	OwnerID     string        `json:"ownerID"`
	VolumeName  string        `json:"volumeName"`
	NodeID      string        `json:"nodeID"`
	EngineImage string        `json:"engineImage"`
	DesireState InstanceState `json:"desireState"`
}

type InstanceStatus struct {
	CurrentState InstanceState `json:"currentState"`
	IP           string        `json:"ip"`
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

type SettingsInfo struct {
	BackupTarget string `json:"backupTarget"`
}
