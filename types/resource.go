package types

type VolumeState string

const (
	VolumeStateAttached  = VolumeState("attached")
	VolumeStateDetached  = VolumeState("detached")
	VolumeStateAttaching = VolumeState("attaching")
	VolumeStateDetaching = VolumeState("detaching")
	VolumeStateDeleting  = VolumeState("deleting")
)

type VolumeRobustness string

const (
	VolumeRobustnessHealthy  = VolumeRobustness("healthy")  // during attached
	VolumeRobustnessDegraded = VolumeRobustness("degraded") // during attached
	VolumeRobustnessFaulted  = VolumeRobustness("faulted")  // during detached
	VolumeRobustnessUnknown  = VolumeRobustness("unknown")  // during detached
)

type VolumeSpec struct {
	OwnerID             string         `json:"ownerID"`
	Size                int64          `json:"size,string"`
	FromBackup          string         `json:"fromBackup"`
	NumberOfReplicas    int            `json:"numberOfReplicas"`
	StaleReplicaTimeout int            `json:"staleReplicaTimeout"`
	NodeID              string         `json:"nodeID"`
	EngineImage         string         `json:"engineImage"`
	RecurringJobs       []RecurringJob `json:"recurringJobs"`
}

type VolumeStatus struct {
	State        VolumeState      `json:"state"`
	Robustness   VolumeRobustness `json:"robustness"`
	Endpoint     string           `json:"endpoint"`
	CurrentImage string           `json:"currentImage"`
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
	VolumeSize  int64         `json:"volumeSize,string"`
	NodeID      string        `json:"nodeID"`
	EngineImage string        `json:"engineImage"`
	DesireState InstanceState `json:"desireState"`
}

type InstanceStatus struct {
	CurrentState InstanceState `json:"currentState"`
	CurrentImage string        `json:"currentImage"`
	IP           string        `json:"ip"`
	Started      bool          `json:"started"`
}

type EngineSpec struct {
	InstanceSpec
	ReplicaAddressMap         map[string]string `json:"replicaAddressMap"`
	UpgradedReplicaAddressMap map[string]string `json:"upgradedReplicaAddressMap"`
}

type EngineStatus struct {
	InstanceStatus
	ReplicaModeMap map[string]ReplicaMode `json:"replicaModeMap"`
	Endpoint       string                 `json:"endpoint"`
}

type ReplicaSpec struct {
	InstanceSpec
	RestoreFrom string `json:"restoreFrom"`
	RestoreName string `json:"restoreName"`
	HealthyAt   string `json:"healthyAt"`
	FailedAt    string `json:"failedAt"`
	DataPath    string `json:"dataPath"`
	Cleanup     bool   `json:"cleanup"`
}

type ReplicaStatus struct {
	InstanceStatus
}

const (
	SettingBackupTarget       = "backupTarget"
	SettingDefaultEngineImage = "defaultEngineImage"
	SettingEngineUpgradeImage = "engineUpgradeImage"
)

type SettingsInfo struct {
	BackupTarget       string `json:"backupTarget"`
	DefaultEngineImage string `json:"defaultEngineImage"`
	EngineUpgradeImage string `json:"engineUpgradeImage"`
}
