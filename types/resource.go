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
	VolumeRobustnessUnknown  = VolumeRobustness("unknown")
)

type VolumeFrontend string

const (
	VolumeFrontendBlockDev = VolumeFrontend("blockdev")
	VolumeFrontendISCSI    = VolumeFrontend("iscsi")
)

type VolumeSpec struct {
	OwnerID             string         `json:"ownerID"`
	Size                int64          `json:"size,string"`
	Frontend            VolumeFrontend `json:"frontend"`
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
	Frontend                  VolumeFrontend    `json:"frontend"`
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
	Active      bool   `json:"active"`
}

type ReplicaStatus struct {
	InstanceStatus
}

const (
	SettingBackupTarget                 = "backupTarget"
	SettingDefaultEngineImage           = "defaultEngineImage"
	SettingEngineUpgradeImage           = "engineUpgradeImage"
	SettingBackupTargetCredentialSecret = "backupTargetCredentialSecret"
)

type SettingsInfo struct {
	BackupTarget                 string `json:"backupTarget"`
	DefaultEngineImage           string `json:"defaultEngineImage"`
	BackupTargetCredentialSecret string `json:"backupTargetCredentialSecret"`
}

type EngineImageState string

const (
	EngineImageStateDeploying    = "deploying"
	EngineImageStateReady        = "ready"
	EngineImageStateIncompatible = "incompatible"
	EngineImageStateError        = "error"
)

type EngineImageSpec struct {
	OwnerID string `json:"ownerID"`
	Image   string `json:"image"`
}

type EngineImageStatus struct {
	State      EngineImageState `json:"state"`
	RefCount   int              `json:"refCount"`
	NoRefSince string           `json:"noRefSince"`

	EngineVersionDetails
}

const (
	InvalidEngineVersion = -1
)

type EngineVersionDetails struct {
	Version   string `json:"version"`
	GitCommit string `json:"gitCommit"`
	BuildDate string `json:"buildDate"`

	CLIAPIVersion           int `json:"cliAPIVersion"`
	CLIAPIMinVersion        int `json:"cliAPIMinVersion"`
	ControllerAPIVersion    int `json:"controllerAPIVersion"`
	ControllerAPIMinVersion int `json:"controllerAPIMinVersion"`
	DataFormatVersion       int `json:"dataFormatVersion"`
	DataFormatMinVersion    int `json:"dataFormatMinVersion"`
}

type NodeSpec struct {
	Name            string `json:"name"`
	AllowScheduling bool   `json:"allowScheduling"`
}

type NodeState string

const (
	NodeStateUp   = NodeState("up")
	NodeStateDown = NodeState("down")
)

type NodeStatus struct {
	State NodeState
}
