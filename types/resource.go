package types

type VolumeState string

const (
	VolumeStateCreating  = VolumeState("creating")
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

type ConditionStatus string

const (
	ConditionStatusTrue    ConditionStatus = "True"
	ConditionStatusFalse   ConditionStatus = "False"
	ConditionStatusUnknown ConditionStatus = "Unknown"
)

type Condition struct {
	Type               string          `json:"type"`
	Status             ConditionStatus `json:"status"`
	LastProbeTime      string          `json:"lastProbeTime"`
	LastTransitionTime string          `json:"lastTransitionTime"`
	Reason             string          `json:"reason"`
	Message            string          `json:"message"`
}

type VolumeConditionType string

const (
	VolumeConditionTypeScheduled = "scheduled"
)

const (
	VolumeConditionReasonReplicaSchedulingFailure = "ReplicaSchedulingFailure"
)

type VolumeSpec struct {
	OwnerID             string         `json:"ownerID"`
	Size                int64          `json:"size,string"`
	Frontend            VolumeFrontend `json:"frontend"`
	FromBackup          string         `json:"fromBackup"`
	NumberOfReplicas    int            `json:"numberOfReplicas"`
	StaleReplicaTimeout int            `json:"staleReplicaTimeout"`
	NodeID              string         `json:"nodeID"`
	MigrationNodeID     string         `json:"migrationNodeID"`
	EngineImage         string         `json:"engineImage"`
	RecurringJobs       []RecurringJob `json:"recurringJobs"`
	BaseImage           string         `json:"baseImage"`
}

type VolumeStatus struct {
	State        VolumeState      `json:"state"`
	Robustness   VolumeRobustness `json:"robustness"`
	CurrentImage string           `json:"currentImage"`

	Conditions map[VolumeConditionType]Condition `json:"conditions"`
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
	EngineName  string `json:"engineName"`
	RestoreFrom string `json:"restoreFrom"`
	RestoreName string `json:"restoreName"`
	HealthyAt   string `json:"healthyAt"`
	FailedAt    string `json:"failedAt"`
	DiskID      string `json:"diskID"`
	DataPath    string `json:"dataPath"`
	BaseImage   string `json:"baseImage"`
	Active      bool   `json:"active"`
}

type ReplicaStatus struct {
	InstanceStatus
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
	Name            string              `json:"name"`
	Disks           map[string]DiskSpec `json:"disks"`
	AllowScheduling bool                `json:"allowScheduling"`
}

type NodeState string

const (
	NodeStateUp   = NodeState("up")
	NodeStateDown = NodeState("down")
)

type NodeStatus struct {
	State            NodeState             `json:"state"`
	DiskStatus       map[string]DiskStatus `json:"diskStatus"`
	MountPropagation bool                  `json:"mountPropagation"`
}

type DiskSpec struct {
	Path            string `json:"path"`
	AllowScheduling bool   `json:"allowScheduling"`
	StorageMaximum  int64  `json:"storageMaximum"`
	StorageReserved int64  `json:"storageReserved"`
}

type DiskState string

const (
	DiskStateSchedulable   = DiskState("schedulable")
	DiskStateUnschedulable = DiskState("unschedulable")
)

type DiskStatus struct {
	State            DiskState `json:"state"`
	StorageAvailable int64     `json:"storageAvailable"`
	StorageScheduled int64     `json:"storageScheduled"`
}
