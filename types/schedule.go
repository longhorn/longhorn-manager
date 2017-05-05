package types

const (
	ScheduleActionCreateController = "create-controller"
	ScheduleActionCreateReplica    = "create-replica"
	ScheduleActionDeleteInstance   = "delete"
	ScheduleActionStartInstance    = "start"
	ScheduleActionStopInstance     = "stop"
)

type SchedulePolicyBinding string

const (
	SchedulePolicyBindingSoftAntiAffinity = "soft.anti-affinity"
)

type Scheduler interface {
	Schedule(item *ScheduleItem, policy *SchedulePolicy) (*InstanceInfo, error)
	Process(spec *ScheduleSpec, item *ScheduleItem) (*InstanceInfo, error)
}

type ScheduleOps interface {
	ListHosts() (map[string]*HostInfo, error)
	GetHost(id string) (*HostInfo, error)
	GetCurrentHostID() string
	ProcessSchedule(item *ScheduleItem) (*InstanceInfo, error)
}

type ScheduleItem struct {
	Action   string
	Instance ScheduleInstance
	Data     ScheduleData
}

type ScheduleInstance struct {
	ID         string
	Type       InstanceType
	HostID     string
	VolumeName string
	Name       string
}

type ScheduleSpec struct {
	HostID string
}

type ScheduleData struct {
	Orchestrator string
	Data         []byte
}

type SchedulePolicy struct {
	Binding   SchedulePolicyBinding
	HostIDMap map[string]struct{}
}
