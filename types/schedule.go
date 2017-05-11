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
	ListNodes() (map[string]*NodeInfo, error)
	GetNode(id string) (*NodeInfo, error)
	GetCurrentNodeID() string
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
	NodeID     string
	VolumeName string
	Name       string
}

type ScheduleSpec struct {
	NodeID string
}

type ScheduleData struct {
	Orchestrator string
	Data         []byte
}

type SchedulePolicy struct {
	Binding   SchedulePolicyBinding
	NodeIDMap map[string]struct{}
}
