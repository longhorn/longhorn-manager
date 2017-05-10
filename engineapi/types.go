package engineapi

type ReplicaMode string

const (
	ReplicaModeRW  = ReplicaMode("RW")
	ReplicaModeWO  = ReplicaMode("WO")
	ReplicaModeERR = ReplicaMode("ERR")
)

type Replica struct {
	Address string
	Mode    ReplicaMode
}

type Controller struct {
	Address string
	HostID  string
}

type EngineClient interface {
	Name() string
	Endpoint() string
	GetReplicaStates() ([]*Replica, error)
	AddReplica(addr string) error
	RemoveReplica(addr string) error
}
