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
	GetReplicaStates() (map[string]*Replica, error)
	AddReplica(addr string) error
	RemoveReplica(addr string) error
}

type EngineClientRequest struct {
	VolumeName     string
	VolumeSize     string
	ControllerAddr string
	ReplicaAddrs   []string
}

type EngineClientCollection interface {
	NewEngineClient(request *EngineClientRequest) (EngineClient, error)
}
