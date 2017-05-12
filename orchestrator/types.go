package orchestrator

import (
	"github.com/yasker/lm-rewrite/types"
)

type Request struct {
	NodeID       string
	InstanceID   string
	InstanceName string

	VolumeName string
	VolumeSize string

	EngineImage string
	ReplicaURLs []string
}

type Orchestrator interface {
	CreateController(request *Request) (*types.ControllerInfo, error)
	CreateReplica(request *Request) (*types.ReplicaInfo, error)

	StartInstance(request *Request) (*types.InstanceInfo, error)
	StopInstance(request *Request) (*types.InstanceInfo, error)
	RemoveInstance(request *Request) error
	InspectInstance(request *Request) (*types.InstanceInfo, error)

	GetCurrentNode() *types.NodeInfo
}
