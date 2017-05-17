package orchestrator

import (
	"github.com/yasker/lm-rewrite/types"
)

type Request struct {
	NodeID       string
	InstanceID   string
	InstanceName string

	VolumeName string
	VolumeSize int64

	ReplicaURLs []string
}

type Orchestrator interface {
	CreateController(request *Request) (*Instance, error)
	CreateReplica(request *Request) (*Instance, error)

	StartInstance(request *Request) (*Instance, error)
	StopInstance(request *Request) (*Instance, error)
	DeleteInstance(request *Request) error
	InspectInstance(request *Request) (*Instance, error)

	GetCurrentNode() *types.NodeInfo
}

type Instance struct {
	ID      string
	Name    string
	Running bool
	Address string
}
