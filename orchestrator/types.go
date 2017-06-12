package orchestrator

import (
	"github.com/rancher/longhorn-manager/types"
)

type Request struct {
	NodeID       string
	InstanceID   string
	InstanceName string

	VolumeName string
	VolumeSize int64

	ReplicaURLs []string
	RestoreFrom string
	RestoreName string
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
	NodeID  string
	Running bool
	IP      string
}

type InstanceOperationType string

const (
	InstanceOperationTypeCreateController = InstanceOperationType("createcontroller")
	InstanceOperationTypeCreateReplica    = InstanceOperationType("createreplica")
	InstanceOperationTypeStartInstance    = InstanceOperationType("start")
	InstanceOperationTypeStopInstance     = InstanceOperationType("stop")
	InstanceOperationTypeDeleteInstance   = InstanceOperationType("delete")
	InstanceOperationTypeInspectInstance  = InstanceOperationType("inspect")
)

type NodeLocator interface {
	Node2OrchestratorAddress(nodeID string) (string, error)
}
