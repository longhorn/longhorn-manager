package engineapi

import (
	imapi "github.com/longhorn/longhorn-instance-manager/api"

	"github.com/longhorn/longhorn-manager/types"
)

func EngineProcessToInstanceStatus(engineProcess *imapi.Engine) *types.InstanceProcessStatus {
	if engineProcess == nil {
		return nil
	}

	return &types.InstanceProcessStatus{
		Name:      engineProcess.Name,
		Type:      types.InstanceTypeEngine,
		State:     types.InstanceState(engineProcess.ProcessStatus.State),
		ErrorMsg:  engineProcess.ProcessStatus.ErrorMsg,
		PortStart: engineProcess.ProcessStatus.PortStart,
		PortEnd:   engineProcess.ProcessStatus.PortEnd,

		Listen:   engineProcess.Listen,
		Endpoint: engineProcess.Endpoint,
	}
}

func ReplicaProcessToInstanceStatus(replicaProcess *imapi.Process) *types.InstanceProcessStatus {
	if replicaProcess == nil {
		return nil
	}

	return &types.InstanceProcessStatus{
		Name:      replicaProcess.Name,
		Type:      types.InstanceTypeReplica,
		State:     types.InstanceState(replicaProcess.ProcessStatus.State),
		ErrorMsg:  replicaProcess.ProcessStatus.ErrorMsg,
		PortStart: replicaProcess.ProcessStatus.PortStart,
		PortEnd:   replicaProcess.ProcessStatus.PortEnd,

		Listen:   "",
		Endpoint: "",
	}
}
