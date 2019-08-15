package engineapi

import (
	"fmt"

	"github.com/sirupsen/logrus"

	imapi "github.com/longhorn/longhorn-instance-manager/api"
	imclient "github.com/longhorn/longhorn-instance-manager/client"
	imutil "github.com/longhorn/longhorn-instance-manager/util"

	"github.com/longhorn/longhorn-manager/types"
)

func EngineProcessToInstanceProcess(engineProcess *imapi.Engine) *types.InstanceProcess {
	if engineProcess == nil {
		return nil
	}

	return &types.InstanceProcess{
		Spec: types.InstanceProcessSpec{
			Name: engineProcess.Name,
		},
		Status: types.InstanceProcessStatus{
			Type:      types.InstanceTypeEngine,
			State:     types.InstanceState(engineProcess.ProcessStatus.State),
			ErrorMsg:  engineProcess.ProcessStatus.ErrorMsg,
			PortStart: engineProcess.ProcessStatus.PortStart,
			PortEnd:   engineProcess.ProcessStatus.PortEnd,

			Listen:   engineProcess.Listen,
			Endpoint: engineProcess.Endpoint,
		},
	}

}

func ReplicaProcessToInstanceProcess(replicaProcess *imapi.Process) *types.InstanceProcess {
	if replicaProcess == nil {
		return nil
	}

	return &types.InstanceProcess{
		Spec: types.InstanceProcessSpec{
			Name: replicaProcess.Name,
		},
		Status: types.InstanceProcessStatus{
			Type:      types.InstanceTypeReplica,
			State:     types.InstanceState(replicaProcess.ProcessStatus.State),
			ErrorMsg:  replicaProcess.ProcessStatus.ErrorMsg,
			PortStart: replicaProcess.ProcessStatus.PortStart,
			PortEnd:   replicaProcess.ProcessStatus.PortEnd,

			Listen:   "",
			Endpoint: "",
		},
	}
}

func Endpoint(ip, engineName string) (string, error) {
	c := imclient.NewEngineManagerClient(imutil.GetURL(ip, InstanceManagerDefaultPort))
	engineProcess, err := c.EngineGet(engineName)
	if err != nil {
		logrus.Warn("Fail to get engine process info: ", err)
		return "", err
	}

	switch engineProcess.Frontend {
	case string(FrontendISCSI):
		// it will looks like this in the end
		// iscsi://10.42.0.12:3260/iqn.2014-09.com.rancher:vol-name/1
		return "iscsi://" + ip + ":" + DefaultISCSIPort + "/" + engineProcess.Endpoint + "/" + DefaultISCSILUN, nil
	case string(FrontendBlockDev):
		return engineProcess.Endpoint, nil
	case "":
		return "", nil
	}
	return "", fmt.Errorf("Unknown frontend %v", engineProcess.Frontend)
}
