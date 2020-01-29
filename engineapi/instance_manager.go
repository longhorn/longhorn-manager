package engineapi

import (
	"fmt"

	imapi "github.com/longhorn/longhorn-engine/pkg/instance-manager/api"
	imclient "github.com/longhorn/longhorn-engine/pkg/instance-manager/client"
	imutil "github.com/longhorn/longhorn-engine/pkg/instance-manager/util"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

func ProcessToInstanceProcess(p *imapi.Process) *types.InstanceProcess {
	if p == nil {
		return nil
	}

	return &types.InstanceProcess{
		Spec: types.InstanceProcessSpec{
			Name: p.Name,
		},
		Status: types.InstanceProcessStatus{
			State:     types.InstanceState(p.ProcessStatus.State),
			ErrorMsg:  p.ProcessStatus.ErrorMsg,
			PortStart: p.ProcessStatus.PortStart,
			PortEnd:   p.ProcessStatus.PortEnd,

			// These fields are not used, maybe we can deprecate them later.
			Type:     "",
			Listen:   "",
			Endpoint: "",
		},
	}

}

func GetProcessManagerClient(im *longhorn.InstanceManager) (*imclient.ProcessManagerClient, error) {
	if im.Status.CurrentState != types.InstanceManagerStateRunning || im.Status.IP == "" {
		return nil, fmt.Errorf("invalid Instance Manager %v", im.Name)
	}

	return imclient.NewProcessManagerClient(imutil.GetURL(im.Status.IP, InstanceManagerDefaultPort)), nil
}
