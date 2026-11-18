package controller

import (
	. "gopkg.in/check.v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (s *NodeControllerSuite) TestSyncInstanceManagersKeepsUnknownWhenSystemManagedSelectorDoesNotMatch(c *C) {
	defaultInstanceManagerName, err := types.GetInstanceManagerName(longhorn.InstanceManagerTypeAllInOne, TestNode1, TestInstanceManagerImage, string(longhorn.DataEngineTypeV1))
	c.Assert(err, IsNil)
	existingIM := DefaultInstanceManagerTestNode1.DeepCopy()
	existingIM.Name = defaultInstanceManagerName
	existingIM.Status.CurrentState = longhorn.InstanceManagerStateUnknown
	existingIM.Status.InstanceEngines = map[string]longhorn.InstanceProcess{}
	existingIM.Status.InstanceEngineFrontends = map[string]longhorn.InstanceProcess{}
	existingIM.Status.InstanceReplicas = map[string]longhorn.InstanceProcess{}

	names := s.syncInstanceManagersNodeSelectorCase(c, "lh-12834-node-selector:selected", map[string]string{"lh-12834-node-selector": "excluded"}, map[string]*longhorn.InstanceManager{defaultInstanceManagerName: existingIM})

	c.Assert(names, DeepEquals, map[string]bool{defaultInstanceManagerName: true})
}
