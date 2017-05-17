package orchsim

import (
	"testing"

	"github.com/yasker/lm-rewrite/engineapi"
	"github.com/yasker/lm-rewrite/orchestrator"

	. "gopkg.in/check.v1"
)

var (
	VolumeName           = "vol"
	VolumeSize     int64 = 10 * 1024 * 1024 * 1024
	ControllerName       = VolumeName + "-controller"
	Replica1Name         = VolumeName + "-replica1"
	Replica2Name         = VolumeName + "-replica2"
	Replica3Name         = VolumeName + "-replica3"
	Replica4Name         = VolumeName + "-replica4"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestBasic(c *C) {
	engines := engineapi.NewEngineSimulatorCollection()
	orch, err := NewOrchestratorSimulator(engines)
	c.Assert(err, IsNil)
	c.Assert(orch.GetCurrentNode(), NotNil)

	CurrentNodeID := orch.GetCurrentNode().ID

	replica1Instance, err := orch.CreateReplica(&orchestrator.Request{
		NodeID:       CurrentNodeID,
		InstanceName: Replica1Name,
		VolumeName:   VolumeName,
		VolumeSize:   VolumeSize,
	})
	c.Assert(err, IsNil)
	c.Assert(replica1Instance.Name, Equals, Replica1Name)
	c.Assert(replica1Instance.ID, Not(Equals), "")
	c.Assert(replica1Instance.Address, Equals, "")
	c.Assert(replica1Instance.Running, Equals, false)

	replica2Instance, err := orch.CreateReplica(&orchestrator.Request{
		NodeID:       CurrentNodeID,
		InstanceName: Replica2Name,
		VolumeName:   VolumeName,
		VolumeSize:   VolumeSize,
	})
	c.Assert(err, IsNil)

	instance, err := orch.StartInstance(&orchestrator.Request{
		NodeID:       CurrentNodeID,
		InstanceName: replica1Instance.Name,
		VolumeName:   VolumeName,
	})
	c.Assert(err, IsNil)
	replica1Instance.Running = true
	replica1Instance.Address = instance.Address
	c.Assert(instance.Address, Not(Equals), "")
	c.Assert(instance, DeepEquals, replica1Instance)

	instance, err = orch.StartInstance(&orchestrator.Request{
		NodeID:       CurrentNodeID,
		InstanceName: replica2Instance.Name,
		VolumeName:   VolumeName,
	})
	c.Assert(err, IsNil)
	replica2Instance.Running = true
	replica2Instance.Address = instance.Address
	c.Assert(instance.Address, Not(Equals), "")
	c.Assert(instance, DeepEquals, replica2Instance)

	ctrlName := "controller-id-" + VolumeName
	ctrlInstance, err := orch.CreateController(&orchestrator.Request{
		NodeID:       CurrentNodeID,
		InstanceName: ctrlName,
		VolumeName:   VolumeName,
		VolumeSize:   VolumeSize,
		ReplicaURLs: []string{
			replica1Instance.Address,
			replica2Instance.Address,
		},
	})
	c.Assert(err, IsNil)
	c.Assert(ctrlInstance.Name, Equals, ctrlName)
	c.Assert(ctrlInstance.ID, Not(Equals), "")
	c.Assert(ctrlInstance.Running, Equals, true)
	c.Assert(ctrlInstance.Address, Not(Equals), "")

	engine, err := engines.GetEngineSimulator(VolumeName)
	c.Assert(err, IsNil)
	c.Assert(engine.Name(), Equals, VolumeName)

	replicas, err := engine.GetReplicaStates()
	c.Assert(err, IsNil)
	c.Assert(replicas, HasLen, 2)
	c.Assert(replicas[replica1Instance.Address].Mode, Equals, engineapi.ReplicaModeRW)
	c.Assert(replicas[replica2Instance.Address].Mode, Equals, engineapi.ReplicaModeRW)

	instance, err = orch.InspectInstance(&orchestrator.Request{
		NodeID:       CurrentNodeID,
		InstanceName: ctrlInstance.Name,
		VolumeName:   VolumeName,
	})
	c.Assert(err, IsNil)
	c.Assert(instance, DeepEquals, ctrlInstance)

	rep1IP := replica1Instance.Address
	instance, err = orch.StopInstance(&orchestrator.Request{
		NodeID:       CurrentNodeID,
		InstanceName: replica1Instance.Name,
		VolumeName:   VolumeName,
	})
	c.Assert(err, IsNil)
	replica1Instance.Running = false
	replica1Instance.Address = ""
	c.Assert(instance, DeepEquals, replica1Instance)

	replicas, err = engine.GetReplicaStates()
	c.Assert(err, IsNil)
	c.Assert(replicas, HasLen, 2)
	c.Assert(replicas[rep1IP].Mode, Equals, engineapi.ReplicaModeERR)
	c.Assert(replicas[replica2Instance.Address].Mode, Equals, engineapi.ReplicaModeRW)

	err = orch.DeleteInstance(&orchestrator.Request{
		NodeID:       CurrentNodeID,
		InstanceName: replica1Instance.Name,
		VolumeName:   VolumeName,
	})
	c.Assert(err, IsNil)

	replicas, err = engine.GetReplicaStates()
	c.Assert(err, IsNil)
	c.Assert(replicas, HasLen, 2)
	c.Assert(replicas[rep1IP].Mode, Equals, engineapi.ReplicaModeERR)
	c.Assert(replicas[replica2Instance.Address].Mode, Equals, engineapi.ReplicaModeRW)

	err = orch.DeleteInstance(&orchestrator.Request{
		NodeID:       CurrentNodeID,
		InstanceName: ctrlInstance.Name,
		VolumeName:   VolumeName,
	})
	c.Assert(err, IsNil)

	engine, err = engines.GetEngineSimulator(VolumeName)
	c.Assert(err, NotNil)

	instance, err = orch.InspectInstance(&orchestrator.Request{
		NodeID:       CurrentNodeID,
		InstanceName: ctrlInstance.Name,
		VolumeName:   VolumeName,
	})
	c.Assert(err, ErrorMatches, "unable to find instance.*")
}
