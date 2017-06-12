package orchsim

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/orchestrator"
	"github.com/rancher/longhorn-manager/types"

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

	NodeCount     = 3
	OrchPortStart = 50000
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	engines *engineapi.EngineSimulatorCollection
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
	rand.Seed(time.Now().UTC().UnixNano())
	s.engines = engineapi.NewEngineSimulatorCollection()
}

func (s *TestSuite) TestBasic(c *C) {
	orch := NewOrchestratorSimulator(types.DefaultOrchestratorPort, s.engines)
	c.Assert(orch.GetCurrentNode(), NotNil)

	s.basicFlowTest(c, []orchestrator.Orchestrator{orch})
}

func getRandomOrch(orchs []orchestrator.Orchestrator) orchestrator.Orchestrator {
	return orchs[rand.Intn(len(orchs))]
}

func (s *TestSuite) basicFlowTest(c *C, orchs []orchestrator.Orchestrator) {
	orch := getRandomOrch(orchs)
	replica1NodeID := orch.GetCurrentNode().ID
	replica1Instance, err := orch.CreateReplica(&orchestrator.Request{
		NodeID:       replica1NodeID,
		InstanceName: Replica1Name,
		VolumeName:   VolumeName,
		VolumeSize:   VolumeSize,
	})
	c.Assert(err, IsNil)
	c.Assert(replica1Instance.Name, Equals, Replica1Name)
	c.Assert(replica1Instance.ID, Not(Equals), "")
	c.Assert(replica1Instance.NodeID, Equals, replica1NodeID)
	c.Assert(replica1Instance.IP, Equals, "")
	c.Assert(replica1Instance.Running, Equals, false)

	orch = getRandomOrch(orchs)
	replica2NodeID := orch.GetCurrentNode().ID
	replica2Instance, err := orch.CreateReplica(&orchestrator.Request{
		NodeID:       replica2NodeID,
		InstanceName: Replica2Name,
		VolumeName:   VolumeName,
		VolumeSize:   VolumeSize,
	})
	c.Assert(err, IsNil)

	orch = getRandomOrch(orchs)
	instance, err := orch.StartInstance(&orchestrator.Request{
		NodeID:       replica1Instance.NodeID,
		InstanceID:   replica1Instance.ID,
		InstanceName: replica1Instance.Name,
		VolumeName:   VolumeName,
	})
	c.Assert(err, IsNil)
	replica1Instance.Running = true
	replica1Instance.IP = instance.IP
	c.Assert(instance.IP, Not(Equals), "")
	c.Assert(instance, DeepEquals, replica1Instance)

	orch = getRandomOrch(orchs)
	instance, err = orch.StartInstance(&orchestrator.Request{
		NodeID:       replica2Instance.NodeID,
		InstanceID:   replica2Instance.ID,
		InstanceName: replica2Instance.Name,
		VolumeName:   VolumeName,
	})
	c.Assert(err, IsNil)
	replica2Instance.Running = true
	replica2Instance.IP = instance.IP
	c.Assert(instance.IP, Not(Equals), "")
	c.Assert(instance, DeepEquals, replica2Instance)

	orch = getRandomOrch(orchs)
	ctrlName := "controller-id-" + VolumeName
	ctrlNodeID := orch.GetCurrentNode().ID
	ctrlInstance, err := orch.CreateController(&orchestrator.Request{
		NodeID:       ctrlNodeID,
		InstanceName: ctrlName,
		VolumeName:   VolumeName,
		VolumeSize:   VolumeSize,
		ReplicaURLs: []string{
			engineapi.GetReplicaDefaultURL(replica1Instance.IP),
			engineapi.GetReplicaDefaultURL(replica2Instance.IP),
		},
	})
	c.Assert(err, IsNil)
	c.Assert(ctrlInstance.Name, Equals, ctrlName)
	c.Assert(ctrlInstance.ID, Not(Equals), "")
	c.Assert(ctrlInstance.NodeID, Equals, ctrlNodeID)
	c.Assert(ctrlInstance.Running, Equals, true)
	c.Assert(ctrlInstance.IP, Not(Equals), "")

	engine, err := s.engines.GetEngineSimulator(VolumeName)
	c.Assert(err, IsNil)
	c.Assert(engine.Name(), Equals, VolumeName)

	replicas, err := engine.ReplicaList()
	c.Assert(err, IsNil)
	c.Assert(replicas, HasLen, 2)
	c.Assert(replicas[engineapi.GetReplicaDefaultURL(replica1Instance.IP)].Mode, Equals, engineapi.ReplicaModeRW)
	c.Assert(replicas[engineapi.GetReplicaDefaultURL(replica2Instance.IP)].Mode, Equals, engineapi.ReplicaModeRW)

	orch = getRandomOrch(orchs)
	instance, err = orch.InspectInstance(&orchestrator.Request{
		NodeID:       ctrlInstance.NodeID,
		InstanceID:   ctrlInstance.ID,
		InstanceName: ctrlInstance.Name,
		VolumeName:   VolumeName,
	})
	c.Assert(err, IsNil)
	c.Assert(instance, DeepEquals, ctrlInstance)

	orch = getRandomOrch(orchs)
	rep1IP := replica1Instance.IP
	instance, err = orch.StopInstance(&orchestrator.Request{
		NodeID:       replica1Instance.NodeID,
		InstanceID:   replica1Instance.ID,
		InstanceName: replica1Instance.Name,
		VolumeName:   VolumeName,
	})
	c.Assert(err, IsNil)
	replica1Instance.Running = false
	replica1Instance.IP = ""
	c.Assert(instance, DeepEquals, replica1Instance)

	replicas, err = engine.ReplicaList()
	c.Assert(err, IsNil)
	c.Assert(replicas, HasLen, 2)
	c.Assert(replicas[engineapi.GetReplicaDefaultURL(rep1IP)].Mode, Equals, engineapi.ReplicaModeERR)
	c.Assert(replicas[engineapi.GetReplicaDefaultURL(replica2Instance.IP)].Mode, Equals, engineapi.ReplicaModeRW)

	orch = getRandomOrch(orchs)
	err = orch.DeleteInstance(&orchestrator.Request{
		NodeID:       replica1Instance.NodeID,
		InstanceID:   replica1Instance.ID,
		InstanceName: replica1Instance.Name,
		VolumeName:   VolumeName,
	})
	c.Assert(err, IsNil)

	replicas, err = engine.ReplicaList()
	c.Assert(err, IsNil)
	c.Assert(replicas, HasLen, 2)
	c.Assert(replicas[engineapi.GetReplicaDefaultURL(rep1IP)].Mode, Equals, engineapi.ReplicaModeERR)
	c.Assert(replicas[engineapi.GetReplicaDefaultURL(replica2Instance.IP)].Mode, Equals, engineapi.ReplicaModeRW)

	orch = getRandomOrch(orchs)
	err = orch.DeleteInstance(&orchestrator.Request{
		NodeID:       ctrlInstance.NodeID,
		InstanceID:   ctrlInstance.ID,
		InstanceName: ctrlInstance.Name,
		VolumeName:   VolumeName,
	})
	c.Assert(err, IsNil)

	engine, err = s.engines.GetEngineSimulator(VolumeName)
	c.Assert(err, NotNil)

	orch = getRandomOrch(orchs)
	instance, err = orch.InspectInstance(&orchestrator.Request{
		NodeID:       ctrlInstance.NodeID,
		InstanceID:   ctrlInstance.ID,
		InstanceName: ctrlInstance.Name,
		VolumeName:   VolumeName,
	})
	c.Assert(err, NotNil)
}

type NodeMap struct {
	nodes map[string]string
}

func (m *NodeMap) Node2OrchestratorAddress(nodeID string) (string, error) {
	addr := m.nodes[nodeID]
	if addr == "" {
		return addr, fmt.Errorf("cannot find node %v", nodeID)
	}
	return addr, nil
}

func (s *TestSuite) TestForwarder(c *C) {
	var err error

	orchs := make([]orchestrator.Orchestrator, NodeCount)
	nodeMap := &NodeMap{
		nodes: map[string]string{},
	}

	for i := 0; i < NodeCount; i++ {
		orch := NewOrchestratorSimulator(OrchPortStart+i, s.engines)

		node := orch.GetCurrentNode()
		nodeAddress := node.IP + ":" + strconv.Itoa(node.OrchestratorPort)
		nodeMap.nodes[node.ID] = nodeAddress

		forwarder := orchestrator.NewForwarder(orch)
		forwarder.SetLocator(nodeMap)

		err = forwarder.StartServer(nodeAddress)
		c.Assert(err, IsNil)
		orchs[i] = forwarder
	}

	s.basicFlowTest(c, orchs)
}
