package docker

import (
	"flag"
	"os"
	"testing"

	"github.com/rancher/longhorn-manager/orchestrator"
	"github.com/rancher/longhorn-manager/types"

	. "gopkg.in/check.v1"
)

const (
	TestPrefix = "longhorn-manager-test"

	EnvCompTest    = "LONGHORN_MANAGER_TEST_COMP"
	EnvEngineImage = "LONGHORN_ENGINE_IMAGE"
)

var (
	quick = flag.Bool("quick", false, "Skip tests require other services")

	VolumeName     = TestPrefix + "-vol"
	ControllerName = VolumeName + "-controller"
	Replica1Name   = VolumeName + "-replica1"
	Replica2Name   = VolumeName + "-replica2"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	d           orchestrator.Orchestrator
	engineImage string

	// Index by instance.ID
	instanceBin map[string]*orchestrator.Instance
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpSuite(c *C) {
	compTest := os.Getenv(EnvCompTest)
	if compTest != "true" {
		c.Skip("-quick specified")
	}
}

func (s *TestSuite) SetUpTest(c *C) {
	var err error

	s.instanceBin = make(map[string]*orchestrator.Instance)

	s.engineImage = os.Getenv(EnvEngineImage)
	c.Assert(s.engineImage, Not(Equals), "")

	cfg := &Config{
		EngineImage: s.engineImage,
	}
	s.d, err = NewDockerOrchestrator(cfg)
	c.Assert(err, IsNil)
}

func (s *TestSuite) Cleanup() {
	for _, instance := range s.instanceBin {
		s.d.StopInstance(&orchestrator.Request{
			NodeID:       instance.NodeID,
			InstanceID:   instance.ID,
			InstanceName: instance.Name,
		})
		s.d.DeleteInstance(&orchestrator.Request{
			NodeID:       instance.NodeID,
			InstanceID:   instance.ID,
			InstanceName: instance.Name,
		})
	}
}

func (s *TestSuite) TestCreateVolume(c *C) {
	var instance *orchestrator.Instance

	defer s.Cleanup()

	volume := &types.VolumeInfo{
		Size: 8 * 1024 * 1024, // 8M
		Metadata: types.Metadata{
			Name: VolumeName,
		},
	}
	replica1Req := &orchestrator.Request{
		NodeID:       s.d.GetCurrentNode().ID,
		VolumeName:   volume.Name,
		VolumeSize:   volume.Size,
		InstanceName: Replica1Name,
	}
	replica1, err := s.d.CreateReplica(replica1Req)
	c.Assert(err, IsNil)
	c.Assert(replica1.ID, NotNil)
	s.instanceBin[replica1.ID] = replica1

	c.Assert(replica1.NodeID, Equals, s.d.GetCurrentNode().ID)
	c.Assert(replica1.Running, Equals, false)
	c.Assert(replica1.Name, Equals, replica1Req.InstanceName)

	replica1Req.InstanceID = replica1.ID
	instance, err = s.d.StartInstance(replica1Req)
	c.Assert(err, IsNil)
	c.Assert(instance.ID, Equals, replica1.ID)
	c.Assert(instance.Name, Equals, replica1.Name)
	c.Assert(instance.Running, Equals, true)
	c.Assert(instance.IP, Not(Equals), "")

	instance, err = s.d.StopInstance(replica1Req)
	c.Assert(err, IsNil)
	c.Assert(instance.ID, Equals, replica1.ID)
	c.Assert(instance.Name, Equals, replica1.Name)
	c.Assert(instance.Running, Equals, false)
	c.Assert(instance.IP, Equals, "")

	instance, err = s.d.StartInstance(replica1Req)
	c.Assert(err, IsNil)
	c.Assert(instance.ID, Equals, replica1.ID)
	c.Assert(instance.Name, Equals, replica1.Name)
	c.Assert(instance.Running, Equals, true)
	c.Assert(instance.IP, Not(Equals), "")

	replica1 = instance

	replica2Req := &orchestrator.Request{
		NodeID:       s.d.GetCurrentNode().ID,
		VolumeName:   volume.Name,
		VolumeSize:   volume.Size,
		InstanceName: Replica2Name,
	}
	replica2, err := s.d.CreateReplica(replica2Req)
	c.Assert(err, IsNil)
	c.Assert(replica2.ID, NotNil)
	s.instanceBin[replica2.ID] = replica2

	replica2Req.InstanceID = replica2.ID
	instance, err = s.d.StartInstance(replica2Req)
	c.Assert(err, IsNil)
	c.Assert(instance.ID, Equals, replica2.ID)
	c.Assert(instance.Name, Equals, replica2.Name)
	c.Assert(instance.Running, Equals, true)
	c.Assert(instance.IP, Not(Equals), "")

	replica2 = instance

	controllerReq := &orchestrator.Request{
		NodeID:       s.d.GetCurrentNode().ID,
		InstanceName: ControllerName,
		VolumeName:   volume.Name,
		VolumeSize:   volume.Size,
		ReplicaURLs: []string{
			"tcp://" + replica1.IP + ":9502",
			"tcp://" + replica2.IP + ":9502",
		},
	}
	controller, err := s.d.CreateController(controllerReq)
	c.Assert(err, IsNil)
	c.Assert(controller.ID, NotNil)
	s.instanceBin[controller.ID] = controller

	c.Assert(controller.NodeID, Equals, s.d.GetCurrentNode().ID)
	c.Assert(controller.Running, Equals, true)
	c.Assert(controller.Name, Equals, ControllerName)

	controllerReq.InstanceID = controller.ID
	instance, err = s.d.StopInstance(controllerReq)
	c.Assert(err, IsNil)
	c.Assert(instance.ID, Equals, controller.ID)
	c.Assert(instance.Name, Equals, controller.Name)
	c.Assert(instance.Running, Equals, false)

	err = s.d.DeleteInstance(controllerReq)
	c.Assert(err, IsNil)
	delete(s.instanceBin, controller.ID)

	instance, err = s.d.StopInstance(replica2Req)
	c.Assert(err, IsNil)
	c.Assert(instance.ID, Equals, replica2.ID)
	c.Assert(instance.Name, Equals, replica2.Name)
	c.Assert(instance.Running, Equals, false)

	err = s.d.DeleteInstance(replica2Req)
	c.Assert(err, IsNil)
	delete(s.instanceBin, replica2.ID)

	instance, err = s.d.StopInstance(replica1Req)
	c.Assert(err, IsNil)
	c.Assert(instance.ID, Equals, replica1.ID)
	c.Assert(instance.Name, Equals, replica1.Name)
	c.Assert(instance.Running, Equals, false)

	err = s.d.DeleteInstance(replica1Req)
	c.Assert(err, IsNil)
	delete(s.instanceBin, replica1.ID)
}
