package manager

import (
	"os"
	"testing"
	"time"

	"github.com/yasker/lm-rewrite/engineapi"
	"github.com/yasker/lm-rewrite/kvstore"
	"github.com/yasker/lm-rewrite/orchestrator/orchsim"
	"github.com/yasker/lm-rewrite/types"
	"github.com/yasker/lm-rewrite/util"

	. "gopkg.in/check.v1"
)

const (
	TestPrefix     = "longhorn-manager-test"
	EnvEtcdServer  = "LONGHORN_MANAGER_TEST_ETCD_SERVER"
	EnvEngineImage = "LONGHORN_ENGINE_IMAGE"

	VolumeName                = "vol"
	VolumeSize                = 10 * 1024 * 1024 * 1024
	VolumeNumberOfReplicas    = 3
	VolumeStaleReplicaTimeout = 3600

	RetryCounts   = 20
	RetryInterval = 100 * time.Millisecond
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	etcd    *kvstore.KVStore
	engines *engineapi.EngineSimulatorCollection
	orch    *orchsim.OrchSim
	rpc     *MockRPCManager
	manager *VolumeManager

	engineImage string
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
	var err error

	etcdIP := os.Getenv(EnvEtcdServer)
	c.Assert(etcdIP, Not(Equals), "")

	s.engineImage = os.Getenv(EnvEngineImage)
	c.Assert(s.engineImage, Not(Equals), "")

	etcdBackend, err := kvstore.NewETCDBackend([]string{"http://" + etcdIP + ":2379"})
	c.Assert(err, IsNil)

	etcd, err := kvstore.NewKVStore("/longhorn_manager_test", etcdBackend)
	c.Assert(err, IsNil)
	s.etcd = etcd

	err = s.etcd.Nuclear("nuke key value store")
	c.Assert(err, IsNil)

	s.engines = engineapi.NewEngineSimulatorCollection()
	orch, err := orchsim.NewOrchestratorSimulator(s.engines)
	s.orch = orch.(*orchsim.OrchSim)
	c.Assert(err, IsNil)
	s.rpc = NewMockRPCManager().(*MockRPCManager)

	currentNode := s.orch.GetCurrentNode()
	c.Assert(currentNode, NotNil)

	s.manager, err = NewVolumeManager(s.etcd, s.orch, s.engines, s.rpc)
	c.Assert(err, IsNil)
}

func (s *TestSuite) TeardownTest(c *C) {
	if s.etcd != nil {
		err := s.etcd.Nuclear("nuke key value store")
		c.Assert(err, IsNil)
	}
}

func (s *TestSuite) TestVolume(c *C) {
	node, err := s.manager.GetRandomNode()
	c.Assert(err, IsNil)
	err = s.manager.NewVolume(&types.VolumeInfo{
		Name:                VolumeName,
		Size:                VolumeSize,
		NumberOfReplicas:    VolumeNumberOfReplicas,
		StaleReplicaTimeout: VolumeStaleReplicaTimeout,

		Created:      util.Now(),
		TargetNodeID: node.ID,
		State:        types.VolumeStateCreated,
		DesireState:  types.VolumeStateDetached,
	})
	c.Assert(err, IsNil)

	volume, err := s.manager.GetVolume(VolumeName)
	c.Assert(err, IsNil)
	c.Assert(volume.Name, Equals, VolumeName)
	c.Assert(volume.Controller, IsNil)
	c.Assert(volume.Replicas, NotNil)
	c.Assert(volume.countReplicas(), Equals, 0)

	err = volume.create()
	c.Assert(err, IsNil)
	c.Assert(volume.Controller, IsNil)
	for i := 0; i < RetryCounts; i++ {
		if volume.countReplicas() != VolumeNumberOfReplicas {
			time.Sleep(RetryInterval)
		}
	}
	c.Assert(volume.countReplicas(), Equals, VolumeNumberOfReplicas)
	s.checkVolumeConsistency(c, volume)

	err = volume.start()
	c.Assert(err, IsNil)
	c.Assert(volume.Controller, NotNil)
	c.Assert(volume.Controller.Running, Equals, true)
	for i := 0; i < RetryCounts; i++ {
		if volume.countReplicas() != VolumeNumberOfReplicas {
			time.Sleep(RetryInterval)
		}
	}
	c.Assert(volume.countReplicas(), Equals, VolumeNumberOfReplicas)
	s.checkVolumeConsistency(c, volume)

	err = volume.stop()
	c.Assert(err, IsNil)
	c.Assert(volume.Controller, IsNil)
	for i := 0; i < RetryCounts; i++ {
		if volume.countReplicas() != VolumeNumberOfReplicas {
			time.Sleep(RetryInterval)
		}
	}
	c.Assert(volume.countReplicas(), Equals, VolumeNumberOfReplicas)
	s.checkVolumeConsistency(c, volume)

	err = volume.stop()
	c.Assert(err, IsNil)
	c.Assert(volume.Controller, IsNil)
	for i := 0; i < RetryCounts; i++ {
		if volume.countReplicas() != VolumeNumberOfReplicas {
			time.Sleep(RetryInterval)
		}
	}
	c.Assert(volume.countReplicas(), Equals, VolumeNumberOfReplicas)
	s.checkVolumeConsistency(c, volume)

	err = volume.destroy()
	c.Assert(err, IsNil)
	c.Assert(volume.Controller, IsNil)
	c.Assert(volume.countReplicas(), Equals, 0)
	s.checkVolumeConsistency(c, volume)
}

func (s *TestSuite) checkVolumeConsistency(c *C, volume *Volume) {
	newVol, err := s.manager.GetVolume(volume.Name)
	c.Assert(err, IsNil)
	kvstore.UpdateKVIndex(newVol.VolumeInfo, volume.VolumeInfo)
	c.Assert(newVol.VolumeInfo, DeepEquals, volume.VolumeInfo)

	if volume.Controller != nil {
		kvstore.UpdateKVIndex(newVol.Controller, volume.Controller)
		c.Assert(newVol.VolumeInfo, DeepEquals, volume.VolumeInfo)
	}
	if volume.countReplicas() != 0 {
		for name := range volume.Replicas {
			replica := volume.getReplica(name)
			c.Assert(newVol.Replicas[name], NotNil)
			kvstore.UpdateKVIndex(newVol.Replicas[name], replica)
			c.Assert(newVol.Replicas[name], DeepEquals, replica)
		}
	}
}
