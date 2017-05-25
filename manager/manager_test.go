package manager

import (
	"os"
	"testing"
	"time"

	"github.com/yasker/lm-rewrite/engineapi"
	"github.com/yasker/lm-rewrite/kvstore"
	"github.com/yasker/lm-rewrite/orchestrator"
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
	VolumeSizeString          = "10g"
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
	rpcdb   *MockRPCDB

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
	orch, err := orchsim.NewOrchestratorSimulator(types.DefaultOrchestratorPort, s.engines)
	s.orch = orch.(*orchsim.OrchSim)
	c.Assert(err, IsNil)

	s.rpcdb = NewMockRPCDB()
	s.rpc = NewMockRPCManager(s.rpcdb).(*MockRPCManager)

	currentNode := s.orch.GetCurrentNode()
	c.Assert(currentNode, NotNil)

	s.manager, err = NewVolumeManager(s.etcd, s.orch, s.engines, s.rpc, types.DefaultManagerPort)
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

func (s *TestSuite) TestVolumeReconcile(c *C) {
	infos, err := s.manager.VolumeList()
	c.Assert(err, IsNil)
	c.Assert(len(infos), Equals, 0)

	err = s.manager.VolumeCreate(&VolumeCreateRequest{
		Name:                VolumeName,
		Size:                VolumeSizeString,
		NumberOfReplicas:    VolumeNumberOfReplicas,
		StaleReplicaTimeout: VolumeStaleReplicaTimeout,
	})
	c.Assert(err, IsNil)

	node := s.manager.GetCurrentNode()

	ReconcileInterval = 1 * time.Second

	volume, err := s.manager.GetVolume(VolumeName)
	c.Assert(err, IsNil)
	c.Assert(volume.Name, Equals, VolumeName)
	c.Assert(volume.Controller, IsNil)
	c.Assert(volume.TargetNodeID, Equals, node.ID)
	c.Assert(volume.DesireState, Equals, types.VolumeStateDetached)

	volume = s.waitForVolumeState(c, VolumeName, types.VolumeStateDetached)
	c.Assert(volume.countReplicas(), Equals, VolumeNumberOfReplicas)
	for _, replica := range volume.Replicas {
		c.Assert(replica.Name, Not(Equals), "")
		c.Assert(replica.IP, Equals, "")
		c.Assert(replica.Running, Equals, false)
		c.Assert(replica.VolumeName, Equals, VolumeName)
	}
	c.Assert(volume.NodeID, Equals, node.ID)

	err = s.manager.VolumeAttach(&VolumeAttachRequest{
		Name:   VolumeName,
		NodeID: node.ID,
	})
	c.Assert(err, IsNil)
	volume, err = s.manager.GetVolume(VolumeName)
	c.Assert(volume.TargetNodeID, Equals, node.ID)
	c.Assert(volume.DesireState, Equals, types.VolumeStateHealthy)

	volume = s.waitForVolumeState(c, VolumeName, types.VolumeStateHealthy)
	c.Assert(volume.Controller, NotNil)

	err = s.manager.VolumeDetach(&VolumeDetachRequest{
		Name: VolumeName,
	})
	c.Assert(err, IsNil)
	volume, err = s.manager.GetVolume(VolumeName)
	c.Assert(volume.TargetNodeID, Equals, node.ID)
	c.Assert(volume.DesireState, Equals, types.VolumeStateDetached)

	volume = s.waitForVolumeState(c, VolumeName, types.VolumeStateDetached)
	c.Assert(volume.Controller, IsNil)

	err = s.manager.VolumeDelete(&VolumeDeleteRequest{
		Name: VolumeName,
	})
	c.Assert(err, IsNil)
	volume, err = s.manager.GetVolume(VolumeName)
	c.Assert(volume.TargetNodeID, Equals, node.ID)
	c.Assert(volume.DesireState, Equals, types.VolumeStateDeleted)

	infos = map[string]*types.VolumeInfo{}
	for i := 0; i < RetryCounts; i++ {
		infos, err = s.manager.VolumeList()
		c.Assert(err, IsNil)
		if infos[VolumeName] == nil {
			break
		}
		time.Sleep(RetryInterval)
	}
	c.Assert(infos[VolumeName], IsNil)
}

func (s *TestSuite) waitForVolumeState(c *C, volumeName string, state types.VolumeState) *Volume {
	var (
		volume *Volume
		err    error
	)
	for i := 0; i < RetryCounts; i++ {
		volume, err = s.manager.GetVolume(VolumeName)
		c.Assert(err, IsNil)
		if volume.State == state {
			break
		}
		time.Sleep(RetryInterval)
	}
	c.Assert(volume.State, Equals, state)
	return volume
}

func (s *TestSuite) TestVolumeHeal(c *C) {
	err := s.manager.VolumeCreate(&VolumeCreateRequest{
		Name:                VolumeName,
		Size:                VolumeSizeString,
		NumberOfReplicas:    VolumeNumberOfReplicas,
		StaleReplicaTimeout: VolumeStaleReplicaTimeout,
	})
	c.Assert(err, IsNil)

	node := s.manager.GetCurrentNode()

	ReconcileInterval = 1 * time.Second

	volume, err := s.manager.GetVolume(VolumeName)
	c.Assert(err, IsNil)
	c.Assert(volume.Name, Equals, VolumeName)
	c.Assert(volume.Controller, IsNil)
	c.Assert(volume.TargetNodeID, Equals, node.ID)
	c.Assert(volume.DesireState, Equals, types.VolumeStateDetached)

	volume = s.waitForVolumeState(c, VolumeName, types.VolumeStateDetached)
	c.Assert(volume.NodeID, Equals, node.ID)
	c.Assert(volume.countReplicas(), Equals, VolumeNumberOfReplicas)

	err = s.manager.VolumeAttach(&VolumeAttachRequest{
		Name:   VolumeName,
		NodeID: node.ID,
	})
	c.Assert(err, IsNil)
	volume, err = s.manager.GetVolume(VolumeName)
	c.Assert(volume.TargetNodeID, Equals, node.ID)
	c.Assert(volume.DesireState, Equals, types.VolumeStateHealthy)
	volume = s.waitForVolumeState(c, VolumeName, types.VolumeStateHealthy)
	c.Assert(volume.Controller, NotNil)

	//stop one random replica
	for _, replica := range volume.Replicas {
		_, err := s.orch.StopInstance(&orchestrator.Request{
			NodeID:       node.ID,
			InstanceID:   replica.ID,
			InstanceName: replica.Name,
			VolumeName:   VolumeName,
		})
		c.Assert(err, IsNil)
		break
	}
	for i := 0; i < RetryCounts; i++ {
		volume, err = s.manager.GetVolume(VolumeName)
		c.Assert(err, IsNil)
		if len(volume.Replicas) == VolumeNumberOfReplicas+1 {
			break
		}
		time.Sleep(RetryInterval)
	}
	c.Assert(volume.Replicas, HasLen, VolumeNumberOfReplicas+1)
	c.Assert(volume.badReplicas, HasLen, 1)
	volume = s.waitForVolumeState(c, VolumeName, types.VolumeStateHealthy)
	c.Assert(volume.Controller, NotNil)

	err = s.manager.VolumeDelete(&VolumeDeleteRequest{
		Name: VolumeName,
	})
	c.Assert(err, IsNil)
	volume, err = s.manager.GetVolume(VolumeName)
	c.Assert(volume.TargetNodeID, Equals, node.ID)
	c.Assert(volume.DesireState, Equals, types.VolumeStateDeleted)

	infos := map[string]*types.VolumeInfo{}
	for i := 0; i < RetryCounts; i++ {
		infos, err = s.manager.VolumeList()
		c.Assert(err, IsNil)
		if infos[VolumeName] == nil {
			break
		}
		time.Sleep(RetryInterval)
	}
	c.Assert(infos[VolumeName], IsNil)
}
