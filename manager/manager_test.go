package manager

import (
	"os"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/kvstore"
	"github.com/rancher/longhorn-manager/orchestrator"
	"github.com/rancher/longhorn-manager/orchestrator/orchsim"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

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

	NodeCounts       = 3
	OrchestratorPort = 5000
	ManagerPort      = 4000
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	etcd       *kvstore.KVStore
	engines    *engineapi.EngineSimulatorCollection
	orchsims   []*orchsim.OrchSim
	forwarder  *orchestrator.Forwarder
	forwarders []*orchestrator.Forwarder
	rpc        *MockRPCManager
	rpcdb      *MockRPCDB
	manager    *VolumeManager
	managers   []*VolumeManager

	engineImage string
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpSuite(c *C) {
	logrus.SetLevel(logrus.DebugLevel)
	ReconcileInterval = 1 * time.Second
}

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
	s.orchsims = make([]*orchsim.OrchSim, NodeCounts)
	for i := 0; i < NodeCounts; i++ {
		s.orchsims[i] = orchsim.NewOrchestratorSimulator(OrchestratorPort+i, s.engines)
	}

	s.rpcdb = NewMockRPCDB()
	s.rpc = NewMockRPCManager(s.rpcdb)

	s.managers = make([]*VolumeManager, NodeCounts)
	s.forwarders = make([]*orchestrator.Forwarder, NodeCounts)
	for i := 0; i < NodeCounts; i++ {
		s.forwarders[i] = orchestrator.NewForwarder(s.orchsims[i])
		s.managers[i], err = NewVolumeManager(s.etcd, s.forwarders[i], s.engines, s.rpc, ManagerPort+i)
		c.Assert(err, IsNil)
		s.forwarders[i].SetLocator(s.managers[i])
		s.forwarders[i].StartServer(s.managers[i].GetCurrentNode().GetOrchestratorAddress())
		c.Assert(s.forwarders[i].GetCurrentNode().ID, Equals, s.managers[i].GetCurrentNode().ID)
	}
	s.forwarder = s.forwarders[0]
	s.manager = s.managers[0]
}

func (s *TestSuite) TearDownTest(c *C) {
	for i := 0; i < NodeCounts; i++ {
		s.forwarders[i].StopServer()
		s.managers[i].Shutdown()
	}
	if s.etcd != nil {
		err := s.etcd.Nuclear("nuke key value store")
		c.Assert(err, IsNil)
	}
}

func (s *TestSuite) TestVolume(c *C) {
	nodes, err := s.manager.ListNodes()
	c.Assert(err, IsNil)
	c.Assert(len(nodes), Equals, NodeCounts)

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

	volume, err := s.manager.getManagedVolume(VolumeName, true)
	c.Assert(err, IsNil)

	volume.mutex.Lock()
	defer volume.mutex.Unlock()

	c.Assert(volume.Name, Equals, VolumeName)
	c.Assert(volume.Controller, IsNil)
	c.Assert(volume.Replicas, NotNil)
	c.Assert(volume.countReplicas(), Equals, 0)

	err = volume.create()
	c.Assert(err, IsNil)

	c.Assert(volume.Controller, IsNil)

	for i := 0; i < RetryCounts; i++ {
		if volume.countReplicas() != VolumeNumberOfReplicas {
			volume.mutex.Unlock()
			time.Sleep(RetryInterval)
			volume.mutex.Lock()
		}
	}
	c.Assert(volume.countReplicas(), Equals, VolumeNumberOfReplicas)
	s.checkVolumeConsistency(c, volume)

	v, err := s.manager.getManagedVolume(VolumeName, false)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, volume)

	err = volume.start()
	c.Assert(err, IsNil)
	c.Assert(volume.Controller, NotNil)
	c.Assert(volume.Controller.Running, Equals, true)
	for i := 0; i < RetryCounts; i++ {
		if volume.countReplicas() != VolumeNumberOfReplicas {
			volume.mutex.Unlock()
			time.Sleep(RetryInterval)
			volume.mutex.Lock()
		}
	}
	c.Assert(volume.countReplicas(), Equals, VolumeNumberOfReplicas)
	s.checkVolumeConsistency(c, volume)

	err = volume.stop()
	c.Assert(err, IsNil)
	c.Assert(volume.Controller, IsNil)
	for i := 0; i < RetryCounts; i++ {
		if volume.countReplicas() != VolumeNumberOfReplicas {
			volume.mutex.Unlock()
			time.Sleep(RetryInterval)
			volume.mutex.Lock()
		}
	}
	c.Assert(volume.countReplicas(), Equals, VolumeNumberOfReplicas)
	s.checkVolumeConsistency(c, volume)

	err = volume.stop()
	c.Assert(err, IsNil)
	c.Assert(volume.Controller, IsNil)
	for i := 0; i < RetryCounts; i++ {
		if volume.countReplicas() != VolumeNumberOfReplicas {
			volume.mutex.Unlock()
			time.Sleep(RetryInterval)
			volume.mutex.Lock()
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

func (s *TestSuite) checkVolumeConsistency(c *C, volume *ManagedVolume) {
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

	volume, err := s.manager.GetVolume(VolumeName)
	c.Assert(err, IsNil)
	c.Assert(volume.Name, Equals, VolumeName)
	c.Assert(volume.Controller, IsNil)
	c.Assert(volume.DesireState, Equals, types.VolumeStateDetached)

	volume = s.waitForVolumeState(c, VolumeName, types.VolumeStateDetached)
	c.Assert(volume.Replicas, HasLen, VolumeNumberOfReplicas)
	for _, replica := range volume.Replicas {
		c.Assert(replica.Name, Not(Equals), "")
		c.Assert(replica.IP, Equals, "")
		c.Assert(replica.Running, Equals, false)
		c.Assert(replica.VolumeName, Equals, VolumeName)
	}

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
	c.Assert(volume.NodeID, Equals, node.ID)

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

	volume, err := s.manager.GetVolume(VolumeName)
	c.Assert(err, IsNil)
	c.Assert(volume.Name, Equals, VolumeName)
	c.Assert(volume.Controller, IsNil)
	c.Assert(volume.DesireState, Equals, types.VolumeStateDetached)

	volume = s.waitForVolumeState(c, VolumeName, types.VolumeStateDetached)
	c.Assert(volume.Replicas, HasLen, VolumeNumberOfReplicas)

	//stop one random replica
	allocateNodes := map[string]struct{}{}
	for _, replica := range volume.Replicas {
		c.Assert(replica.Running, Equals, false)
		allocateNodes[replica.NodeID] = struct{}{}
	}
	// because NodeCounts >= VolumeNumberOfReplicas
	c.Assert(allocateNodes, HasLen, VolumeNumberOfReplicas)

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
	c.Assert(volume.NodeID, Equals, node.ID)

	//stop one random replica
	for _, replica := range volume.Replicas {
		_, err := s.forwarder.StopInstance(&orchestrator.Request{
			NodeID:       replica.NodeID,
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

	badReplicas := 0
	for _, replica := range volume.Replicas {
		if replica.FailedAt != "" {
			badReplicas++
		}
	}
	c.Assert(badReplicas, Equals, 1)

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
