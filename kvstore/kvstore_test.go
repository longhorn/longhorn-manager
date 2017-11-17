package kvstore

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

	. "gopkg.in/check.v1"
)

const (
	TestPrefix = "longhorn-manager-test"

	EnvEtcdServer  = "LONGHORN_MANAGER_TEST_ETCD_SERVER"
	EnvEngineImage = "LONGHORN_ENGINE_IMAGE"

	ConcurrentThread = 10
)

var (
	VolumeName     = TestPrefix + "-vol"
	ControllerName = VolumeName + "-controller"
	Replica1Name   = VolumeName + "-replica1"
	Replica2Name   = VolumeName + "-replica2"
	Replica3Name   = VolumeName + "-replica3"
	Replica4Name   = VolumeName + "-replica4"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	etcd        *KVStore
	memory      *KVStore
	engineImage string
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
	var err error

	memoryBackend, err := NewMemoryBackend()
	c.Assert(err, IsNil)

	memory, err := NewKVStore("/longhorn", memoryBackend)
	c.Assert(err, IsNil)
	s.memory = memory

	// Setup ETCD kv store
	etcdIP := os.Getenv(EnvEtcdServer)
	c.Assert(etcdIP, Not(Equals), "")

	s.engineImage = os.Getenv(EnvEngineImage)
	c.Assert(s.engineImage, Not(Equals), "")

	etcdBackend, err := NewETCDBackend([]string{"http://" + etcdIP + ":2379"})
	c.Assert(err, IsNil)

	etcd, err := NewKVStore("/longhorn_kvstore_test", etcdBackend)
	c.Assert(err, IsNil)
	s.etcd = etcd

	err = s.etcd.Nuclear("nuke key value store")
	c.Assert(err, IsNil)
}

func (s *TestSuite) TearDownTest(c *C) {
	if s.memory != nil {
		err := s.memory.Nuclear("nuke key value store")
		c.Assert(err, IsNil)
	}

	if s.etcd != nil {
		err := s.etcd.Nuclear("nuke key value store")
		c.Assert(err, IsNil)
	}
}

func (s *TestSuite) TestNode(c *C) {
	if s.memory != nil {
		s.testNode(c, s.memory)
	}

	if s.etcd != nil {
		s.testNode(c, s.etcd)
	}
}

func (s *TestSuite) testNode(c *C, st *KVStore) {
	node1 := &types.NodeInfo{
		ID:   util.UUID(),
		Name: "node-1",
		IP:   "127.0.1.1",
	}
	node2 := &types.NodeInfo{
		ID:   util.UUID(),
		Name: "node-2",
		IP:   "127.0.1.2",
	}
	node3 := &types.NodeInfo{
		ID:   util.UUID(),
		Name: "node-3",
		IP:   "127.0.1.3",
	}

	node, err := st.GetNode("random")
	c.Assert(err, IsNil)
	c.Assert(node, IsNil)

	nodes, err := st.ListNodes()
	c.Assert(err, IsNil)
	c.Assert(len(nodes), Equals, 0)

	err = st.CreateNode(node1)
	c.Assert(err, IsNil)
	node1.IP = "127.0.0.1"
	err = st.UpdateNode(node1)
	c.Assert(err, IsNil)
	err = st.DeleteNode(node1.ID)
	c.Assert(err, IsNil)

	s.verifyConcurrentExecution(c, st.CreateNode, node1)

	node, err = st.GetNode(node1.ID)
	c.Assert(err, IsNil)
	UpdateKVIndex(node1, node)
	c.Assert(node, DeepEquals, node1)

	node1.IP = "127.0.2.2"
	s.verifyConcurrentExecution(c, st.UpdateNode, node1)

	node, err = st.GetNode(node1.ID)
	c.Assert(err, IsNil)
	UpdateKVIndex(node1, node)
	c.Assert(node, DeepEquals, node1)

	s.verifyConcurrentExecution(c, st.CreateNode, node2)
	s.verifyConcurrentExecution(c, st.CreateNode, node3)

	node, err = st.GetNode(node1.ID)
	c.Assert(err, IsNil)
	UpdateKVIndex(node1, node)
	c.Assert(node, DeepEquals, node1)

	node, err = st.GetNode(node2.ID)
	c.Assert(err, IsNil)
	UpdateKVIndex(node2, node)
	c.Assert(node, DeepEquals, node2)

	node, err = st.GetNode(node3.ID)
	c.Assert(err, IsNil)
	UpdateKVIndex(node3, node)
	c.Assert(node, DeepEquals, node3)

	nodes, err = st.ListNodes()
	c.Assert(err, IsNil)

	c.Assert(nodes[node1.ID], DeepEquals, node1)
	c.Assert(nodes[node2.ID], DeepEquals, node2)
	c.Assert(nodes[node3.ID], DeepEquals, node3)

	err = st.DeleteNode(node1.ID)
	c.Assert(err, IsNil)

	nodes, err = st.ListNodes()
	c.Assert(err, IsNil)

	c.Assert(nodes[node2.ID], DeepEquals, node2)
	c.Assert(nodes[node3.ID], DeepEquals, node3)

}

func (s *TestSuite) TestSettings(c *C) {
	if s.memory != nil {
		s.testSettings(c, s.memory)
	}

	if s.etcd != nil {
		s.testSettings(c, s.etcd)
	}
}

func (s *TestSuite) testSettings(c *C, st *KVStore) {
	existing, err := st.GetSettings()
	c.Assert(err, IsNil)
	c.Assert(existing, IsNil)

	settings := &types.SettingsInfo{
		BackupTarget: "nfs://1.2.3.4:/test",
	}

	s.verifyConcurrentExecution(c, st.CreateSettings, settings)

	newSettings, err := st.GetSettings()
	c.Assert(err, IsNil)
	UpdateKVIndex(settings, newSettings)
	c.Assert(newSettings, DeepEquals, settings)

	settings.BackupTarget = "nfs://4.3.2.1:/test"
	s.verifyConcurrentExecution(c, st.UpdateSettings, settings)

	newSettings, err = st.GetSettings()
	c.Assert(err, IsNil)
	UpdateKVIndex(settings, newSettings)
	c.Assert(newSettings, DeepEquals, settings)
}

func generateTestVolume(name string) *types.VolumeInfo {
	return &types.VolumeInfo{
		Name:                name,
		Size:                1024 * 1024,
		NumberOfReplicas:    2,
		StaleReplicaTimeout: 1,
	}
}

func generateTestController(volName string) *types.ControllerInfo {
	return &types.ControllerInfo{
		types.InstanceInfo{
			ID:         "controller-id-" + volName,
			Type:       types.InstanceTypeController,
			Name:       "controller-name-" + volName,
			Running:    true,
			IP:         "1.2.3.4",
			VolumeName: volName,
		},
	}
}

func generateTestReplica(volName, replicaName string) *types.ReplicaInfo {
	return &types.ReplicaInfo{
		InstanceInfo: types.InstanceInfo{
			ID:         "replica-id-" + replicaName + "-" + volName,
			Type:       types.InstanceTypeReplica,
			Name:       "replica-name-" + replicaName + "-" + volName,
			Running:    true,
			IP:         "5.6.7.8",
			VolumeName: volName,
		},
	}
}

func (s *TestSuite) createUpdateVerifyVolume(c *C, st *KVStore, volume *types.VolumeInfo) {
	vol, err := st.GetVolume(volume.Name)
	c.Assert(err, IsNil)
	c.Assert(vol, IsNil)

	err = st.CreateVolume(volume)
	c.Assert(err, IsNil)
	volume.StaleReplicaTimeout = 3
	err = st.UpdateVolume(volume)
	c.Assert(err, IsNil)
	err = st.DeleteVolume(volume.Name)
	c.Assert(err, IsNil)

	s.verifyConcurrentExecution(c, st.CreateVolume, volume)

	vol, err = st.GetVolume(volume.Name)
	c.Assert(err, IsNil)
	UpdateKVIndex(volume, vol)
	c.Assert(vol, DeepEquals, volume)

	volume.StaleReplicaTimeout = 2
	s.verifyConcurrentExecution(c, st.UpdateVolume, volume)
	c.Assert(err, IsNil)

	vol, err = st.GetVolume(volume.Name)
	c.Assert(err, IsNil)
	UpdateKVIndex(volume, vol)
	c.Assert(vol, DeepEquals, volume)
}

func (s *TestSuite) deleteAndVerifyVolume(c *C, st *KVStore, volume *types.VolumeInfo) {
	err := st.DeleteVolume(volume.Name)
	c.Assert(err, IsNil)
	vol, err := st.GetVolume(volume.Name)
	c.Assert(err, IsNil)
	c.Assert(vol, IsNil)
}

func (s *TestSuite) verifyVolumes(c *C, st *KVStore, volumes ...*types.VolumeInfo) {
	vols, err := st.ListVolumes()
	c.Assert(err, IsNil)
	c.Assert(len(vols), Equals, len(volumes))

	for _, volume := range volumes {
		c.Assert(vols[volume.Name], DeepEquals, volume)
	}
}

func (s *TestSuite) createUpdateVerifyController(c *C, st *KVStore, controller *types.ControllerInfo) {
	ctl, err := st.GetVolumeController(controller.VolumeName)
	c.Assert(err, IsNil)
	c.Assert(ctl, IsNil)

	err = st.CreateVolumeController(controller)
	c.Assert(err, IsNil)
	controller.NodeID = "123"
	err = st.UpdateVolumeController(controller)
	c.Assert(err, IsNil)
	err = st.DeleteVolumeController(controller.VolumeName)
	c.Assert(err, IsNil)

	s.verifyConcurrentExecution(c, st.CreateVolumeController, controller)

	ctl, err = st.GetVolumeController(controller.VolumeName)
	c.Assert(err, IsNil)
	UpdateKVIndex(controller, ctl)
	c.Assert(ctl, DeepEquals, controller)

	controller.Running = false
	s.verifyConcurrentExecution(c, st.UpdateVolumeController, controller)
	c.Assert(err, IsNil)

	ctl, err = st.GetVolumeController(controller.VolumeName)
	c.Assert(err, IsNil)
	UpdateKVIndex(controller, ctl)
	c.Assert(ctl, DeepEquals, controller)
}

func (s *TestSuite) deleteAndVerifyController(c *C, st *KVStore, controller *types.ControllerInfo) {
	err := st.DeleteVolumeController(controller.VolumeName)
	c.Assert(err, IsNil)
	ctl, err := st.GetVolumeController(controller.VolumeName)
	c.Assert(err, IsNil)
	c.Assert(ctl, IsNil)
}

func (s *TestSuite) createUpdateVerifyReplica(c *C, st *KVStore, replica *types.ReplicaInfo) {
	rep, err := st.GetVolumeReplica(replica.VolumeName, replica.Name)
	c.Assert(err, IsNil)
	c.Assert(rep, IsNil)

	err = st.CreateVolumeReplica(replica)
	c.Assert(err, IsNil)
	replica.NodeID = "123"
	err = st.UpdateVolumeReplica(replica)
	c.Assert(err, IsNil)
	err = st.DeleteVolumeReplica(replica.VolumeName, replica.Name)
	c.Assert(err, IsNil)

	s.verifyConcurrentExecution(c, st.CreateVolumeReplica, replica)

	rep, err = st.GetVolumeReplica(replica.VolumeName, replica.Name)
	c.Assert(err, IsNil)
	UpdateKVIndex(replica, rep)
	c.Assert(rep, DeepEquals, replica)

	reps, err := st.ListVolumeReplicas(replica.VolumeName)
	c.Assert(err, IsNil)
	c.Assert(reps[replica.Name], DeepEquals, replica)

	replica.Running = false
	s.verifyConcurrentExecution(c, st.UpdateVolumeReplica, replica)
	c.Assert(err, IsNil)

	rep, err = st.GetVolumeReplica(replica.VolumeName, replica.Name)
	c.Assert(err, IsNil)
	UpdateKVIndex(replica, rep)
	c.Assert(rep, DeepEquals, replica)
}

func (s *TestSuite) deleteAndVerifyReplica(c *C, st *KVStore, replica *types.ReplicaInfo) {
	err := st.DeleteVolumeReplica(replica.VolumeName, replica.Name)
	c.Assert(err, IsNil)

	rep, err := st.GetVolumeReplica(replica.VolumeName, replica.Name)
	c.Assert(err, IsNil)
	c.Assert(rep, IsNil)

	reps, err := st.ListVolumeReplicas(replica.VolumeName)
	c.Assert(err, IsNil)
	c.Assert(reps[replica.Name], IsNil)
}

func (s *TestSuite) verifyReplicas(c *C, st *KVStore, volumeName string, replicas ...*types.ReplicaInfo) {
	reps, err := st.ListVolumeReplicas(volumeName)
	c.Assert(err, IsNil)
	c.Assert(len(replicas), Equals, len(replicas))

	for _, replica := range replicas {
		c.Assert(reps[replica.Name], DeepEquals, replica)
	}
}

func (s *TestSuite) TestVolume(c *C) {
	if s.memory != nil {
		s.testVolume(c, s.memory)
	}

	if s.etcd != nil {
		s.testVolume(c, s.etcd)
	}
}

func (s *TestSuite) testVolume(c *C, st *KVStore) {
	var err error

	volume, err := st.GetVolume("random")
	c.Assert(err, IsNil)
	c.Assert(volume, IsNil)

	volume1 := generateTestVolume("volume1")
	volume1Controller := generateTestController(volume1.Name)
	volume1Replica1 := generateTestReplica(volume1.Name, "replica1")
	volume1Replica2 := generateTestReplica(volume1.Name, "replica2")

	s.createUpdateVerifyVolume(c, st, volume1)
	s.createUpdateVerifyController(c, st, volume1Controller)
	s.deleteAndVerifyController(c, st, volume1Controller)
	s.createUpdateVerifyController(c, st, volume1Controller)

	s.verifyReplicas(c, st, volume1.Name)
	s.createUpdateVerifyReplica(c, st, volume1Replica1)
	s.verifyReplicas(c, st, volume1.Name, volume1Replica1)
	s.createUpdateVerifyReplica(c, st, volume1Replica2)
	s.verifyReplicas(c, st, volume1.Name, volume1Replica1, volume1Replica2)
	s.deleteAndVerifyReplica(c, st, volume1Replica1)
	s.verifyReplicas(c, st, volume1.Name, volume1Replica2)
	s.deleteAndVerifyReplica(c, st, volume1Replica2)
	s.verifyReplicas(c, st, volume1.Name)

	volume2 := generateTestVolume("volume2")
	volume2Controller := generateTestController(volume2.Name)
	volume2Replica1 := generateTestReplica(volume2.Name, "replica1")
	volume2Replica2 := generateTestReplica(volume2.Name, "replica2")

	s.createUpdateVerifyVolume(c, st, volume2)
	s.createUpdateVerifyController(c, st, volume2Controller)
	s.createUpdateVerifyReplica(c, st, volume2Replica1)
	s.createUpdateVerifyReplica(c, st, volume2Replica2)
	s.verifyReplicas(c, st, volume2.Name, volume2Replica1, volume2Replica2)

	s.verifyVolumes(c, st, volume1, volume2)
	s.deleteAndVerifyVolume(c, st, volume1)
	s.verifyVolumes(c, st, volume2)
	s.deleteAndVerifyVolume(c, st, volume2)
	s.verifyVolumes(c, st)
}

func (s *TestSuite) verifyConcurrentExecution(c *C, f interface{}, obj interface{}) {
	var (
		failureCount int32
		wg           sync.WaitGroup
	)

	for i := 0; i < ConcurrentThread; i++ {
		wg.Add(1)
		go func(obj interface{}) {
			var err error
			defer wg.Done()

			switch obj.(type) {
			case *types.VolumeInfo:
				info := *(obj.(*types.VolumeInfo))
				err = f.(func(*types.VolumeInfo) error)(&info)
			case *types.ControllerInfo:
				info := *(obj.(*types.ControllerInfo))
				err = f.(func(*types.ControllerInfo) error)(&info)
			case *types.ReplicaInfo:
				info := *(obj.(*types.ReplicaInfo))
				err = f.(func(*types.ReplicaInfo) error)(&info)
			case *types.NodeInfo:
				info := *(obj.(*types.NodeInfo))
				err = f.(func(*types.NodeInfo) error)(&info)
			case *types.SettingsInfo:
				info := *(obj.(*types.SettingsInfo))
				err = f.(func(*types.SettingsInfo) error)(&info)
			}
			if err != nil {
				atomic.AddInt32(&failureCount, 1)
			}
		}(obj)
	}

	wg.Wait()
	c.Assert(failureCount, Equals, int32(ConcurrentThread)-1)
}
