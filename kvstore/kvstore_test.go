package kvstore

import (
	"os"
	"testing"
	"time"

	"github.com/yasker/lm-rewrite/types"
	"github.com/yasker/lm-rewrite/util"

	. "gopkg.in/check.v1"
)

const (
	TestPrefix = "longhorn-manager-test"

	EnvCompTest    = "LONGHORN_MANAGER_TEST_COMP"
	EnvEtcdServer  = "LONGHORN_MANAGER_TEST_ETCD_SERVER"
	EnvEngineImage = "LONGHORN_ENGINE_IMAGE"
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

	compTest := os.Getenv(EnvCompTest)

	memoryBackend, err := NewMemoryBackend()
	c.Assert(err, IsNil)

	memory, err := NewKVStore("/longhorn", memoryBackend)
	c.Assert(err, IsNil)
	s.memory = memory

	// Skip other backends if quick is set
	if compTest != "true" {
		return
	}

	// Setup ETCD kv store
	etcdIP := os.Getenv(EnvEtcdServer)
	c.Assert(etcdIP, Not(Equals), "")

	s.engineImage = os.Getenv(EnvEngineImage)
	c.Assert(s.engineImage, Not(Equals), "")

	etcdBackend, err := NewETCDBackend([]string{"http://" + etcdIP + ":2379"})
	c.Assert(err, IsNil)

	etcd, err := NewKVStore("/longhorn", etcdBackend)
	c.Assert(err, IsNil)
	s.etcd = etcd

	err = s.etcd.kvNuclear("nuke key value store")
	c.Assert(err, IsNil)
}

func (s *TestSuite) TeardownTest(c *C) {
	err := s.memory.kvNuclear("nuke key value store")
	c.Assert(err, IsNil)

	if s.etcd != nil {
		err := s.etcd.kvNuclear("nuke key value store")
		c.Assert(err, IsNil)
	}
}

func (s *TestSuite) TestHost(c *C) {
	s.testHost(c, s.memory)

	if s.etcd != nil {
		s.testHost(c, s.etcd)
	}
}

func (s *TestSuite) testHost(c *C, st *KVStore) {
	host1 := &types.HostInfo{
		UUID:    util.UUID(),
		Name:    "host-1",
		Address: "127.0.1.1",
	}
	host2 := &types.HostInfo{
		UUID:    util.UUID(),
		Name:    "host-2",
		Address: "127.0.1.2",
	}
	host3 := &types.HostInfo{
		UUID:    util.UUID(),
		Name:    "host-3",
		Address: "127.0.1.3",
	}

	err := st.SetHost(host1)
	c.Assert(err, IsNil)

	host, err := st.GetHost(host1.UUID)
	c.Assert(err, IsNil)
	c.Assert(host, DeepEquals, host1)

	host1.Address = "127.0.2.2"
	err = st.SetHost(host1)
	c.Assert(err, IsNil)

	host, err = st.GetHost(host1.UUID)
	c.Assert(err, IsNil)
	c.Assert(host, DeepEquals, host1)

	err = st.SetHost(host2)
	c.Assert(err, IsNil)

	err = st.SetHost(host3)
	c.Assert(err, IsNil)

	host, err = st.GetHost(host1.UUID)
	c.Assert(err, IsNil)
	c.Assert(host, DeepEquals, host1)

	host, err = st.GetHost(host2.UUID)
	c.Assert(err, IsNil)
	c.Assert(host, DeepEquals, host2)

	host, err = st.GetHost(host3.UUID)
	c.Assert(err, IsNil)
	c.Assert(host, DeepEquals, host3)

	hosts, err := st.ListHosts()
	c.Assert(err, IsNil)

	c.Assert(hosts[host1.UUID], DeepEquals, host1)
	c.Assert(hosts[host2.UUID], DeepEquals, host2)
	c.Assert(hosts[host3.UUID], DeepEquals, host3)

	host, err = st.GetHost("random")
	c.Assert(err, IsNil)
	c.Assert(host, IsNil)
}

func (s *TestSuite) TestSettings(c *C) {
	s.testSettings(c, s.memory)

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
		EngineImage:  "rancher/longhorn",
	}

	err = st.SetSettings(settings)
	c.Assert(err, IsNil)

	newSettings, err := st.GetSettings()
	c.Assert(err, IsNil)
	c.Assert(newSettings.BackupTarget, Equals, settings.BackupTarget)
	c.Assert(newSettings.EngineImage, Equals, settings.EngineImage)
}

func generateTestVolume(name string) *types.VolumeInfo {
	return &types.VolumeInfo{
		Name:                name,
		Size:                1024 * 1024,
		NumberOfReplicas:    2,
		StaleReplicaTimeout: 1 * time.Minute,
	}
}

func generateTestController(volName string) *types.ControllerInfo {
	return &types.ControllerInfo{
		types.InstanceInfo{
			ID:         "controller-id-" + volName,
			Type:       types.InstanceTypeController,
			Name:       "controller-name-" + volName,
			Running:    true,
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
			VolumeName: volName,
		},
		Mode: types.ReplicaModeRW,
	}
}

func (s *TestSuite) verifyVolume(c *C, st *KVStore, volume *types.VolumeInfo) {
	var (
		volumeBase1, volumeBase2 types.VolumeInfo
	)
	comp, err := st.GetVolume(volume.Name)
	volumeBase1 = *comp
	volumeBase1.Controller = nil
	volumeBase1.Replicas = nil
	volumeBase2 = *volume
	volumeBase2.Controller = nil
	volumeBase2.Replicas = nil

	c.Assert(err, IsNil)
	c.Assert(volumeBase1, DeepEquals, volumeBase2)
	c.Assert(comp.Controller, DeepEquals, volume.Controller)
	c.Assert(len(comp.Replicas), Equals, len(volume.Replicas))
	for key := range comp.Replicas {
		c.Assert(comp.Replicas[key], DeepEquals, volume.Replicas[key])
	}
}

func (s *TestSuite) TestVolume(c *C) {
	s.testVolume(c, s.memory)

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
	controller1 := generateTestController(volume1.Name)
	replica11 := generateTestReplica(volume1.Name, "replica1")
	replica12 := generateTestReplica(volume1.Name, "replica2")
	volume1.Controller = controller1
	volume1.Replicas = map[string]*types.ReplicaInfo{
		replica11.Name: replica11,
		replica12.Name: replica12,
	}

	err = st.SetVolume(volume1)
	c.Assert(err, IsNil)
	s.verifyVolume(c, st, volume1)

	volume2 := generateTestVolume("volume2")
	controller2 := generateTestController(volume2.Name)
	replica21 := generateTestReplica(volume2.Name, "replica1")
	replica22 := generateTestReplica(volume2.Name, "replica2")
	volume2.Controller = controller2
	volume2.Replicas = map[string]*types.ReplicaInfo{
		replica21.Name: replica21,
		replica22.Name: replica22,
	}

	err = st.SetVolume(volume2)
	c.Assert(err, IsNil)
	s.verifyVolume(c, st, volume2)

	volumes, err := st.ListVolumes()
	c.Assert(err, IsNil)
	c.Assert(len(volumes), Equals, 2)

	volume2.Controller = nil
	err = st.SetVolume(volume2)
	c.Assert(err, IsNil)
	s.verifyVolume(c, st, volume2)

	volume2.Replicas = nil
	err = st.SetVolume(volume2)
	c.Assert(err, IsNil)
	s.verifyVolume(c, st, volume2)

	volume2.Replicas = map[string]*types.ReplicaInfo{
		replica21.Name: replica21,
	}
	err = st.SetVolume(volume2)
	c.Assert(err, IsNil)
	s.verifyVolume(c, st, volume2)

	volume2.Replicas[replica22.Name] = replica22
	err = st.SetVolume(volume2)
	c.Assert(err, IsNil)
	s.verifyVolume(c, st, volume2)

	volume2.Controller = controller2
	err = st.SetVolume(volume2)
	c.Assert(err, IsNil)
	s.verifyVolume(c, st, volume2)

	err = st.DeleteVolumeReplicas(volume2.Name)
	c.Assert(err, IsNil)
	volume2.Replicas = nil
	s.verifyVolume(c, st, volume2)

	err = st.SetVolumeReplica(replica21)
	c.Assert(err, IsNil)
	volume2.Replicas = map[string]*types.ReplicaInfo{
		replica21.Name: replica21,
	}
	s.verifyVolume(c, st, volume2)

	err = st.SetVolumeReplica(replica22)
	c.Assert(err, IsNil)
	volume2.Replicas[replica22.Name] = replica22
	s.verifyVolume(c, st, volume2)

	err = st.DeleteVolumeReplicas(volume2.Name)
	c.Assert(err, IsNil)
	volume2.Replicas = nil
	s.verifyVolume(c, st, volume2)

	volume2.Replicas = map[string]*types.ReplicaInfo{
		replica21.Name: replica21,
		replica22.Name: replica22,
	}
	err = st.SetVolumeReplicas(volume2.Replicas)
	c.Assert(err, IsNil)
	s.verifyVolume(c, st, volume2)

	err = st.DeleteVolumeController(volume2.Name)
	c.Assert(err, IsNil)
	volume2.Controller = nil
	s.verifyVolume(c, st, volume2)

	err = st.SetVolumeController(controller2)
	c.Assert(err, IsNil)
	volume2.Controller = controller2
	s.verifyVolume(c, st, volume2)

	err = st.DeleteVolume(volume1.Name)
	c.Assert(err, IsNil)

	volumes, err = st.ListVolumes()
	c.Assert(err, IsNil)
	c.Assert(len(volumes), Equals, 1)
	c.Assert(volumes[0], DeepEquals, volume2)

	err = st.DeleteVolume(volume2.Name)
	c.Assert(err, IsNil)

	volumes, err = st.ListVolumes()
	c.Assert(err, IsNil)
	c.Assert(len(volumes), Equals, 0)
}
