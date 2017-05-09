package kvstore

import (
	"os"
	"testing"

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
	engineImage string
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
	var err error

	compTest := os.Getenv(EnvCompTest)

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
	if s.etcd != nil {
		err := s.etcd.kvNuclear("nuke key value store")
		c.Assert(err, IsNil)
	}
}

func (s *TestSuite) TestHost(c *C) {
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

	host, err := st.GetHost("random")
	c.Assert(err, IsNil)
	c.Assert(host, IsNil)

	hosts, err := st.ListHosts()
	c.Assert(err, IsNil)
	c.Assert(len(hosts), Equals, 0)

	err = st.SetHost(host1)
	c.Assert(err, IsNil)

	host, err = st.GetHost(host1.UUID)
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

	hosts, err = st.ListHosts()
	c.Assert(err, IsNil)

	c.Assert(hosts[host1.UUID], DeepEquals, host1)
	c.Assert(hosts[host2.UUID], DeepEquals, host2)
	c.Assert(hosts[host3.UUID], DeepEquals, host3)
}

func (s *TestSuite) TestSettings(c *C) {
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

func (s *TestSuite) createUpdateVerifyVolume(c *C, st *KVStore, volume *types.VolumeInfo) {
	vol, err := st.GetVolume(volume.Name)
	c.Assert(err, IsNil)
	c.Assert(vol, IsNil)

	err = st.CreateVolume(volume)
	c.Assert(err, IsNil)

	err = st.CreateVolume(volume)
	c.Assert(err, NotNil)

	vol, err = st.GetVolume(volume.Name)
	c.Assert(err, IsNil)
	c.Assert(vol, DeepEquals, volume)

	volume.StaleReplicaTimeout = 2
	err = st.UpdateVolume(volume)
	c.Assert(err, IsNil)

	vol, err = st.GetVolume(volume.Name)
	c.Assert(err, IsNil)
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

	err = st.CreateVolumeController(controller)
	c.Assert(err, NotNil)

	ctl, err = st.GetVolumeController(controller.VolumeName)
	c.Assert(err, IsNil)
	c.Assert(ctl, DeepEquals, controller)

	controller.Running = false
	err = st.UpdateVolumeController(controller)
	c.Assert(err, IsNil)

	ctl, err = st.GetVolumeController(controller.VolumeName)
	c.Assert(err, IsNil)
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

	err = st.CreateVolumeReplica(replica)
	c.Assert(err, NotNil)

	rep, err = st.GetVolumeReplica(replica.VolumeName, replica.Name)
	c.Assert(err, IsNil)
	c.Assert(rep, DeepEquals, replica)

	reps, err := st.ListVolumeReplicas(replica.VolumeName)
	c.Assert(err, IsNil)
	c.Assert(reps[replica.Name], DeepEquals, replica)

	replica.Running = false
	err = st.UpdateVolumeReplica(replica)
	c.Assert(err, IsNil)

	rep, err = st.GetVolumeReplica(replica.VolumeName, replica.Name)
	c.Assert(err, IsNil)
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
