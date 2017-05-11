package manager

import (
	"os"
	"testing"

	"github.com/yasker/lm-rewrite/engineapi"
	"github.com/yasker/lm-rewrite/kvstore"
	"github.com/yasker/lm-rewrite/orchestrator/orchsim"

	. "gopkg.in/check.v1"
)

const (
	TestPrefix = "longhorn-manager-test"

	EnvEtcdServer  = "LONGHORN_MANAGER_TEST_ETCD_SERVER"
	EnvEngineImage = "LONGHORN_ENGINE_IMAGE"

	VolumeName                = "vol"
	VolumeSize                = "10g"
	VolumeNumberOfReplicas    = 3
	VolumeStaleReplicaTimeout = 3600
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	etcd        *kvstore.KVStore
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
}

func (s *TestSuite) TeardownTest(c *C) {
	if s.etcd != nil {
		err := s.etcd.Nuclear("nuke key value store")
		c.Assert(err, IsNil)
	}
}

func (s *TestSuite) TestBasic(c *C) {
	engines := engineapi.NewEngineSimulatorCollection()
	orch, err := orchsim.NewOrchestratorSimulator(engines)
	c.Assert(err, IsNil)

	currentNode := orch.GetCurrentNode()
	c.Assert(currentNode, NotNil)

	manager, err := NewVolumeManager(s.etcd, orch, engines)
	c.Assert(err, IsNil)

	err = manager.VolumeCreate(&VolumeCreateRequest{
		Name:                VolumeName,
		Size:                VolumeSize,
		NumberOfReplicas:    VolumeNumberOfReplicas,
		StaleReplicaTimeout: VolumeStaleReplicaTimeout,
	})
	c.Assert(err, IsNil)

	err = manager.VolumeDelete(&VolumeDeleteRequest{
		Name: VolumeName,
	})
	c.Assert(err, IsNil)
}
