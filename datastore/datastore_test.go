package datastore

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

	. "gopkg.in/check.v1"
)

const (
	// Fake CRD doesn't support ResourceVersion validate
	// so no currency test
	ConcurrentThread = 1
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	crd *CRDStore
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
	// Fake CRD doesn't support ResourceVersion validate
	// so no currency test
	s.crd = NewFakeCRDStore()
}

func (s *TestSuite) TestNode(c *C) {
	s.testNode(c, s.crd)
}

func (s *TestSuite) testNode(c *C, st DataStore) {
	node1 := &types.NodeInfo{
		ID: util.UUID(),
		IP: "127.0.1.1",
	}
	node1.Metadata.Name = node1.ID
	node2 := &types.NodeInfo{
		ID: util.UUID(),
		IP: "127.0.1.2",
	}
	node2.Metadata.Name = node2.ID
	node3 := &types.NodeInfo{
		ID: util.UUID(),
		IP: "127.0.1.3",
	}
	node3.Metadata.Name = node3.ID

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
	UpdateResourceVersion(node1, node)
	c.Assert(node, DeepEquals, node1)

	node1.IP = "127.0.2.2"
	s.verifyConcurrentExecution(c, st.UpdateNode, node1)

	node, err = st.GetNode(node1.ID)
	c.Assert(err, IsNil)
	UpdateResourceVersion(node1, node)
	c.Assert(node, DeepEquals, node1)

	s.verifyConcurrentExecution(c, st.CreateNode, node2)
	s.verifyConcurrentExecution(c, st.CreateNode, node3)

	node, err = st.GetNode(node1.ID)
	c.Assert(err, IsNil)
	UpdateResourceVersion(node1, node)
	c.Assert(node, DeepEquals, node1)

	node, err = st.GetNode(node2.ID)
	c.Assert(err, IsNil)
	UpdateResourceVersion(node2, node)
	c.Assert(node, DeepEquals, node2)

	node, err = st.GetNode(node3.ID)
	c.Assert(err, IsNil)
	UpdateResourceVersion(node3, node)
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
	s.testSettings(c, s.crd)
}

func (s *TestSuite) testSettings(c *C, st DataStore) {
	existing, err := st.GetSettings()
	c.Assert(err, IsNil)
	c.Assert(existing, IsNil)

	settings := &types.SettingsInfo{
		BackupTarget: "nfs://1.2.3.4:/test",
	}

	s.verifyConcurrentExecution(c, st.CreateSettings, settings)

	newSettings, err := st.GetSettings()
	c.Assert(err, IsNil)
	UpdateResourceVersion(settings, newSettings)
	c.Assert(newSettings, DeepEquals, settings)

	settings.BackupTarget = "nfs://4.3.2.1:/test"

	s.verifyConcurrentExecution(c, st.UpdateSettings, settings)

	newSettings, err = st.GetSettings()
	c.Assert(err, IsNil)
	UpdateResourceVersion(settings, newSettings)
	c.Assert(newSettings, DeepEquals, settings)
}

func generateTestVolume(name string) *types.VolumeInfo {
	return &types.VolumeInfo{
		VolumeSpec: types.VolumeSpec{
			Size:                "1M",
			NumberOfReplicas:    2,
			StaleReplicaTimeout: 1,
		},
		Metadata: types.Metadata{
			Name: name,
		},
	}
}

func generateTestController(volName string) *types.ControllerInfo {
	return &types.ControllerInfo{
		types.InstanceInfo{
			ID:         volName + "-controller",
			Type:       types.InstanceTypeController,
			Running:    true,
			IP:         "1.2.3.4",
			VolumeName: volName,
			Metadata: types.Metadata{
				Name: volName + "-controller",
			},
		},
	}
}

func generateTestReplica(volName, replicaName string) *types.ReplicaInfo {
	return &types.ReplicaInfo{
		InstanceInfo: types.InstanceInfo{
			ID:         volName + "-replica-" + replicaName,
			Type:       types.InstanceTypeReplica,
			Running:    true,
			IP:         "5.6.7.8",
			VolumeName: volName,
			Metadata: types.Metadata{
				Name: volName + "-replica-" + replicaName,
			},
		},
	}
}

func (s *TestSuite) createUpdateVerifyVolume(c *C, st DataStore, volume *types.VolumeInfo) {
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
	UpdateResourceVersion(volume, vol)
	c.Assert(vol, DeepEquals, volume)

	volume.StaleReplicaTimeout = 2
	s.verifyConcurrentExecution(c, st.UpdateVolume, volume)
	c.Assert(err, IsNil)

	vol, err = st.GetVolume(volume.Name)
	c.Assert(err, IsNil)
	UpdateResourceVersion(volume, vol)
	c.Assert(vol, DeepEquals, volume)
}

func (s *TestSuite) deleteAndVerifyVolume(c *C, st DataStore, volume *types.VolumeInfo) {
	err := st.DeleteVolume(volume.Name)
	c.Assert(err, IsNil)
	vol, err := st.GetVolume(volume.Name)
	c.Assert(err, IsNil)
	c.Assert(vol, IsNil)
}

func (s *TestSuite) verifyVolumes(c *C, st DataStore, volumes ...*types.VolumeInfo) {
	vols, err := st.ListVolumes()
	c.Assert(err, IsNil)
	c.Assert(len(vols), Equals, len(volumes))

	for _, volume := range volumes {
		c.Assert(vols[volume.Name], DeepEquals, volume)
	}
}

func (s *TestSuite) createUpdateVerifyController(c *C, st DataStore, controller *types.ControllerInfo) {
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
	UpdateResourceVersion(controller, ctl)
	c.Assert(ctl, DeepEquals, controller)

	controller.Running = false
	s.verifyConcurrentExecution(c, st.UpdateVolumeController, controller)
	c.Assert(err, IsNil)

	ctl, err = st.GetVolumeController(controller.VolumeName)
	c.Assert(err, IsNil)
	UpdateResourceVersion(controller, ctl)
	c.Assert(ctl, DeepEquals, controller)
}

func (s *TestSuite) deleteAndVerifyController(c *C, st DataStore, controller *types.ControllerInfo) {
	err := st.DeleteVolumeController(controller.VolumeName)
	c.Assert(err, IsNil)
	ctl, err := st.GetVolumeController(controller.VolumeName)
	c.Assert(err, IsNil)
	c.Assert(ctl, IsNil)
}

func (s *TestSuite) createUpdateVerifyReplica(c *C, st DataStore, replica *types.ReplicaInfo) {
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
	UpdateResourceVersion(replica, rep)
	c.Assert(rep, DeepEquals, replica)

	reps, err := st.ListVolumeReplicas(replica.VolumeName)
	c.Assert(err, IsNil)
	c.Assert(reps[replica.Name], DeepEquals, replica)

	replica.Running = false
	s.verifyConcurrentExecution(c, st.UpdateVolumeReplica, replica)
	c.Assert(err, IsNil)

	rep, err = st.GetVolumeReplica(replica.VolumeName, replica.Name)
	c.Assert(err, IsNil)
	UpdateResourceVersion(replica, rep)
	c.Assert(rep, DeepEquals, replica)
}

func (s *TestSuite) deleteAndVerifyReplica(c *C, st DataStore, replica *types.ReplicaInfo) {
	err := st.DeleteVolumeReplica(replica.VolumeName, replica.Name)
	c.Assert(err, IsNil)

	rep, err := st.GetVolumeReplica(replica.VolumeName, replica.Name)
	c.Assert(err, IsNil)
	c.Assert(rep, IsNil)

	reps, err := st.ListVolumeReplicas(replica.VolumeName)
	c.Assert(err, IsNil)
	c.Assert(reps[replica.Name], IsNil)
}

func (s *TestSuite) verifyReplicas(c *C, st DataStore, volumeName string, replicas ...*types.ReplicaInfo) {
	reps, err := st.ListVolumeReplicas(volumeName)
	c.Assert(err, IsNil)
	c.Assert(len(replicas), Equals, len(replicas))

	for _, replica := range replicas {
		c.Assert(reps[replica.Name], DeepEquals, replica)
	}
}

func (s *TestSuite) TestVolume(c *C) {
	s.testVolume(c, s.crd)
}

func (s *TestSuite) testVolume(c *C, st DataStore) {
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
