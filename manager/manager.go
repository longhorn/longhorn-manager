package manager

import (
	"fmt"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/yasker/lm-rewrite/engineapi"
	"github.com/yasker/lm-rewrite/kvstore"
	"github.com/yasker/lm-rewrite/orchestrator"
	"github.com/yasker/lm-rewrite/scheduler"
	"github.com/yasker/lm-rewrite/types"
	"github.com/yasker/lm-rewrite/util"
)

type VolumeManager struct {
	currentNode *Node

	kv        *kvstore.KVStore
	orch      orchestrator.Orchestrator
	engines   engineapi.EngineClientCollection
	rpc       RPCManager
	scheduler *scheduler.Scheduler

	EventChan           chan Event
	managedVolumes      map[string]VolumeChan
	managedVolumesMutex *sync.Mutex
}

func NewVolumeManager(kv *kvstore.KVStore,
	orch orchestrator.Orchestrator,
	engines engineapi.EngineClientCollection,
	rpc RPCManager, port int) (*VolumeManager, error) {
	if port == 0 {
		return nil, fmt.Errorf("invalid manager port")
	}
	manager := &VolumeManager{
		kv:      kv,
		orch:    orch,
		engines: engines,
		rpc:     rpc,

		EventChan:           make(chan Event),
		managedVolumes:      map[string]VolumeChan{},
		managedVolumesMutex: &sync.Mutex{},
	}
	manager.scheduler = scheduler.NewScheduler(manager)

	if err := manager.RegisterNode(port); err != nil {
		return nil, err
	}
	go manager.startProcessing()
	return manager, nil
}

func (m *VolumeManager) VolumeCreate(request *VolumeCreateRequest) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "unable to create volume")
		}
	}()

	size, err := util.ConvertSize(request.Size)
	if err != nil {
		return err
	}

	// make it random node's responsibility
	node, err := m.GetRandomNode()
	if err != nil {
		return err
	}
	info := &types.VolumeInfo{
		Name:                request.Name,
		Size:                size,
		BaseImage:           request.BaseImage,
		FromBackup:          request.FromBackup,
		NumberOfReplicas:    request.NumberOfReplicas,
		StaleReplicaTimeout: request.StaleReplicaTimeout,

		Created:      util.Now(),
		TargetNodeID: node.ID,
		State:        types.VolumeStateCreated,
		DesireState:  types.VolumeStateDetached,
	}
	if err := m.NewVolume(info); err != nil {
		return err
	}

	if err := node.Notify(info.Name); err != nil {
		//Don't rollback here, target node is still possible to pickup
		//the volume. User can call delete explicitly for the volume
		return err
	}
	return nil
}

func (m *VolumeManager) VolumeAttach(request *VolumeAttachRequest) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "unable to attach volume")
		}
	}()

	volume, err := m.kv.GetVolume(request.Name)
	if err != nil {
		return err
	}
	if volume == nil {
		return fmt.Errorf("cannot find volume %v", request.Name)
	}

	node, err := m.GetNode(request.NodeID)
	if err != nil {
		return err
	}

	if volume.State != types.VolumeStateDetached {
		return fmt.Errorf("invalid state to attach: %v", volume.State)
	}

	volume.TargetNodeID = request.NodeID
	volume.DesireState = types.VolumeStateHealthy
	if err := m.kv.UpdateVolume(volume); err != nil {
		return err
	}

	if err := node.Notify(volume.Name); err != nil {
		return err
	}
	return nil
}

func (m *VolumeManager) VolumeDetach(request *VolumeDetachRequest) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "unable to detach volume")
		}
	}()

	volume, err := m.kv.GetVolume(request.Name)
	if err != nil {
		return err
	}
	if volume == nil {
		return fmt.Errorf("cannot find volume %v", request.Name)
	}

	node, err := m.GetNode(volume.TargetNodeID)
	if err != nil {
		return err
	}

	if volume.State != types.VolumeStateHealthy && volume.State != types.VolumeStateDegraded {
		return fmt.Errorf("invalid state to detach: %v", volume.State)
	}

	volume.DesireState = types.VolumeStateDetached
	if err := m.kv.UpdateVolume(volume); err != nil {
		return err
	}

	if err := node.Notify(volume.Name); err != nil {
		return err
	}
	return nil
}

func (m *VolumeManager) VolumeDelete(request *VolumeDeleteRequest) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "unable to delete volume")
		}
	}()

	volume, err := m.kv.GetVolume(request.Name)
	if err != nil {
		return err
	}
	if volume == nil {
		return fmt.Errorf("cannot find volume %v", request.Name)
	}

	node, err := m.GetNode(volume.TargetNodeID)
	if err != nil {
		return err
	}

	volume.DesireState = types.VolumeStateDeleted
	if err := m.kv.UpdateVolume(volume); err != nil {
		return err
	}

	if err := node.Notify(volume.Name); err != nil {
		return err
	}
	return nil
}

func (m *VolumeManager) VolumeSalvage(request *VolumeSalvageRequest) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "unable to salvage volume")
		}
	}()

	volume, err := m.kv.GetVolume(request.Name)
	if err != nil {
		return err
	}
	if volume == nil {
		return fmt.Errorf("cannot find volume %v", request.Name)
	}

	node, err := m.GetNode(volume.TargetNodeID)
	if err != nil {
		return err
	}

	if volume.State != types.VolumeStateFault {
		return fmt.Errorf("invalid state to salvage: %v", volume.State)
	}

	for _, repName := range request.SalvageReplicaNames {
		replica, err := m.kv.GetVolumeReplica(volume.Name, repName)
		if err != nil {
			return err
		}
		if replica.FailedAt == "" {
			return fmt.Errorf("replica %v is not bad", repName)
		}
		replica.FailedAt = ""
		if err := m.kv.UpdateVolumeReplica(replica); err != nil {
			return err
		}
	}

	volume.DesireState = types.VolumeStateDetached
	if err := m.kv.UpdateVolume(volume); err != nil {
		return err
	}

	if err := node.Notify(volume.Name); err != nil {
		return err
	}
	return nil
}

func (m *VolumeManager) Shutdown() {
	m.rpc.StopServer()
}

func (m *VolumeManager) VolumeList() (map[string]*types.VolumeInfo, error) {
	return m.kv.ListVolumes()
}

func (m *VolumeManager) VolumeInfo(volumeName string) (*types.VolumeInfo, error) {
	return m.kv.GetVolume(volumeName)
}

func (m *VolumeManager) VolumeControllerInfo(volumeName string) (*types.ControllerInfo, error) {
	return m.kv.GetVolumeController(volumeName)
}

func (m *VolumeManager) VolumeReplicaList(volumeName string) (map[string]*types.ReplicaInfo, error) {
	return m.kv.ListVolumeReplicas(volumeName)
}

func (m *VolumeManager) ScheduleReplica(volume *types.VolumeInfo, nodeIDs map[string]struct{}) (string, error) {
	spec := &scheduler.Spec{
		Size: volume.Size,
	}
	policy := &scheduler.Policy{
		Binding: scheduler.PolicyBindingTypeSoftAntiAffinity,
		NodeIDs: nodeIDs,
	}

	schedNode, err := m.scheduler.Schedule(spec, policy)
	if err != nil {
		return "", err
	}
	return schedNode.ID, nil
}

func (m *VolumeManager) GetEngineClient(volumeName string) (engineapi.EngineClient, error) {
	volume, err := m.getManagedVolume(volumeName)
	if err != nil {
		return nil, err
	}
	if volume.Controller == nil {
		return nil, fmt.Errorf("cannot find volume %v controller", volumeName)
	}
	engine, err := m.engines.NewEngineClient(&engineapi.EngineClientRequest{
		VolumeName:    volume.Name,
		ControllerURL: engineapi.GetControllerDefaultURL(volume.Controller.IP),
	})
	if err != nil {
		return nil, err
	}
	return engine, nil
}

func (m *VolumeManager) SettingsGet() (*types.SettingsInfo, error) {
	settings, err := m.kv.GetSettings()
	if err != nil {
		return nil, err
	}
	if settings == nil {
		settings = &types.SettingsInfo{}
		err := m.kv.CreateSettings(settings)
		if err != nil {
			logrus.Warnf("fail to create settings")
		}
		settings, err = m.kv.GetSettings()
	}
	return settings, err
}

func (m *VolumeManager) SettingsSet(settings *types.SettingsInfo) error {
	return m.kv.UpdateSettings(settings)
}

func (m *VolumeManager) SnapshotPurge(volumeName string) error {
	volume, err := m.getManagedVolume(volumeName)
	if err != nil {
		return err
	}
	return volume.SnapshotPurge()
}

func (m *VolumeManager) SnapshotBackup(volumeName, snapshotName, backupTarget string) error {
	volume, err := m.getManagedVolume(volumeName)
	if err != nil {
		return err
	}
	return volume.SnapshotBackup(snapshotName, backupTarget)
}

func (m *VolumeManager) ReplicaRemove(volumeName, replicaName string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to remove replica %v of volume %v", replicaName, volumeName)
	}()
	volume, err := m.getManagedVolume(volumeName)
	if err != nil {
		return err
	}
	replica := volume.Replicas[replicaName]
	if replica == nil {
		return fmt.Errorf("cannot find replica %v", replicaName)
	}
	if replica.Running {
		if err := volume.stopReplica(replicaName); err != nil {
			return err
		}
	}
	if err := volume.deleteReplica(replicaName); err != nil {
		return err
	}
	return nil
}
