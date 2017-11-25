package manager

import (
	"fmt"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/orchestrator"
	"github.com/rancher/longhorn-manager/scheduler"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

type VolumeManager struct {
	currentNode *Node

	ds        datastore.DataStore
	orch      orchestrator.Orchestrator
	engines   engineapi.EngineClientCollection
	rpc       RPCManager
	scheduler *scheduler.Scheduler

	EventChan           chan Event
	managedVolumes      map[string]*ManagedVolume
	managedVolumesMutex *sync.Mutex
}

func NewVolumeManager(ds datastore.DataStore,
	orch orchestrator.Orchestrator,
	engines engineapi.EngineClientCollection,
	rpc RPCManager, port int) (*VolumeManager, error) {
	if port == 0 {
		return nil, fmt.Errorf("invalid manager port")
	}
	manager := &VolumeManager{
		ds:      ds,
		orch:    orch,
		engines: engines,
		rpc:     rpc,

		EventChan:           make(chan Event),
		managedVolumes:      make(map[string]*ManagedVolume),
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
		Size:                size,
		BaseImage:           request.BaseImage,
		FromBackup:          request.FromBackup,
		NumberOfReplicas:    request.NumberOfReplicas,
		StaleReplicaTimeout: request.StaleReplicaTimeout,
		Metadata: types.Metadata{
			Name: request.Name,
		},

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

	volume, err := m.ds.GetVolume(request.Name)
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
	if err := m.ds.UpdateVolume(volume); err != nil {
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

	volume, err := m.ds.GetVolume(request.Name)
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
	if err := m.ds.UpdateVolume(volume); err != nil {
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

	volume, err := m.ds.GetVolume(request.Name)
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
	if err := m.ds.UpdateVolume(volume); err != nil {
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

	volume, err := m.ds.GetVolume(request.Name)
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
		replica, err := m.ds.GetVolumeReplica(volume.Name, repName)
		if err != nil {
			return err
		}
		if replica.FailedAt == "" {
			return fmt.Errorf("replica %v is not bad", repName)
		}
		replica.FailedAt = ""
		if err := m.ds.UpdateVolumeReplica(replica); err != nil {
			return err
		}
	}

	volume.DesireState = types.VolumeStateDetached
	if err := m.ds.UpdateVolume(volume); err != nil {
		return err
	}

	if err := node.Notify(volume.Name); err != nil {
		return err
	}
	return nil
}

func (m *VolumeManager) VolumeRecurringUpdate(request *VolumeRecurringUpdateRequest) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "unable to update volume recurring jobs")
		}
	}()

	volume, err := m.ds.GetVolume(request.Name)
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

	volume.RecurringJobs = request.RecurringJobs
	if err := m.ds.UpdateVolume(volume); err != nil {
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
	return m.ds.ListVolumes()
}

func (m *VolumeManager) VolumeInfo(volumeName string) (*types.VolumeInfo, error) {
	return m.ds.GetVolume(volumeName)
}

func (m *VolumeManager) VolumeControllerInfo(volumeName string) (*types.ControllerInfo, error) {
	return m.ds.GetVolumeController(volumeName)
}

func (m *VolumeManager) VolumeReplicaList(volumeName string) (map[string]*types.ReplicaInfo, error) {
	return m.ds.ListVolumeReplicas(volumeName)
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

func (m *VolumeManager) SettingsGet() (*types.SettingsInfo, error) {
	settings, err := m.ds.GetSettings()
	if err != nil {
		return nil, err
	}
	if settings == nil {
		settings = &types.SettingsInfo{}
		err := m.ds.CreateSettings(settings)
		if err != nil {
			logrus.Warnf("fail to create settings")
		}
		settings, err = m.ds.GetSettings()
	}
	return settings, err
}

func (m *VolumeManager) SettingsSet(settings *types.SettingsInfo) error {
	return m.ds.UpdateSettings(settings)
}

func (m *VolumeManager) GetEngineClient(volumeName string) (engineapi.EngineClient, error) {
	volume, err := m.getManagedVolume(volumeName, false)
	if err != nil {
		return nil, err
	}
	return volume.GetEngineClient()
}

func (m *VolumeManager) SnapshotPurge(volumeName string) error {
	volume, err := m.getManagedVolume(volumeName, false)
	if err != nil {
		return err
	}
	return volume.SnapshotPurge()
}

func (m *VolumeManager) SnapshotBackup(volumeName, snapshotName, backupTarget string) error {
	volume, err := m.getManagedVolume(volumeName, false)
	if err != nil {
		return err
	}
	return volume.SnapshotBackup(snapshotName, backupTarget)
}

func (m *VolumeManager) ReplicaRemove(volumeName, replicaName string) error {
	volume, err := m.getManagedVolume(volumeName, false)
	if err != nil {
		return err
	}
	return volume.ReplicaRemove(replicaName)
}

func (m *VolumeManager) JobList(volumeName string) (map[string]Job, error) {
	volume, err := m.getManagedVolume(volumeName, false)
	if err != nil {
		return nil, err
	}
	return volume.ListJobsInfo(), nil
}
