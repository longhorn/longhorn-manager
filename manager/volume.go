package manager

import (
	"fmt"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

const (
	// NameMaximumLength restricted the length due to Kubernetes name limitation
	NameMaximumLength = 32
)

func (m *VolumeManager) NewVolume(info *types.VolumeInfo) error {
	if info.Name == "" || info.Size == 0 || info.NumberOfReplicas == 0 {
		return fmt.Errorf("missing required parameter %+v", info)
	}
	errs := validation.IsDNS1123Label(info.Name)
	if len(errs) != 0 {
		return fmt.Errorf("Invalid volume name: %+v", errs)
	}
	if len(info.Name) > NameMaximumLength {
		return fmt.Errorf("Volume name is too long %v, must be less than %v characters",
			info.Name, NameMaximumLength)
	}
	vol, err := m.ds.GetVolume(info.Name)
	if err != nil {
		return err
	}
	if vol != nil {
		return fmt.Errorf("volume %v already exists", info.Name)
	}

	if err := m.ds.CreateVolume(info); err != nil {
		return err
	}

	return nil
}

func (m *VolumeManager) GetVolume(volumeName string) (*Volume, error) {
	if volumeName == "" {
		return nil, fmt.Errorf("invalid empty volume name")
	}

	info, err := m.ds.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, fmt.Errorf("cannot find volume %v", volumeName)
	}
	controller, err := m.ds.GetVolumeController(volumeName)
	if err != nil {
		return nil, err
	}
	replicas, err := m.ds.ListVolumeReplicas(volumeName)
	if err != nil {
		return nil, err
	}
	if replicas == nil {
		replicas = make(map[string]*types.ReplicaInfo)
	}

	return &Volume{
		VolumeInfo: *info,
		Controller: controller,
		Replicas:   replicas,
	}, nil
}

func (m *VolumeManager) getManagedVolume(volumeName string, create bool) (*ManagedVolume, error) {
	var v *ManagedVolume

	m.managedVolumesMutex.Lock()
	defer m.managedVolumesMutex.Unlock()

	v = m.managedVolumes[volumeName]
	if v != nil {
		return v, nil
	}

	if !create {
		return nil, fmt.Errorf("cannot find managed volume %v", volumeName)
	}

	volume, err := m.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}

	// Things may change when we hit here because the mutex lock above
	if volume.TargetNodeID != m.currentNode.ID {
		return nil, fmt.Errorf("volume no longer belong to the current node")
	}

	v = &ManagedVolume{
		Volume:    *volume,
		mutex:     &sync.RWMutex{},
		Jobs:      map[string]*Job{},
		jobsMutex: &sync.RWMutex{},
		Notify:    make(chan struct{}),
		m:         m,
	}
	m.managedVolumes[volumeName] = v
	go v.process()
	return v, nil
}

func (v *ManagedVolume) refresh() error {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	volume, err := v.m.GetVolume(v.Name)
	if err != nil {
		return err
	}
	v.Volume = *volume

	if err := v.updateRecurringJobs(); err != nil {
		return err
	}
	return nil
}

func (m *VolumeManager) releaseVolume(volumeName string) {
	m.managedVolumesMutex.Lock()
	defer m.managedVolumesMutex.Unlock()

	logrus.Debugf("Releasing volume %v", volumeName)
	defer logrus.Debugf("Released volume %v", volumeName)
	volume := m.managedVolumes[volumeName]
	if volume == nil {
		logrus.Warnf("Cannot find volume to be released: %v", volumeName)
		return
	}

	if volume.recurringCron != nil {
		volume.recurringCron.Stop()
	}
	delete(m.managedVolumes, volumeName)

	if volume.TargetNodeID != m.currentNode.ID {
		return
	}
	if volume.State == types.VolumeStateDeleted {
		if err := m.ds.DeleteVolume(volumeName); err != nil {
			logrus.Warnf("Fail to remove volume entry from data store: %v", err)
		}
		return
	}
	logrus.Errorf("BUG: release volume processed but don't know the reason")
}

func (v *ManagedVolume) Cleanup() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "cannot cleanup stale replicas")
		}
	}()

	v.mutex.Lock()
	defer v.mutex.Unlock()

	staleReplicas := map[string]*types.ReplicaInfo{}

	for _, replica := range v.Replicas {
		if replica.FailedAt != "" {
			if util.TimestampAfterTimeout(replica.FailedAt, v.StaleReplicaTimeout*60) {
				staleReplicas[replica.Name] = replica
			}
		}
	}

	for _, replica := range staleReplicas {
		if err := v.deleteReplica(replica.Name); err != nil {
			return err
		}
	}
	return nil
}

func (v *ManagedVolume) create() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "cannot create volume")
		}
	}()

	ready := len(v.Replicas) - v.badReplicaCounts()
	nodesWithReplica := v.getNodesWithReplica()

	creatingJobs := v.listOngoingJobsByType(JobTypeReplicaCreate)
	creating := len(creatingJobs)
	for _, job := range creatingJobs {
		data := job.Data
		if data["NodeID"] != "" {
			nodesWithReplica[data["NodeID"]] = struct{}{}
		}
	}

	for i := 0; i < v.NumberOfReplicas-creating-ready; i++ {
		nodeID, err := v.m.ScheduleReplica(&v.VolumeInfo, nodesWithReplica)
		if err != nil {
			return err
		}

		if err := v.createReplica(nodeID); err != nil {
			return err
		}
		nodesWithReplica[nodeID] = struct{}{}
	}
	return nil
}

func (v *ManagedVolume) start() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "cannot start volume")
		}
	}()

	startReplicas := map[string]*types.ReplicaInfo{}
	for _, replica := range v.Replicas {
		if replica.FailedAt != "" {
			continue
		}
		if err := v.startReplica(replica.Name); err != nil {
			return err
		}
		startReplicas[replica.Name] = replica
	}
	if len(startReplicas) == 0 {
		return fmt.Errorf("cannot start with no replicas")
	}
	if v.Controller == nil {
		if err := v.createController(startReplicas); err != nil {
			return err
		}
	}
	if v.recurringCron != nil {
		v.recurringCron.Start()
	}
	return nil
}

func (v *ManagedVolume) stop() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "cannot stop volume")
		}
	}()

	if v.recurringCron != nil {
		v.recurringCron.Stop()
	}
	if err := v.stopRebuild(); err != nil {
		return err
	}
	if v.Controller != nil {
		if err := v.deleteController(); err != nil {
			return err
		}
	}
	if v.Replicas != nil {
		for name := range v.Replicas {
			if err := v.stopReplica(name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *ManagedVolume) heal() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "cannot heal volume")
		}
	}()

	if v.Controller == nil {
		return fmt.Errorf("cannot heal without controller")
	}
	if err := v.startRebuild(); err != nil {
		return err
	}
	return nil
}

func (v *ManagedVolume) destroy() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "cannot destroy volume")
		}
	}()

	if err := v.stop(); err != nil {
		return err
	}

	if v.Replicas != nil {
		for name := range v.Replicas {
			if err := v.deleteReplica(name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *ManagedVolume) getNodesWithReplica() map[string]struct{} {
	ret := map[string]struct{}{}

	for _, replica := range v.Replicas {
		if replica.FailedAt != "" {
			ret[replica.NodeID] = struct{}{}
		}
	}
	return ret
}

func (v *ManagedVolume) SnapshotPurge() error {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	purgingJobs := v.listOngoingJobsByType(JobTypeSnapshotPurge)
	if len(purgingJobs) != 0 {
		return nil
	}

	errCh := make(chan error)
	go func() {
		errCh <- v.jobSnapshotPurge()
	}()

	if _, err := v.registerJob(JobTypeSnapshotPurge, v.Name, nil, errCh); err != nil {
		return err
	}
	return nil
}

func (v *ManagedVolume) SnapshotBackup(snapName, backupTarget string) error {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	backupJobs := v.listOngoingJobsByTypeAndAssociateID(JobTypeSnapshotBackup, snapName)
	if len(backupJobs) != 0 {
		return nil
	}

	errCh := make(chan error)
	go func() {
		errCh <- v.jobSnapshotBackup(snapName, backupTarget)
	}()

	if _, err := v.registerJob(JobTypeSnapshotBackup, snapName, nil, errCh); err != nil {
		return err
	}
	return nil
}

func (v *ManagedVolume) GetEngineClient() (engineapi.EngineClient, error) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	if v.Controller == nil {
		return nil, fmt.Errorf("cannot find volume %v controller", v.Name)
	}
	engine, err := v.m.engines.NewEngineClient(&engineapi.EngineClientRequest{
		VolumeName:    v.Name,
		ControllerURL: engineapi.GetControllerDefaultURL(v.Controller.IP),
	})
	if err != nil {
		return nil, err
	}
	return engine, nil
}

func (v *ManagedVolume) ReplicaRemove(replicaName string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to remove replica %v of volume %v", replicaName, v.Name)
	}()

	v.mutex.Lock()
	defer v.mutex.Unlock()

	replica := v.Replicas[replicaName]
	if replica == nil {
		return fmt.Errorf("cannot find replica %v", replicaName)
	}
	if replica.Running {
		if err := v.stopReplica(replicaName); err != nil {
			return err
		}
	}
	if err := v.deleteReplica(replicaName); err != nil {
		return err
	}
	return nil
}

func (v *ManagedVolume) StartReplicaAndGetURL(replicaName string) (string, error) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	if err := v.startReplica(replicaName); err != nil {
		return "", err
	}

	replica := v.Replicas[replicaName]
	if replica == nil {
		return "", fmt.Errorf("cannot find replica %v", replicaName)
	}

	if replica.IP == "" {
		return "", fmt.Errorf("cannot add replica %v without IP", replicaName)
	}

	return engineapi.GetReplicaDefaultURL(replica.IP), nil
}
