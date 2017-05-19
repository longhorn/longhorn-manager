package manager

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"

	"github.com/yasker/lm-rewrite/types"
	"github.com/yasker/lm-rewrite/util"
)

func (m *VolumeManager) NewVolume(info *types.VolumeInfo) error {
	if info.Name == "" || info.Size == 0 || info.NumberOfReplicas == 0 {
		return fmt.Errorf("missing required parameter %+v", info)
	}
	vol, err := m.kv.GetVolume(info.Name)
	if err != nil {
		return err
	}
	if vol != nil {
		return fmt.Errorf("volume %v already exists", info.Name)
	}

	if err := m.kv.CreateVolume(info); err != nil {
		return err
	}

	return nil
}

func (m *VolumeManager) GetVolume(volumeName string) (*Volume, error) {
	if volumeName == "" {
		return nil, fmt.Errorf("invalid empty volume name")
	}

	info, err := m.kv.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, fmt.Errorf("cannot find volume %v", volumeName)
	}
	controller, err := m.kv.GetVolumeController(volumeName)
	if err != nil {
		return nil, err
	}
	replicas, err := m.kv.ListVolumeReplicas(volumeName)
	if err != nil {
		return nil, err
	}
	if replicas == nil {
		replicas = make(map[string]*types.ReplicaInfo)
	}

	return &Volume{
		VolumeInfo: *info,
		mutex:      &sync.RWMutex{},
		Controller: controller,
		Replicas:   replicas,
		Jobs:       map[string]*Job{},
		m:          m,
	}, nil
}

func (v *Volume) Cleanup() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "cannot cleanup stale replicas")
		}
	}()

	staleReplicas := map[string]*types.ReplicaInfo{}

	for _, replica := range v.Replicas {
		if replica.BadTimestamp != "" {
			if util.TimestampAfterTimeout(replica.BadTimestamp, v.StaleReplicaTimeout*60) {
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

func (v *Volume) create() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "cannot create volume")
		}
	}()

	ready := 0
	for _, replica := range v.Replicas {
		if replica.BadTimestamp != "" {
			ready++
		}
	}

	creatingJobs := v.listOngoingJobsByType(JobTypeReplicaCreate)
	creating := len(creatingJobs)

	for i := 0; i < v.NumberOfReplicas-creating-ready; i++ {
		if err := v.createReplica(); err != nil {
			return err
		}
	}
	return nil
}

func (v *Volume) start() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "cannot start volume")
		}
	}()

	startReplicas := map[string]*types.ReplicaInfo{}
	for _, replica := range v.Replicas {
		if replica.BadTimestamp != "" {
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
	return nil
}

func (v *Volume) stop() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "cannot stop volume")
		}
	}()

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

func (v *Volume) heal() (err error) {
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

func (v *Volume) destroy() (err error) {
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
