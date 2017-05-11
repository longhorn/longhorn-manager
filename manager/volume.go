package manager

import (
	"fmt"

	"github.com/yasker/lm-rewrite/types"
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
	return &Volume{
		VolumeInfo: *info,
		m:          m,
	}, nil
}

func (v *Volume) Attach(nodeID string) error {
	if v.State != types.VolumeStateDetached {
		return fmt.Errorf("invalid state to attach: %v", v.State)
	}

	v.TargetNodeID = nodeID
	v.DesireState = types.VolumeStateHealthy
	if err := v.m.kv.UpdateVolume(&v.VolumeInfo); err != nil {
		return err
	}
	return nil
}

func (v *Volume) Detach() error {
	if v.State != types.VolumeStateHealthy || v.State != types.VolumeStateDegraded {
		return fmt.Errorf("invalid state to detach: %v", v.State)
	}

	v.DesireState = types.VolumeStateDetached
	if err := v.m.kv.UpdateVolume(&v.VolumeInfo); err != nil {
		return err
	}
	return nil
}

func (v *Volume) Delete() error {
	v.DesireState = types.VolumeStateDeleted
	if err := v.m.kv.UpdateVolume(&v.VolumeInfo); err != nil {
		return err
	}
	return nil
}

func (v *Volume) Salvage(replicaNames []string) error {
	if v.State != types.VolumeStateFault {
		return fmt.Errorf("invalid state to salvage: %v", v.State)
	}

	for _, repName := range replicaNames {
		replica, err := v.m.kv.GetVolumeReplica(v.Name, repName)
		if err != nil {
			return err
		}
		if replica.BadTimestamp == "" {
			return fmt.Errorf("replica %v is not bad", repName)
		}
		replica.BadTimestamp = ""
		if err := v.m.kv.UpdateVolumeReplica(replica); err != nil {
			return err
		}
	}

	v.DesireState = types.VolumeStateDetached
	if err := v.m.kv.UpdateVolume(&v.VolumeInfo); err != nil {
		return err
	}
	return nil
}
