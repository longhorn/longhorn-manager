package kvstore

import (
	"fmt"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/yasker/lm-rewrite/types"
)

const (
	keyVolumes = "volumes"

	keyVolumeBase      = "base"
	keyVolumeInstances = "instances"

	keyVolumeInstanceController = "controller"
	keyVolumeInstanceReplicas   = "replicas"
)

type VolumeKey struct {
	rootKey string
}

func (s *KVStore) volumeRootKey(id string) string {
	return filepath.Join(s.key(keyVolumes), id)
}

func (s *KVStore) NewVolumeKeyFromName(name string) *VolumeKey {
	return &VolumeKey{
		rootKey: s.volumeRootKey(name),
	}
}

func (s *KVStore) NewVolumeKeyFromRootKey(rootKey string) *VolumeKey {
	return &VolumeKey{
		rootKey: rootKey,
	}
}

func (k *VolumeKey) RootKey() string {
	return k.rootKey
}

func (k *VolumeKey) Base() string {
	return filepath.Join(k.rootKey, keyVolumeBase)
}

func (k *VolumeKey) Instances() string {
	return filepath.Join(k.rootKey, keyVolumeInstances)
}

func (k *VolumeKey) Controller() string {
	return filepath.Join(k.Instances(), keyVolumeInstanceController)
}

func (k *VolumeKey) Replicas() string {
	return filepath.Join(k.Instances(), keyVolumeInstanceReplicas)
}

func (k *VolumeKey) Replica(replicaName string) string {
	return filepath.Join(k.Replicas(), replicaName)
}

func (s *KVStore) checkVolume(volume *types.VolumeInfo) error {
	if volume.Name == "" || volume.Size == 0 || volume.NumberOfReplicas == 0 {
		return fmt.Errorf("BUG: missing required field %+v", volume)
	}
	return nil
}

func (s *KVStore) CreateVolume(volume *types.VolumeInfo) error {
	if err := s.checkVolume(volume); err != nil {
		return err
	}
	index, err := s.b.Create(s.NewVolumeKeyFromName(volume.Name).Base(), volume)
	if err != nil {
		return err
	}
	volume.KVIndex = index
	return nil
}

func (s *KVStore) UpdateVolume(volume *types.VolumeInfo) error {
	if err := s.checkVolume(volume); err != nil {
		return err
	}
	index, err := s.b.Update(s.NewVolumeKeyFromName(volume.Name).Base(), volume, volume.KVIndex)
	if err != nil {
		return err
	}
	volume.KVIndex = index
	return nil
}

func (s *KVStore) checkVolumeInstance(instance *types.InstanceInfo) error {
	if instance.ID == "" || instance.Name == "" || instance.VolumeName == "" {
		return fmt.Errorf("BUG: missing required field %+v", instance)
	}
	if instance.Running && instance.Address == "" {
		return fmt.Errorf("BUG: instance is running but lack of address %+v", instance)
	}
	return nil
}

func (s *KVStore) CreateVolumeController(controller *types.ControllerInfo) error {
	if err := s.checkVolumeInstance(&controller.InstanceInfo); err != nil {
		return err
	}
	index, err := s.b.Create(s.NewVolumeKeyFromName(controller.VolumeName).Controller(), controller)
	if err != nil {
		return err
	}
	controller.KVIndex = index
	return nil
}

func (s *KVStore) UpdateVolumeController(controller *types.ControllerInfo) error {
	if err := s.checkVolumeInstance(&controller.InstanceInfo); err != nil {
		return err
	}
	index, err := s.b.Update(s.NewVolumeKeyFromName(controller.VolumeName).Controller(), controller, controller.KVIndex)
	if err != nil {
		return err
	}
	controller.KVIndex = index
	return nil
}

func (s *KVStore) CreateVolumeReplica(replica *types.ReplicaInfo) error {
	if err := s.checkVolumeInstance(&replica.InstanceInfo); err != nil {
		return err
	}
	index, err := s.b.Create(s.NewVolumeKeyFromName(replica.VolumeName).Replica(replica.Name), replica)
	if err != nil {
		return err
	}
	replica.KVIndex = index
	return nil
}

func (s *KVStore) UpdateVolumeReplica(replica *types.ReplicaInfo) error {
	if err := s.checkVolumeInstance(&replica.InstanceInfo); err != nil {
		return err
	}
	index, err := s.b.Update(s.NewVolumeKeyFromName(replica.VolumeName).Replica(replica.Name), replica, replica.KVIndex)
	if err != nil {
		return err
	}
	replica.KVIndex = index
	return nil
}

func (s *KVStore) GetVolume(id string) (*types.VolumeInfo, error) {
	volume, err := s.getVolumeBaseByKey(s.NewVolumeKeyFromName(id).Base())
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get volume %v", id)
	}
	return volume, nil
}

func (s *KVStore) getVolumeBaseByKey(key string) (*types.VolumeInfo, error) {
	volume := types.VolumeInfo{}
	index, err := s.b.Get(key, &volume)
	if err != nil {
		if s.b.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	volume.KVIndex = index
	return &volume, nil
}

func (s *KVStore) GetVolumeController(volumeName string) (*types.ControllerInfo, error) {
	controller, err := s.getVolumeControllerByKey(s.NewVolumeKeyFromName(volumeName).Controller())
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get controller of volume %v", volumeName)
	}
	return controller, nil
}

func (s *KVStore) getVolumeControllerByKey(key string) (*types.ControllerInfo, error) {
	controller := types.ControllerInfo{}
	index, err := s.b.Get(key, &controller)
	if err != nil {
		if s.b.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	controller.KVIndex = index
	return &controller, nil
}

func (s *KVStore) GetVolumeReplica(volumeName, replicaName string) (*types.ReplicaInfo, error) {
	replica, err := s.getVolumeReplicaByKey(s.NewVolumeKeyFromName(volumeName).Replica(replicaName))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get replica %v of volume %v", replicaName, volumeName)
	}
	return replica, nil
}

func (s *KVStore) getVolumeReplicaByKey(key string) (*types.ReplicaInfo, error) {
	replica := types.ReplicaInfo{}
	index, err := s.b.Get(key, &replica)
	if err != nil {
		if s.b.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	replica.KVIndex = index
	return &replica, nil
}

func (s *KVStore) ListVolumeReplicas(volumeName string) (map[string]*types.ReplicaInfo, error) {
	replicas, err := s.getVolumeReplicasByKey(s.NewVolumeKeyFromName(volumeName).Replicas())
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get replicas of volume %v", volumeName)
	}
	return replicas, nil
}

func (s *KVStore) getVolumeReplicasByKey(key string) (map[string]*types.ReplicaInfo, error) {
	replicaKeys, err := s.b.Keys(key)
	if err != nil {
		return nil, err
	}

	replicas := map[string]*types.ReplicaInfo{}
	for _, key := range replicaKeys {
		replica, err := s.getVolumeReplicaByKey(key)
		if err != nil {
			return nil, err
		}
		if replica != nil {
			replicas[replica.Name] = replica
		}
	}
	return replicas, nil
}

func (s *KVStore) DeleteVolumeController(volumeName string) error {
	if err := s.b.Delete(s.NewVolumeKeyFromName(volumeName).Controller()); err != nil {
		return errors.Wrapf(err, "unable to delete controller of volume %v", volumeName)
	}
	return nil
}

func (s *KVStore) DeleteVolumeReplica(volumeName, replicaName string) error {
	if err := s.b.Delete(s.NewVolumeKeyFromName(volumeName).Replica(replicaName)); err != nil {
		return errors.Wrapf(err, "unable to delete replica %v of volume %v", replicaName, volumeName)
	}
	return nil
}

func (s *KVStore) DeleteVolume(id string) error {
	if err := s.b.Delete(s.volumeRootKey(id)); err != nil {
		return errors.Wrap(err, "unable to delete volume")
	}
	return nil
}

func (s *KVStore) ListVolumes() (map[string]*types.VolumeInfo, error) {
	volumeKeys, err := s.b.Keys(s.key(keyVolumes))
	if err != nil {
		return nil, errors.Wrap(err, "unable to list volumes")
	}
	volumes := map[string]*types.VolumeInfo{}
	for _, key := range volumeKeys {
		volumeKey := s.NewVolumeKeyFromRootKey(key)
		volume, err := s.getVolumeBaseByKey(volumeKey.Base())
		if err != nil {
			return nil, errors.Wrapf(err, "unable to list volumes")
		}
		if volume != nil {
			volumes[volume.Name] = volume
		}
	}
	return volumes, nil
}
