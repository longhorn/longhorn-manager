package kvstore

import (
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

func (s *KVStore) SetVolumeBase(volume *types.VolumeInfo) error {
	// copy the content of volume
	volumeBase := *volume
	volumeBase.Controller = nil
	volumeBase.Replicas = nil
	return s.b.Set(s.NewVolumeKeyFromName(volume.Name).Base(), &volumeBase)
}

func (s *KVStore) SetVolumeController(controller *types.ControllerInfo) error {
	if controller.VolumeName == "" {
		return errors.Errorf("controller doesn't have valid volume name: %+v", controller)
	}
	return s.b.Set(s.NewVolumeKeyFromName(controller.VolumeName).Controller(), controller)
}

func (s *KVStore) SetVolumeReplicas(replicas map[string]*types.ReplicaInfo) error {
	for _, replica := range replicas {
		if err := s.SetVolumeReplica(replica); err != nil {
			return errors.Wrapf(err, "unable to set volume's replicas: %+v", replica)
		}
	}
	return nil
}

func (s *KVStore) SetVolumeReplica(replica *types.ReplicaInfo) error {
	if replica.VolumeName == "" {
		return errors.Errorf("replica doesn't have valid volume name: %+v", replica)
	}
	return s.b.Set(s.NewVolumeKeyFromName(replica.VolumeName).Replica(replica.Name), replica)
}

func (s *KVStore) GetVolumeBase(id string) (*types.VolumeInfo, error) {
	volume, err := s.getVolumeBaseByKey(s.NewVolumeKeyFromName(id).Base())
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get volume %v", id)
	}
	return volume, nil
}

func (s *KVStore) getVolumeBaseByKey(key string) (*types.VolumeInfo, error) {
	volume := types.VolumeInfo{}
	if err := s.b.Get(key, &volume); err != nil {
		if s.b.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	if volume.Controller != nil || volume.Replicas != nil {
		return nil, errors.Errorf("BUG: volume base shouldn't have instances info: %+v", volume)
	}
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
	if err := s.b.Get(key, &controller); err != nil {
		if s.b.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
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
	if err := s.b.Get(key, &replica); err != nil {
		if s.b.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	return &replica, nil
}

func (s *KVStore) GetVolumeReplicas(volumeName string) (map[string]*types.ReplicaInfo, error) {
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
		return errors.Wrapf(err, "unable to remove controller of volume %v", volumeName)
	}
	return nil
}

func (s *KVStore) DeleteVolumeReplicas(volumeName string) error {
	if err := s.b.Delete(s.NewVolumeKeyFromName(volumeName).Replicas()); err != nil {
		return errors.Wrapf(err, "unable to remove replicas of volume %v", volumeName)
	}
	return nil
}

func (s *KVStore) DeleteVolumeReplica(volumeName, replicaName string) error {
	if err := s.b.Delete(s.NewVolumeKeyFromName(volumeName).Replica(replicaName)); err != nil {
		return errors.Wrapf(err, "unable to remove replica %v of volume %v", replicaName, volumeName)
	}
	return nil
}

func (s *KVStore) SetVolume(volume *types.VolumeInfo) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "unable to set volume %+v", volume)
		}
	}()

	if err := s.SetVolumeBase(volume); err != nil {
		return err
	}

	if err := s.DeleteVolumeController(volume.Name); err != nil {
		return err
	}
	if volume.Controller != nil {
		if err := s.SetVolumeController(volume.Controller); err != nil {
			return err
		}
	}

	if err := s.DeleteVolumeReplicas(volume.Name); err != nil {
		return err
	}
	if volume.Replicas != nil {
		if err := s.SetVolumeReplicas(volume.Replicas); err != nil {
			return err
		}
	}
	return nil
}

func (s *KVStore) GetVolume(id string) (*types.VolumeInfo, error) {
	volume, err := s.getVolumeByKey(s.volumeRootKey(id))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get volume %v", id)
	}
	return volume, nil
}

func (s *KVStore) getVolumeByKey(key string) (*types.VolumeInfo, error) {
	volumeKey := s.NewVolumeKeyFromRootKey(key)
	volume, err := s.getVolumeBaseByKey(volumeKey.Base())
	if err != nil {
		return nil, err
	}
	if volume == nil {
		return nil, nil
	}

	volume.Controller = nil
	controller, err := s.getVolumeControllerByKey(volumeKey.Controller())
	if err != nil {
		return nil, err
	}
	if controller != nil {
		volume.Controller = controller
	}

	volume.Replicas = nil
	replicas, err := s.getVolumeReplicasByKey(volumeKey.Replicas())
	if err != nil {
		return nil, err
	}
	if replicas != nil && len(replicas) != 0 {
		volume.Replicas = replicas
	}

	return volume, nil
}

func (s *KVStore) DeleteVolume(id string) error {
	if err := s.b.Delete(s.volumeRootKey(id)); err != nil {
		return errors.Wrap(err, "unable to remove volume")
	}
	return nil
}

func (s *KVStore) ListVolumes() ([]*types.VolumeInfo, error) {
	volumeKeys, err := s.b.Keys(s.key(keyVolumes))
	if err != nil {
		return nil, errors.Wrap(err, "unable to list volumes")
	}
	volumes := []*types.VolumeInfo{}
	for _, key := range volumeKeys {
		volume, err := s.getVolumeByKey(key)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to list volumes")
		}
		if volume != nil {
			volumes = append(volumes, volume)
		}
	}
	return volumes, nil
}
