package crdstore


import (
	"fmt"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/crd/types/vtype"
	"strconv"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"strings"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/rancher/longhorn-manager/crd/types/ctype"
	"github.com/rancher/longhorn-manager/crd/types/rtype"
)

func (s *CRDStore) checkVolume(volume *types.VolumeInfo) error {
	if volume.Name == "" || volume.Size == 0 || volume.NumberOfReplicas == 0 {
		return fmt.Errorf("BUG: missing required field %+v", volume)
	}
	return nil
}

func (s *CRDStore) CreateVolume(volume *types.VolumeInfo) error {
	if err := s.checkVolume(volume); err != nil {
		return err
	}

	CRDobj := vtype.Crdvolume{}
	vtype.LhVoulme2CRDVolume(volume, &CRDobj)
	result, err := s.VolumeClient.Create(&CRDobj)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			fmt.Printf("ALREADY EXISTS: %#v\n", result)
		}
		return  err
	}

	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return err
	}

	volume.KVIndex = index
	return nil
}

func (s *CRDStore) UpdateVolume(volume *types.VolumeInfo) error {
	if err := s.checkVolume(volume); err != nil {
		return err
	}

	CRDobj := vtype.Crdvolume{}
	CRDobj.ResourceVersion = strconv.FormatUint(volume.KVIndex, 10)
	vtype.LhVoulme2CRDVolume(volume, &CRDobj)
	r, err := s.VolumeClient.Update(&CRDobj, volume.Name)
	if err != nil {
		return err
	}

	index, err := strconv.ParseUint(r.ResourceVersion, 10, 64)

	if err != nil {
		return err
	}
	volume.KVIndex = index
	return nil
}

func (s *CRDStore) checkVolumeInstance(instance *types.InstanceInfo) error {
	if instance.ID == "" || instance.Name == "" || instance.VolumeName == "" {
		return fmt.Errorf("BUG: missing required field %+v", instance)
	}
	if instance.Running && instance.IP == "" {
		return fmt.Errorf("BUG: instance is running but lack of address %+v", instance)
	}
	return nil
}

func (s *CRDStore) CreateVolumeController(controller *types.ControllerInfo) error {
	if err := s.checkVolumeInstance(&controller.InstanceInfo); err != nil {
		return err
	}

	CRDobj := ctype.Crdcontroller{}

	ctype.LhController2CRDController(controller, &CRDobj, controller.VolumeName)
	r, err := s.ControllerClient.Create(&CRDobj)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			fmt.Printf("ALREADY EXISTS: %#v\n", r)
		}
		return  err
	}
	index, err := strconv.ParseUint(r.ResourceVersion, 10, 64)
	if err != nil {
		return err
	}
	controller.KVIndex = index
	return nil
}

func (s *CRDStore) UpdateVolumeController(controller *types.ControllerInfo) error {
	if err := s.checkVolumeInstance(&controller.InstanceInfo); err != nil {
		return err
	}

	CRDobj := ctype.Crdcontroller{}
	CRDobj.ResourceVersion = strconv.FormatUint(controller.KVIndex, 10)
	ctype.LhController2CRDController(controller, &CRDobj, controller.VolumeName)
	result, err := s.ControllerClient.Update(&CRDobj, controller.VolumeName)
	if err != nil {
		return err
	}
	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return err
	}
	controller.KVIndex = index
	return nil
}

func (s *CRDStore) CreateVolumeReplica(replica *types.ReplicaInfo) error {
	if err := s.checkVolumeInstance(&replica.InstanceInfo); err != nil {
		return err
	}

	CRDobj := rtype.Crdreplica{}
	rtype.LhReplicas2CRDReplicas(replica, &CRDobj, replica.Name)
	r, err := s.ReplicasClient.Create(&CRDobj)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			fmt.Printf("ALREADY EXISTS: %#v\n", r)
		}
		return  err
	}
	index, err := strconv.ParseUint(r.ResourceVersion, 10, 64)
	if err != nil {
		return err
	}
	replica.KVIndex = index
	return nil
}

func (s *CRDStore) UpdateVolumeReplica(replica *types.ReplicaInfo) error {
	if err := s.checkVolumeInstance(&replica.InstanceInfo); err != nil {
		return err
	}

	CRDobj := rtype.Crdreplica{}
	CRDobj.ResourceVersion = strconv.FormatUint(replica.KVIndex, 10)
	rtype.LhReplicas2CRDReplicas(replica, &CRDobj, replica.Name)

	result, err := s.ReplicasClient.Update(&CRDobj, replica.Name)
	if err != nil {
		return err
	}

	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)

	replica.KVIndex = index
	return nil
}

func (s *CRDStore) GetVolume(id string) (*types.VolumeInfo, error) {
	volume, err := s.getVolumeBaseByKey(id)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get volume %v", id)
	}
	return volume, nil
}

func (s *CRDStore) getVolumeBaseByKey(key string) (*types.VolumeInfo, error) {

	v := types.VolumeInfo{}
	r, err := s.VolumeClient.Get(key)

	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	vtype.CRDVolume2LhVoulme(r, &v)
	index, err := strconv.ParseUint(r.ResourceVersion, 10, 64)
	if err != nil {
		return nil, err
	}
	v.KVIndex = index
	return &v, nil
}

func (s *CRDStore) GetVolumeController(volumeName string) (*types.ControllerInfo, error) {
	controller, err := s.getVolumeControllerByKey(volumeName)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get controller of volume %v", volumeName)
	}
	return controller, nil
}

func (s *CRDStore) getVolumeControllerByKey(key string) (*types.ControllerInfo, error) {
	controller := types.ControllerInfo{}

	result, err := s.ControllerClient.Get(key)

	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	ctype.CRDController2LhController(result, &controller)
	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return nil, err
	}

	controller.KVIndex = index
	return &controller, nil
}

func (s *CRDStore) GetVolumeReplica(volumeName, replicaName string) (*types.ReplicaInfo, error) {
	replica, err := s.getVolumeReplicaByKey(replicaName)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get replica %v of volume %v", replicaName, volumeName)
	}
	return replica, nil
}

func (s *CRDStore) getVolumeReplicaByKey(key string) (*types.ReplicaInfo, error) {
	replica := types.ReplicaInfo{}


	r, err := s.ReplicasClient.Get(key)

	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	rtype.CRDReplicas2LhReplicas(r, &replica)
	index, err := strconv.ParseUint(r.ResourceVersion, 10, 64)
	if err != nil {
		return nil, err
	}

	replica.KVIndex = index
	return &replica, nil
}

func (s *CRDStore) ListVolumeReplicas(volumeName string) (map[string]*types.ReplicaInfo, error) {
	replicas, err := s.getVolumeReplicasByKey(volumeName)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get replicas of volume %v", volumeName)
	}
	return replicas, nil
}

func (s *CRDStore) getVolumeReplicasByKey(key string) (map[string]*types.ReplicaInfo, error) {
	replicas := make(map[string]*types.ReplicaInfo)

	r, err := s.ReplicasClient.List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	if len(r.Items) <= 0 {
		return nil, nil
	}

	var replica = make([]types.ReplicaInfo, len(r.Items))

	for i, item := range r.Items {
		if strings.HasPrefix(item.Name, key) {
			rtype.CRDReplicas2LhReplicas(&item, &replica[i])
			index, err := strconv.ParseUint(item.ResourceVersion, 10, 64)
			if err != nil {
				return nil, err
			}
			replica[i].KVIndex = index
			replicas[replica[i].Name] = &replica[i]
		}
	}
	return replicas, nil
}

func (s *CRDStore) DeleteVolumeController(volumeName string) error {
	err := s.ControllerClient.Delete(volumeName, &meta_v1.DeleteOptions{})
	if err != nil {
		return errors.Wrapf(err, "unable to delete controller of volume %v", volumeName)
	}

	return nil
}

func (s *CRDStore) DeleteVolumeReplica(volumeName, replicaName string) error {
	if err := s.ReplicasClient.Delete(replicaName, &meta_v1.DeleteOptions{}); err != nil {
		return errors.Wrapf(err, "unable to delete replica %v of volume %v", replicaName, volumeName)
	}
	return nil
}

func (s *CRDStore) DeleteVolume(id string) error {
	err := s.VolumeClient.Delete(id, &meta_v1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil

}

func (s *CRDStore) ListVolumes() (map[string]*types.VolumeInfo, error) {
	volumes := make(map[string]*types.VolumeInfo)

	r, err := s.VolumeClient.List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	if len(r.Items) <= 0 {
		return nil, nil
	}

	var v = make([]types.VolumeInfo, len(r.Items))

	for i, item := range r.Items {
		vtype.CRDVolume2LhVoulme(&item, &v[i])
		index, err := strconv.ParseUint(item.ResourceVersion, 10, 64)
		if err != nil {
			return nil, err
		}
		v[i].KVIndex = index
		volumes[v[i].Name] = &v[i]
	}
	return volumes, nil
}
