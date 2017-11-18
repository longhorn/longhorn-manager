package crdstore

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-manager/types"
	"strconv"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"strings"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/rancher/longhorn-manager/crd/crdtype"
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

	CRDobj := crdtype.Crdvolume{}
	crdtype.LhVoulme2CRDVolume(volume, &CRDobj)
	var result crdtype.Crdvolume
	err := s.VolumeOperator.Create(&CRDobj, &result)
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

	CRDobj := crdtype.Crdvolume{}
	CRDobj.ResourceVersion = strconv.FormatUint(volume.KVIndex, 10)
	crdtype.LhVoulme2CRDVolume(volume, &CRDobj)
	var result crdtype.Crdvolume
	err := s.VolumeOperator.Update(&CRDobj, &result, volume.Name)
	if err != nil {
		return err
	}

	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)

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

	CRDobj := crdtype.Crdcontroller{}

	crdtype.LhController2CRDController(controller, &CRDobj, controller.VolumeName)
	var result crdtype.Crdcontroller
	err := s.ControllerOperator.Create(&CRDobj, &result)
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
	controller.KVIndex = index
	return nil
}

func (s *CRDStore) UpdateVolumeController(controller *types.ControllerInfo) error {
	if err := s.checkVolumeInstance(&controller.InstanceInfo); err != nil {
		return err
	}

	CRDobj := crdtype.Crdcontroller{}
	CRDobj.ResourceVersion = strconv.FormatUint(controller.KVIndex, 10)
	crdtype.LhController2CRDController(controller, &CRDobj, controller.VolumeName)
	var result crdtype.Crdcontroller
	err := s.ControllerOperator.Update(&CRDobj, &result, controller.VolumeName)
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

	CRDobj := crdtype.Crdreplica{}
	crdtype.LhReplicas2CRDReplicas(replica, &CRDobj, replica.Name)
	var result crdtype.Crdreplica
	err := s.ReplicasOperator.Create(&CRDobj, &result)
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
	replica.KVIndex = index
	return nil
}

func (s *CRDStore) UpdateVolumeReplica(replica *types.ReplicaInfo) error {
	if err := s.checkVolumeInstance(&replica.InstanceInfo); err != nil {
		return err
	}

	CRDobj := crdtype.Crdreplica{}
	CRDobj.ResourceVersion = strconv.FormatUint(replica.KVIndex, 10)
	crdtype.LhReplicas2CRDReplicas(replica, &CRDobj, replica.Name)
	var result crdtype.Crdreplica
	err := s.ReplicasOperator.Update(&CRDobj, &result, replica.Name)
	if err != nil {
		return err
	}

	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)

	replica.KVIndex = index
	return nil
}


func (s *CRDStore) GetVolume(key string) (*types.VolumeInfo, error) {

	v := types.VolumeInfo{}
	var result crdtype.Crdvolume
	err := s.VolumeOperator.Get(key, &result)

	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "unable to get volume %v", key)
	}

	crdtype.CRDVolume2LhVoulme(&result, &v)
	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return nil, err
	}
	v.KVIndex = index
	return &v, nil
}


func (s *CRDStore) GetVolumeController(volumeName string) (*types.ControllerInfo, error) {
	controller := types.ControllerInfo{}
	var result crdtype.Crdcontroller
	err := s.ControllerOperator.Get(volumeName, &result)

	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "unable to get controller of volume %v", volumeName)
	}

	crdtype.CRDController2LhController(&result, &controller)
	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return nil, err
	}

	controller.KVIndex = index
	return &controller, nil
}

func (s *CRDStore) GetVolumeReplica(volumeName, replicaName string) (*types.ReplicaInfo, error) {
	replica := types.ReplicaInfo{}

	var result crdtype.Crdreplica
	err := s.ReplicasOperator.Get(replicaName, &result)

	if err != nil {
		return nil, errors.Wrapf(err, "unable to get replica %v of volume %v", replicaName, volumeName)
	}

	crdtype.CRDReplicas2LhReplicas(&result, &replica)
	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return nil, err
	}

	replica.KVIndex = index
	return &replica, nil
}

func (s *CRDStore) ListVolumeReplicas(volumeName string) (map[string]*types.ReplicaInfo, error) {
	replicas := make(map[string]*types.ReplicaInfo)
	var result crdtype.CrdreplicaList
	err := s.ReplicasOperator.List(meta_v1.ListOptions{}, &result)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "unable to get replicas of volume %v", volumeName)
	}

	if len(result.Items) <= 0 {
		return nil, nil
	}

	var replica = make([]types.ReplicaInfo, len(result.Items))

	for i, item := range result.Items {
		if strings.HasPrefix(item.Name, volumeName) {
			crdtype.CRDReplicas2LhReplicas(&item, &replica[i])
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
	err := s.ControllerOperator.Delete(volumeName, &meta_v1.DeleteOptions{})
	if err != nil {
		return errors.Wrapf(err, "unable to delete controller of volume %v", volumeName)
	}

	return nil
}

func (s *CRDStore) DeleteVolumeReplica(volumeName, replicaName string) error {
	if err := s.ReplicasOperator.Delete(replicaName, &meta_v1.DeleteOptions{}); err != nil {
		return errors.Wrapf(err, "unable to delete replica %v of volume %v", replicaName, volumeName)
	}
	return nil
}

func (s *CRDStore) DeleteVolume(id string) error {
	err := s.VolumeOperator.Delete(id, &meta_v1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil

}

func (s *CRDStore) ListVolumes() (map[string]*types.VolumeInfo, error) {
	volumes := make(map[string]*types.VolumeInfo)
	var result crdtype.CrdvolumeList

	err := s.VolumeOperator.List(meta_v1.ListOptions{}, &result)
	if err != nil {
		return nil, err
	}

	if len(result.Items) <= 0 {
		return nil, nil
	}

	var v = make([]types.VolumeInfo, len(result.Items))

	for i, item := range result.Items {
		crdtype.CRDVolume2LhVoulme(&item, &v[i])
		index, err := strconv.ParseUint(item.ResourceVersion, 10, 64)
		if err != nil {
			return nil, err
		}
		v[i].KVIndex = index
		volumes[v[i].Name] = &v[i]
	}
	return volumes, nil
}
