package crdstore

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/crd/crdtype"
	"github.com/rancher/longhorn-manager/types"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	CRDobj := crdtype.CrdVolume{}
	crdtype.LhVoulme2CRDVolume(volume, &CRDobj)
	var result crdtype.CrdVolume
	if err := s.Operator.Create(&CRDobj, crdtype.CrdMap[crdtype.KeyVolume].CrdPlural, &result); err != nil {
		if apierrors.IsAlreadyExists(err) {
			fmt.Printf("ALREADY EXISTS: %#v\n", result)
		}
		return err
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

	CRDobj := crdtype.CrdVolume{}
	CRDobj.ResourceVersion = strconv.FormatUint(volume.KVIndex, 10)
	crdtype.LhVoulme2CRDVolume(volume, &CRDobj)

	var result crdtype.CrdVolume
	if err := s.Operator.Update(&CRDobj, crdtype.CrdMap[crdtype.KeyVolume].CrdPlural, &result, volume.Name); err != nil {
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

	CRDobj := crdtype.CrdController{}
	crdtype.LhController2CRDController(controller, &CRDobj, controller.VolumeName)

	var result crdtype.CrdController
	if err := s.Operator.Create(&CRDobj, crdtype.CrdMap[crdtype.KeyController].CrdPlural, &result); err != nil {
		if apierrors.IsAlreadyExists(err) {
			fmt.Printf("ALREADY EXISTS: %#v\n", result)
		}
		return err
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

	CRDobj := crdtype.CrdController{}
	CRDobj.ResourceVersion = strconv.FormatUint(controller.KVIndex, 10)
	crdtype.LhController2CRDController(controller, &CRDobj, controller.VolumeName)

	var result crdtype.CrdController
	if err := s.Operator.Update(&CRDobj, crdtype.CrdMap[crdtype.KeyController].CrdPlural, &result, controller.VolumeName); err != nil {
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

	CRDobj := crdtype.CrdReplica{}
	crdtype.LhReplicas2CRDReplicas(replica, &CRDobj, replica.Name)
	var result crdtype.CrdReplica
	if err := s.Operator.Create(&CRDobj, crdtype.CrdMap[crdtype.KeyReplica].CrdPlural, &result); err != nil {
		if apierrors.IsAlreadyExists(err) {
			fmt.Printf("ALREADY EXISTS: %#v\n", result)
		}
		return err
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

	CRDobj := crdtype.CrdReplica{}
	CRDobj.ResourceVersion = strconv.FormatUint(replica.KVIndex, 10)
	crdtype.LhReplicas2CRDReplicas(replica, &CRDobj, replica.Name)
	var result crdtype.CrdReplica
	if err := s.Operator.Update(&CRDobj, crdtype.CrdMap[crdtype.KeyReplica].CrdPlural, &result, replica.Name); err != nil {
		return err
	}

	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return err
	}

	replica.KVIndex = index
	return nil
}

func (s *CRDStore) GetVolume(key string) (*types.VolumeInfo, error) {

	v := types.VolumeInfo{}
	var result crdtype.CrdVolume
	if err := s.Operator.Get(key, crdtype.CrdMap[crdtype.KeyVolume].CrdPlural, &result); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "unable to get volume %v", key)
	}

	crdtype.CRDVolume2LhVolume(&result, &v)
	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return nil, err
	}
	v.KVIndex = index
	return &v, nil
}

func (s *CRDStore) GetVolumeController(volumeName string) (*types.ControllerInfo, error) {
	controller := types.ControllerInfo{}
	var result crdtype.CrdController
	if err := s.Operator.Get(volumeName, crdtype.CrdMap[crdtype.KeyController].CrdPlural, &result); err != nil {
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

	var result crdtype.CrdReplica
	if err := s.Operator.Get(replicaName, crdtype.CrdMap[crdtype.KeyReplica].CrdPlural, &result); err != nil {
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
	replicaMap := make(map[string]*types.ReplicaInfo)
	var result crdtype.CrdReplicaList
	if err := s.Operator.List(metav1.ListOptions{}, crdtype.CrdMap[crdtype.KeyReplica].CrdPlural, &result); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "unable to get replicas of volume %v", volumeName)
	}

	if len(result.Items) <= 0 {
		return nil, nil
	}

	var replicaList = make([]types.ReplicaInfo, len(result.Items))

	for i, item := range result.Items {
		if strings.HasPrefix(item.Name, volumeName) {
			crdtype.CRDReplicas2LhReplicas(&item, &replicaList[i])
			index, err := strconv.ParseUint(item.ResourceVersion, 10, 64)
			if err != nil {
				return nil, err
			}
			replicaList[i].KVIndex = index
			replicaMap[replicaList[i].Name] = &replicaList[i]
		}
	}
	return replicaMap, nil
}

func (s *CRDStore) DeleteVolumeController(volumeName string) error {
	if err := s.Operator.Delete(volumeName, crdtype.CrdMap[crdtype.KeyController].CrdPlural, &metav1.DeleteOptions{}); err != nil {
		return errors.Wrapf(err, "unable to delete controller of volume %v", volumeName)
	}

	return nil
}

func (s *CRDStore) DeleteVolumeReplica(volumeName, replicaName string) error {
	if err := s.Operator.Delete(replicaName, crdtype.CrdMap[crdtype.KeyReplica].CrdPlural, &metav1.DeleteOptions{}); err != nil {
		return errors.Wrapf(err, "unable to delete replica %v of volume %v", replicaName, volumeName)
	}
	return nil
}

func (s *CRDStore) DeleteVolume(id string) error {
	if err := s.Operator.Delete(id, crdtype.CrdMap[crdtype.KeyVolume].CrdPlural, &metav1.DeleteOptions{}); err != nil {
		return err
	}
	return nil

}

func (s *CRDStore) ListVolumes() (map[string]*types.VolumeInfo, error) {

	volumeMap := make(map[string]*types.VolumeInfo)
	var result crdtype.CrdVolumeList

	if err := s.Operator.List(metav1.ListOptions{}, crdtype.CrdMap[crdtype.KeyVolume].CrdPlural, &result); err != nil {
		return nil, err
	}

	if len(result.Items) <= 0 {
		return nil, nil
	}

	var volumeList = make([]types.VolumeInfo, len(result.Items))

	for i, item := range result.Items {
		crdtype.CRDVolume2LhVolume(&item, &volumeList[i])
		index, err := strconv.ParseUint(item.ResourceVersion, 10, 64)
		if err != nil {
			return nil, err
		}
		volumeList[i].KVIndex = index
		volumeMap[volumeList[i].Name] = &volumeList[i]
	}
	return volumeMap, nil
}
