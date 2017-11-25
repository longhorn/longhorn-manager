package datastore

import (
	"fmt"
	"reflect"

	"github.com/rancher/longhorn-manager/types"
)

type DataStore interface {
	CreateNode(node *types.NodeInfo) error
	UpdateNode(node *types.NodeInfo) error
	DeleteNode(nodeID string) error
	GetNode(id string) (*types.NodeInfo, error)
	ListNodes() (map[string]*types.NodeInfo, error)

	CreateSettings(settings *types.SettingsInfo) error
	UpdateSettings(settings *types.SettingsInfo) error
	GetSettings() (*types.SettingsInfo, error)

	Nuclear(nuclearCode string) error

	CreateVolume(volume *types.VolumeInfo) error
	UpdateVolume(volume *types.VolumeInfo) error
	GetVolume(id string) (*types.VolumeInfo, error)
	DeleteVolume(id string) error
	ListVolumes() (map[string]*types.VolumeInfo, error)

	CreateVolumeController(controller *types.ControllerInfo) error
	UpdateVolumeController(controller *types.ControllerInfo) error
	GetVolumeController(volumeName string) (*types.ControllerInfo, error)
	DeleteVolumeController(volumeName string) error

	CreateVolumeReplica(replica *types.ReplicaInfo) error
	UpdateVolumeReplica(replica *types.ReplicaInfo) error
	GetVolumeReplica(volumeName, replicaName string) (*types.ReplicaInfo, error)
	ListVolumeReplicas(volumeName string) (map[string]*types.ReplicaInfo, error)
	DeleteVolumeReplica(volumeName, replicaName string) error
}

func getFieldString(obj interface{}, field string) (string, error) {
	if reflect.TypeOf(obj).Kind() != reflect.Ptr {
		return "", fmt.Errorf("BUG: Non-pointer was passed in")
	}
	t := reflect.TypeOf(obj).Elem()
	if _, found := t.FieldByName(field); !found {
		return "", fmt.Errorf("BUG: %v doesn't have required field %v", t, field)
	}
	return reflect.ValueOf(obj).Elem().FieldByName(field).String(), nil
}

func setFieldString(obj interface{}, field string, value string) error {
	if reflect.TypeOf(obj).Kind() != reflect.Ptr {
		return fmt.Errorf("BUG: Non-pointer was passed in")
	}
	t := reflect.TypeOf(obj).Elem()
	if _, found := t.FieldByName(field); !found {
		return fmt.Errorf("BUG: %v doesn't have required field %v", t, field)
	}
	v := reflect.ValueOf(obj).Elem().FieldByName(field)
	if !v.CanSet() {
		return fmt.Errorf("BUG: %v doesn't have setable field %v", t, field)
	}
	v.SetString(value)
	return nil
}

func UpdateResourceVersion(dst, src interface{}) error {
	srcVersion, err := getFieldString(src, "ResourceVersion")
	if err != nil {
		return err
	}
	return setFieldString(dst, "ResourceVersion", srcVersion)
}

func CheckNode(node *types.NodeInfo) error {
	if node.ID == "" || node.Name == "" || node.IP == "" {
		return fmt.Errorf("BUG: missing required field %+v", node)
	}
	return nil
}

func CheckVolume(volume *types.VolumeInfo) error {
	if volume.Name == "" || volume.Size == 0 || volume.NumberOfReplicas == 0 {
		return fmt.Errorf("BUG: missing required field %+v", volume)
	}
	return nil
}

func CheckVolumeInstance(instance *types.InstanceInfo) error {
	if instance.ID == "" || instance.Name == "" || instance.VolumeName == "" {
		return fmt.Errorf("BUG: missing required field %+v", instance)
	}
	if instance.Running && instance.IP == "" {
		return fmt.Errorf("BUG: instance is running but lack of address %+v", instance)
	}
	return nil
}
