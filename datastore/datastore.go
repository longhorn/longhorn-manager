package datastore

import (
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
