package manager

import (
	"github.com/pkg/errors"

	"github.com/yasker/lm-rewrite/engineapi"
	"github.com/yasker/lm-rewrite/kvstore"
	"github.com/yasker/lm-rewrite/orchestrator"
	"github.com/yasker/lm-rewrite/types"
	"github.com/yasker/lm-rewrite/util"
)

type VolumeManager struct {
	currentNode *Node

	kv      *kvstore.KVStore
	orch    orchestrator.Orchestrator
	engines engineapi.EngineClientCollection
}

func NewVolumeManager(kv *kvstore.KVStore,
	orch orchestrator.Orchestrator,
	engines engineapi.EngineClientCollection) (*VolumeManager, error) {
	manager := &VolumeManager{
		kv:      kv,
		orch:    orch,
		engines: engines,
	}
	if err := manager.RegisterNode(); err != nil {
		return nil, err
	}
	return manager, nil
}

func (m *VolumeManager) VolumeCreate(request *VolumeCreateRequest) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "unable to create volume")
		}
	}()

	size, err := util.ConvertSize(request.Size)
	if err != nil {
		return err
	}

	// make it random node's responsibility
	node, err := m.GetRandomNode()
	if err != nil {
		return err
	}
	info := &types.VolumeInfo{
		Name:                request.Name,
		Size:                size,
		BaseImage:           request.BaseImage,
		FromBackup:          request.FromBackup,
		NumberOfReplicas:    request.NumberOfReplicas,
		StaleReplicaTimeout: request.StaleReplicaTimeout,

		Created:      util.Now(),
		TargetNodeID: node.ID,
		State:        types.VolumeStateCreated,
		DesireState:  types.VolumeStateDetached,
	}
	if err := m.NewVolume(info); err != nil {
		return err
	}

	if err := node.Notify(info.Name); err != nil {
		//Don't rollback here, target node is still possible to pickup
		//the volume. User can call delete explicitly for the volume
		return err
	}
	return nil
}

func (m *VolumeManager) VolumeAttach(request *VolumeAttachRequest) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "unable to attach volume")
		}
	}()

	volume, err := m.GetVolume(request.Name)
	if err != nil {
		return err
	}

	node, err := m.GetNode(request.NodeID)
	if err != nil {
		return err
	}

	if err := volume.Attach(request.NodeID); err != nil {
		return err
	}

	if err := node.Notify(volume.Name); err != nil {
		return err
	}
	return nil
}

func (m *VolumeManager) VolumeDetach(request *VolumeDetachRequest) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "unable to detach volume")
		}
	}()

	volume, err := m.GetVolume(request.Name)
	if err != nil {
		return err
	}

	node, err := m.GetNode(volume.TargetNodeID)
	if err != nil {
		return err
	}

	if err := volume.Detach(); err != nil {
		return err
	}

	if err := node.Notify(volume.Name); err != nil {
		return err
	}
	return nil
}

func (m *VolumeManager) VolumeDelete(request *VolumeDeleteRequest) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "unable to delete volume")
		}
	}()

	volume, err := m.GetVolume(request.Name)
	if err != nil {
		return err
	}

	node, err := m.GetNode(volume.TargetNodeID)
	if err != nil {
		return err
	}

	if err := volume.Delete(); err != nil {
		return err
	}

	if err := node.Notify(volume.Name); err != nil {
		return err
	}
	return nil
}

func (m *VolumeManager) SalvageVolume(request *VolumeSalvageRequest) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "unable to salvage volume")
		}
	}()

	volume, err := m.GetVolume(request.Name)
	if err != nil {
		return err
	}

	node, err := m.GetNode(volume.TargetNodeID)
	if err != nil {
		return err
	}

	if err := volume.Salvage(request.SalvageReplicaNames); err != nil {
		return err
	}

	if err := node.Notify(volume.Name); err != nil {
		return err
	}
	return nil
}
