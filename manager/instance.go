package manager

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/yasker/lm-rewrite/engineapi"
	"github.com/yasker/lm-rewrite/orchestrator"
	"github.com/yasker/lm-rewrite/types"
	"github.com/yasker/lm-rewrite/util"
)

func (v *Volume) getControllerName() string {
	return v.Name + "-controller"
}

func (v *Volume) generateReplicaName() string {
	return v.Name + "-replica-" + util.RandomID()
}

func (v *Volume) createReplica() (replicaName string, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to create replica for volume %v", v.Name)
		}
	}()

	nodeID, err := v.m.ScheduleReplica(&v.VolumeInfo, v.Replicas)
	if err != nil {
		return "", err
	}
	instance, err := v.m.orch.CreateReplica(&orchestrator.Request{
		NodeID:       nodeID,
		InstanceName: v.generateReplicaName(),
		VolumeName:   v.Name,
		VolumeSize:   v.Size,
	})
	if err != nil {
		return "", err
	}
	replica := &types.ReplicaInfo{
		InstanceInfo: types.InstanceInfo{
			ID:         instance.ID,
			Type:       types.InstanceTypeReplica,
			Name:       instance.Name,
			NodeID:     nodeID,
			Address:    instance.Address,
			Running:    instance.Running,
			VolumeName: v.Name,
		},
	}
	if err := v.m.kv.CreateVolumeReplica(replica); err != nil {
		return "", err
	}
	v.Replicas[replica.Name] = replica
	return replica.Name, nil
}

func (v *Volume) startReplica(replicaName string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to start replica %v for volume %v", replicaName, v.Name)
		}
	}()

	replica := v.Replicas[replicaName]
	if replica == nil {
		return fmt.Errorf("cannot find replica %v", replicaName)
	}
	if replica.Running {
		return nil
	}

	instance, err := v.m.orch.StartInstance(&orchestrator.Request{
		NodeID:       replica.NodeID,
		InstanceID:   replica.ID,
		InstanceName: replica.Name,
		VolumeName:   replica.VolumeName,
		VolumeSize:   v.Size,
	})
	if err != nil {
		return err
	}
	replica.Running = instance.Running
	replica.Address = instance.Address
	if err := v.m.kv.UpdateVolumeReplica(replica); err != nil {
		return err
	}
	v.Replicas[replica.Name] = replica
	return nil
}

func (v *Volume) stopReplica(replicaName string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to stop replica %v for volume %v", replicaName, v.Name)
		}
	}()

	replica := v.Replicas[replicaName]
	if replica == nil {
		return fmt.Errorf("cannot find replica %v", replicaName)
	}
	if !replica.Running {
		return nil
	}

	instance, err := v.m.orch.StopInstance(&orchestrator.Request{
		NodeID:       replica.NodeID,
		InstanceID:   replica.ID,
		InstanceName: replica.Name,
		VolumeName:   v.Name,
	})
	if err != nil {
		return err
	}
	replica.Running = instance.Running
	replica.Address = instance.Address
	if err := v.m.kv.UpdateVolumeReplica(replica); err != nil {
		return err
	}
	v.Replicas[replica.Name] = replica
	return nil
}

func (v *Volume) markBadReplica(replicaName string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to mark bad replica %v for volume %v", replicaName, v.Name)
		}
	}()
	replica := v.Replicas[replicaName]
	if replica == nil {
		return fmt.Errorf("cannot find replica %v", replicaName)
	}

	replica.BadTimestamp = util.Now()
	if err := v.m.kv.UpdateVolumeReplica(replica); err != nil {
		return err
	}
	v.Replicas[replicaName] = replica

	if replica.Running {
		if err := v.stopReplica(replica.Name); err != nil {
			return err
		}
	}
	return nil
}

func (v *Volume) deleteReplica(replicaName string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to delete replica %v for volume %v", replicaName, v.Name)
		}
	}()

	replica := v.Replicas[replicaName]
	if replica == nil {
		return fmt.Errorf("cannot find replica %v", replicaName)
	}
	if err := v.m.orch.DeleteInstance(&orchestrator.Request{
		NodeID:       replica.NodeID,
		InstanceID:   replica.ID,
		InstanceName: replica.Name,
		VolumeName:   v.Name,
	}); err != nil {
		return err
	}
	if err := v.m.kv.DeleteVolumeReplica(replica.VolumeName, replica.Name); err != nil {
		return err
	}
	delete(v.Replicas, replicaName)
	return nil
}

func (v *Volume) createController(startReplicas map[string]*types.ReplicaInfo) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to create controller for volume %v", v.Name)
		}
	}()
	if v.Controller != nil {
		return nil
	}

	urls := []string{}
	for _, replica := range startReplicas {
		urls = append(urls, replica.Address+ReplicaPort)
	}
	nodeID := v.m.orch.GetCurrentNode().ID
	instance, err := v.m.orch.CreateController(&orchestrator.Request{
		NodeID:       nodeID,
		InstanceName: v.getControllerName(),
		VolumeName:   v.Name,
		VolumeSize:   v.Size,
		ReplicaURLs:  urls,
	})
	if err != nil {
		return err
	}
	controller := &types.ControllerInfo{
		InstanceInfo: types.InstanceInfo{
			ID:         instance.ID,
			Type:       types.InstanceTypeController,
			Name:       instance.Name,
			NodeID:     nodeID,
			Address:    instance.Address,
			Running:    instance.Running,
			VolumeName: v.Name,
		},
	}
	if err := v.m.kv.CreateVolumeController(controller); err != nil {
		return err
	}
	v.Controller = controller
	return nil
}

func (v *Volume) deleteController() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to delete controller for volume %v", v.Name)
		}
	}()
	if v.Controller == nil {
		return nil
	}

	if err := v.m.orch.DeleteInstance(&orchestrator.Request{
		NodeID:       v.m.orch.GetCurrentNode().ID,
		InstanceName: v.getControllerName(),
		VolumeName:   v.Name,
	}); err != nil {
		return err
	}
	if err := v.m.kv.DeleteVolumeController(v.Name); err != nil {
		return err
	}
	v.Controller = nil
	return nil
}

func (v *Volume) startRebuild() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to start rebuild for volume %v", v.Name)
		}
	}()

	if v.RebuildingReplica != nil {
		return fmt.Errorf("rebuild already started by replica %v", v.RebuildingReplica.Name)
	}

	if len(v.Replicas)-len(v.BadReplicas) >= v.NumberOfReplicas {
		return fmt.Errorf("there are enough healthy replicas for the volume")
	}

	//TODO add as a job
	replicaName, err := v.createReplica()
	if err != nil {
		return err
	}

	replica := v.Replicas[replicaName]
	if replica == nil {
		return fmt.Errorf("cannot find replica %v", replicaName)
	}

	engine, err := v.m.engines.NewEngineClient(&engineapi.EngineClientRequest{
		VolumeName:     v.Name,
		ControllerAddr: v.Controller.Address + ControllerPort,
	})
	if err != nil {
		return err
	}
	v.RebuildingReplica = replica

	url := replica.Address + ReplicaPort
	//TODO add as a job
	if err := engine.AddReplica(url); err != nil {
		return err
	}

	return nil
}

func (v *Volume) stopRebuild() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to stop rebuild for volume %v", v.Name)
		}
	}()
	replica := v.RebuildingReplica
	if replica == nil {
		return fmt.Errorf("cannot find rebuilding replica")
	}
	engine, err := v.m.engines.NewEngineClient(&engineapi.EngineClientRequest{
		VolumeName:     v.Name,
		ControllerAddr: v.Controller.Address + ControllerPort,
	})
	if err != nil {
		return err
	}
	url := replica.Address + ReplicaPort
	if err := engine.RemoveReplica(url); err != nil {
		return err
	}

	if err := v.deleteReplica(v.RebuildingReplica.Name); err != nil {
		return err
	}

	v.RebuildingReplica = nil
	return nil
}
