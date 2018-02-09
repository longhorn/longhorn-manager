package manager

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/orchestrator"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

func (v *ManagedVolume) getControllerName() string {
	return v.Name + "-controller"
}

func (v *ManagedVolume) generateReplicaName() string {
	return v.Name + "-replica-" + util.RandomID()
}

func (v *ManagedVolume) createReplica(name string) error {
	replica := &types.ReplicaInfo{
		types.ReplicaSpec{
			InstanceSpec: types.InstanceSpec{
				VolumeName:    v.Name,
				EngineImage:   v.m.GetEngineImage(),
				DesireState:   types.InstanceStateStopped,
				DesireOwnerID: v.TargetNodeID,
			},
			VolumeSize: v.Size,
		},
		types.ReplicaStatus{},
		types.Metadata{
			Name: name,
		},
	}
	if v.FromBackup != "" {
		backupID, err := util.GetBackupID(v.FromBackup)
		if err != nil {
			return err
		}
		replica.RestoreFrom = v.FromBackup
		replica.RestoreName = backupID
	}

	if err := v.m.ds.CreateVolumeReplica(replica); err != nil {
		return errors.Wrapf(err, "cannot create replica")
	}
	v.Replicas[replica.Name] = replica
	return nil
}

func (v *ManagedVolume) startReplica(replicaName string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to start replica %v for volume %v", replicaName, v.Name)
		}
	}()

	replica := v.getReplica(replicaName)
	if replica == nil {
		return fmt.Errorf("cannot find replica %v", replicaName)
	}
	if replica.Running() {
		return nil
	}

	/*
		// the first time replica.NodeID will be empty, allow scheduler to work
		// later it must be pinned to that node
		req := &orchestrator.Request{
			NodeID:      replica.NodeID,
			Instance:    replica.Name,
			VolumeName:  replica.VolumeName,
			VolumeSize:  replica.VolumeSize,
			RestoreFrom: replica.RestoreFrom,
			RestoreName: replica.RestoreName,
		}

		instance, err := v.m.orch.StartReplica(req)
		if err != nil {
			return err
		}
		if !instance.Running {
			return fmt.Errorf("Failed to start replica %v", replicaName)
		}
		replica.NodeID = instance.NodeID
		replica.State = types.InstanceStateRunning
		replica.IP = instance.IP
	*/
	replica.DesireState = types.InstanceStateRunning
	if err := v.setReplica(replica); err != nil {
		return err
	}

	return nil
}

func (v *ManagedVolume) stopReplica(replicaName string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to stop replica %v for volume %v", replicaName, v.Name)
		}
	}()

	replica := v.getReplica(replicaName)
	if replica == nil {
		return fmt.Errorf("cannot find replica %v", replicaName)
	}
	if !replica.Running() {
		return nil
	}

	/*
		instance, err := v.m.orch.StopInstance(&orchestrator.Request{
			NodeID:     replica.NodeID,
			Instance:   replica.Name,
			VolumeName: v.Name,
		})
		if err != nil {
			return err
		}
		if instance.Running {
			return fmt.Errorf("Failed to stop replica %v", replicaName)
		}
		replica.State = types.InstanceStateStopped
		replica.IP = instance.IP
	*/
	replica.DesireState = types.InstanceStateStopped
	if err := v.setReplica(replica); err != nil {
		return err
	}
	return nil
}

func (v *ManagedVolume) markBadReplica(replicaName string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to mark bad replica %v for volume %v", replicaName, v.Name)
		}
	}()
	replica := v.getReplica(replicaName)
	if replica == nil {
		return fmt.Errorf("cannot find replica %v", replicaName)
	}

	replica.FailedAt = util.Now()
	if err := v.setReplica(replica); err != nil {
		return err
	}

	logrus.Warnf("Maked %v as bad replica", replicaName)

	/*
		if replica.Running() {
			if err := v.stopReplica(replica.Name); err != nil {
				return err
			}
		}
	*/
	return nil
}

func (v *ManagedVolume) deleteReplica(replicaName string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to delete replica %v for volume %v", replicaName, v.Name)
		}
	}()

	replica := v.getReplica(replicaName)
	if replica == nil {
		return fmt.Errorf("cannot find replica %v", replicaName)
	}
	if err := v.rmReplica(replica.Name); err != nil {
		return err
	}

	/*
		if err := v.m.orch.CleanupReplica(&orchestrator.Request{
			NodeID:     replica.NodeID,
			Instance:   replica.Name,
			VolumeName: v.Name,
		}); err != nil {
			return err
		}
		if err := v.rmReplica(replica.Name); err != nil {
			return err
		}
	*/
	return nil
}

func (v *ManagedVolume) startController(startReplicas map[string]*types.ReplicaInfo) (err error) {
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
		urls = append(urls, engineapi.GetReplicaDefaultURL(replica.IP))
	}
	nodeID := v.m.orch.GetCurrentNode().ID

	instance, err := v.m.orch.StartController(&orchestrator.Request{
		NodeID:      nodeID,
		Instance:    v.getControllerName(),
		VolumeName:  v.Name,
		ReplicaURLs: urls,
	})
	if err != nil {
		return err
	}
	controller := &types.ControllerInfo{
		types.EngineSpec{
			types.InstanceSpec{
				NodeID:     nodeID,
				VolumeName: v.Name,
			},
			nil,
		},
		types.EngineStatus{
			types.InstanceStatus{
				IP:    instance.IP,
				State: instance.State(),
			},
			nil,
		},
		types.Metadata{
			Name: instance.Name,
		},
	}
	if err := v.m.ds.CreateVolumeController(controller); err != nil {
		return err
	}
	v.Controller = controller
	return nil
}

func (v *ManagedVolume) deleteController() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to delete controller for volume %v", v.Name)
		}
	}()
	if v.Controller == nil {
		return nil
	}

	req := &orchestrator.Request{
		NodeID:     v.Controller.NodeID,
		Instance:   v.Controller.Name,
		VolumeName: v.Controller.VolumeName,
	}
	// TODO make it idempotent
	if _, err := v.m.orch.StopInstance(req); err != nil {
		return err
	}
	if err := v.m.ds.DeleteVolumeController(v.Name); err != nil {
		return err
	}
	v.Controller = nil
	return nil
}

func (v *ManagedVolume) startRebuild() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to start rebuild for volume %v", v.Name)
		}
	}()

	if len(v.Replicas)-v.badReplicaCounts() >= v.NumberOfReplicas {
		return fmt.Errorf("there are enough healthy replicas for the volume")
	}

	rebuildingJobs := v.listOngoingJobsByType(JobTypeReplicaRebuild)
	if len(rebuildingJobs) != 0 {
		return nil
	}

	replicaName := v.generateReplicaName()

	errCh := make(chan error)
	go func() {
		errCh <- v.jobReplicaRebuild(&orchestrator.Request{
			NodeID:     "",
			Instance:   replicaName,
			VolumeName: v.Name,
			VolumeSize: v.Size,
		})
	}()

	if _, err := v.registerJob(JobTypeReplicaRebuild, replicaName, nil, errCh); err != nil {
		return err
	}
	return nil
}

func (v *ManagedVolume) stopRebuild() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to stop rebuild for volume %v", v.Name)
		}
	}()

	rebuildingJobs := v.listOngoingJobsByType(JobTypeReplicaRebuild)
	if len(rebuildingJobs) == 0 {
		return nil
	}

	engine, err := v.GetEngineClient()
	if err != nil {
		return err
	}
	for _, job := range rebuildingJobs {
		replicaName := job.AssoicateID
		replica := v.getReplica(replicaName)
		if err := engine.ReplicaRemove(engineapi.GetReplicaDefaultURL(replica.IP)); err != nil {
			return err
		}
		if err := v.deleteReplica(replicaName); err != nil {
			return err
		}
	}

	return nil
}

func (v *ManagedVolume) setReplica(replica *types.ReplicaInfo) error {
	if err := v.m.ds.UpdateVolumeReplica(replica); err != nil {
		return err
	}
	v.Replicas[replica.Name] = replica
	return nil
}

func (v *ManagedVolume) getReplica(name string) *types.ReplicaInfo {
	return v.Replicas[name]
}

func (v *ManagedVolume) rmReplica(name string) error {
	if err := v.m.ds.DeleteVolumeReplica(v.Name, name); err != nil {
		return err
	}
	delete(v.Replicas, name)
	return nil
}

func (v *ManagedVolume) countReplicas() int {
	return len(v.Replicas)
}

func (v *ManagedVolume) badReplicaCounts() int {
	count := 0
	for _, replica := range v.Replicas {
		if replica.FailedAt != "" {
			count++
		}
	}
	return count
}

func (v *ManagedVolume) refreshInstances() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to refresh instances for volume %v", v.Name)
		}
	}()

	controller := v.Controller
	if controller != nil {
		if err := v.refreshInstance(controller.Name, &controller.InstanceSpec, &controller.InstanceStatus); err != nil {
			return err
		}
		if !controller.Running() {
			if err := v.deleteController(); err != nil {
				return err
			}
		}
	}

	for _, replica := range v.Replicas {
		if err := v.refreshInstance(replica.Name, &replica.InstanceSpec, &replica.InstanceStatus); err != nil {
			return err
		}
	}
	return nil
}

func (v *ManagedVolume) refreshInstance(name string, spec *types.InstanceSpec, status *types.InstanceStatus) error {
	req := &orchestrator.Request{
		NodeID:     spec.NodeID,
		Instance:   name,
		VolumeName: v.Name,
	}
	n, err := v.m.orch.InspectInstance(req)
	if err != nil {
		return fmt.Errorf("fail to refresh instance state for %v: %v", name, err)
	}
	status.State = n.State()
	status.IP = n.IP
	return nil
}
