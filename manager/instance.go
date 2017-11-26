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

func (v *ManagedVolume) createReplica(nodeID string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to create replica for volume %v", v.Name)
	}()

	replicaName := v.generateReplicaName()

	req := &orchestrator.Request{
		NodeID:       nodeID,
		InstanceName: replicaName,
		VolumeName:   v.Name,
		VolumeSize:   v.Size,
	}
	if v.FromBackup != "" {
		backupID, err := util.GetBackupID(v.FromBackup)
		if err != nil {
			return err
		}
		req.RestoreFrom = v.FromBackup
		req.RestoreName = backupID
	}
	errCh := make(chan error)
	go func() {
		errCh <- v.jobReplicaCreate(req)
	}()

	data := map[string]string{
		"NodeID": nodeID,
	}

	if _, err := v.registerJob(JobTypeReplicaCreate, replicaName, data, errCh); err != nil {
		return err
	}
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
	replica.IP = instance.IP
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
	replica.IP = instance.IP
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

	if replica.Running {
		if err := v.stopReplica(replica.Name); err != nil {
			return err
		}
	}
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
	if err := v.m.orch.DeleteInstance(&orchestrator.Request{
		NodeID:       replica.NodeID,
		InstanceID:   replica.ID,
		InstanceName: replica.Name,
		VolumeName:   v.Name,
	}); err != nil {
		return err
	}
	if err := v.rmReplica(replica.Name); err != nil {
		return err
	}
	return nil
}

func (v *ManagedVolume) createController(startReplicas map[string]*types.ReplicaInfo) (err error) {
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
			NodeID:     nodeID,
			IP:         instance.IP,
			Running:    instance.Running,
			VolumeName: v.Name,
			Metadata: types.Metadata{
				Name: instance.Name,
			},
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
		NodeID:       v.Controller.NodeID,
		InstanceID:   v.Controller.ID,
		InstanceName: v.Controller.Name,
		VolumeName:   v.Controller.VolumeName,
	}
	// TODO make it idempotent
	if _, err := v.m.orch.StopInstance(req); err != nil {
		return err
	}
	if err := v.m.orch.DeleteInstance(req); err != nil {
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

	nodesWithReplica := v.getNodesWithReplica()

	nodeID, err := v.m.ScheduleReplica(&v.VolumeInfo, nodesWithReplica)
	if err != nil {
		return err
	}

	errCh := make(chan error)
	go func() {
		errCh <- v.jobReplicaRebuild(&orchestrator.Request{
			NodeID:       nodeID,
			InstanceName: replicaName,
			VolumeName:   v.Name,
			VolumeSize:   v.Size,
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
