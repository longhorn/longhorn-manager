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
	size, err := util.ConvertSize(v.Size)
	if err != nil {
		return err
	}

	req := &orchestrator.Request{
		NodeID:     nodeID,
		Instance:   replicaName,
		VolumeName: v.Name,
		VolumeSize: size,
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

	size, err := util.ConvertSize(v.Size)
	if err != nil {
		return err
	}

	req := &orchestrator.Request{
		NodeID:     replica.NodeID,
		Instance:   replica.Name,
		VolumeName: replica.VolumeName,
		VolumeSize: size,
	}
	if v.FromBackup != "" {
		backupID, err := util.GetBackupID(v.FromBackup)
		if err != nil {
			return err
		}
		req.RestoreFrom = v.FromBackup
		req.RestoreName = backupID
	}

	instance, err := v.m.orch.StartInstance(req)
	if err != nil {
		return err
	}
	if !instance.Running {
		return fmt.Errorf("Failed to start replica %v", replicaName)
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
		NodeID:     replica.NodeID,
		Instance:   replica.Name,
		VolumeName: v.Name,
	}); err != nil {
		return err
	}
	if err := v.rmReplica(replica.Name); err != nil {
		return err
	}
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
	size, err := util.ConvertSize(v.Size)
	if err != nil {
		return err
	}

	instance, err := v.m.orch.StartController(&orchestrator.Request{
		NodeID:      nodeID,
		Instance:    v.getControllerName(),
		VolumeName:  v.Name,
		VolumeSize:  size,
		ReplicaURLs: urls,
	})
	if err != nil {
		return err
	}
	controller := &types.ControllerInfo{
		types.InstanceInfo{
			types.InstanceSpec{
				NodeID:     nodeID,
				VolumeName: v.Name,
			},
			types.InstanceStatus{
				IP:      instance.IP,
				Running: instance.Running,
			},
			types.Metadata{
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
		NodeID:     v.Controller.NodeID,
		Instance:   v.Controller.Name,
		VolumeName: v.Controller.VolumeName,
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

	size, err := util.ConvertSize(v.Size)
	if err != nil {
		return err
	}

	errCh := make(chan error)
	go func() {
		errCh <- v.jobReplicaRebuild(&orchestrator.Request{
			NodeID:     nodeID,
			Instance:   replicaName,
			VolumeName: v.Name,
			VolumeSize: size,
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

	if v.Controller != nil {
		if err := v.refreshInstance(&v.Controller.InstanceInfo); err != nil {
			return err
		}
		if !v.Controller.InstanceInfo.Running {
			if err := v.deleteController(); err != nil {
				return err
			}
		}
	}

	for _, replica := range v.Replicas {
		if err := v.refreshInstance(&replica.InstanceInfo); err != nil {
			return err
		}
	}
	return nil
}

func (v *ManagedVolume) refreshInstance(i *types.InstanceInfo) error {
	req := &orchestrator.Request{
		NodeID:     i.NodeID,
		Instance:   i.Name,
		VolumeName: v.Name,
	}
	n, err := v.m.orch.InspectInstance(req)
	if err != nil {
		return fmt.Errorf("fail to refresh instance state for %v: %v", i.Name, err)
	}
	i.InstanceStatus.Running = n.Running
	i.InstanceStatus.IP = n.IP
	return nil
}
