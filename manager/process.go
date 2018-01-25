package manager

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/types"
)

var (
	ConfirmationInterval = 1 * time.Second
	ConfirmationCounts   = 30

	ReconcileInterval = 5 * time.Second
)

func (m *VolumeManager) startProcessing() {
	volumes, err := m.VolumeList()
	if err != nil {
		logrus.Fatalf("Failed to acquire existing volume list")
	}
	if volumes != nil {
		for name, volume := range volumes {
			if volume.TargetNodeID == m.GetCurrentNodeID() {
				go m.notifyVolume(name)
			}
		}
	}
	for event := range m.EventChan {
		switch event.Type {
		case EventTypeNotify:
			go m.notifyVolume(event.VolumeName)
		case EventTypeCreate:
			go m.VolumeCreateBySpec(event.VolumeName)
		default:
			logrus.Errorf("Unrecongized event %+v", event)
		}
	}
}

func (m *VolumeManager) notifyVolume(volumeName string) (err error) {
	defer func() {
		if err != nil {
			logrus.Warnf("Failed to notify volume due to %v", err)
		}
	}()

	var volume *Volume

	currentNode := m.GetCurrentNode()
	// wait until we can confirm that this node owns volume
	for i := 0; i < ConfirmationCounts; i++ {
		volume, err = m.GetVolume(volumeName)
		if err == nil {
			if volume == nil {
				logrus.Warnf("Cannot manage volume, volume %v has been deleted", volumeName)
				return nil
			}
			if volume.TargetNodeID == currentNode.ID {
				if volume.NodeID == "" || volume.NodeID == currentNode.ID {
					break
				}
				logrus.Debugf("Waiting for the other manager %v to yield volume %v, this node %v",
					volume.NodeID, volumeName, currentNode.ID)
			} else {
				err = fmt.Errorf("Target node ID %v doesn't match with the current one %v",
					volume.TargetNodeID, currentNode.ID)
			}
		}
		time.Sleep(ConfirmationInterval)
	}
	if err != nil {
		return err
	}
	if volume.NodeID != currentNode.ID {
		volume.NodeID = currentNode.ID
		if err := m.ds.UpdateVolume(&volume.VolumeInfo); err != nil {
			return err
		}
	}

	v, err := m.getManagedVolume(volumeName, true)
	if err != nil {
		return err
	}
	v.Notify <- struct{}{}

	return nil
}

func (v *ManagedVolume) process() {
	defer v.m.releaseVolume(v.Name)
	defer logrus.Debugf("Stop managing volume %v", v.Name)

	logrus.Debugf("Start managing volume %v", v.Name)
	tick := time.NewTicker(ReconcileInterval)
	for {
		select {
		case <-tick.C:
			break
		case <-v.Notify:
			break
		}
		if err := v.refresh(); err != nil {
			if _, ok := err.(*types.NotFoundError); ok {
				logrus.Errorf("Cannot found volume %v, releasing it", v.Name)
				break
			}
			logrus.Warnf("Failed to get volume: %v", err)
			continue
		}
		if v.getTargetNodeID() != v.m.currentNode.ID {
			logrus.Infof("Volume %v no longer belong to current node, releasing it: target node ID %v, currentNode ID %v ",
				v.Name, v.getTargetNodeID(), v.m.currentNode.ID)
			break
		}

		if err := v.RefreshState(); err != nil {
			logrus.Warnf("Failed to refresh volume state: %v", err)
			continue
		}

		if err := v.Cleanup(); err != nil {
			logrus.Warnf("Failed to cleanup stale replicas: %v", err)
		}

		if err := v.Reconcile(); err != nil {
			logrus.Warnf("Fail to reconcile volume state: %v", err)
		}
		if err := v.RefreshState(); err != nil {
			logrus.Warnf("Fail to refresh volume state: %v", err)
		}
		if v.isDeleted() {
			break
		}
	}
}

func (v *ManagedVolume) isDeleted() bool {
	v.mutex.RLock()
	defer v.mutex.RUnlock()

	return v.State == types.VolumeStateDeleted
}

func (v *ManagedVolume) getTargetNodeID() string {
	v.mutex.RLock()
	defer v.mutex.RUnlock()

	return v.TargetNodeID
}

func (v *ManagedVolume) RefreshState() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "cannot refresh volume state")
		} else {
			logrus.Debugf("volume %v state is %v", v.Name, v.State)
		}
	}()

	if v.DeletionPending && v.DesireState != types.VolumeStateDeleted {
		logrus.Debugf("Update volume %v's DesireState to Deleted due to deletion pending", v.Name)
		v.DesireState = types.VolumeStateDeleted
		if err := v.m.ds.UpdateVolume(&v.VolumeInfo); err != nil {
			return err
		}
		return fmt.Errorf("volume is in deletion pending state")
	}

	engineReps := map[string]*engineapi.Replica{}
	endpoint := ""
	if v.Controller != nil {
		engine, err := v.GetEngineClient()
		if err != nil {
			return err
		}
		endpoint = engine.Endpoint()
		engineReps, err = engine.ReplicaList()
		if err != nil {
			return err
		}
		for url, rep := range engineReps {
			if rep.Mode == engineapi.ReplicaModeERR {
				if err := engine.ReplicaRemove(url); err != nil {
					logrus.Warnf("fail to clean up ERR replica %v for volume %v", url, v.Name)
				}
			}
		}
	}

	v.mutex.Lock()
	defer v.mutex.Unlock()

	badReplicas := v.syncWithEngineState(engineReps)

	for name := range badReplicas {
		if v.Replicas[name].FailedAt == "" {
			if err := v.markBadReplica(name); err != nil {
				return err
			}
		}
	}

	// all calls here must be idempotent
	if v.State == types.VolumeStateCreated || v.State == types.VolumeStateDetached ||
		v.State == types.VolumeStateFault || v.State == types.VolumeStateDeleted {
		for name, replica := range v.Replicas {
			if !replica.Running() {
				continue
			}
			if err := v.stopReplica(name); err != nil {
				return err
			}
		}
		if err := v.deleteController(); err != nil {
			return err
		}
	}

	v.Endpoint = endpoint
	if err := v.m.ds.UpdateVolume(&v.VolumeInfo); err != nil {
		return err
	}
	return nil
}

// syncWithEngineState() will return all bad replicas for the volume
func (v *ManagedVolume) syncWithEngineState(engineReps map[string]*engineapi.Replica) map[string]struct{} {
	healthyReplicaCount := 0
	rebuildingReplicaCount := 0

	badReplicas := map[string]struct{}{}
	for _, replica := range v.Replicas {
		if replica.FailedAt != "" {
			badReplicas[replica.Name] = struct{}{}
		}
	}

	if v.Controller == nil {
		for _, rep := range v.Replicas {
			if rep.FailedAt == "" {
				healthyReplicaCount++
			}
		}
	} else {
		addr2Replica := make(map[string]*types.ReplicaInfo)
		for _, replica := range v.Replicas {
			if replica.Running() {
				addr2Replica[engineapi.GetReplicaDefaultURL(replica.IP)] = replica
			}
		}

		for addr, engineRep := range engineReps {
			if engineRep.Mode == engineapi.ReplicaModeRW {
				healthyReplicaCount++
			} else if engineRep.Mode == engineapi.ReplicaModeWO {
				rebuildingReplicaCount++
			} else {
				// means engineRep.Mode == engineapi.ReplicaModeERR
				if replica, ok := addr2Replica[addr]; ok {
					badReplicas[replica.Name] = struct{}{}
				}
			}
			if addr2Replica[addr] == nil {
				logrus.Errorf("BUG: cannot find replica address %v in replicas", addr)
			}
			delete(addr2Replica, addr)
		}
		// those replicas doesn't show up in controller as WO or RW,
		// assuming there are rebuilding
		rebuildingReplicaCount += len(addr2Replica)
	}

	state := v.State
	if v.State == types.VolumeStateCreated {
		if healthyReplicaCount == v.NumberOfReplicas {
			if v.Controller != nil {
				state = types.VolumeStateFault
			} else {
				state = types.VolumeStateDetached
			}
		}
	} else if healthyReplicaCount == 0 {
		if len(badReplicas) == 0 && v.DesireState == types.VolumeStateDeleted {
			state = types.VolumeStateDeleted
		} else {
			state = types.VolumeStateFault
		}
	} else if healthyReplicaCount < v.NumberOfReplicas {
		if v.Controller == nil {
			state = types.VolumeStateDetached
		} else {
			state = types.VolumeStateDegraded
		}
	} else if healthyReplicaCount == v.NumberOfReplicas {
		if v.Controller == nil {
			state = types.VolumeStateDetached
		} else {
			state = types.VolumeStateHealthy
		}
	} else {
		//healthyReplicaCount > v.NumberOfReplicas
		logrus.Warnf("volume %v healthy replica counts %v is more than specified %v",
			v.Name, healthyReplicaCount, v.NumberOfReplicas)
		state = types.VolumeStateDetached
	}
	v.State = state

	return badReplicas
}

func (v *ManagedVolume) Reconcile() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "fail to transit volume from state %v to %v", v.State, v.DesireState)
		}
	}()

	v.mutex.Lock()
	defer v.mutex.Unlock()

	logrus.Debugf("volume %v desire state is %v", v.Name, v.DesireState)
	if v.State == v.DesireState {
		return nil
	}

	if v.State == types.VolumeStateFault {
		if v.DesireState == types.VolumeStateDeleted {
			return v.destroy()
		}
		// we don't reconcile fault state unless user want it deleted
		return nil
	}

	switch v.DesireState {
	case types.VolumeStateDetached:
		switch v.State {
		case types.VolumeStateCreated:
			if err := v.create(); err != nil {
				return err
			}
		case types.VolumeStateHealthy:
			if err := v.stop(); err != nil {
				return err
			}
		case types.VolumeStateDegraded:
			if err := v.stop(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("BUG: illegal state transition")
		}
	case types.VolumeStateHealthy:
		switch v.State {
		case types.VolumeStateDetached:
			if err := v.start(); err != nil {
				return err
			}
		case types.VolumeStateDegraded:
			if err := v.heal(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("BUG: illegal state transition")
		}
	case types.VolumeStateDeleted:
		if err := v.destroy(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("BUG: invalid desire state %v", v.DesireState)
	}
	return nil
}
