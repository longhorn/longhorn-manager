package manager

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/yasker/lm-rewrite/engineapi"
	"github.com/yasker/lm-rewrite/types"
)

var (
	ConfirmationInterval = 5 * time.Second
	ConfirmationCounts   = 6

	ReconcileInterval = 5 * time.Second
)

func (m *VolumeManager) startProcessing() {
	volumes, err := m.VolumeList()
	if err != nil {
		logrus.Fatalf("fail to acquire existing volume list")
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
			break
		default:
			logrus.Errorf("Unrecongized event %+v", event)
		}
	}
}

func (m *VolumeManager) notifyVolume(volumeName string) (err error) {
	defer func() {
		if err != nil {
			logrus.Errorf("fail to notify volume due to %v", err)
		}
	}()
	currentNode := m.GetCurrentNode()
	// wait until we can confirm that this node owns volume
	for i := 0; i < ConfirmationCounts; i++ {
		volume, err := m.GetVolume(volumeName)
		if err == nil {
			if volume == nil {
				logrus.Errorf("volume %v has been deleted", volumeName)
				return nil
			}
			if volume.TargetNodeID == currentNode.ID {
				volume.NodeID = currentNode.ID
				if err := m.kv.UpdateVolume(&volume.VolumeInfo); err != nil {
					return err
				}
				break
			} else {
				err = fmt.Errorf("target node ID %v doesn't match with the current one %v",
					volume.TargetNodeID, currentNode.ID)
			}
		}
		time.Sleep(ConfirmationInterval)
	}
	if err != nil {
		return err
	}

	volume, err := m.getManagedVolume(volumeName)
	if err != nil {
		return err
	}
	volume.Notify <- struct{}{}

	return nil
}

func (v *ManagedVolume) process() {
	defer v.m.releaseVolume(v.Name)

	tick := time.NewTicker(ReconcileInterval)
	for {
		select {
		case <-tick.C:
			break
		case <-v.Notify:
			break
		}
		if err := v.refresh(); err != nil {
			logrus.Errorf("Fail get volume: %v", err)
			continue
		}
		if v.TargetNodeID != v.m.currentNode.ID {
			logrus.Infof("Volume %v no longer belong to current node, release it", v.Name)
			break
		}

		if err := v.RefreshState(); err != nil {
			logrus.Errorf("Fail to refresh volume state: %v", err)
			continue
		}
		logrus.Debugf("volume %v state is %v", v.Name, v.State)

		if err := v.Cleanup(); err != nil {
			logrus.Errorf("Fail to cleanup stale replicas: %v", err)
		}

		logrus.Debugf("volume %v desire state is %v", v.Name, v.DesireState)

		if err := v.Reconcile(); err != nil {
			logrus.Errorf("Fail to reconcile volume state: %v", err)
		}
		logrus.Debugf("volume %v refreshed state is %v", v.Name, v.State)
		if v.State == types.VolumeStateDeleted {
			break
		}
	}
}

func (v *ManagedVolume) RefreshState() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "cannot refresh volume state")
		}
	}()

	engineReps := map[string]*engineapi.Replica{}
	if v.Controller != nil {
		engine, err := v.GetEngineClient()
		if err != nil {
			return err
		}
		engineReps, err = engine.ReplicaList()
		if err != nil {
			return err
		}
		for url, rep := range engineReps {
			if rep.Mode == engineapi.ReplicaModeERR {
				if err := engine.ReplicaRemove(url); err != nil {
					logrus.Errorf("fail to clean up ERR replica %v for volume %v", url, v.Name)
				}
			}
		}
	}

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
			if !replica.Running {
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

	if err := v.m.kv.UpdateVolume(&v.VolumeInfo); err != nil {
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
			if replica.Running {
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
				continue
			}
			if addr2Replica[addr] == nil {
				logrus.Errorf("BUG: cannot find replica address %v in replicas", addr)
			}
			delete(addr2Replica, addr)
		}
		// those replicas doesn't show up in controller as WO or RW
		for _, replica := range addr2Replica {
			badReplicas[replica.Name] = struct{}{}
		}
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

	defer func() {
		if err == nil {
			err = v.RefreshState()
		}
	}()
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
