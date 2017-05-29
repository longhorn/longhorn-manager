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

	volumeChan := m.getManagedVolumeChan(volumeName)
	volumeChan.Notify <- struct{}{}

	return nil
}

func (m *VolumeManager) getManagedVolumeChan(volumeName string) VolumeChan {
	m.managedVolumesMutex.Lock()
	defer m.managedVolumesMutex.Unlock()

	volumeChan, ok := m.managedVolumes[volumeName]
	if !ok {
		volumeChan = VolumeChan{
			Notify: make(chan struct{}),
		}
		m.managedVolumes[volumeName] = volumeChan
		go m.processVolume(volumeName, volumeChan)
	}
	return volumeChan
}

func (m *VolumeManager) processVolume(volumeName string, volumeChan VolumeChan) {
	defer m.releaseVolume(volumeName)

	tick := time.NewTicker(ReconcileInterval)
	for {
		select {
		case <-tick.C:
			break
		case <-volumeChan.Notify:
			break
		}
		volume, err := m.GetVolume(volumeName)
		if err != nil {
			logrus.Errorf("Fail get volume: %v", err)
			continue
		}
		if volume.TargetNodeID != m.currentNode.ID {
			logrus.Infof("Volume %v no longer belong to current node, release it", volumeName)
			break
		}

		if err := volume.RefreshState(); err != nil {
			logrus.Errorf("Fail to refresh volume state: %v", err)
			continue
		}
		logrus.Debugf("volume %v state is %v", volumeName, volume.State)

		if err := volume.Cleanup(); err != nil {
			logrus.Errorf("Fail to cleanup stale replicas: %v", err)
		}

		logrus.Debugf("volume %v desire state is %v", volumeName, volume.DesireState)

		if err := volume.Reconcile(); err != nil {
			logrus.Errorf("Fail to reconcile volume state: %v", err)
		}
		logrus.Debugf("volume %v refreshed state is %v", volumeName, volume.State)
		if volume.State == types.VolumeStateDeleted {
			break
		}
	}
}

func (m *VolumeManager) releaseVolume(volumeName string) {
	m.managedVolumesMutex.Lock()
	defer m.managedVolumesMutex.Unlock()

	delete(m.managedVolumes, volumeName)

	volume, err := m.GetVolume(volumeName)
	if err != nil {
		logrus.Errorf("Fail to release volume: %v", err)
		return
	}

	if volume.TargetNodeID != m.currentNode.ID {
		return
	}
	if volume.State == types.VolumeStateDeleted {
		if err := m.kv.DeleteVolume(volumeName); err != nil {
			logrus.Errorf("Fail to remove volume entry from kvstore: %v", err)
		}
		return
	}
	logrus.Errorf("BUG: release volume processed but don't know the reason")
}

func (v *Volume) RefreshState() (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "cannot refresh volume state")
		}
	}()

	engineReps := map[string]*engineapi.Replica{}
	if v.Controller != nil {
		engine, err := v.m.engines.NewEngineClient(&engineapi.EngineClientRequest{
			VolumeName:    v.Name,
			ControllerURL: engineapi.GetControllerDefaultURL(v.Controller.IP),
		})
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

	v.syncWithEngineState(engineReps)

	for name := range v.badReplicas {
		if err := v.markBadReplica(name); err != nil {
			return err
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
	}

	if err := v.m.kv.UpdateVolume(&v.VolumeInfo); err != nil {
		return err
	}
	return nil
}

func (v *Volume) syncWithEngineState(engineReps map[string]*engineapi.Replica) {
	healthyReplicaCount := 0
	rebuildingReplicaCount := 0

	for _, replica := range v.Replicas {
		if replica.FailedAt != "" {
			v.badReplicas[replica.Name] = struct{}{}
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
			v.badReplicas[replica.Name] = struct{}{}
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
		if len(v.badReplicas) == 0 && v.DesireState == types.VolumeStateDeleted {
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
}

func (v *Volume) Reconcile() (err error) {
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
