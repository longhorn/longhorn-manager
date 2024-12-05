package monitor

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	NodeDataEngineUpgradeMonitorSyncPeriod = 3 * time.Second
)

type NodeDataEngineUpgradeMonitor struct {
	sync.RWMutex
	*baseMonitor

	upgradeManagerName string
	nodeUpgradeName    string
	syncCallback       func(key string)

	collectedData     *longhorn.NodeDataEngineUpgradeStatus
	nodeUpgradeStatus *longhorn.NodeDataEngineUpgradeStatus

	proxyConnCounter util.Counter
}

func NewNodeDataEngineUpgradeMonitor(logger logrus.FieldLogger, ds *datastore.DataStore, nodeUpgradeName, nodeID string, syncCallback func(key string)) (*NodeDataEngineUpgradeMonitor, error) {
	nodeUpgrade, err := ds.GetNodeDataEngineUpgradeRO(nodeUpgradeName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get longhorn nodeDataEngineUpgrade %v", nodeUpgradeName)
	}

	if !types.IsDataEngineV2(nodeUpgrade.Spec.DataEngine) {
		return nil, errors.Errorf("unsupported data engine %v", nodeUpgrade.Spec.DataEngine)
	}

	ctx, quit := context.WithCancel(context.Background())

	m := &NodeDataEngineUpgradeMonitor{
		baseMonitor:        newBaseMonitor(ctx, quit, logger, ds, NodeDataEngineUpgradeMonitorSyncPeriod),
		upgradeManagerName: nodeUpgrade.Spec.DataEngineUpgradeManager,
		nodeUpgradeName:    nodeUpgradeName,
		syncCallback:       syncCallback,
		collectedData:      &longhorn.NodeDataEngineUpgradeStatus{},
		nodeUpgradeStatus: &longhorn.NodeDataEngineUpgradeStatus{
			OwnerID: nodeID,
			Volumes: map[string]*longhorn.VolumeUpgradeStatus{},
		},
		proxyConnCounter: util.NewAtomicCounter(),
	}

	go m.Start()

	return m, nil
}

func (m *NodeDataEngineUpgradeMonitor) Start() {
	m.logger.Infof("Start monitoring nodeDataEngineUpgrade %v with sync period %v", m.nodeUpgradeName, m.syncPeriod)

	if err := wait.PollUntilContextCancel(m.ctx, m.syncPeriod, true, func(context.Context) (bool, error) {
		if err := m.run(struct{}{}); err != nil {
			m.logger.WithError(err).Error("Stopped monitoring upgrade monitor")
		}
		return false, nil
	}); err != nil {
		if errors.Cause(err) == context.Canceled {
			m.logger.Infof("Stopped monitoring nodeDataEngineUpgrade %v due to context cancellation", m.nodeUpgradeName)
		} else {
			m.logger.WithError(err).Error("Failed to start nodeDataEngineUpgrade monitor")
		}
	}

	m.logger.Infof("Stopped monitoring nodeDataEngineUpgrade %v", m.nodeUpgradeName)
}

func (m *NodeDataEngineUpgradeMonitor) Close() {
	m.quit()
}

func (m *NodeDataEngineUpgradeMonitor) RunOnce() error {
	return m.run(struct{}{})
}

func (m *NodeDataEngineUpgradeMonitor) UpdateConfiguration(map[string]interface{}) error {
	return nil
}

func (m *NodeDataEngineUpgradeMonitor) GetCollectedData() (interface{}, error) {
	m.RLock()
	defer m.RUnlock()
	return m.collectedData.DeepCopy(), nil
}

func (m *NodeDataEngineUpgradeMonitor) run(unused interface{}) error {
	nodeUpgrade, err := m.ds.GetNodeDataEngineUpgrade(m.nodeUpgradeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get longhorn nodeDataEngineUpgrade %v", m.nodeUpgradeName)
	}

	existingNodeUpgradeStatus := m.nodeUpgradeStatus.DeepCopy()

	m.handleNodeUpgrade(nodeUpgrade)
	if !reflect.DeepEqual(existingNodeUpgradeStatus, m.nodeUpgradeStatus) {
		func() {
			m.Lock()
			defer m.Unlock()

			m.collectedData.State = m.nodeUpgradeStatus.State
			m.collectedData.Message = m.nodeUpgradeStatus.Message
			m.collectedData.Volumes = map[string]*longhorn.VolumeUpgradeStatus{}
			for k, v := range m.nodeUpgradeStatus.Volumes {
				m.collectedData.Volumes[k] = &longhorn.VolumeUpgradeStatus{
					State:   v.State,
					Message: v.Message,
				}
			}
		}()

		key := nodeUpgrade.Namespace + "/" + m.nodeUpgradeName
		m.syncCallback(key)
	}
	return nil
}

func (m *NodeDataEngineUpgradeMonitor) handleNodeUpgrade(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	m.logger.WithFields(logrus.Fields{"nodeDataEngineUpgrade": nodeUpgrade.Name}).Debugf("Handling nodeDataEngineUpgrade %v state %v",
		nodeUpgrade.Name, m.nodeUpgradeStatus.State)

	switch m.nodeUpgradeStatus.State {
	case longhorn.UpgradeStateUndefined:
		m.handleUpgradeStateUndefined()
	case longhorn.UpgradeStateInitializing:
		m.handleUpgradeStateInitializing(nodeUpgrade)
	case longhorn.UpgradeStateFailingReplicas:
		m.handleUpgradeStateFailingReplicas(nodeUpgrade)
	case longhorn.UpgradeStateSwitchingOver:
		m.handleUpgradeStateSwitchingOver(nodeUpgrade)
	case longhorn.UpgradeStateUpgradingInstanceManager:
		m.handleUpgradeStateUpgradingInstanceManager(nodeUpgrade)
	case longhorn.UpgradeStateSwitchingBack:
		m.handleUpgradeStateSwitchingBack(nodeUpgrade)
	case longhorn.UpgradeStateRebuildingReplica:
		m.handleUpgradeStateRebuildingReplica(nodeUpgrade)
	case longhorn.UpgradeStateFinalizing:
		m.handleUpgradeStateFinalizing(nodeUpgrade)
	case longhorn.UpgradeStateCompleted, longhorn.UpgradeStateError:
		return
	default:
		m.handleUpgradeStateUnknown()
	}
}

// handleUpgradeStateUndefined handles the undefined state of the node data engine upgrade resource.
// It simply set the state to initializing and clear the message.
func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateUndefined() {
	m.nodeUpgradeStatus.State = longhorn.UpgradeStateInitializing
	m.nodeUpgradeStatus.Message = ""
}

// handleUpgradeStateInitializing handles the initializing state of the node data engine upgrade resource.
// It will check if the node is existing and ready, and if the node is not ready, it will wait until the node is ready.
// If the node is ready, it will check if the spec.dataEngineUpgradeRequested is set to true, and if not, it will set it to true.
// If the spec.dataEngineUpgradeRequested is set to true, it will check if the node is schedulable, and if the node is schedulable, it will wait until the node is unschedulable.
// If the node is unschedulable, it will list the local volumes and check if they are ready for upgrade.
// If the volumes are ready for upgrade, it will snapshot the volumes and set the state to UpgradeStateFailingReplicas.
func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateInitializing(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	var err error

	defer func() {
		if err != nil {
			// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
			m.nodeUpgradeStatus.Message = err.Error()
		}
	}()

	// Check if the node is existing and ready
	node, err := m.ds.GetNode(nodeUpgrade.Status.OwnerID)
	if err != nil {
		err = errors.Wrapf(err, "failed to get node %v for nodeDataEngineUpgrade %v", nodeUpgrade.Status.OwnerID, nodeUpgrade.Name)
		return
	}
	condition := types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeReady)
	if condition.Status != longhorn.ConditionStatusTrue {
		err = errors.Errorf("node %v is not ready", node.Name)
		return
	}

	if !node.Spec.DataEngineUpgradeRequested {
		node.Spec.DataEngineUpgradeRequested = true
		_, err = m.ds.UpdateNode(node)
		if err != nil {
			err = errors.Wrapf(err, "failed to update node %v to set DataEngineUpgradeRequested to true", nodeUpgrade.Status.OwnerID)
		}
		return
	}

	var volumes map[string]*longhorn.VolumeUpgradeStatus

	condition = types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeSchedulable)
	if condition.Status == longhorn.ConditionStatusTrue {
		// Return here and check again in the next reconciliation
		err = errors.Errorf("spec.dataEngineUpgradeRequested of node %v is set to true, but the node is still schedulable",
			nodeUpgrade.Status.OwnerID)
		return
	}

	err = m.areVolumesReadyForUpgrade(nodeUpgrade)
	if err != nil {
		return
	}

	volumes, err = m.listLocalVolumes(nodeUpgrade)
	if err != nil {
		return
	}
	m.nodeUpgradeStatus.Volumes = volumes

	// TODO: make this optional
	err = m.snapshotVolumes(volumes)
	if err != nil {
		return
	}

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateFailingReplicas
	m.nodeUpgradeStatus.Message = ""
}

// handleUpgradeStateFailingReplicas fails the replicas, waits until all replicas are stopped or failed, deletes them, and waits until all stopped or failed replicas are deleted.
// It then sets the state to UpgradeStateSwitchingOver.
func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateFailingReplicas(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	var err error

	defer func() {
		if err != nil {
			// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
			m.nodeUpgradeStatus.Message = err.Error()
		}
	}()

	err = m.failReplicas(nodeUpgrade)
	if err != nil {
		return
	}
	allStoppedOrFailed, err := m.areAllReplicasStoppedOrFailed(nodeUpgrade)
	if err != nil {
		return
	}
	if !allStoppedOrFailed {
		err = fmt.Errorf("not all replicas are stopped or failed")
		return
	}

	err = m.deleteAllStoppedOrFailedReplicas(nodeUpgrade)
	if err != nil {
		return
	}
	allDeleted, err := m.areAllStoppedOrFailedReplicasDeleted(nodeUpgrade)
	if err != nil {
		return
	}
	if !allDeleted {
		err = fmt.Errorf("not all stopped or failed replicas are deleted")
		return
	}

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateSwitchingOver
	m.nodeUpgradeStatus.Message = ""
}

// handleUpgradeStateSwitchingOver handles the SwitchingOver state of the node data engine upgrade resource.
// It finds an available node to replace the target instance, updates the volumes for switch over,
// waits until all volumes are switched over, and then sets the state to UpgradingInstanceManager.
func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateSwitchingOver(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	var err error

	defer func() {
		if err != nil {
			// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
			m.nodeUpgradeStatus.Message = err.Error()
		}
	}()

	targetNode, err := m.findAvailableNodeForTargetInstanceReplacement(nodeUpgrade)
	if err != nil {
		return
	}

	m.updateVolumesForSwitchOver(nodeUpgrade, targetNode)
	if !m.areAllVolumesSwitchedOver(nodeUpgrade) {
		err = fmt.Errorf("not all volumes are switched over")
		return
	}

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateUpgradingInstanceManager
	m.nodeUpgradeStatus.Message = ""
}

// handleUpgradeStateUpgradingInstanceManager handles the UpgradingInstanceManager state of the node data engine upgrade resource.
// It deletes the non-default instance manager, waits until the default instance manager is running,
// waits until all initiator instances are recreated, and then sets the state to SwitchingBack.
func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateUpgradingInstanceManager(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	var err error

	defer func() {
		if err != nil {
			// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
			m.nodeUpgradeStatus.Message = err.Error()
		}
	}()

	err = m.deleteNonDefaultInstanceManager(nodeUpgrade)
	if err != nil {
		return
	}

	err = m.isDefaultInstanceManagerRunning(nodeUpgrade)
	if err != nil {
		return
	}

	allRecreated, err := m.areAllInitiatorInstancesRecreated(nodeUpgrade)
	if err != nil {
		return
	}
	if !allRecreated {
		return
	}

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateSwitchingBack
	m.nodeUpgradeStatus.Message = ""
}

// handleUpgradeStateSwitchingBack handles the SwitchingBack state of the node data engine upgrade resource.
// It updates the volumes for switch back, waits until all volumes are switched back, clears the target node for all volumes,
// waits until all volumes are cleared target node, and then sets the state to RebuildingReplica.
func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateSwitchingBack(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	var err error

	defer func() {
		if err != nil {
			// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
			m.nodeUpgradeStatus.Message = err.Error()
		}
	}()

	m.updateVolumesForSwitchBack(nodeUpgrade)
	if !m.areAllVolumesSwitchedBack(nodeUpgrade) {
		err = fmt.Errorf("not all volumes are switched back")
		return
	}

	m.clearVolumesTargetNode()
	if !m.areAllVolumesClearedTargetNode() {
		err = fmt.Errorf("not all volumes are cleared target node")
		return
	}

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateRebuildingReplica
	m.nodeUpgradeStatus.Message = ""
}

// handleUpgradeStateRebuildingReplica handles the RebuildingReplica state of the node data engine upgrade resource.
// It checks if the node's spec.dataEngineUpgradeRequested is set to true, and if so, sets it to false.
// It then checks if all volumes are healthy, and if not, sets the state to error.
// If all volumes are healthy, it sets the state to Finalizing.
func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateRebuildingReplica(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	var err error

	defer func() {
		if err != nil {
			// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
			m.nodeUpgradeStatus.Message = err.Error()
		}
	}()

	node, err := m.ds.GetNode(nodeUpgrade.Status.OwnerID)
	if err != nil {
		return
	}

	if node.Spec.DataEngineUpgradeRequested {
		node.Spec.DataEngineUpgradeRequested = false
		_, err = m.ds.UpdateNode(node)
		if err != nil {
			err = errors.Wrapf(err, "failed to update node %v for updating upgrade requested to false", nodeUpgrade.Status.OwnerID)
			return
		}
	}

	allHealthy, err := m.areAllVolumeHealthy()
	if err != nil {
		err = errors.Wrap(err, "failed to check if all volumes are healthy")
		return
	}
	if !allHealthy {
		err = fmt.Errorf("not all volumes are healthy")
		return
	}

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateFinalizing
	m.nodeUpgradeStatus.Message = ""
}

// handleUpgradeStateFinalizing handles the Finalizing state of the node data engine upgrade resource.
// It will update the spec.image of all volumes to the instance manager image, and then set the state to Completed if all volumes are updated.
func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateFinalizing(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	var err error

	defer func() {
		if err != nil {
			// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
			m.nodeUpgradeStatus.Message = err.Error()
		}
	}()

	err = m.updateAllVolumesSpecImage(nodeUpgrade)
	if err != nil {
		return
	}

	if !m.areAllVolumesSpecImageUpdated() {
		err = fmt.Errorf("not all volumes spec.image is updated")
		return
	}

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateCompleted
	m.nodeUpgradeStatus.Message = ""
}

// handleUpgradeStateUnknown handles the unknown state of the node upgrade resource.
// It sets the state to UpgradeStateError and records an event.
func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateUnknown() {
	m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
	m.nodeUpgradeStatus.Message = fmt.Sprintf("Unknown state %v", m.nodeUpgradeStatus.State)
}

// updateAllVolumesSpecImage updates the spec.image of all volumes to the instance manager image specified in the node data engine upgrade resource.
// It will update the status of the node data engine upgrade resource accordingly.
// If a volume is not found, it sets the state to UpgradeStateCompleted.
// If a volume's spec.image is already up to date, it sets the state to UpgradeStateCompleted.
// If a volume's spec.image is not up to date, it updates the volume and sets the state to UpgradeStateCompleted if successful, or UpgradeStateError if not.
func (m *NodeDataEngineUpgradeMonitor) updateAllVolumesSpecImage(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (err error) {
	for volumeName := range m.nodeUpgradeStatus.Volumes {
		volume, err := m.ds.GetVolumeRO(volumeName)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[volumeName].Message = err.Error()
			if datastore.ErrorIsNotFound(err) {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateCompleted
			} else {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateError
			}
			continue
		}

		if volume.Spec.NodeID == "" && volume.Status.OwnerID == nodeUpgrade.Status.OwnerID {
			if volume.Spec.Image == nodeUpgrade.Spec.InstanceManagerImage {
				m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateCompleted
				m.nodeUpgradeStatus.Volumes[volume.Name].Message = ""
				continue
			}

			// Update the volume to the new instance manager image
			volume.Spec.Image = nodeUpgrade.Spec.InstanceManagerImage
			if _, err := m.ds.UpdateVolume(volume); err != nil {
				m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateError
				m.nodeUpgradeStatus.Volumes[volume.Name].Message = errors.Wrapf(err, "failed to update volume %v to image %v", volume.Name, nodeUpgrade.Spec.InstanceManagerImage).Error()
			} else {
				m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateCompleted
				m.nodeUpgradeStatus.Volumes[volume.Name].Message = ""
			}
		}
	}

	return nil
}

// areVolumesReadyForUpgrade checks if the volumes with replicas on the upgrading node are ready for upgrade.
func (m *NodeDataEngineUpgradeMonitor) areVolumesReadyForUpgrade(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to check if volumes are ready for upgrade")
	}()

	replicas, err := m.ds.ListReplicasByNodeRO(nodeUpgrade.Status.OwnerID)
	if err != nil {
		return errors.Wrapf(err, "failed to list replicas on node %v", nodeUpgrade.Status.OwnerID)
	}

	for _, r := range replicas {
		if r.Spec.DataEngine != nodeUpgrade.Spec.DataEngine {
			continue
		}

		volume, err := m.ds.GetVolumeRO(r.Spec.VolumeName)
		if err != nil {
			return errors.Wrapf(err, "failed to get volume %v for replica %v", r.Spec.VolumeName, r.Name)
		}

		// No need to care about the detached volumes
		if volume.Status.State == longhorn.VolumeStateDetached {
			continue
		}

		if volume.Spec.NumberOfReplicas == 1 {
			return fmt.Errorf("volume %v has only 1 replica, which is not supported for live upgrade", volume.Name)
		}

		if volume.Status.Robustness != longhorn.VolumeRobustnessHealthy {
			return fmt.Errorf("volume %v is not healthy, which is not supported for live upgrade", volume.Name)
		}
	}

	return nil
}

// snapshotVolumes creates a snapshot for all volumes with replicas on the upgrading node.
// It snapshots all volumes that are running and have replicas on the upgrading node.
// It does not snapshot volumes that are not running or do not have replicas on the upgrading node.
// It sets the state of the volume upgrade status to UpgradeStateSnapshotCreated if the snapshot is created successfully, or UpgradeStateError if not.
func (m *NodeDataEngineUpgradeMonitor) snapshotVolumes(volumes map[string]*longhorn.VolumeUpgradeStatus) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to snapshot volumes")
	}()

	for name := range volumes {
		engine, err := m.ds.GetVolumeCurrentEngine(name)
		if err != nil {
			return fmt.Errorf("failed to get volume %v for snapshot creation", name)
		}

		freezeFilesystem, err := m.ds.GetFreezeFilesystemForSnapshotSetting(engine)
		if err != nil {
			return err
		}

		if engine == nil {
			return fmt.Errorf("failed to get engine for volume %v", name)
		}

		if engine.Status.CurrentState == longhorn.InstanceStateRunning {
			engineCliClient, err := engineapi.GetEngineBinaryClient(m.ds, engine.Spec.VolumeName, m.nodeUpgradeStatus.OwnerID)
			if err != nil {
				return err
			}

			engineClientProxy, err := engineapi.GetCompatibleClient(engine, engineCliClient, m.ds, m.logger, m.proxyConnCounter)
			if err != nil {
				return err
			}
			defer engineClientProxy.Close()

			snapLabels := map[string]string{types.GetLonghornLabelKey(types.LonghornLabelSnapshotForDataEngineLiveUpgrade): m.nodeUpgradeName}
			_, err = engineClientProxy.SnapshotCreate(engine, m.upgradeManagerName+"-"+util.RandomID(), snapLabels, freezeFilesystem)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// clearVolumesTargetNode clears the targetNodeID of all volumes that are managed by the node upgrade monitor.
// If a volume is not found, it sets the state to UpgradeStateCompleted.
// If a volume's targetNodeID is empty, it sets the state to UpgradeStateCompleted.
// If a volume's targetNodeID is not empty, it updates the volume to clear the targetNodeID and sets the state to UpgradeStateCompleted if successful, or UpgradeStateError if not.
func (m *NodeDataEngineUpgradeMonitor) clearVolumesTargetNode() {
	for volumeName := range m.nodeUpgradeStatus.Volumes {
		volume, err := m.ds.GetVolumeRO(volumeName)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[volumeName].Message = err.Error()
			if datastore.ErrorIsNotFound(err) {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateCompleted
			} else {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateError
			}
			continue
		}

		if volume.Spec.TargetNodeID == "" {
			m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateCompleted
			continue
		}

		volume.Spec.TargetNodeID = ""
		_, err = m.ds.UpdateVolume(volume)
		if err != nil {
			m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Message = errors.Wrap(err, "failed to empty spec.targetNodeID").Error()
			continue
		}

		m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateCompleted
		m.nodeUpgradeStatus.Volumes[volumeName].Message = ""
	}
}

// areAllVolumeHealthy checks if all volumes are healthy before proceeding to the next state.
// It skips volumes that are detached.
// Returns true if all volumes are healthy, false otherwise.
func (m *NodeDataEngineUpgradeMonitor) areAllVolumeHealthy() (bool, error) {
	volumes, err := m.ds.ListVolumesRO()
	if err != nil {
		return false, errors.Wrapf(err, "failed to list volumes")
	}

	for _, volume := range volumes {
		if volume.Status.State == longhorn.VolumeStateDetached {
			continue
		}

		if volume.Status.Robustness != longhorn.VolumeRobustnessHealthy {
			return false, fmt.Errorf("need to make sure all volumes are healthy before proceeding to the next state")
		}
	}

	return true, nil
}

// areAllInitiatorInstancesRecreated returns true if all the initiator instances for the given node data engine upgrade have been recreated,
// false if not all have been recreated, and an error if there's any error during the check.
// It will set the state of the volume to UpgradeStateError if there's any error during the check.
func (m *NodeDataEngineUpgradeMonitor) areAllInitiatorInstancesRecreated(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (allRecreated bool, err error) {
	im, err := m.ds.GetDefaultInstanceManagerByNodeRO(nodeUpgrade.Status.OwnerID, longhorn.DataEngineTypeV2)
	if err != nil {
		return false, err
	}

	allRecreated = true
	for volumeName := range m.nodeUpgradeStatus.Volumes {
		if m.nodeUpgradeStatus.Volumes[volumeName].State == longhorn.UpgradeStateCompleted {
			continue
		}

		volume, err := m.ds.GetVolumeRO(volumeName)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[volumeName].Message = err.Error()
			if datastore.ErrorIsNotFound(err) {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateCompleted
			} else {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateError
				allRecreated = false
			}
			continue
		}

		if volume.Status.State == longhorn.VolumeStateDetached {
			m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateCompleted
			m.nodeUpgradeStatus.Volumes[volumeName].Message = "Volume is detached"
			continue
		}

		engine, err := m.ds.GetVolumeCurrentEngine(volumeName)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[volumeName].Message = err.Error()
			continue
		}

		_, ok := im.Status.InstanceEngines[engine.Name]
		if !ok {
			allRecreated = false
		}
	}

	return allRecreated, nil
}

// listLocalVolumes lists all local volumes that are running on the node and
// target the same data engine as the node data engine upgrade. It returns a
// map from volume name to volume upgrade status.
func (m *NodeDataEngineUpgradeMonitor) listLocalVolumes(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (volumeUpgradeStatus map[string]*longhorn.VolumeUpgradeStatus, err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to list volumes on node %v", nodeUpgrade.Status.OwnerID)
	}()

	volumes, err := m.ds.ListVolumesRO()
	if err != nil {
		return nil, err
	}

	volumeUpgradeStatus = make(map[string]*longhorn.VolumeUpgradeStatus)
	for _, volume := range volumes {
		if volume.Status.OwnerID != nodeUpgrade.Status.OwnerID {
			continue
		}

		if volume.Spec.DataEngine != nodeUpgrade.Spec.DataEngine {
			continue
		}

		volumeUpgradeStatus[volume.Name] = &longhorn.VolumeUpgradeStatus{
			State: longhorn.UpgradeStateInitializing,
		}
	}

	return volumeUpgradeStatus, nil
}

// areAllStoppedOrFailedReplicasDeleted checks if all replicas on the node are stopped or failed and deleted.
func (m *NodeDataEngineUpgradeMonitor) areAllStoppedOrFailedReplicasDeleted(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (allDeleted bool, err error) {
	replicas, err := m.ds.ListReplicasByNodeRO(nodeUpgrade.Status.OwnerID)
	if err != nil {
		return false, err
	}

	return len(replicas) == 0, nil
}

// areAllReplicasStoppedOrFailed checks if all replicas on the node are stopped or failed.
func (m *NodeDataEngineUpgradeMonitor) areAllReplicasStoppedOrFailed(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (allStoppedOrFailed bool, err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to check if all replicas are stopped or failed")
	}()

	replicas, err := m.ds.ListReplicasByNodeRO(nodeUpgrade.Status.OwnerID)
	if err != nil {
		return false, err
	}

	for _, r := range replicas {
		if r.Status.CurrentState != longhorn.InstanceStateStopped && r.Spec.DesireState != longhorn.InstanceStateError {
			return false, fmt.Errorf("not all replicas are stopped or failed")
		}
	}

	return true, nil
}

// deleteAllStoppedOrFailedReplicas deletes all stopped or failed replicas on the node. This function should only be called when the node is being upgraded and all replicas are stopped or failed.
func (m *NodeDataEngineUpgradeMonitor) deleteAllStoppedOrFailedReplicas(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (err error) {
	replicas, err := m.ds.ListReplicasByNodeRO(nodeUpgrade.Status.OwnerID)
	if err != nil {
		return err
	}

	for _, r := range replicas {
		if r.Status.CurrentState == longhorn.InstanceStateStopped || r.Spec.DesireState == longhorn.InstanceStateError {
			if errDelete := m.ds.DeleteReplica(r.Name); errDelete != nil && !datastore.ErrorIsNotFound(errDelete) {
				if err == nil {
					err = errors.Wrapf(errDelete, "failed to delete replicas on node %v", nodeUpgrade.Status.OwnerID)
				}
			}
		}
	}

	return err
}

// isDefaultInstanceManagerRunning checks if the default instance manager for the given node
// is in the running state. It returns an error if the instance manager cannot be retrieved or
// if the instance manager is not in the running state.
func (m *NodeDataEngineUpgradeMonitor) isDefaultInstanceManagerRunning(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (err error) {
	im, err := m.ds.GetDefaultInstanceManagerByNodeRO(nodeUpgrade.Status.OwnerID, longhorn.DataEngineTypeV2)
	if err != nil {
		return errors.Wrapf(err, "failed to get default instance manager for node %v", nodeUpgrade.Status.OwnerID)
	}

	if im.Status.CurrentState != longhorn.InstanceManagerStateRunning {
		return errors.Wrapf(err, "instance manager %v is not running and in state %v", im.Name, im.Status.CurrentState)
	}

	return nil
}

// failReplicas sets the desired state of all replicas on the specified node to "stopped"
// and updates their failure timestamp. If a replica is already stopped, it is skipped.
// The function attempts to update each replica in the datastore and returns an error
// if any update operation fails.
func (m *NodeDataEngineUpgradeMonitor) failReplicas(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (err error) {
	replicas, err := m.ds.ListReplicasByNodeRO(nodeUpgrade.Status.OwnerID)
	if err != nil {
		return err
	}

	for _, r := range replicas {
		if r.Spec.DesireState == longhorn.InstanceStateStopped {
			continue
		}

		setReplicaFailedAt(r, util.Now())
		r.Spec.DesireState = longhorn.InstanceStateStopped

		_, errUpdate := m.ds.UpdateReplica(r)
		if errUpdate != nil {
			if err != nil {
				err = errors.Wrapf(errUpdate, "failed to fail replica %v on node %v", r.Name, nodeUpgrade.Status.OwnerID)
			}
		}
	}

	return err
}

// r.Spec.FailedAt and r.Spec.LastFailedAt should both be set when a replica failure occurs.
// r.Spec.FailedAt may be cleared (before rebuilding), but r.Spec.LastFailedAt must not be.
func setReplicaFailedAt(r *longhorn.Replica, timestamp string) {
	r.Spec.FailedAt = timestamp
	if timestamp != "" {
		r.Spec.LastFailedAt = timestamp
	}
}

// findAvailableNodeForTargetInstanceReplacement finds an available node to replace the target instance.
// The preferred node is the one that has completed the upgrade.
// If no node has completed the upgrade, it will return any available node.
// If no node is available, it will return an error.
func (m *NodeDataEngineUpgradeMonitor) findAvailableNodeForTargetInstanceReplacement(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (availableNode string, err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to find available node for target instance replacement")
	}()

	upgradeManager, err := m.ds.GetDataEngineUpgradeManager(nodeUpgrade.Spec.DataEngineUpgradeManager)
	if err != nil {
		return "", err
	}

	ims, err := m.ds.ListInstanceManagersBySelectorRO("", "", longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2)
	if err != nil {
		return "", err
	}

	availableNode = ""
	for _, im := range ims {
		if im.Status.CurrentState != longhorn.InstanceManagerStateRunning {
			continue
		}

		if im.Spec.NodeID == nodeUpgrade.Status.OwnerID {
			continue
		}

		if availableNode == "" {
			availableNode = im.Spec.NodeID
		}

		upgradeNodeStatus, ok := upgradeManager.Status.UpgradeNodes[im.Spec.NodeID]
		if !ok {
			continue
		}

		// Prefer the node that has completed the upgrade
		if upgradeNodeStatus.State == longhorn.UpgradeStateCompleted {
			availableNode = im.Spec.NodeID
			break
		}
	}

	if availableNode == "" {
		return "", fmt.Errorf("failed to find available node for target")
	}

	return availableNode, nil
}

// updateVolumesForSwitchOver updates the volumes for switch over.
// It will update the volumes with the new instance manager image and target node.
// If a volume is detached, no need to switch over,
// because it will use the default instance manager after being attached.
// If a volume is not found, we don't need to switch over it and therefore consider it as completed.
// If a volume is in neither detached nor attached state, it will set the state to UpgradeStateError.
// If a volume is being switched over, it will not update the volume.
// If a volume is not being switched over, it will update the volume with the new instance manager image and target node.
func (m *NodeDataEngineUpgradeMonitor) updateVolumesForSwitchOver(nodeUpgrade *longhorn.NodeDataEngineUpgrade, tempTargetNode string) {
	for volumeName := range nodeUpgrade.Status.Volumes {
		volume, errGet := m.ds.GetVolume(volumeName)
		if errGet != nil {
			m.nodeUpgradeStatus.Volumes[volumeName].Message = errGet.Error()
			if datastore.ErrorIsNotFound(errGet) {
				// If the volume is not found, we don't need to switch over it and therefore consider it as completed.
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateCompleted
			} else {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateError
			}
			continue
		}

		// If a volume is detached, no need to switch over,
		// because it will use the default instance manager after being attached.
		if volume.Status.State == longhorn.VolumeStateDetached {
			m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateCompleted
			m.nodeUpgradeStatus.Volumes[volume.Name].Message = "Volume is detached, so no need to switch over"
			continue
		}

		if volume.Status.State != longhorn.VolumeStateAttached {
			m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[volumeName].Message = fmt.Sprintf("Volume %v is in neither detached nor attached state", volume.Name)
			continue
		}

		// If a volume's targetNodeID is not empty, it means the volume is being switched over
		if volume.Spec.TargetNodeID != "" {
			if volume.Spec.TargetNodeID != volume.Spec.NodeID {
				m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateSwitchingOver
				continue
			}
		}

		if volume.Spec.Image != nodeUpgrade.Spec.InstanceManagerImage || volume.Spec.TargetNodeID != tempTargetNode {
			volume.Spec.Image = nodeUpgrade.Spec.InstanceManagerImage
			volume.Spec.TargetNodeID = tempTargetNode

			_, errUpdate := m.ds.UpdateVolume(volume)
			if errUpdate != nil {
				errUpdate = errors.Wrapf(errUpdate, "failed to update volume %v to image %v and target node %v for switch over",
					volumeName, nodeUpgrade.Spec.InstanceManagerImage, tempTargetNode)
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateError
				m.nodeUpgradeStatus.Volumes[volumeName].Message = errUpdate.Error()
				continue
			}

			m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateSwitchingOver
		}
	}
}

// updateVolumesForSwitchBack updates the volumes for switch back.
// It will update the volumes with the original target node.
// If a volume is detached, no need to switch back,
// because it will use the default instance manager after being attached.
// If a volume is in neither detached nor attached state, it will set the state to UpgradeStateError.
// If a volume's targetNodeID is not empty, it means the volume is being switched back
// If a volume is not being switched back, it will update the volume with the original target node.
func (m *NodeDataEngineUpgradeMonitor) updateVolumesForSwitchBack(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	targetNodeID := nodeUpgrade.Status.OwnerID

	for volumeName := range nodeUpgrade.Status.Volumes {
		volume, err := m.ds.GetVolume(volumeName)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[volumeName].Message = err.Error()
			if datastore.ErrorIsNotFound(err) {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateCompleted
			} else {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateError
			}
			continue
		}

		if volume.Status.State == longhorn.VolumeStateDetached {
			m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateCompleted
			m.nodeUpgradeStatus.Volumes[volume.Name].Message = "Volume is detached"
			continue
		}

		if volume.Status.State != longhorn.VolumeStateAttached {
			m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[volume.Name].Message = fmt.Sprintf("Volume %v is in neither detached nor attached state", volume.Name)
			continue
		}

		if volume.Spec.TargetNodeID == "" {
			m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateCompleted
			continue
		}

		if volume.Spec.TargetNodeID == volume.Spec.NodeID {
			m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateSwitchingBack
			continue
		}

		if volume.Spec.TargetNodeID != targetNodeID {
			volume.Spec.TargetNodeID = targetNodeID
			_, err := m.ds.UpdateVolume(volume)
			if err != nil {
				m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateError
				m.nodeUpgradeStatus.Volumes[volume.Name].Message = err.Error()
				continue
			}
			m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateSwitchingBack
			m.nodeUpgradeStatus.Volumes[volume.Name].Message = ""
		}
	}
}

// areAllVolumesSwitchedOver checks if all volumes are switched over to the target node and new instance manager image.
// It will update the upgrade status of each volume accordingly.
// If a volume is not found, it will set the state to UpgradeStateCompleted.
// If the volume is in neither detached nor attached state, it will set the state to UpgradeStateError.
// If the volume is being switched over, it will not update the volume.
// If the volume is not being switched over, it will update the volume with the original target node and new instance manager image.
func (m *NodeDataEngineUpgradeMonitor) areAllVolumesSwitchedOver(nodeUpgrade *longhorn.NodeDataEngineUpgrade) bool {
	allSwitched := true
	for name := range m.nodeUpgradeStatus.Volumes {
		if m.nodeUpgradeStatus.Volumes[name].State == longhorn.UpgradeStateCompleted {
			continue
		}

		volume, err := m.ds.GetVolume(name)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[name].Message = err.Error()
			if datastore.ErrorIsNotFound(err) {
				m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateCompleted
			} else {
				m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
				allSwitched = false
			}
			continue
		}

		if volume.Spec.TargetNodeID != volume.Status.CurrentTargetNodeID ||
			volume.Spec.Image != volume.Status.CurrentImage ||
			volume.Status.CurrentImage != nodeUpgrade.Spec.InstanceManagerImage {
			allSwitched = false
			continue
		}

		m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateSwitchedOver
		m.nodeUpgradeStatus.Volumes[name].Message = ""
	}
	return allSwitched
}

// areAllVolumesSwitchedBack checks if all volumes are switched back to the target node and original instance manager image.
// It will update the upgrade status of each volume accordingly.
// If a volume is not found, it will set the state to UpgradeStateCompleted.
// If the volume is in neither detached nor attached state, it will set the state to UpgradeStateError.
// If the volume is being switched back, it will not update the volume.
// If the volume is not being switched back, it will update the volume with the original target node and original instance manager image.
func (m *NodeDataEngineUpgradeMonitor) areAllVolumesSwitchedBack(nodeUpgrade *longhorn.NodeDataEngineUpgrade) bool {
	allSwitched := true

	for volumeName := range m.nodeUpgradeStatus.Volumes {
		if m.nodeUpgradeStatus.Volumes[volumeName].State == longhorn.UpgradeStateCompleted {
			continue
		}

		volume, err := m.ds.GetVolumeRO(volumeName)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[volumeName].Message = err.Error()
			if datastore.ErrorIsNotFound(err) {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateCompleted
			} else {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateError
				allSwitched = false
			}
			continue
		}

		if volume.Spec.TargetNodeID != volume.Spec.NodeID ||
			volume.Spec.TargetNodeID != volume.Status.CurrentTargetNodeID ||
			volume.Spec.Image != volume.Status.CurrentImage ||
			volume.Status.CurrentImage != nodeUpgrade.Spec.InstanceManagerImage {
			allSwitched = false
			m.nodeUpgradeStatus.Volumes[volumeName].Message = fmt.Sprintf("Volume %v is not switched back to the target node %v", volumeName, nodeUpgrade.Status.OwnerID)
			continue
		}

		m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateSwitchedBack
		m.nodeUpgradeStatus.Volumes[volumeName].Message = ""
	}
	return allSwitched
}

// areAllVolumesClearedTargetNode checks if all volumes have been cleared of their target node.
// It returns true if all volumes have reached the UpgradeStateCompleted state, indicating that
// they are no longer associated with a target node.
func (m *NodeDataEngineUpgradeMonitor) areAllVolumesClearedTargetNode() bool {
	for volumeName := range m.nodeUpgradeStatus.Volumes {
		if m.nodeUpgradeStatus.Volumes[volumeName].State != longhorn.UpgradeStateCompleted {
			return false
		}
	}
	return true
}

// areAllVolumesSpecImageUpdated checks if all volumes have been updated to the specified image in their specifications.
// It returns true if all volumes have reached the UpgradeStateCompleted state, indicating the update is complete.
func (m *NodeDataEngineUpgradeMonitor) areAllVolumesSpecImageUpdated() bool {
	for volumeName := range m.nodeUpgradeStatus.Volumes {
		if m.nodeUpgradeStatus.Volumes[volumeName].State != longhorn.UpgradeStateCompleted {
			return false
		}
	}
	return true
}

// deleteNonDefaultInstanceManager deletes instance managers that do not match the specified image
// in the node data engine upgrade specification. It logs the deletion of each non-default instance manager
// and returns an error if any deletions fail. The function retrieves instance managers for the node
// identified by the owner ID in the node upgrade status and filters out those matching the target image.
func (m *NodeDataEngineUpgradeMonitor) deleteNonDefaultInstanceManager(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (err error) {
	log := m.logger.WithFields(logrus.Fields{"nodeDataEngineUpgrade": nodeUpgrade.Name})

	ims, err := m.ds.ListInstanceManagersByNodeRO(nodeUpgrade.Status.OwnerID, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2)
	if err != nil {
		return errors.Wrapf(err, "failed to list instance managers for node %v for deleting non-default instance managers", nodeUpgrade.Status.OwnerID)
	}

	for _, im := range ims {
		if im.Spec.Image == nodeUpgrade.Spec.InstanceManagerImage {
			continue
		}

		log.Infof("Deleting non-default instance manager %v", im.Name)
		if errDelete := m.ds.DeleteInstanceManager(im.Name); errDelete != nil {
			err = multierr.Append(err, errors.Wrapf(errDelete, "failed to delete non-default instance manager %v", im.Name))
		}
	}

	return err
}
