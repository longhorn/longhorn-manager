package monitor

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"

	"k8s.io/apimachinery/pkg/util/wait"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	DataEngineUpgradeMonitorMonitorSyncPeriod = 3 * time.Second
)

type DataEngineUpgradeManagerMonitor struct {
	sync.RWMutex
	*baseMonitor

	upgradeManagerName string
	syncCallback       func(key string)

	collectedData        *longhorn.DataEngineUpgradeManagerStatus
	upgradeManagerStatus *longhorn.DataEngineUpgradeManagerStatus
}

func NewDataEngineUpgradeManagerMonitor(logger logrus.FieldLogger, ds *datastore.DataStore, upgradeManagerName, nodeID string, syncCallback func(key string)) (*DataEngineUpgradeManagerMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())

	m := &DataEngineUpgradeManagerMonitor{
		baseMonitor:        newBaseMonitor(ctx, quit, logger, ds, DataEngineUpgradeMonitorMonitorSyncPeriod),
		upgradeManagerName: upgradeManagerName,
		syncCallback:       syncCallback,
		collectedData:      &longhorn.DataEngineUpgradeManagerStatus{},
		upgradeManagerStatus: &longhorn.DataEngineUpgradeManagerStatus{
			OwnerID: nodeID,
		},
	}

	go m.Start()

	return m, nil
}

func (m *DataEngineUpgradeManagerMonitor) Start() {
	m.logger.Infof("Start monitoring dataEngineUpgradeManager %v with sync period %v", m.upgradeManagerName, m.syncPeriod)

	if err := wait.PollUntilContextCancel(m.ctx, m.syncPeriod, true, func(context.Context) (bool, error) {
		if err := m.run(struct{}{}); err != nil {
			m.logger.WithError(err).Error("Stopped monitoring upgrade monitor")
		}
		return false, nil
	}); err != nil {
		if errors.Cause(err) == context.Canceled {
			m.logger.Infof("Stopped monitoring dataEngineUpgradeManager %v due to context cancellation", m.upgradeManagerName)
		} else {
			m.logger.WithError(err).Error("Failed to start dataEngineUpgradeManager monitor")
		}
	}

	m.logger.Infof("Stopped monitoring dataEngineUpgradeManager %v", m.upgradeManagerName)
}

func (m *DataEngineUpgradeManagerMonitor) Close() {
	m.quit()
}

func (m *DataEngineUpgradeManagerMonitor) RunOnce() error {
	return m.run(struct{}{})
}

func (m *DataEngineUpgradeManagerMonitor) UpdateConfiguration(map[string]interface{}) error {
	return nil
}

func (m *DataEngineUpgradeManagerMonitor) GetCollectedData() (interface{}, error) {
	m.RLock()
	defer m.RUnlock()
	return m.collectedData.DeepCopy(), nil
}

func (m *DataEngineUpgradeManagerMonitor) run(value interface{}) error {
	upgradeManager, err := m.ds.GetDataEngineUpgradeManager(m.upgradeManagerName)
	if err != nil {
		return errors.Wrapf(err, "failed to get longhorn dataEngineUpgradeManager %v", m.upgradeManagerName)
	}

	existingUpgradeManagerStatus := m.upgradeManagerStatus.DeepCopy()

	m.handleUpgradeManager(upgradeManager)
	if !reflect.DeepEqual(existingUpgradeManagerStatus, m.upgradeManagerStatus) {
		func() {
			m.Lock()
			defer m.Unlock()

			m.collectedData.InstanceManagerImage = m.upgradeManagerStatus.InstanceManagerImage
			m.collectedData.State = m.upgradeManagerStatus.State
			m.collectedData.Message = m.upgradeManagerStatus.Message
			m.collectedData.UpgradingNode = m.upgradeManagerStatus.UpgradingNode
			m.collectedData.UpgradeNodes = map[string]*longhorn.UpgradeNodeStatus{}
			for k, v := range m.upgradeManagerStatus.UpgradeNodes {
				m.collectedData.UpgradeNodes[k] = &longhorn.UpgradeNodeStatus{
					State:   v.State,
					Message: v.Message,
				}
			}
		}()

		key := upgradeManager.Namespace + "/" + m.upgradeManagerName
		m.syncCallback(key)
	}
	return nil
}

func (m *DataEngineUpgradeManagerMonitor) handleUpgradeManager(upgradeManager *longhorn.DataEngineUpgradeManager) {
	log := m.logger.WithFields(logrus.Fields{"dataEngineUpgradeManager": upgradeManager.Name})
	log.Infof("Handling dataEngineUpgradeManager %v state %v", upgradeManager.Name, m.upgradeManagerStatus.State)

	switch m.upgradeManagerStatus.State {
	case longhorn.UpgradeStateUndefined:
		m.handleUpgradeStateUndefined(upgradeManager)
	case longhorn.UpgradeStateInitializing:
		m.handleUpgradeStateInitializing(upgradeManager)
	case longhorn.UpgradeStateUpgrading:
		m.handleUpgradeStateUpgrading(upgradeManager)
	case longhorn.UpgradeStateCompleted, longhorn.UpgradeStateError:
		return
	default:
		m.handleUpgradeStateUnknown()
	}
}

func (m *DataEngineUpgradeManagerMonitor) handleUpgradeStateUndefined(upgradeManager *longhorn.DataEngineUpgradeManager) {
	var err error

	defer func() {
		if err != nil {
			m.upgradeManagerStatus.State = longhorn.UpgradeStateError
			m.upgradeManagerStatus.Message = err.Error()
		}
	}()

	if !types.IsDataEngineV2(upgradeManager.Spec.DataEngine) {
		err = fmt.Errorf("unsupported data engine %v", upgradeManager.Spec.DataEngine)
		return
	}

	if m.upgradeManagerStatus.UpgradeNodes == nil {
		m.upgradeManagerStatus.UpgradeNodes = map[string]*longhorn.UpgradeNodeStatus{}
	}

	defaultInstanceManagerImage, err := m.ds.GetSettingValueExisted(types.SettingNameDefaultInstanceManagerImage)
	if err != nil {
		err = errors.Wrap(err, "failed to get default instance manager image")
		return
	}

	m.upgradeManagerStatus.State = longhorn.UpgradeStateInitializing
	m.upgradeManagerStatus.InstanceManagerImage = defaultInstanceManagerImage

	err = m.checkInstanceManagerImageReady(upgradeManager, defaultInstanceManagerImage)
	if err != nil {
		err = errors.Wrapf(err, "failed to check instance manager image %v ready", defaultInstanceManagerImage)
	}
}

func (m *DataEngineUpgradeManagerMonitor) checkInstanceManagerImageReady(upgradeManager *longhorn.DataEngineUpgradeManager, defaultInstanceManagerImage string) (err error) {
	for _, nodeName := range upgradeManager.Spec.Nodes {
		kubeNode, errGet := m.ds.GetKubernetesNodeRO(nodeName)
		if errGet != nil {
			err = multierr.Append(err, errors.Wrapf(errGet, "failed to get kubeNode %v", nodeName))
			continue
		}

		foundImage := false
		for _, image := range kubeNode.Status.Images {
			for _, name := range image.Names {
				if strings.Contains(name, defaultInstanceManagerImage) {
					foundImage = true
					break
				}
			}
			if foundImage {
				break
			}
		}
		if !foundImage {
			err = multierr.Append(err, fmt.Errorf("failed to find default instance manager image %v in node %v",
				defaultInstanceManagerImage, nodeName))
		}
	}

	return err
}

func (m *DataEngineUpgradeManagerMonitor) handleUpgradeStateInitializing(upgradeManager *longhorn.DataEngineUpgradeManager) {
	if len(upgradeManager.Spec.Nodes) == 0 {
		nodes, err := m.ds.ListNodesRO()
		if err != nil {
			m.upgradeManagerStatus.State = longhorn.UpgradeStateError
			m.upgradeManagerStatus.Message = fmt.Sprintf("Failed to list nodes for dataEngineUpgradeManager resource %v: %v", upgradeManager.Name, err.Error())
			return
		}

		for _, node := range nodes {
			// Skip the node if v2 data engine is disabled
			v2DataEngineDisabled, err := m.ds.IsV2DataEngineDisabledForNode(node.Name)
			if err != nil {
				m.upgradeManagerStatus.State = longhorn.UpgradeStateError
				m.upgradeManagerStatus.Message = fmt.Sprintf("Failed to check if v2 data engine is disabled for node %v for dataEngineUpgradeManager resource %v: %v", node.Name, upgradeManager.Name, err.Error())
				return
			}
			if v2DataEngineDisabled {
				m.upgradeManagerStatus.UpgradeNodes[node.Name] = &longhorn.UpgradeNodeStatus{
					State:   longhorn.UpgradeStateCompleted,
					Message: fmt.Sprintf("V2 data engine is disabled for node %v", node.Name),
				}
				continue
			}

			// Skip the node that old instance manager is not running.
			// The default instance manager will be automatically started and running,
			// so we don't need to upgrade the it.
			ims, err := m.ds.ListInstanceManagersByNodeRO(node.Name, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2)
			if err != nil {
				m.upgradeManagerStatus.State = longhorn.UpgradeStateError
				m.upgradeManagerStatus.Message = fmt.Sprintf("Failed to list instanceManagers for node %v for dataEngineUpgradeManager resource %v: %v", node.Name, upgradeManager.Name, err.Error())
				return
			}

			for _, im := range ims {
				// If the old instance manager is running, we need to upgrade it.
				// Therefore, set the node to pending state and wait for the upgrade.
				if string(im.Spec.Image) != m.upgradeManagerStatus.InstanceManagerImage &&
					string(im.Status.CurrentState) == string(longhorn.InstanceManagerStateRunning) {
					m.upgradeManagerStatus.UpgradeNodes[im.Spec.NodeID] = &longhorn.UpgradeNodeStatus{
						State: longhorn.UpgradeStatePending,
					}
					break
				}
			}

			_, ok := m.upgradeManagerStatus.UpgradeNodes[node.Name]
			if !ok {
				m.upgradeManagerStatus.UpgradeNodes[node.Name] = &longhorn.UpgradeNodeStatus{
					State:   longhorn.UpgradeStateCompleted,
					Message: fmt.Sprintf("Old instance manager is not running for node %v", node.Name),
				}
			}
		}
	} else {
		for _, nodeName := range upgradeManager.Spec.Nodes {
			v2DataEngineDisabled, err := m.ds.IsV2DataEngineDisabledForNode(nodeName)
			if err != nil {
				m.upgradeManagerStatus.State = longhorn.UpgradeStateError
				m.upgradeManagerStatus.Message = fmt.Sprintf("Failed to check if v2 data engine is disabled for node %v for dataEngineUpgradeManager resource %v: %v", nodeName, upgradeManager.Name, err.Error())
				return
			}
			if v2DataEngineDisabled {
				m.upgradeManagerStatus.UpgradeNodes[nodeName] = &longhorn.UpgradeNodeStatus{
					State:   longhorn.UpgradeStateCompleted,
					Message: fmt.Sprintf("V2 data engine is disabled for node %v", nodeName),
				}
				continue
			}

			m.upgradeManagerStatus.UpgradeNodes[nodeName] = &longhorn.UpgradeNodeStatus{
				State: longhorn.UpgradeStatePending,
			}
		}
	}

	if len(m.upgradeManagerStatus.UpgradeNodes) == 0 {
		m.upgradeManagerStatus.State = longhorn.UpgradeStateCompleted
	} else {
		m.upgradeManagerStatus.State = longhorn.UpgradeStateUpgrading
	}
}

func (m *DataEngineUpgradeManagerMonitor) handleUpgradeStateUpgrading(upgradeManager *longhorn.DataEngineUpgradeManager) {
	log := m.logger.WithFields(logrus.Fields{"dataEngineUpgradeManager": upgradeManager.Name})

	// Check if the active nodeUpgrade is matching the m.upgradeManagerStatus.UpgradingNode
	nodeUpgrades, err := m.ds.ListNodeDataEngineUpgrades()
	if err != nil {
		m.upgradeManagerStatus.State = longhorn.UpgradeStateError
		m.upgradeManagerStatus.Message = fmt.Sprintf("Failed to list nodeUpgrades for dataEngineUpgradeManager resource %v: %v", upgradeManager.Name, err.Error())
		return
	}

	if m.upgradeManagerStatus.UpgradingNode != "" {
		foundNodeUpgrade := false
		nodeUpgrade := &longhorn.NodeDataEngineUpgrade{}
		for _, nodeUpgrade = range nodeUpgrades {
			if nodeUpgrade.Spec.NodeID != m.upgradeManagerStatus.UpgradingNode {
				continue
			}
			foundNodeUpgrade = true
			break
		}
		if foundNodeUpgrade {
			m.upgradeManagerStatus.UpgradeNodes[m.upgradeManagerStatus.UpgradingNode].Message = nodeUpgrade.Status.Message
			if nodeUpgrade.Status.State == longhorn.UpgradeStateCompleted {
				m.upgradeManagerStatus.UpgradeNodes[m.upgradeManagerStatus.UpgradingNode].State = longhorn.UpgradeStateCompleted
				m.upgradeManagerStatus.UpgradingNode = ""
			} else {
				m.upgradeManagerStatus.UpgradeNodes[m.upgradeManagerStatus.UpgradingNode].State = nodeUpgrade.Status.State
				m.upgradeManagerStatus.UpgradingNode = nodeUpgrade.Spec.NodeID
			}
		} else {
			m.upgradeManagerStatus.UpgradeNodes[m.upgradeManagerStatus.UpgradingNode].State = longhorn.UpgradeStateError
			m.upgradeManagerStatus.UpgradeNodes[m.upgradeManagerStatus.UpgradingNode].Message = "NodeUpgrade resource not found"
			m.upgradeManagerStatus.UpgradingNode = ""
		}
		return
	}

	// TODO: Check if there is any nodeDataEngineUpgrade in progress but not tracked by m.upgradeManagerStatus.UpgradingNode

	// Pick a node to upgrade
	for nodeName, nodeStatus := range m.upgradeManagerStatus.UpgradeNodes {
		if nodeStatus.State == longhorn.UpgradeStateCompleted ||
			nodeStatus.State == longhorn.UpgradeStateError {
			continue
		}

		// Create a new upgrade resource for the node
		log.Infof("Creating NodeDataEngineUpgrade resource for node %v", nodeName)
		_, err := m.ds.GetNode(nodeName)
		if err != nil {
			nodeStatus.State = longhorn.UpgradeStateError
			nodeStatus.Message = err.Error()
			continue
		}

		_, err = m.ds.CreateNodeDataEngineUpgrade(&longhorn.NodeDataEngineUpgrade{
			ObjectMeta: metav1.ObjectMeta{
				Name:            types.GenerateNodeDataEngineUpgradeName(upgradeManager.Name, nodeName),
				Namespace:       upgradeManager.Namespace,
				OwnerReferences: datastore.GetOwnerReferencesForDataEngineUpgradeManager(upgradeManager),
				Labels:          types.GetNodeDataEngineUpgradeLabels(upgradeManager.Name, nodeName),
			},
			Spec: longhorn.NodeDataEngineUpgradeSpec{
				NodeID:                   nodeName,
				DataEngine:               longhorn.DataEngineTypeV2,
				InstanceManagerImage:     m.upgradeManagerStatus.InstanceManagerImage,
				DataEngineUpgradeManager: upgradeManager.Name,
			},
		})
		if err != nil {
			if !types.ErrorAlreadyExists(err) {
				nodeStatus.State = longhorn.UpgradeStateError
				nodeStatus.Message = err.Error()
				continue
			}
		} else {
			log.Infof("Created NodeDataEngineUpgrade resource for node %v", nodeName)
		}
		m.upgradeManagerStatus.UpgradingNode = nodeName
		break
	}

	if m.upgradeManagerStatus.UpgradingNode == "" {
		m.upgradeManagerStatus.State = longhorn.UpgradeStateCompleted
	}
}

func (m *DataEngineUpgradeManagerMonitor) handleUpgradeStateUnknown() {
	m.upgradeManagerStatus.State = longhorn.UpgradeStateError
	m.upgradeManagerStatus.Message = fmt.Sprintf("Unknown dataEngineUpgradeManager state %v", m.upgradeManagerStatus.State)
}
