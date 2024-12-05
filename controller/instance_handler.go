package controller

import (
	"context"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"

	corev1 "k8s.io/api/core/v1"

	imapi "github.com/longhorn/longhorn-instance-manager/pkg/api"
	imtypes "github.com/longhorn/longhorn-instance-manager/pkg/types"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// InstanceHandler can handle the state transition of correlated instance and
// engine/replica object. It assumed the instance it's going to operate with is using
// the SAME NAME from the engine/replica object
type InstanceHandler struct {
	ds                     *datastore.DataStore
	instanceManagerHandler InstanceManagerHandler
	eventRecorder          record.EventRecorder
}

type InstanceManagerHandler interface {
	GetInstance(obj interface{}, isInstanceOnRemoteNode bool) (*longhorn.InstanceProcess, error)
	CreateInstance(obj interface{}, isInstanceOnRemoteNode bool) (*longhorn.InstanceProcess, error)
	DeleteInstance(obj interface{}) error
	LogInstance(ctx context.Context, obj interface{}) (*engineapi.InstanceManagerClient, *imapi.LogStream, error)
	SuspendInstance(obj interface{}) error
	ResumeInstance(obj interface{}) error
	SwitchOverTarget(obj interface{}) error
	DeleteTarget(obj interface{}) error
	RequireRemoteTargetInstance(obj interface{}) (bool, error)
	IsEngine(obj interface{}) bool
}

func NewInstanceHandler(ds *datastore.DataStore, instanceManagerHandler InstanceManagerHandler, eventRecorder record.EventRecorder) *InstanceHandler {
	return &InstanceHandler{
		ds:                     ds,
		instanceManagerHandler: instanceManagerHandler,
		eventRecorder:          eventRecorder,
	}
}

func (h *InstanceHandler) syncStatusIPsAndPorts(im *longhorn.InstanceManager, obj interface{}, spec *longhorn.InstanceSpec, status *longhorn.InstanceStatus, instanceName string, instance longhorn.InstanceProcess) {
	imPod, err := h.ds.GetPodRO(im.Namespace, im.Name)
	if err != nil {
		logrus.WithError(err).Errorf("Failed to get instance manager pod from %v", im.Name)
		return
	}
	if imPod == nil {
		logrus.Warnf("Instance manager pod from %v not exist in datastore", im.Name)
		return
	}

	storageIP := h.ds.GetStorageIPFromPod(imPod)
	if status.StorageIP != storageIP {
		status.StorageIP = storageIP
		logrus.Infof("Instance %v starts running, Storage IP %v", instanceName, status.StorageIP)
	}

	if status.IP != im.Status.IP {
		status.IP = im.Status.IP
		logrus.Infof("Instance %v starts running, IP %v", instanceName, status.IP)
	}

	if status.Port != int(instance.Status.PortStart) {
		status.Port = int(instance.Status.PortStart)
		logrus.Infof("Instance %v starts running, Port %d", instanceName, status.Port)
	}

	if !h.instanceManagerHandler.IsEngine(obj) {
		return
	}

	if types.IsDataEngineV2(spec.DataEngine) && spec.TargetNodeID != "" {
		targetIM, err := h.ds.GetRunningInstanceManagerByNodeRO(spec.TargetNodeID, spec.DataEngine)
		if err != nil {
			logrus.WithError(err).Errorf("Failed to get running instance manager for node %s", spec.TargetNodeID)
			return
		}
		targetClient, err := engineapi.NewInstanceManagerClient(targetIM, false)
		if err != nil {
			logrus.WithError(err).Errorf("Failed to get instance manager client for target instance manager %v", targetIM.Name)
			return
		}
		defer targetClient.Close()

		targetInstance, err := targetClient.InstanceGet(spec.DataEngine, instanceName, string(longhorn.InstanceManagerTypeEngine))
		if err != nil {
			logrus.WithError(err).Errorf("Failed to get target instance %s from instance manager %s", instanceName, targetIM.Name)
		} else {
			if status.TargetIP != targetIM.Status.IP {
				status.TargetIP = targetIM.Status.IP
				logrus.Infof("Instance %v starts running, Target IP %v", instanceName, status.TargetIP)
			}

			if status.TargetPort != int(targetInstance.Status.TargetPortStart) {
				status.TargetPort = int(targetInstance.Status.TargetPortStart)
				logrus.Infof("Instance %v starts running, Target Port %v", instanceName, status.TargetPort)
			}

			// Get storage target IP from target instance manager
			targetIMPod, err := h.ds.GetPodRO(targetIM.Namespace, targetIM.Name)
			if err != nil {
				logrus.WithError(err).Errorf("Failed to get instance manager pod from %v", targetIM.Name)
				return
			}
			if targetIMPod == nil {
				logrus.Warnf("Instance manager pod from %v not exist in datastore", targetIM.Name)
				return
			}

			storageTargetIP := h.ds.GetStorageIPFromPod(targetIMPod)
			if status.StorageTargetIP != storageTargetIP {
				status.StorageTargetIP = storageTargetIP
				logrus.Infof("Instance %v starts running, Storage Target IP %v", instanceName, status.StorageTargetIP)
			}
		}

		// Check if the target instance replacement is running on the target node
		if status.CurrentTargetNodeID != spec.TargetNodeID && !status.TargetInstanceReplacementCreated {
			running, err := h.isTargetInstanceReplacementRunning(instanceName, spec, status)
			if err != nil {
				logrus.WithError(err).Errorf("Failed to check if target instance %v is running", instanceName)
				return
			}
			if running {
				logrus.Infof("Target instance replacement %v is running on target node %v", instanceName, spec.TargetNodeID)
				status.TargetInstanceReplacementCreated = true
			}
		}
	} else {
		if status.StorageTargetIP != storageIP {
			status.StorageTargetIP = storageIP
			logrus.Infof("Instance %v starts running, Storage Target IP %v", instanceName, status.StorageTargetIP)
		}

		if status.TargetIP != im.Status.IP {
			status.TargetIP = im.Status.IP
			logrus.Infof("Instance %v starts running, Target IP %v", instanceName, status.TargetIP)
		}

		if status.TargetPort != int(instance.Status.TargetPortStart) {
			status.TargetPort = int(instance.Status.TargetPortStart)
			logrus.Infof("Instance %v starts running, Target Port %v", instanceName, status.TargetPort)
		}

		status.CurrentTargetNodeID = ""
		status.TargetInstanceReplacementCreated = false
	}
}

func (h *InstanceHandler) syncStatusWithInstanceManager(im *longhorn.InstanceManager, obj interface{}, spec *longhorn.InstanceSpec, status *longhorn.InstanceStatus, instances map[string]longhorn.InstanceProcess) {
	runtimeObj, ok := obj.(runtime.Object)
	if !ok {
		return
	}
	instanceName, err := h.getNameFromObj(runtimeObj)
	if err != nil {
		return
	}

	defer func() {
		if status.CurrentState == longhorn.InstanceStateStopped {
			status.InstanceManagerName = ""
			status.CurrentTargetNodeID = ""
			status.TargetInstanceReplacementCreated = false
		}
	}()

	isDelinquent := false
	if im != nil {
		isDelinquent, _ = h.ds.IsNodeDelinquent(im.Spec.NodeID, spec.VolumeName)
	}

	if im == nil || im.Status.CurrentState == longhorn.InstanceManagerStateUnknown || isDelinquent {
		if status.Started {
			if status.CurrentState != longhorn.InstanceStateUnknown {
				logrus.Warnf("Marking the instance as state UNKNOWN since the related node %v of instance %v is down or deleted", spec.NodeID, instanceName)
			}
			status.CurrentState = longhorn.InstanceStateUnknown
		} else {
			status.CurrentState = longhorn.InstanceStateStopped
			status.CurrentImage = ""
		}
		status.IP = ""
		status.TargetIP = ""
		status.StorageIP = ""
		status.StorageTargetIP = ""
		status.Port = 0
		status.TargetPort = 0
		h.resetInstanceErrorCondition(status)
		return
	}

	if im.Status.CurrentState == longhorn.InstanceManagerStateStopped ||
		im.Status.CurrentState == longhorn.InstanceManagerStateError ||
		im.DeletionTimestamp != nil {
		if status.Started {
			if h.isEngineOfV2DataEngine(obj, spec.DataEngine) {
				if h.isV2DataEngineBeingUpgraded(spec, status) {
					logrus.Warnf("Skipping the instance %v since the instance manager %v is %v", instanceName, im.Name, im.Status.CurrentState)
					return
				}

				if spec.Image == status.CurrentImage {
					if status.CurrentState != longhorn.InstanceStateError {
						upgradeRequested, err := h.ds.IsNodeDataEngineUpgradeRequested(spec.NodeID)
						if err != nil {
							// TODO: should we return here or mark the instance as error?
							logrus.WithError(err).Errorf("Failed to check if node %v is being upgrade requested", spec.NodeID)
							return
						}
						if upgradeRequested {
							logrus.Warnf("Skipping the instance %v since the instance manager %v is %v since the node %v is being upgrade requested",
								instanceName, im.Name, im.Status.CurrentState, spec.NodeID)
							return
						}
						logrus.Warnf("Marking the instance as state ERROR since failed to find the instance manager for the running instance %v", instanceName)
					}
					status.CurrentState = longhorn.InstanceStateError
				} else {
					logrus.Warnf("Skipping the instance %v since the instance manager %v is %v and spec image %v is different from the current image %v",
						instanceName, im.Name, im.Status.CurrentState, spec.Image, status.CurrentImage)
					return
				}
			} else {
				if status.CurrentState != longhorn.InstanceStateError {
					logrus.Warnf("Marking the instance as state ERROR since failed to find the instance manager for the running instance %v", instanceName)
				}
				status.CurrentState = longhorn.InstanceStateError
			}
		} else {
			status.CurrentState = longhorn.InstanceStateStopped
		}
		status.CurrentImage = ""
		status.IP = ""
		status.TargetIP = ""
		status.StorageIP = ""
		status.StorageTargetIP = ""
		status.Port = 0
		status.TargetPort = 0
		h.resetInstanceErrorCondition(status)
		return
	}

	if im.Status.CurrentState == longhorn.InstanceManagerStateStarting {
		if status.Started {
			if h.isEngineOfV2DataEngine(obj, spec.DataEngine) {
				if spec.Image == status.CurrentImage {
					upgradeRequested, err := h.ds.IsNodeDataEngineUpgradeRequested(spec.NodeID)
					if err != nil {
						logrus.WithError(err).Errorf("Failed to get node %v", spec.NodeID)
						return
					}
					if upgradeRequested {
						logrus.Warnf("Skipping the instance %v since the instance manager %v is %v", instanceName, im.Name, im.Status.CurrentState)
						return
					}
					h.handleIfInstanceManagerStarting(im.Name, instanceName, status)
				}
			} else {
				h.handleIfInstanceManagerStarting(im.Name, instanceName, status)
			}
		}
		return
	}

	instance, exists := instances[instanceName]
	if !exists {
		if status.Started {
			if h.isEngineOfV2DataEngine(obj, spec.DataEngine) {
				if h.isV2DataEngineBeingUpgraded(spec, status) {
					logrus.Warnf("Skipping checking the instance %v since the instance manager %v is in %v state",
						instanceName, im.Name, im.Status.CurrentState)
					return
				}
				if status.CurrentState != longhorn.InstanceStateError {
					// Because the async nature, directly check instance here
					if spec.TargetNodeID != "" {
						logrus.Infof("Recreated initiator instance %v is not found in instance manager %v, directly checking it in instance manager %v",
							instanceName, im.Name, spec.NodeID)

						exist, err := h.isInstanceExist(instanceName, spec)
						if exist {
							logrus.Infof("Initiator instance %v is found in instance manager %v", instanceName, spec.NodeID)
							return
						}
						if err != nil {
							logrus.WithError(err).Errorf("Failed to check if recreated initiator instance %v exists in instance manager %v",
								instanceName, spec.NodeID)
							return
						}
					}
					logrus.Warnf("Marking the instance as state ERROR since failed to find the instance status in instance manager %v for the running instance %v",
						im.Name, instanceName)
				}
				status.CurrentState = longhorn.InstanceStateError
			} else {
				if status.CurrentState != longhorn.InstanceStateError {
					logrus.Warnf("Marking the instance as state ERROR since failed to find the instance status in instance manager %v for the running instance %v", im.Name, instanceName)
				}
				status.CurrentState = longhorn.InstanceStateError
			}
		} else {
			status.CurrentState = longhorn.InstanceStateStopped
		}

		status.CurrentImage = ""
		status.IP = ""
		status.TargetIP = ""
		status.StorageIP = ""
		status.StorageTargetIP = ""
		status.Port = 0
		status.TargetPort = 0
		h.resetInstanceErrorCondition(status)
		return
	}

	if status.InstanceManagerName != "" && status.InstanceManagerName != im.Name {
		logrus.Warnf("The related process of instance %v is found in the instance manager %v, but the instance manager name in the instance status is %v. "+
			"The instance manager name shouldn't change except for cleanup",
			instanceName, im.Name, status.InstanceManagerName)
	}
	// `status.InstanceManagerName` should be set when the related instance process status
	// exists in the instance manager.
	// `status.InstanceManagerName` can be used to clean up the process in instance manager
	// and fetch log even if the instance status becomes `error` or `stopped`
	status.InstanceManagerName = im.Name

	switch instance.Status.State {
	case longhorn.InstanceStateStarting:
		status.CurrentState = longhorn.InstanceStateStarting
		status.CurrentImage = ""
		status.IP = ""
		status.TargetIP = ""
		status.StorageIP = ""
		status.StorageTargetIP = ""
		status.Port = 0
		status.TargetPort = 0
		h.resetInstanceErrorCondition(status)
	case longhorn.InstanceStateRunning:
		status.CurrentState = longhorn.InstanceStateRunning

		h.syncStatusIPsAndPorts(im, obj, spec, status, instanceName, instance)

		// only set CurrentImage when first started, since later we may specify
		// different spec.Image for upgrade
		if status.CurrentImage == "" {
			if types.IsDataEngineV1(spec.DataEngine) {
				status.CurrentImage = spec.Image
			} else {
				if h.instanceManagerHandler.IsEngine(obj) {
					status.CurrentImage = spec.Image
				} else {
					status.CurrentImage = im.Spec.Image
				}
			}
		}

		h.syncInstanceCondition(instance, status)
	case longhorn.InstanceStateSuspended:
		status.CurrentState = longhorn.InstanceStateSuspended
		status.CurrentTargetNodeID = spec.TargetNodeID
	case longhorn.InstanceStateStopping:
		if status.Started {
			status.CurrentState = longhorn.InstanceStateError
		} else {
			status.CurrentState = longhorn.InstanceStateStopping
		}
		status.CurrentImage = ""
		status.IP = ""
		status.TargetIP = ""
		status.StorageIP = ""
		status.StorageTargetIP = ""
		status.Port = 0
		status.TargetPort = 0
		h.resetInstanceErrorCondition(status)
	case longhorn.InstanceStateStopped:
		if status.Started {
			status.CurrentState = longhorn.InstanceStateError
		} else {
			status.CurrentState = longhorn.InstanceStateStopped
		}
		status.CurrentImage = ""
		status.IP = ""
		status.TargetIP = ""
		status.StorageIP = ""
		status.StorageTargetIP = ""
		status.Port = 0
		status.TargetPort = 0
		h.resetInstanceErrorCondition(status)
	default:
		if status.CurrentState != longhorn.InstanceStateError {
			logrus.Warnf("Instance %v is state %v, error message: %v", instanceName, instance.Status.State, instance.Status.ErrorMsg)
		}
		status.CurrentState = longhorn.InstanceStateError
		status.CurrentImage = ""
		status.IP = ""
		status.TargetIP = ""
		status.StorageIP = ""
		status.StorageTargetIP = ""
		status.Port = 0
		status.TargetPort = 0
		h.resetInstanceErrorCondition(status)
	}
}

func (h *InstanceHandler) syncInstanceCondition(instance longhorn.InstanceProcess, status *longhorn.InstanceStatus) {
	for condition, flag := range instance.Status.Conditions {
		conditionStatus := longhorn.ConditionStatusFalse
		if flag {
			conditionStatus = longhorn.ConditionStatusTrue
		}
		status.Conditions = types.SetCondition(status.Conditions, condition, conditionStatus, "", "")
	}
}

// resetInstanceErrorCondition resets the error condition to false when the instance is not running
func (h *InstanceHandler) resetInstanceErrorCondition(status *longhorn.InstanceStatus) {
	status.Conditions = types.SetCondition(status.Conditions, imtypes.EngineConditionFilesystemReadOnly, longhorn.ConditionStatusFalse, "", "")
}

// getNameFromObj will get the name from the object metadata, which will be used
// as podName later
func (h *InstanceHandler) getNameFromObj(obj runtime.Object) (string, error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return "", err
	}
	return metadata.GetName(), nil
}

func (h *InstanceHandler) ReconcileInstanceState(obj interface{}, spec *longhorn.InstanceSpec, status *longhorn.InstanceStatus) (err error) {
	runtimeObj, ok := obj.(runtime.Object)
	if !ok {
		return fmt.Errorf("obj is not a runtime.Object: %v", obj)
	}
	instanceName, err := h.getNameFromObj(runtimeObj)
	if err != nil {
		return err
	}

	log := logrus.WithField("instance", instanceName)

	var im *longhorn.InstanceManager
	if status.InstanceManagerName != "" {
		im, err = h.ds.GetInstanceManagerRO(status.InstanceManagerName)
		if err != nil {
			if !datastore.ErrorIsNotFound(err) {
				return err
			}
		}
	}
	// There should be an available instance manager for a scheduled instance when its related engine image is compatible
	if im == nil && spec.NodeID != "" && h.isSpecImageReady(obj, spec) {
		dataEngineEnabled, err := h.ds.IsDataEngineEnabled(spec.DataEngine)
		if err != nil {
			return err
		}
		if !dataEngineEnabled {
			return nil
		}
		// The related node maybe cleaned up then there is no available instance manager for this instance (typically it's replica).
		isNodeDownOrDeleted, err := h.ds.IsNodeDownOrDeletedOrDelinquent(spec.NodeID, spec.VolumeName)
		if err != nil {
			return err
		}
		if !isNodeDownOrDeleted {
			im, err = h.getInstanceManagerRO(obj, spec, status)
			if err != nil {
				return errors.Wrapf(err, "failed to get instance manager for instance %v", instanceName)
			}
		}
	}

	if spec.LogRequested {
		if !status.LogFetched {
			// No need to get the log for instance manager if the data engine is not "longhorn"
			if types.IsDataEngineV1(spec.DataEngine) {
				log.Warnf("Getting requested log for %v in instance manager %v", instanceName, status.InstanceManagerName)
				if im == nil {
					log.Warnf("Failed to get the log for %v due to Instance Manager is already gone", status.InstanceManagerName)
				} else if err := h.printInstanceLogs(instanceName, runtimeObj); err != nil {
					log.WithError(err).Warnf("Failed to get requested log for instance %v on node %v", instanceName, im.Spec.NodeID)
				}
			}
			status.LogFetched = true
		}
	} else { // spec.LogRequested = false
		status.LogFetched = false
	}

	if status.SalvageExecuted && !spec.SalvageRequested {
		status.SalvageExecuted = false
	}

	status.Conditions = types.SetCondition(status.Conditions,
		longhorn.InstanceConditionTypeInstanceCreation, longhorn.ConditionStatusTrue,
		"", "")

	instances := map[string]longhorn.InstanceProcess{}
	if im != nil {
		instances, err = h.getInstancesFromInstanceManager(runtimeObj, im)
		if err != nil {
			return err
		}
	}
	// do nothing for incompatible instance except for deleting
	switch spec.DesireState {
	case longhorn.InstanceStateRunning:
		if im == nil {
			break
		}

		if i, exists := instances[instanceName]; exists && i.Status.State == longhorn.InstanceStateRunning {
			status.Started = true
			if h.isEngineOfV2DataEngine(obj, spec.DataEngine) {
				if isDataEngineNotBeingLiveUpgraded(spec, status) {
					// Not in data engine live upgrade
					break
				}
			} else {
				break
			}
		}

		// there is a delay between createInstance() invocation and InstanceManager update,
		// createInstance() may be called multiple times.
		if h.isEngineOfV2DataEngine(obj, spec.DataEngine) {
			if status.CurrentState != longhorn.InstanceStateStopped && status.CurrentState != longhorn.InstanceStateSuspended {
				if status.CurrentState != longhorn.InstanceStateRunning || isDataEngineNotBeingLiveUpgraded(spec, status) {
					break
				}
			}
		} else {
			if status.CurrentState != longhorn.InstanceStateStopped {
				break
			}
		}

		if h.isEngineOfV2DataEngine(obj, spec.DataEngine) {
			if spec.TargetNodeID != "" {
				if h.isEngineResumeRequired(spec, status) {
					// Resume the suspended initiator instance
					err = h.resumeInstance(instanceName, spec.DataEngine, runtimeObj)
				} else {
					// Create target instance if it's not created yet
					err = h.createInstance(instanceName, spec.DataEngine, runtimeObj)
				}
			} else {
				err = h.createInstance(instanceName, spec.DataEngine, runtimeObj)
			}
		} else {
			err = h.createInstance(instanceName, spec.DataEngine, runtimeObj)
		}
		if err != nil {
			return err
		}

		// Set the SalvageExecuted flag to clear the SalvageRequested flag.
		if spec.SalvageRequested {
			status.SalvageExecuted = true
		}

	case longhorn.InstanceStateStopped:
		if im != nil && im.DeletionTimestamp == nil {
			// there is a delay between deleteInstance() invocation and state/InstanceManager update,
			// deleteInstance() may be called multiple times.
			if instance, exists := instances[instanceName]; exists {
				if shouldDeleteInstance(&instance) {
					if err := h.deleteInstance(instanceName, runtimeObj); err != nil {
						return err
					}
				}
			}
		}
		if h.isEngineOfV2DataEngine(obj, spec.DataEngine) {
			if status.CurrentTargetNodeID != "" {
				targetIM, err := h.ds.GetRunningInstanceManagerByNodeRO(status.CurrentTargetNodeID, spec.DataEngine)
				if err != nil {
					return err
				}
				if targetIM != nil {
					if err := h.deleteTarget(instanceName, runtimeObj); err != nil {
						if !types.ErrorIsNotFound(err) {
							return err
						}
					}
				}
			}
		}
		status.Started = false
	case longhorn.InstanceStateSuspended:
		if !h.isEngineOfV2DataEngine(obj, spec.DataEngine) {
			return fmt.Errorf("instance %v is not an engine of v2 volume", instanceName)
		}

		if err := h.suspendInstance(instanceName, runtimeObj); err != nil {
			return err
		}

		if err := h.switchOverTarget(instanceName, runtimeObj); err != nil {
			logrus.Infof("Resuming instance %v after failing to switch over target", instanceName)
			errResume := h.resumeInstance(instanceName, spec.DataEngine, runtimeObj)
			if errResume != nil {
				logrus.WithError(errResume).Errorf("Failed to resume instance %v after failing to switch over target", instanceName)
			}
			return err
		}

		if spec.TargetNodeID != status.CurrentTargetNodeID {
			if err := h.deleteTarget(instanceName, runtimeObj); err != nil {
				if !types.ErrorIsNotFound(err) {
					return err
				}
			}
		}
	default:
		return fmt.Errorf("unknown instance desire state: desire %v", spec.DesireState)
	}

	h.syncStatusWithInstanceManager(im, obj, spec, status, instances)

	switch status.CurrentState {
	case longhorn.InstanceStateRunning:
		// If `spec.DesireState` is `longhorn.InstanceStateStopped`, `spec.NodeID` has been unset by volume controller.
		if spec.DesireState != longhorn.InstanceStateStopped {
			if spec.NodeID != im.Spec.NodeID {
				status.CurrentState = longhorn.InstanceStateError
				status.IP = ""
				status.TargetIP = ""
				status.StorageIP = ""
				status.StorageTargetIP = ""
				err := fmt.Errorf("instance %v NodeID %v is not the same as the instance manager %v NodeID %v",
					instanceName, spec.NodeID, im.Name, im.Spec.NodeID)
				return err
			}
		}
	case longhorn.InstanceStateError:
		if im == nil {
			break
		}
		if instance, exists := instances[instanceName]; exists {
			// If instance is in error state and the ErrorMsg contains 61 (ENODATA) error code, then it indicates
			// the creation of engine process failed because there is no available backend (replica).
			if spec.DesireState == longhorn.InstanceStateRunning {
				status.Conditions = types.SetCondition(status.Conditions,
					longhorn.InstanceConditionTypeInstanceCreation, longhorn.ConditionStatusFalse,
					longhorn.InstanceConditionReasonInstanceCreationFailure, instance.Status.ErrorMsg)
			}

			if types.IsDataEngineV1(instance.Spec.DataEngine) {
				logrus.Warnf("Instance %v crashed on Instance Manager %v at %v, getting log",
					instanceName, im.Name, im.Spec.NodeID)
				if err := h.printInstanceLogs(instanceName, runtimeObj); err != nil {
					logrus.WithError(err).Warnf("failed to get crash log for instance %v on Instance Manager %v at %v",
						instanceName, im.Name, im.Spec.NodeID)
				}
			}
		}
	}
	return nil
}

func shouldDeleteInstance(instance *longhorn.InstanceProcess) bool {
	// For a replica of a SPDK volume, a stopped replica means the lvol is not exposed,
	// but the lvol is still there. We don't need to delete it.
	if types.IsDataEngineV2(instance.Spec.DataEngine) {
		if instance.Status.State == longhorn.InstanceStateStopped {
			return false
		}
	}
	return true
}

func (h *InstanceHandler) getInstancesFromInstanceManager(obj runtime.Object, instanceManager *longhorn.InstanceManager) (map[string]longhorn.InstanceProcess, error) {
	switch obj.(type) {
	case *longhorn.Engine:
		return types.ConsolidateInstances(instanceManager.Status.InstanceEngines, instanceManager.Status.Instances), nil // nolint: staticcheck
	case *longhorn.Replica:
		return types.ConsolidateInstances(instanceManager.Status.InstanceReplicas, instanceManager.Status.Instances), nil // nolint: staticcheck
	}
	return nil, fmt.Errorf("unknown type for getInstancesFromInstanceManager: %+v", obj)
}

func (h *InstanceHandler) printInstanceLogs(instanceName string, obj runtime.Object) error {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	client, stream, err := h.instanceManagerHandler.LogInstance(ctx, obj)
	if err != nil {
		return err
	}
	defer client.Close()
	for {
		line, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		logrus.Warnf("%s: %s", instanceName, line)
	}
	return nil
}

func (h *InstanceHandler) createInstance(instanceName string, dataEngine longhorn.DataEngineType, obj runtime.Object) (err error) {
	if h.isEngineOfV2DataEngine(obj, dataEngine) {
		instanceExists := false
		targetInstanceRequired := false

		instance, err := h.instanceManagerHandler.GetInstance(obj, false)
		if err == nil {
			instanceExists = true

			targetInstanceRequired, err = h.instanceManagerHandler.RequireRemoteTargetInstance(obj)
			if err != nil {
				return errors.Wrapf(err, "failed to check if remote target instance for %v is required", instanceName)
			}
			if targetInstanceRequired {
				_, err = h.instanceManagerHandler.GetInstance(obj, true)
				if err == nil {
					return nil
				}
			} else {
				if instance.Status.StandbyTargetPortStart != 0 || instance.Status.TargetPortStart != 0 {
					return nil
				}

				targetInstanceRequired = true
				err = fmt.Errorf("cannot find local target instance for %v", instanceName)
			}
		}

		if !types.ErrorIsNotFound(err) && !(types.IsDataEngineV2(dataEngine) && types.ErrorIsStopped(err)) {
			return errors.Wrapf(err, "failed to get instance %v", instanceName)
		}

		if !instanceExists {
			logrus.Infof("Creating instance %v", instanceName)
			if _, err := h.instanceManagerHandler.CreateInstance(obj, false); err != nil {
				if !types.ErrorAlreadyExists(err) {
					h.eventRecorder.Eventf(obj, corev1.EventTypeWarning, constant.EventReasonFailedStarting, "Error starting %v: %v", instanceName, err)
					return err
				}
				// Already exists, lost track may due to previous datastore conflict
				return nil
			}
			h.eventRecorder.Eventf(obj, corev1.EventTypeNormal, constant.EventReasonStart, "Starts instance %v", instanceName)
		}

		if targetInstanceRequired {
			logrus.Infof("Creating target instance %v", instanceName)
			if _, err := h.instanceManagerHandler.CreateInstance(obj, true); err != nil {
				if !types.ErrorAlreadyExists(err) {
					h.eventRecorder.Eventf(obj, corev1.EventTypeWarning, constant.EventReasonFailedStarting, "Error starting %v: %v", instanceName, err)
					return err
				}
				// Already exists, lost track may due to previous datastore conflict
				return nil
			}
			h.eventRecorder.Eventf(obj, corev1.EventTypeNormal, constant.EventReasonStart, "Starts target instance %v", instanceName)
		}
	} else {
		_, err := h.instanceManagerHandler.GetInstance(obj, false)
		if err == nil {
			return nil
		}
		if !types.ErrorIsNotFound(err) && !(types.IsDataEngineV2(dataEngine) && types.ErrorIsStopped(err)) {
			return errors.Wrapf(err, "Failed to get instance process %v", instanceName)
		}

		logrus.Infof("Creating instance %v", instanceName)
		if _, err := h.instanceManagerHandler.CreateInstance(obj, false); err != nil {
			if !types.ErrorAlreadyExists(err) {
				h.eventRecorder.Eventf(obj, corev1.EventTypeWarning, constant.EventReasonFailedStarting, "Error starting %v: %v", instanceName, err)
				return err
			}
			// Already exists, lost track may due to previous datastore conflict
			return nil
		}
		h.eventRecorder.Eventf(obj, corev1.EventTypeNormal, constant.EventReasonStart, "Starts %v", instanceName)
	}

	return nil
}

func (h *InstanceHandler) deleteInstance(instanceName string, obj runtime.Object) error {
	// May try to force deleting instances on lost node. Don't need to check the instance
	logrus.Infof("Deleting instance %v", instanceName)
	if err := h.instanceManagerHandler.DeleteInstance(obj); err != nil {
		h.eventRecorder.Eventf(obj, corev1.EventTypeWarning, constant.EventReasonFailedStopping, "Error stopping %v: %v", instanceName, err)
		return err
	}
	h.eventRecorder.Eventf(obj, corev1.EventTypeNormal, constant.EventReasonStop, "Stops %v", instanceName)

	return nil
}

func (h *InstanceHandler) suspendInstance(instanceName string, obj runtime.Object) error {
	logrus.Infof("Suspending instance %v", instanceName)

	if _, err := h.instanceManagerHandler.GetInstance(obj, false); err != nil {
		return errors.Wrapf(err, "failed to get instance %v for suspension", instanceName)
	}

	if err := h.instanceManagerHandler.SuspendInstance(obj); err != nil {
		return errors.Wrapf(err, "failed to suspend instance %v", instanceName)
	}

	return nil
}

func (h *InstanceHandler) resumeInstance(instanceName string, dataEngine longhorn.DataEngineType, obj runtime.Object) error {
	logrus.Infof("Resuming instance %v", instanceName)

	if types.IsDataEngineV1(dataEngine) {
		return fmt.Errorf("resuming instance is not supported for data engine %v", dataEngine)
	}

	if _, err := h.instanceManagerHandler.GetInstance(obj, false); err != nil {
		return errors.Wrapf(err, "failed to get instance %v for resumption", instanceName)
	}

	if err := h.instanceManagerHandler.ResumeInstance(obj); err != nil {
		return errors.Wrapf(err, "failed to resume instance %v", instanceName)
	}

	return nil
}

func (h *InstanceHandler) switchOverTarget(instanceName string, obj runtime.Object) error {
	logrus.Infof("Switching over target for instance %v", instanceName)

	if err := h.instanceManagerHandler.SwitchOverTarget(obj); err != nil {
		return errors.Wrapf(err, "failed to switch over target for instance %s", instanceName)
	}

	return nil
}

func (h *InstanceHandler) deleteTarget(instanceName string, obj runtime.Object) error {
	logrus.Infof("Deleting target for instance %s", instanceName)

	if err := h.instanceManagerHandler.DeleteTarget(obj); err != nil {
		return errors.Wrapf(err, "failed to delete target for instance %s", instanceName)
	}

	return nil
}

func (h *InstanceHandler) isV2DataEngineBeingUpgraded(spec *longhorn.InstanceSpec, status *longhorn.InstanceStatus) bool {
	if !types.IsDataEngineV2(spec.DataEngine) {
		return false
	}

	upgradeRequested, err := h.ds.IsNodeDataEngineUpgradeRequested(spec.NodeID)
	if err != nil {
		logrus.WithError(err).Errorf("Failed to get node %v", spec.NodeID)
		return false
	}

	if !upgradeRequested {
		return false
	}

	if spec.TargetNodeID == "" {
		return false
	}

	return spec.NodeID != spec.TargetNodeID && spec.TargetNodeID == status.CurrentTargetNodeID
}

func isVolumeBeingSwitchedBack(spec *longhorn.InstanceSpec, status *longhorn.InstanceStatus) bool {
	return spec.NodeID == spec.TargetNodeID && spec.TargetNodeID != status.CurrentTargetNodeID
}

func isTargetInstanceReplacementCreated(instance *longhorn.InstanceProcess) bool {
	return instance.Status.StandbyTargetPortStart != 0
}

func isTargetInstanceRemote(instance *longhorn.InstanceProcess) bool {
	return instance.Status.PortStart != 0 && instance.Status.TargetPortStart == 0
}

func (h *InstanceHandler) getInstanceManagerRO(obj interface{}, spec *longhorn.InstanceSpec, status *longhorn.InstanceStatus) (*longhorn.InstanceManager, error) {
	// Only happen when upgrading instance-manager image
	if spec.DesireState == longhorn.InstanceStateRunning && status.CurrentState == longhorn.InstanceStateSuspended {
		return h.ds.GetRunningInstanceManagerByNodeRO(spec.NodeID, spec.DataEngine)
	}

	return h.ds.GetInstanceManagerByInstanceRO(obj, false)
}

func (h *InstanceHandler) handleIfInstanceManagerStarting(imName, instanceName string, status *longhorn.InstanceStatus) {
	if status.CurrentState != longhorn.InstanceStateError {
		logrus.Warnf("Marking the instance as state ERROR since the starting instance manager %v shouldn't contain the running instance %v", imName, instanceName)
	}
	status.CurrentState = longhorn.InstanceStateError
	status.CurrentImage = ""
	status.IP = ""
	status.TargetIP = ""
	status.StorageIP = ""
	status.StorageTargetIP = ""
	status.Port = 0
	status.TargetPort = 0
	h.resetInstanceErrorCondition(status)
}

func (h *InstanceHandler) isInstanceExist(instanceName string, spec *longhorn.InstanceSpec) (bool, error) {
	var err error

	im, err := h.ds.GetRunningInstanceManagerByNodeRO(spec.NodeID, spec.DataEngine)
	if err != nil {
		return false, err
	}

	client, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return false, err
	}
	defer client.Close()

	_, err = client.InstanceGet(spec.DataEngine, instanceName, string(longhorn.InstanceManagerTypeEngine))
	if err != nil {
		return false, err
	}

	return true, nil
}

func (h *InstanceHandler) isSpecImageReady(obj interface{}, spec *longhorn.InstanceSpec) bool {
	if types.IsDataEngineV1(spec.DataEngine) {
		return spec.Image != ""
	}

	if h.instanceManagerHandler.IsEngine(obj) {
		return spec.Image != ""
	}

	// spec.image is not required for replica because the image of a v2 replica
	// is the same as the running instance manager.
	return true
}

func (h *InstanceHandler) isTargetInstanceReplacementRunning(instanceName string, spec *longhorn.InstanceSpec, status *longhorn.InstanceStatus) (bool, error) {
	if spec.TargetNodeID == "" {
		return false, nil
	}

	logrus.Infof("Checking whether instance %v is running on target node %v", instanceName, spec.TargetNodeID)

	im, err := h.ds.GetRunningInstanceManagerByNodeRO(spec.TargetNodeID, spec.DataEngine)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get instance manager on node %v for checking if target instance is running", spec.TargetNodeID)
	}

	c, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return false, errors.Wrapf(err, "failed to create instance manager client for target instance")
	}
	defer c.Close()

	instance, err := c.InstanceGet(spec.DataEngine, instanceName, string(longhorn.InstanceManagerTypeEngine))
	if err != nil {
		return false, errors.Wrapf(err, "failed to get target instance %v on node %v", instanceName, spec.TargetNodeID)
	}

	if isVolumeBeingSwitchedBack(spec, status) {
		return isTargetInstanceRemote(instance) && isTargetInstanceReplacementCreated(instance), nil
	}

	return true, nil
}

func (h *InstanceHandler) isEngineOfV2DataEngine(obj interface{}, dataEngine longhorn.DataEngineType) bool {
	return types.IsDataEngineV2(dataEngine) && h.instanceManagerHandler.IsEngine(obj)
}

func (h *InstanceHandler) isEngineResumeRequired(spec *longhorn.InstanceSpec, status *longhorn.InstanceStatus) bool {
	return spec.TargetNodeID == status.CurrentTargetNodeID && status.CurrentState == longhorn.InstanceStateSuspended
}

func isDataEngineNotBeingLiveUpgraded(spec *longhorn.InstanceSpec, status *longhorn.InstanceStatus) bool {
	return spec.TargetNodeID == "" && !status.TargetInstanceReplacementCreated
}
