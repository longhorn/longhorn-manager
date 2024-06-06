package controller

import (
	"context"
	"fmt"
	"io"
	"net"

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
	GetInstance(obj interface{}, remoteTargetInstance bool) (*longhorn.InstanceProcess, error)
	CreateInstance(obj interface{}, remoteTargetInstance bool) (*longhorn.InstanceProcess, error)
	DeleteInstance(obj interface{}) error
	LogInstance(ctx context.Context, obj interface{}) (*engineapi.InstanceManagerClient, *imapi.LogStream, error)
	SuspendInstance(obj interface{}) error
	ResumeInstance(obj interface{}) error
	SwitchOverTargetInstance(obj interface{}, targetInstanceAddress string) error
	RequireTargetInstance(obj interface{}) (bool, error)
}

func NewInstanceHandler(ds *datastore.DataStore, instanceManagerHandler InstanceManagerHandler, eventRecorder record.EventRecorder) *InstanceHandler {
	return &InstanceHandler{
		ds:                     ds,
		instanceManagerHandler: instanceManagerHandler,
		eventRecorder:          eventRecorder,
	}
}

func (h *InstanceHandler) isTargetInstanceRunning(instanceName string, spec *longhorn.InstanceSpec, status *longhorn.InstanceStatus) (bool, error) {
	if spec.TargetNodeID == "" {
		return false, nil
	}

	logrus.Infof("Checking if instance %v is running on target node %v", instanceName, spec.TargetNodeID)

	im, err := h.ds.GetRunningInstanceManagerByNodeRO(spec.TargetNodeID, spec.DataEngine)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get instance manager on node %v for target instance", spec.TargetNodeID)
	}

	c, err := engineapi.NewInstanceManagerClient(im)
	if err != nil {
		return false, errors.Wrapf(err, "failed to create instance manager client for target instance")
	}

	defer c.Close()

	_, err = c.InstanceGet(spec.DataEngine, instanceName, string(longhorn.InstanceManagerTypeEngine))
	if err != nil {
		return false, errors.Wrapf(err, "failed to get target instance %v on node %v", instanceName, spec.TargetNodeID)
	}

	logrus.Infof("Instance %v is running on target node %v", instanceName, spec.TargetNodeID)

	return true, nil
}

func (h *InstanceHandler) syncStatusWithInstanceManager(im *longhorn.InstanceManager, instanceName string, spec *longhorn.InstanceSpec, status *longhorn.InstanceStatus, instances map[string]longhorn.InstanceProcess) {
	defer func() {
		if status.CurrentState == longhorn.InstanceStateStopped {
			status.InstanceManagerName = ""
		}
	}()

	if im == nil || im.Status.CurrentState == longhorn.InstanceManagerStateUnknown {
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
		status.StorageIP = ""
		status.Port = 0
		h.resetInstanceErrorCondition(status)
		return
	}

	if im.Status.CurrentState == longhorn.InstanceManagerStateStopped ||
		im.Status.CurrentState == longhorn.InstanceManagerStateError ||
		im.DeletionTimestamp != nil {
		if status.Started {
			if spec.Image == status.CurrentImage {
				if status.CurrentState != longhorn.InstanceStateError {
					logrus.Warnf("Marking the instance as state ERROR since failed to find the instance manager for the running instance %v", instanceName)
				}
				status.CurrentState = longhorn.InstanceStateError
			}
		} else {
			status.CurrentState = longhorn.InstanceStateStopped
		}

		status.CurrentImage = ""
		if status.CurrentState == longhorn.InstanceStateStopped || status.CurrentState == longhorn.InstanceStateError {
			status.IP = ""
			status.StorageIP = ""
			status.Port = 0
		}
		h.resetInstanceErrorCondition(status)
		return
	}

	if im.Status.CurrentState == longhorn.InstanceManagerStateStarting {
		if status.Started {
			if spec.Image == status.CurrentImage {
				if status.CurrentState != longhorn.InstanceStateError {
					logrus.Warnf("Marking the instance as state ERROR since the starting instance manager %v shouldn't contain the running instance %v", im.Name, instanceName)
				}
				status.CurrentState = longhorn.InstanceStateError
				status.CurrentImage = ""
				status.IP = ""
				status.StorageIP = ""
				status.Port = 0
				h.resetInstanceErrorCondition(status)
			}
		}
		return
	}

	instance, exists := instances[instanceName]
	if !exists {
		if status.Started {
			if spec.Image == status.CurrentImage {
				if status.CurrentState != longhorn.InstanceStateError {
					logrus.Warnf("Marking the instance as state ERROR since failed to find the instance status in instance manager %v for the running instance %v", im.Name, instanceName)
				}
				status.CurrentState = longhorn.InstanceStateError
			}
		} else {
			if status.CurrentState != longhorn.InstanceStateSuspended {
				status.CurrentState = longhorn.InstanceStateStopped
			}
		}
		status.CurrentImage = ""
		status.IP = ""
		status.StorageIP = ""
		status.Port = 0
		h.resetInstanceErrorCondition(status)
		return
	}

	if status.InstanceManagerName != "" && status.InstanceManagerName != im.Name {
		logrus.Errorf("The related process of instance %v is found in the instance manager %v, but the instance manager name in the instance status is %v. "+
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
		status.StorageIP = ""
		status.Port = 0
		h.resetInstanceErrorCondition(status)
	case longhorn.InstanceStateRunning:
		status.CurrentState = longhorn.InstanceStateRunning

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
			logrus.Warnf("Instance %v starts running, Storage IP %v", instanceName, status.StorageIP)
		}

		if status.IP != im.Status.IP {
			status.IP = im.Status.IP
			logrus.Warnf("Instance %v starts running, IP %v", instanceName, status.IP)
		}
		if status.Port != int(instance.Status.PortStart) {
			status.Port = int(instance.Status.PortStart)
			logrus.Warnf("Instance %v starts running, Port %d", instanceName, status.Port)
		}
		// only set CurrentImage when first started, since later we may specify
		// different spec.Image for upgrade
		if status.CurrentImage == "" {
			status.CurrentImage = spec.Image
		}

		if spec.TargetNodeID != status.CurrentTargetNodeID {
			running, err := h.isTargetInstanceRunning(instanceName, spec, status)
			if err != nil {
				logrus.WithError(err).Errorf("Failed to check if target instance %v is running", instanceName)
			}
			if running {
				status.CurrentTargetNodeID = spec.TargetNodeID
			}
		}

		h.syncInstanceCondition(instance, status)
	case longhorn.InstanceStateSuspended:
		status.CurrentState = longhorn.InstanceStateSuspended
		status.CurrentTargetConnected = true
	case longhorn.InstanceStateStopping:
		if status.Started {
			status.CurrentState = longhorn.InstanceStateError
		} else {
			status.CurrentState = longhorn.InstanceStateStopping
		}
		status.CurrentImage = ""
		status.IP = ""
		status.StorageIP = ""
		status.Port = 0
		h.resetInstanceErrorCondition(status)
	case longhorn.InstanceStateStopped:
		if status.Started {
			status.CurrentState = longhorn.InstanceStateError
		} else {
			status.CurrentState = longhorn.InstanceStateStopped
		}
		status.CurrentImage = ""
		status.IP = ""
		status.StorageIP = ""
		status.Port = 0
		h.resetInstanceErrorCondition(status)
	default:
		if status.CurrentState != longhorn.InstanceStateError {
			logrus.Warnf("Instance %v is state %v, error message: %v", instanceName, instance.Status.State, instance.Status.ErrorMsg)
		}
		status.CurrentState = longhorn.InstanceStateError
		status.CurrentImage = ""
		status.IP = ""
		status.StorageIP = ""
		status.Port = 0
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

func (h *InstanceHandler) getInstanceManagerRO(obj interface{}, spec *longhorn.InstanceSpec, status *longhorn.InstanceStatus) (*longhorn.InstanceManager, error) {
	// Only happen when upgrading instance-manager image
	if spec.DesireState == longhorn.InstanceStateRunning && status.CurrentState == longhorn.InstanceStateSuspended {
		return h.ds.GetRunningInstanceManagerByNodeRO(spec.NodeID, spec.DataEngine)
	}

	return h.ds.GetInstanceManagerByInstanceRO(obj, false)
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

	isCLIAPIVersionOne := false
	if types.IsDataEngineV1(spec.DataEngine) {
		if status.CurrentImage != "" {
			isCLIAPIVersionOne, err = h.ds.IsEngineImageCLIAPIVersionOne(status.CurrentImage)
			if err != nil {
				return err
			}
		}
	}

	var im *longhorn.InstanceManager
	if !isCLIAPIVersionOne {
		if status.InstanceManagerName != "" {
			im, err = h.ds.GetInstanceManagerRO(status.InstanceManagerName)
			if err != nil {
				if !datastore.ErrorIsNotFound(err) {
					return err
				}
			}
		}
		// There should be an available instance manager for a scheduled instance when its related engine image is compatible
		if im == nil && spec.Image != "" && spec.NodeID != "" {
			dataEngineEnabled, err := h.ds.IsDataEngineEnabled(spec.DataEngine)
			if err != nil {
				return err
			}
			if !dataEngineEnabled {
				return nil
			}
			// The related node maybe cleaned up then there is no available instance manager for this instance (typically it's replica).
			isNodeDownOrDeleted, err := h.ds.IsNodeDownOrDeleted(spec.NodeID)
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
		if isCLIAPIVersionOne {
			return nil
		}

		if im == nil {
			break
		}

		if i, exists := instances[instanceName]; exists && i.Status.State == longhorn.InstanceStateRunning {
			status.Started = true
			if spec.TargetNodeID == "" && status.CurrentTargetNodeID == "" {
				break
			}
		}

		// there is a delay between createInstance() invocation and InstanceManager update,
		// createInstance() may be called multiple times.
		if status.CurrentState != longhorn.InstanceStateStopped && status.CurrentState != longhorn.InstanceStateSuspended {
			if status.CurrentState != longhorn.InstanceStateRunning || spec.TargetNodeID == "" {
				break
			}
		}

		if status.CurrentTargetConnected {
			if spec.TargetNodeID == status.CurrentTargetNodeID && status.CurrentState == longhorn.InstanceStateSuspended {
				logrus.Infof("Resuming instance %v", instanceName)
				err = h.resumeInstance(instanceName, spec.DataEngine, runtimeObj)
			} else {
				if im.Spec.Image == spec.Image && spec.Image != status.CurrentImage {
					err = h.createInstance(instanceName, spec.DataEngine, runtimeObj)
				}
			}
		} else {
			logrus.Infof("Creating instance %v", instanceName)
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
		if isCLIAPIVersionOne {
			if err := h.deleteInstance(instanceName, runtimeObj); err != nil {
				return err
			}
			status.Started = false
			status.CurrentState = longhorn.InstanceStateStopped
			status.CurrentImage = ""
			status.InstanceManagerName = ""
			status.IP = ""
			status.StorageIP = ""
			status.Port = 0
			return nil
		}

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
		status.Started = false
	case longhorn.InstanceStateSuspended:
		if err := h.suspendInstance(instanceName, spec.DataEngine, runtimeObj); err != nil {
			return err
		}

		if err := h.switchOverTargetInstance(instanceName, spec.TargetNodeID, spec.DataEngine, runtimeObj); err != nil {
			return err
		}
	default:
		return fmt.Errorf("BUG: unknown instance desire state: desire %v", spec.DesireState)
	}

	h.syncStatusWithInstanceManager(im, instanceName, spec, status, instances)

	switch status.CurrentState {
	case longhorn.InstanceStateRunning:
		// If `spec.DesireState` is `longhorn.InstanceStateStopped`, `spec.NodeID` has been unset by volume controller.
		if spec.DesireState != longhorn.InstanceStateStopped {
			if spec.NodeID != im.Spec.NodeID {
				status.CurrentState = longhorn.InstanceStateError
				status.IP = ""
				status.StorageIP = ""
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
	instanceExists := false
	targetInstanceExists := false
	targetInstanceRequired := false

	_, err = h.instanceManagerHandler.GetInstance(obj, false)
	if err == nil {
		instanceExists = true

		targetInstanceRequired, err = h.instanceManagerHandler.RequireTargetInstance(obj)
		if err != nil {
			return errors.Wrapf(err, "failed to check if target instance for %v is required", instanceName)
		}
		if !targetInstanceRequired {
			return nil
		}

		// Check if the target instance is already running
		_, err = h.instanceManagerHandler.GetInstance(obj, true)
		if err == nil {
			return nil
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
		if !targetInstanceExists {
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

func (h *InstanceHandler) suspendInstance(instanceName string, dataEngine longhorn.DataEngineType, obj runtime.Object) error {
	logrus.Infof("Suspending instance %v", instanceName)

	if types.IsDataEngineV1(dataEngine) {
		return fmt.Errorf("suspending instance is not supported for data engine %v", dataEngine)
	}

	if _, err := h.instanceManagerHandler.GetInstance(obj, false); err != nil {
		return errors.Wrapf(err, "failed to get instance %v for suspension", instanceName)
	}

	if err := h.instanceManagerHandler.SuspendInstance(obj); err != nil {
		return errors.Wrapf(err, "failed to suspend instance %v", instanceName)
	}

	h.eventRecorder.Eventf(obj, corev1.EventTypeNormal, constant.EventReasonSuspend, "Suspends %v", instanceName)

	return nil
}

func (h *InstanceHandler) resumeInstance(instanceName string, dataEngine longhorn.DataEngineType, obj runtime.Object) error {
	logrus.Infof("Resuming instance %v", instanceName)

	if types.IsDataEngineV1(dataEngine) {
		return fmt.Errorf("resuming instance is not supported for data engine %v", dataEngine)
	}

	if _, err := h.instanceManagerHandler.GetInstance(obj, false); err != nil {
		return errors.Wrapf(err, "failed to get instance %v for suspension", instanceName)
	}

	if err := h.instanceManagerHandler.ResumeInstance(obj); err != nil {
		return errors.Wrapf(err, "failed to resume instance %v", instanceName)
	}

	h.eventRecorder.Eventf(obj, corev1.EventTypeNormal, constant.EventReasonResume, "Resumes %v", instanceName)

	return nil
}

func (h *InstanceHandler) switchOverTargetInstance(instanceName, targetNodeID string, dataEngine longhorn.DataEngineType, obj runtime.Object) error {
	logrus.Infof("Switching over target instance %v", instanceName)

	if types.IsDataEngineV1(dataEngine) {
		return fmt.Errorf("switching over target for instance is not supported for data engine %v", dataEngine)
	}

	if _, err := h.instanceManagerHandler.GetInstance(obj, false); err != nil {
		return errors.Wrapf(err, "failed to get instance %v for switchover", instanceName)
	}

	instance, err := h.instanceManagerHandler.GetInstance(obj, true)
	if err != nil {
		return errors.Wrapf(err, "failed to get target for instance %v for switchover", instanceName)
	}

	im, err := h.ds.GetRunningInstanceManagerByNodeRO(targetNodeID, dataEngine)
	if err != nil {
		return errors.Wrapf(err, "failed to get instance manager on node %v for switching over target for instance", targetNodeID)
	}

	targetAddress := net.JoinHostPort(im.Status.IP, fmt.Sprint(instance.Status.PortStart))

	if err := h.instanceManagerHandler.SwitchOverTargetInstance(obj, targetAddress); err != nil {
		return errors.Wrapf(err, "failed to switch over target for instance %v", instanceName)
	}

	h.eventRecorder.Eventf(obj, corev1.EventTypeNormal, constant.EventReasonSuspend, "Switches over %v", instanceName)

	return nil
}
