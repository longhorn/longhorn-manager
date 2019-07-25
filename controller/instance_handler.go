package controller

import (
	"fmt"
	"io"
	"strings"

	"github.com/sirupsen/logrus"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"

	imapi "github.com/longhorn/longhorn-instance-manager/api"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
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
	GetInstance(obj interface{}) (*types.InstanceProcessStatus, error)
	CreateInstance(obj interface{}) (*types.InstanceProcessStatus, error)
	DeleteInstance(obj interface{}) (*types.InstanceProcessStatus, error)
	LogInstance(obj interface{}) (*imapi.LogStream, error)
}

func NewInstanceHandler(ds *datastore.DataStore, instanceManagerHandler InstanceManagerHandler, eventRecorder record.EventRecorder) *InstanceHandler {
	return &InstanceHandler{
		ds:                     ds,
		instanceManagerHandler: instanceManagerHandler,
		eventRecorder:          eventRecorder,
	}
}

func (h *InstanceHandler) syncStatusWithInstanceManager(im *longhorn.InstanceManager, instanceName string, spec *types.InstanceSpec, status *types.InstanceStatus) {
	if im == nil || im.Status.CurrentState == types.InstanceManagerStateUnknown || im.Status.CurrentState == types.InstanceManagerStateStopped || im.Status.CurrentState == types.InstanceManagerStateError || im.DeletionTimestamp != nil {
		if status.Started {
			status.CurrentState = types.InstanceStateError
		} else {
			status.CurrentState = types.InstanceStateStopped
		}
		status.CurrentImage = ""
		status.InstanceManagerName = ""
		status.IP = ""
		status.Port = 0
		return
	}

	if im.Status.CurrentState == types.InstanceManagerStateStarting {
		if status.Started {
			status.CurrentState = types.InstanceStateError
			status.CurrentImage = ""
			status.InstanceManagerName = ""
			status.IP = ""
			status.Port = 0
		}
		return
	}

	pStatus, exists := im.Status.Instances[instanceName]
	if !exists {
		// need to consider case: instance is just created but haven't been present in instance manager
		if status.CurrentState != types.InstanceStateStopped {
			status.InstanceManagerName = ""
		}

		if status.Started {
			status.CurrentState = types.InstanceStateError
		} else {
			status.CurrentState = types.InstanceStateStopped
		}
		status.CurrentImage = ""
		status.IP = ""
		status.Port = 0
		return
	}

	switch pStatus.State {
	case types.InstanceStateStarting:
		status.CurrentState = types.InstanceStateStarting
		status.CurrentImage = ""
		status.IP = ""
		status.Port = 0
	case types.InstanceStateRunning:
		status.CurrentState = types.InstanceStateRunning
		if status.IP != im.Status.IP {
			status.IP = im.Status.IP
			logrus.Debugf("Instance %v starts running, IP %v", instanceName, status.IP)
		}
		if status.Port != int(pStatus.PortStart) {
			status.Port = int(pStatus.PortStart)
			logrus.Debugf("Instance %v starts running, Port %d", instanceName, status.Port)
		}
		// only set CurrentImage when first started, since later we may specify
		// different spec.EngineImage for upgrade
		if status.CurrentImage == "" {
			status.CurrentImage = spec.EngineImage
		}
		nodeBootID := im.Status.NodeBootID
		if status.NodeBootID == "" {
			status.NodeBootID = nodeBootID
		} else if status.NodeBootID != nodeBootID {
			logrus.Warnf("Instance %v's node %v has been rebooted. Original boot ID is %v, current node boot ID is %v",
				instanceName, im.Spec.NodeID, status.NodeBootID, nodeBootID)
		}
	case types.InstanceStateStopping:
		if status.Started {
			status.CurrentState = types.InstanceStateError
		} else {
			status.CurrentState = types.InstanceStateStopping
		}
		status.CurrentImage = ""
		status.IP = ""
		status.Port = 0
	case types.InstanceStateStopped:
		if status.Started {
			status.CurrentState = types.InstanceStateError
		} else {
			status.CurrentState = types.InstanceStateStopped
		}
		status.CurrentImage = ""
		status.InstanceManagerName = ""
		status.IP = ""
		status.Port = 0
	default:
		logrus.Warnf("instance %v state is %v, error message %v", instanceName, pStatus.State, pStatus.ErrorMsg)
		status.CurrentState = types.InstanceStateError
		status.CurrentImage = ""
		status.InstanceManagerName = ""
		status.IP = ""
		status.Port = 0
		// Don't reset status.NodeBootID, we need it to identify a node reboot
	}
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

func (h *InstanceHandler) ReconcileInstanceState(obj interface{}, spec *types.InstanceSpec, status *types.InstanceStatus, imType types.InstanceManagerType) (err error) {
	runtimeObj, ok := obj.(runtime.Object)
	if !ok {
		return fmt.Errorf("obj is not a runtime.Object: %v", obj)
	}
	instanceName, err := h.getNameFromObj(runtimeObj)
	if err != nil {
		return err
	}

	var im *longhorn.InstanceManager
	if status.InstanceManagerName != "" {
		im, err = h.ds.GetInstanceManager(status.InstanceManagerName)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				logrus.Debugf("cannot find instance manager %v", status.InstanceManagerName)
			} else {
				return err
			}
		}
	}

	if spec.LogRequested {
		if im == nil {
			logrus.Warnf("Cannot get the log for %v due to Instance Manager is already gone", status.InstanceManagerName)
		} else {
			logrus.Warnf("Try to get requested log for %v on node %v", instanceName, im.Spec.NodeID)
			if err := h.printInstanceLogs(instanceName, runtimeObj); err != nil {
				logrus.Warnf("cannot get requested log for instance %v on node %v, error %v", instanceName, im.Spec.NodeID, err)
			}
		}
		spec.LogRequested = false
	}

	switch spec.DesireState {
	case types.InstanceStateRunning:
		if im != nil && im.DeletionTimestamp == nil {
			if i, exists := im.Status.Instances[instanceName]; exists && i.State == types.InstanceStateRunning {
				status.Started = true
				break
			}
		}
		// there is a delay between createInstance() invocation and state/InstanceManager update,
		// hence instance `currentState` can be `stopped` but with `instanceManagerName` set.
		// createInstance() may be called multiple times.
		if status.CurrentState != types.InstanceStateStopped || status.InstanceManagerName != "" {
			break
		}
		im, err = h.prepareToCreateInstance(spec, status, imType)
		if err != nil {
			return err
		}
		err = h.createInstance(instanceName, runtimeObj)
		if err != nil {
			return err
		}
	case types.InstanceStateStopped:
		if im != nil && im.DeletionTimestamp == nil {
			// there is a delay between deleteInstance() invocation and state/InstanceManager update,
			// hence instance `currentState` can be `running` but with false `Started` flag.
			// deleteInstance() may be called multiple times.
			if i, exists := im.Status.Instances[instanceName]; exists && status.Started && (i.State == types.InstanceStateRunning || i.State == types.InstanceStateStarting) {
				if err := h.deleteInstance(instanceName, runtimeObj); err != nil {
					return err
				}
			}
		}
		status.Started = false
		status.NodeBootID = ""
	default:
		return fmt.Errorf("BUG: unknown instance desire state: desire %v", spec.DesireState)
	}

	h.syncStatusWithInstanceManager(im, instanceName, spec, status)

	if status.CurrentState == types.InstanceStateRunning {
		if spec.NodeID != im.Spec.NodeID {
			status.CurrentState = types.InstanceStateError
			status.InstanceManagerName = ""
			status.IP = ""
			status.NodeBootID = ""
			err := fmt.Errorf("BUG: instance %v wasn't pin down to the instance manager at %v", instanceName, im.Spec.NodeID)
			logrus.Errorf("%v", err)
			return err
		}
	} else if status.CurrentState == types.InstanceStateError && im != nil {
		if _, exists := im.Status.Instances[instanceName]; exists {
			logrus.Warnf("Instance %v crashed on Instance Manager %v at %v, try to get log", instanceName, im.Name, im.Spec.NodeID)
			if err := h.printInstanceLogs(instanceName, runtimeObj); err != nil {
				logrus.Warnf("cannot get crash log for instance %v on Instance Manager %v at %v, error %v", instanceName, im.Name, im.Spec.NodeID, err)
			}
		}
	}
	return nil
}

func (h *InstanceHandler) printInstanceLogs(instanceName string, obj runtime.Object) error {
	stream, err := h.instanceManagerHandler.LogInstance(obj)
	if err != nil {
		return err
	}

	for {
		line, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			logrus.Errorf("failed to receive log for instance %v: %v", instanceName, err)
			return err
		}
		logrus.Warnf("%s: %s", instanceName, line)
	}
	return nil
}

func (h *InstanceHandler) prepareToCreateInstance(spec *types.InstanceSpec, status *types.InstanceStatus, managerType types.InstanceManagerType) (*longhorn.InstanceManager, error) {
	// use engine image object name rather than image
	engineImageName := types.GetEngineImageChecksumName(spec.EngineImage)
	im, err := h.ds.GetInstanceManagerBySelector(spec.NodeID, engineImageName, string(managerType))
	if err != nil {
		return nil, err
	}
	if im == nil {
		return nil, fmt.Errorf("cannot find instance manager by Labels: engineImage=%v,nodeID=%v,type=%v", spec.EngineImage, spec.NodeID, managerType)
	}

	status.InstanceManagerName = im.Name
	return im, nil
}

func (h *InstanceHandler) createInstance(instanceName string, obj runtime.Object) error {
	_, err := h.instanceManagerHandler.GetInstance(obj)
	if err != nil {
		if strings.Contains(err.Error(), "cannot find") {
			if _, err := h.instanceManagerHandler.CreateInstance(obj); err != nil {
				if !strings.Contains(err.Error(), "already exists") {
					h.eventRecorder.Eventf(obj, v1.EventTypeWarning, EventReasonFailedStarting, "Error starting %v: %v", instanceName, err)
					return err
				}
			}
			h.eventRecorder.Eventf(obj, v1.EventTypeNormal, EventReasonStart, "Starts %v", instanceName)
		} else {
			return err

		}
	}
	return nil
}

func (h *InstanceHandler) deleteInstance(instanceName string, obj runtime.Object) error {
	pStatus, err := h.instanceManagerHandler.GetInstance(obj)
	if err != nil {
		if strings.Contains(err.Error(), "cannot find") {
			return nil
		}
		return err
	}
	if pStatus.State == types.InstanceStateStarting || pStatus.State == types.InstanceStateRunning {
		if _, err := h.instanceManagerHandler.DeleteInstance(obj); err != nil {
			if !strings.Contains(err.Error(), "cannot find") {
				h.eventRecorder.Eventf(obj, v1.EventTypeWarning, EventReasonFailedStopping, "Error stopping %v: %v", instanceName, err)
				return err
			}
		}
		h.eventRecorder.Eventf(obj, v1.EventTypeNormal, EventReasonStop, "Stops %v", instanceName)
	}

	return nil
}
