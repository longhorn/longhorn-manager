package controller

import (
	"fmt"

	"github.com/Sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"

	"github.com/rancher/longhorn-manager/types"
)

// InstanceHandler can handle the state transition of correlated instance and
// engine/replica object. It assumed the pod it's going to operate with is using
// the SAME NAME from the engine/replica object
type InstanceHandler struct {
	namespace     string
	kubeClient    clientset.Interface
	pLister       corelisters.PodLister
	podCreator    PodCreatorInterface
	eventRecorder record.EventRecorder
}

type PodCreatorInterface interface {
	CreatePodSpec(obj interface{}) (*v1.Pod, error)
}

func NewInstanceHandler(podInformer coreinformers.PodInformer, kubeClient clientset.Interface, namespace string, podCreator PodCreatorInterface, eventRecorder record.EventRecorder) *InstanceHandler {
	return &InstanceHandler{
		namespace:     namespace,
		kubeClient:    kubeClient,
		pLister:       podInformer.Lister(),
		podCreator:    podCreator,
		eventRecorder: eventRecorder,
	}
}

func (h *InstanceHandler) syncStatusWithPod(pod *v1.Pod, status *types.InstanceStatus) {
	if pod == nil {
		if status.Started {
			status.CurrentState = types.InstanceStateError
			status.IP = ""
		} else {
			status.CurrentState = types.InstanceStateStopped
			status.IP = ""
		}
		return
	}

	if pod.DeletionTimestamp != nil {
		status.CurrentState = types.InstanceStateStopping
		status.IP = ""
		return
	}

	switch pod.Status.Phase {
	case v1.PodPending:
		status.CurrentState = types.InstanceStateStarting
		status.IP = ""
	case v1.PodRunning:
		for _, st := range pod.Status.ContainerStatuses {
			// wait until all containers passed readiness probe
			if !st.Ready {
				status.CurrentState = types.InstanceStateStarting
				status.IP = ""
				return
			}
		}
		status.CurrentState = types.InstanceStateRunning
		status.IP = pod.Status.PodIP
		logrus.Debugf("Instance %v starts running, IP %v", pod.Name, status.IP)
	default:
		// TODO Check the reason of pod cannot gracefully shutdown
		logrus.Warnf("instance %v state is failed/unknown, pod state %v",
			pod.Name, pod.Status.Phase)
		status.CurrentState = types.InstanceStateError
		status.IP = ""
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

func (h *InstanceHandler) ReconcileInstanceState(obj interface{}, spec *types.InstanceSpec, status *types.InstanceStatus) (err error) {
	runtimeObj, ok := obj.(runtime.Object)
	if !ok {
		return fmt.Errorf("obj is not a runtime.Object: %v", obj)
	}
	podName, err := h.getNameFromObj(runtimeObj)
	if err != nil {
		return err
	}

	pod, err := h.getPod(podName)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if apierrors.IsNotFound(err) {
		pod = nil
	}

	switch spec.DesireState {
	case types.InstanceStateRunning:
		// don't try to start the instance if in Error or already
		// started (meant the pod exit unexpected)
		if status.CurrentState != types.InstanceStateError &&
			!status.Started && pod == nil {
			podSpec, err := h.podCreator.CreatePodSpec(obj)
			if err != nil {
				return err
			}
			pod, err = h.createPodForObject(runtimeObj, podSpec)
			if err != nil {
				return err
			}
			status.CurrentImage = spec.EngineImage
			status.Started = true
		}
	case types.InstanceStateStopped:
		if pod != nil && pod.DeletionTimestamp == nil {
			if err := h.deletePodForObject(runtimeObj); err != nil {
				return err
			}
		}
		status.CurrentImage = ""
		status.Started = false
	default:
		return fmt.Errorf("BUG: unknown instance desire state: desire %v", spec.DesireState)
	}

	h.syncStatusWithPod(pod, status)

	if status.CurrentState == types.InstanceStateRunning {
		// pin down to this node ID. it's needed for a replica and
		// engine should specify nodeName as well
		if spec.NodeID == "" {
			spec.NodeID = pod.Spec.NodeName
		} else if spec.NodeID != pod.Spec.NodeName {
			status.CurrentState = types.InstanceStateError
			status.IP = ""
			err := fmt.Errorf("BUG: instance %v wasn't pin down to the host %v", pod.Name, spec.NodeID)
			logrus.Errorf("%v", err)
			return err
		}
	}
	return nil
}

func (h *InstanceHandler) getPod(podName string) (*v1.Pod, error) {
	return h.pLister.Pods(h.namespace).Get(podName)
}

func (h *InstanceHandler) createPodForObject(obj runtime.Object, pod *v1.Pod) (*v1.Pod, error) {
	p, err := h.kubeClient.CoreV1().Pods(h.namespace).Create(pod)
	if err != nil {
		h.eventRecorder.Eventf(obj, v1.EventTypeWarning, EventReasonFailedStarting, "Error starting %v: %v", pod.Name, err)
		return nil, err
	}
	h.eventRecorder.Eventf(obj, v1.EventTypeNormal, EventReasonStart, "Starts %v", pod.Name)
	return p, nil
}

func (h *InstanceHandler) deletePodForObject(obj runtime.Object) error {
	podName, err := h.getNameFromObj(obj)
	if err != nil {
		return err
	}

	if err := h.kubeClient.CoreV1().Pods(h.namespace).Delete(podName, nil); err != nil {
		h.eventRecorder.Eventf(obj, v1.EventTypeWarning, EventReasonFailedStopping, "Error stopping %v: %v", podName, err)
		return nil
	}
	h.eventRecorder.Eventf(obj, v1.EventTypeNormal, EventReasonStop, "Stops %v", podName)
	return nil
}

func (h *InstanceHandler) DeleteInstanceForObject(obj runtime.Object) (err error) {
	podName, err := h.getNameFromObj(obj)
	if err != nil {
		return err
	}

	pod, err := h.getPod(podName)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	// pod already stopped
	if apierrors.IsNotFound(err) {
		return nil
	}
	// pod has been already asked to stop
	if pod.DeletionTimestamp != nil {
		return nil
	}
	return h.deletePodForObject(obj)
}
