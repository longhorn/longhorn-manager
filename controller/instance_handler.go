package controller

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"

	"github.com/rancher/longhorn-manager/types"
)

type InstanceHandler struct {
	namespace  string
	kubeClient clientset.Interface
	pLister    corelisters.PodLister
	podCreator PodCreatorInterface
}

type PodCreatorInterface interface {
	CreatePodSpec(obj interface{}) (*corev1.Pod, error)
}

func NewInstanceHandler(podInformer coreinformers.PodInformer, kubeClient clientset.Interface, namespace string, podCreator PodCreatorInterface) *InstanceHandler {
	return &InstanceHandler{
		namespace:  namespace,
		kubeClient: kubeClient,
		pLister:    podInformer.Lister(),
		podCreator: podCreator,
	}
}

func (h *InstanceHandler) syncStatusWithPod(pod *corev1.Pod, status *types.InstanceStatus) {
	if pod == nil {
		status.CurrentState = types.InstanceStateStopped
		status.IP = ""
		return
	}

	if pod.DeletionTimestamp != nil {
		status.CurrentState = types.InstanceStateStopping
		status.IP = ""
		return
	}

	switch pod.Status.Phase {
	case corev1.PodPending:
		status.CurrentState = types.InstanceStateStarting
		status.IP = ""
	case corev1.PodRunning:
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
	default:
		// TODO Check the reason of pod cannot gracefully shutdown
		logrus.Warnf("instance %v state is failed/unknown, pod state %v",
			pod.Name, pod.Status.Phase)
		status.CurrentState = types.InstanceStateError
		status.IP = ""
	}
}

func (h *InstanceHandler) ReconcileInstanceState(podName string, obj interface{}, spec *types.InstanceSpec, status *types.InstanceStatus) (err error) {
	if status.CurrentState == types.InstanceStateError {
		return h.deletePod(podName)
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
		if pod == nil {
			podSpec, err := h.podCreator.CreatePodSpec(obj)
			if err != nil {
				return err
			}
			pod, err = h.createPod(podSpec)
			if err != nil {
				return err
			}
		}
	case types.InstanceStateStopped:
		if pod != nil && pod.DeletionTimestamp == nil {
			if err := h.deletePod(pod.Name); err != nil {
				return err
			}
		}
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

func (h *InstanceHandler) getPod(podName string) (*corev1.Pod, error) {
	return h.pLister.Pods(h.namespace).Get(podName)
}

func (h *InstanceHandler) createPod(pod *corev1.Pod) (p *corev1.Pod, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to start instance %v", pod.Name)
	}()
	logrus.Debugf("Start instance %v", pod.Name)
	return h.kubeClient.CoreV1().Pods(h.namespace).Create(pod)
}

func (h *InstanceHandler) deletePod(podName string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to stop instance %v", podName)
	}()
	logrus.Debugf("Stop instance %v", podName)
	return h.kubeClient.CoreV1().Pods(h.namespace).Delete(podName, nil)
}

func (h *InstanceHandler) Delete(podName string) (err error) {
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
	return h.deletePod(podName)
}
