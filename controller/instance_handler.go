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

func (h *InstanceHandler) SyncInstanceState(podName string, spec *types.InstanceSpec, status *types.InstanceStatus) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync instance status for %v", podName)
	}()
	pod, err := h.getPod(podName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			status.State = types.InstanceStateStopped
			status.IP = ""
		} else {
			return err
		}
	} else {
		switch pod.Status.Phase {
		case corev1.PodPending:
			status.State = types.InstanceStateStopped
			status.IP = ""
		case corev1.PodRunning:
			status.State = types.InstanceStateRunning
			status.IP = pod.Status.PodIP
			// pin down to this node ID for replica
			if spec.NodeID == "" {
				spec.NodeID = pod.Spec.NodeName
			} else if spec.NodeID != pod.Spec.NodeName {
				status.State = types.InstanceStateError
				status.IP = ""
				err := fmt.Errorf("BUG: instance %v wasn't pin down to the host %v", podName, spec.NodeID)
				logrus.Errorf("%v", err)
				return err
			}
		default:
			logrus.Warnf("instance %v state is failed/unknown, pod state %v",
				podName, pod.Status.Phase)
			status.State = types.InstanceStateError
		}
	}
	return nil
}

func (h *InstanceHandler) ReconcileInstanceState(podName string, obj interface{}, spec *types.InstanceSpec, status *types.InstanceStatus) (err error) {
	state := status.State
	desireState := spec.DesireState
	if state == types.InstanceStateError {
		return h.stopInstance(podName)
	}

	if state != desireState {
		switch desireState {
		case types.InstanceStateRunning:
			if state == types.InstanceStateStopped {
				pod, err := h.podCreator.CreatePodSpec(obj)
				if err != nil {
					return err
				}
				if err := h.startInstance(pod); err != nil {
					return err
				}
				break
			}
			logrus.Errorf("unable to do replica transition: current %v, desire %v", state, desireState)
		case types.InstanceStateStopped:
			if state == types.InstanceStateRunning {
				if err := h.stopInstance(podName); err != nil {
					return err
				}
				break
			}
			logrus.Errorf("unable to do replica transition: current %v, desire %v", state, desireState)
		default:
			logrus.Errorf("unknown replica transition: current %v, desire %v", state, desireState)
		}
	}
	return nil
}

func (h *InstanceHandler) getPod(podName string) (*corev1.Pod, error) {
	return h.pLister.Pods(h.namespace).Get(podName)
}

func (h *InstanceHandler) startInstance(pod *corev1.Pod) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to start instance %v", pod.Name)
	}()
	if pod == nil {
		return fmt.Errorf("cannot start empty pod")
	}
	_, err = h.getPod(pod.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	// pod already started
	if !apierrors.IsNotFound(err) {
		return nil
	}
	logrus.Debugf("Starting instance %v", pod.Name)
	if _, err := h.kubeClient.CoreV1().Pods(h.namespace).Create(pod); err != nil {
		return err
	}
	return nil
}

func (h *InstanceHandler) stopInstance(podName string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to stop instance %v", podName)
	}()
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
	logrus.Debugf("Stopping instance %v", podName)
	if err := h.kubeClient.CoreV1().Pods(h.namespace).Delete(podName, nil); err != nil {
		return err
	}
	return nil
}
