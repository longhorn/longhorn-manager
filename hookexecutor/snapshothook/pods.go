package snapshothook

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/sets"

	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// podIsConsumer reports whether the pod may write to its volumes: it has
// not reached a terminal phase and at least one of its containers has
// started.
func podIsConsumer(pod *corev1.Pod) bool {
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return false
	}
	return len(util.PodContainerIDs(pod)) > 0
}

// podsUsingPVC returns the consumer pods that mount the claim, split by
// whether they are being deleted.
func podsUsingPVC(pods []corev1.Pod, pvcName string, terminating bool) []*corev1.Pod {
	var matched []*corev1.Pod
	for i := range pods {
		pod := &pods[i]
		if podIsConsumer(pod) &&
			pod.DeletionTimestamp.IsZero() != terminating &&
			util.PodUsesPVC(pod, pvcName) {
			matched = append(matched, pod)
		}
	}
	return matched
}

// ConsumerPodsUsingPVC returns the pods that mount the claim and may write
// to it. The Kubernetes API cannot list pods by the PVC they use, so
// callers list the whole namespace and filter here.
func ConsumerPodsUsingPVC(pods []corev1.Pod, pvcName string) []*corev1.Pod {
	return podsUsingPVC(pods, pvcName, false)
}

// TerminatingPodsUsingPVC returns the consumer pods that mount the claim
// and are being deleted. Such a pod may keep writing until the kubelet
// stops its containers, so pause-time discovery waits for it to leave.
func TerminatingPodsUsingPVC(pods []corev1.Pod, pvcName string) []*corev1.Pod {
	return podsUsingPVC(pods, pvcName, true)
}

// TerminalPodsUsingPVC returns the pods that mount the claim, ran at least
// one container, and have since reached a terminal phase. The pods must come
// from a list call without a status.phase field selector.
func TerminalPodsUsingPVC(pods []corev1.Pod, pvcName string) []*corev1.Pod {
	var matched []*corev1.Pod
	for i := range pods {
		pod := &pods[i]
		if (pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed) &&
			len(util.PodContainerIDs(pod)) > 0 &&
			util.PodUsesPVC(pod, pvcName) {
			matched = append(matched, pod)
		}
	}
	return matched
}

// PodIdentityChange compares the pods recorded at pause time against the
// live set and returns the first difference, or "" when they match: same
// pods, same UIDs, same container IDs. Any difference means something may
// have written while the application was meant to be paused.
func PodIdentityChange(recorded []longhorn.HookPodStatus, live []*corev1.Pod) string {
	liveByName := make(map[string]*corev1.Pod, len(live))
	for _, pod := range live {
		liveByName[pod.Name] = pod
	}

	recordedNames := make(map[string]bool, len(recorded))
	for _, record := range recorded {
		recordedNames[record.Name] = true
		pod, running := liveByName[record.Name]
		if !running {
			return fmt.Sprintf("pod %v/%v recorded at pause time is no longer running", record.Namespace, record.Name)
		}
		if string(pod.UID) != record.UID {
			return fmt.Sprintf("pod %v/%v was replaced during the hook window (uid %v is now %v)", record.Namespace, record.Name, record.UID, pod.UID)
		}
		if !sets.New(record.ContainerIDs...).Equal(sets.New(util.PodContainerIDs(pod)...)) {
			return fmt.Sprintf("pod %v/%v restarted a container during the hook window", record.Namespace, record.Name)
		}
	}

	for _, pod := range live {
		if !recordedNames[pod.Name] {
			return fmt.Sprintf("pod %v/%v started during the hook window and ran no pre-command", pod.Namespace, pod.Name)
		}
	}
	return ""
}
