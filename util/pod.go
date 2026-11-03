package util

import (
	"sort"

	"k8s.io/apimachinery/pkg/fields"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodContainerIDs returns the pod's container IDs of every kind, sorted.
func PodContainerIDs(pod *corev1.Pod) []string {
	var ids []string
	for _, statuses := range [][]corev1.ContainerStatus{
		pod.Status.ContainerStatuses,
		pod.Status.InitContainerStatuses,
		pod.Status.EphemeralContainerStatuses,
	} {
		for _, status := range statuses {
			if status.ContainerID != "" {
				ids = append(ids, status.ContainerID)
			}
		}
	}
	sort.Strings(ids)
	return ids
}

// PodUsesPVC reports whether the pod mounts the named claim, either through
// a persistentVolumeClaim volume or a generic ephemeral volume.
func PodUsesPVC(pod *corev1.Pod, pvcName string) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvcName {
			return true
		}
		// A generic ephemeral volume's claim is named <pod>-<volume>:
		// https://kubernetes.io/docs/concepts/storage/ephemeral-volumes/#persistentvolumeclaim-naming
		if volume.Ephemeral != nil && pod.Name+"-"+volume.Name == pvcName {
			return true
		}
	}
	return false
}

// NonTerminalPodsListOptions asks the API server for pods that are not
// Succeeded or Failed, keeping the namespace LIST small while still
// including Pending pods whose init containers may already run. A pod
// being deleted stays in its phase until its containers stop, so it is
// still listed.
func NonTerminalPodsListOptions() metav1.ListOptions {
	return metav1.ListOptions{
		FieldSelector: fields.AndSelectors(
			fields.OneTermNotEqualSelector("status.phase", string(corev1.PodSucceeded)),
			fields.OneTermNotEqualSelector("status.phase", string(corev1.PodFailed)),
		).String(),
	}
}
