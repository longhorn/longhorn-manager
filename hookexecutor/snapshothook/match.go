package snapshothook

import (
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/labels"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// HookName returns the name status and events use for the hook.
func HookName(hook *longhorn.VolumeHookPolicyHook) string {
	if hook.Name == "" {
		return longhorn.VolumeHookPolicyDefaultHookName
	}
	return hook.Name
}

// HookRunsFor reports whether the hook runs around operation. A hook that
// lists no operations runs around every one.
func HookRunsFor(hook *longhorn.VolumeHookPolicyHook, operation longhorn.HookOperation) bool {
	return len(hook.Operations) == 0 || slices.Contains(hook.Operations, operation)
}

// PolicyCoversOperation reports whether any hook of the policy runs around
// operation.
func PolicyCoversOperation(policy *longhorn.VolumeHookPolicy, operation longhorn.HookOperation) bool {
	for i := range policy.Spec.Hooks {
		if HookRunsFor(&policy.Spec.Hooks[i], operation) {
			return true
		}
	}
	return false
}

// NoHookForOperationMessage is the rejection message for a member whose
// policy has no hook that runs for the operation.
func NoHookForOperationMessage(policy *longhorn.VolumeHookPolicy, operation longhorn.HookOperation, volumeName string) string {
	return fmt.Sprintf("volume hook policy %v has no hook that runs for operation %v, so member volume %v would pause nothing", PolicyNamespacedName(policy), operation, volumeName)
}

// FindHook returns the policy hook with the given name, or nil when the
// policy no longer has it.
func FindHook(policy *longhorn.VolumeHookPolicy, name string) *longhorn.VolumeHookPolicyHook {
	for i := range policy.Spec.Hooks {
		if HookName(&policy.Spec.Hooks[i]) == name {
			return &policy.Spec.Hooks[i]
		}
	}
	return nil
}

// MatchHookForPod picks the one hook of the policy that applies to the pod
// for operation, and the container the commands run in. It fails when no
// hook runs for the operation, when the pod is selected by zero or several
// hooks, or when the pod has several exec targets and the hook names no
// container: with injected sidecars the first container is often a proxy,
// so the choice must be explicit.
func MatchHookForPod(policy *longhorn.VolumeHookPolicy, pod *corev1.Pod, operation longhorn.HookOperation) (hook *longhorn.VolumeHookPolicyHook, container string, err error) {
	podLabels := labels.Set(pod.Labels)
	var matched []*longhorn.VolumeHookPolicyHook
	covering := 0
	for i := range policy.Spec.Hooks {
		candidate := &policy.Spec.Hooks[i]
		if !HookRunsFor(candidate, operation) {
			continue
		}
		covering++
		if candidate.Exec.PodSelector == nil {
			matched = append(matched, candidate)
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(candidate.Exec.PodSelector)
		if err != nil {
			return nil, "", fmt.Errorf("hook %v has an invalid podSelector: %v", HookName(candidate), err)
		}
		if selector.Matches(podLabels) {
			matched = append(matched, candidate)
		}
	}
	if covering == 0 {
		return nil, "", fmt.Errorf("no hook of the policy runs for operation %v", operation)
	}

	switch len(matched) {
	case 0:
		return nil, "", fmt.Errorf("no hook for operation %v selects pod %v/%v", operation, pod.Namespace, pod.Name)
	case 1:
	default:
		names := make([]string, 0, len(matched))
		for _, candidate := range matched {
			names = append(names, HookName(candidate))
		}
		return nil, "", fmt.Errorf("multiple hooks %v select pod %v/%v; ensure exactly one hook selects the pod", names, pod.Namespace, pod.Name)
	}
	hook = matched[0]

	containers := HookContainerNames(pod)
	if hook.Exec.Container != "" {
		for _, name := range containers {
			if name == hook.Exec.Container {
				return hook, hook.Exec.Container, nil
			}
		}
		return nil, "", fmt.Errorf("hook %v names container %v, which pod %v/%v does not have among its exec targets %v", HookName(hook), hook.Exec.Container, pod.Namespace, pod.Name, containers)
	}
	if len(containers) != 1 {
		return nil, "", fmt.Errorf("hook %v names no container and pod %v/%v has %d exec targets %v", HookName(hook), pod.Namespace, pod.Name, len(containers), containers)
	}
	return hook, containers[0], nil
}

// HookContainerNames returns the pod's exec targets: the regular
// containers, then the native sidecars (init containers with restartPolicy
// Always). It reads the pod spec, not the status, so the list stays stable
// while a sidecar restarts; a named sidecar that is down fails at exec
// time instead. Ordinary init containers have already exited and ephemeral
// containers are debugging aids, so neither is listed.
func HookContainerNames(pod *corev1.Pod) []string {
	var names []string
	for _, c := range pod.Spec.Containers {
		names = append(names, c.Name)
	}
	for _, c := range pod.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			names = append(names, c.Name)
		}
	}
	return names
}
