package snapshothook

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// HooksEnabled reports whether a spec.hooks value runs hooks. Groups from
// before the field existed carry an empty value, which means none.
func HooksEnabled(hooks longhorn.SnapshotGroupHooks) bool {
	switch hooks {
	case "", longhorn.SnapshotGroupHooksNone:
		return false
	}
	return true
}

// IsHooked reports whether the group runs hooks.
func IsHooked(group *longhorn.SnapshotGroup) bool {
	return HooksEnabled(group.Spec.Hooks)
}

// EffectiveHookOperation returns the operation, or snapshot when it is not
// set yet. The mutating webhook sets the same default.
func EffectiveHookOperation(operation longhorn.HookOperation) longhorn.HookOperation {
	if operation == "" {
		return longhorn.HookOperationSnapshot
	}
	return operation
}

// IsTerminal reports whether the group has reached Ready or Failed.
func IsTerminal(group *longhorn.SnapshotGroup) bool {
	switch group.Status.Phase {
	case longhorn.SnapshotGroupPhaseReady, longhorn.SnapshotGroupPhaseFailed:
		return true
	}
	return false
}

// GroupDeadline is when the group's DeadlineSeconds expires. The controller
// fails the group and the executor releases it at this same instant.
func GroupDeadline(group *longhorn.SnapshotGroup) time.Time {
	return group.CreationTimestamp.Add(time.Duration(group.Spec.DeadlineSeconds) * time.Second)
}

// FindMember returns the member with the given snapshot name, or nil.
func FindMember(group *longhorn.SnapshotGroup, snapshotName string) *longhorn.SnapshotGroupMember {
	for i := range group.Spec.Members {
		if group.Spec.Members[i].SnapshotName == snapshotName {
			return &group.Spec.Members[i]
		}
	}
	return nil
}

// FindMemberByVolume returns the member with the given volume name, or nil.
func FindMemberByVolume(group *longhorn.SnapshotGroup, volumeName string) *longhorn.SnapshotGroupMember {
	for i := range group.Spec.Members {
		if group.Spec.Members[i].VolumeName == volumeName {
			return &group.Spec.Members[i]
		}
	}
	return nil
}

// FindHookStatus returns the member's status.hooks record, or nil when the
// executor has not started on it.
func FindHookStatus(group *longhorn.SnapshotGroup, snapshotName string) *longhorn.HookStatus {
	for i := range group.Status.Hooks {
		if group.Status.Hooks[i].SnapshotName == snapshotName {
			return &group.Status.Hooks[i]
		}
	}
	return nil
}

// FailureMessage renders a pod failure for logs and events: the reason,
// followed by the exit code when there is one.
func FailureMessage(reason longhorn.HookReason, exitCode *int32) string {
	if exitCode == nil {
		return string(reason)
	}
	return fmt.Sprintf("%v (exit %d)", reason, *exitCode)
}

// BoundPVC returns the PVC currently bound to the volume. It reports false
// for a volume that never had a PVC or whose PVC was released.
func BoundPVC(volume *longhorn.Volume) (namespace, name string, ok bool) {
	ks := volume.Status.KubernetesStatus
	if ks.PVCName == "" || ks.Namespace == "" || ks.LastPVCRefAt != "" {
		return "", "", false
	}
	return ks.Namespace, ks.PVCName, true
}

// IsPodHookReleased reports whether a recorded pod needs no more release
// work. A PostFailed pod may still be paused, but it is left to its owner.
func IsPodHookReleased(phase longhorn.HookPhase) bool {
	return phase == longhorn.HookPhasePostDone || phase == longhorn.HookPhasePostFailed
}

// IsPodPastPause reports whether a recorded pod's post-command has started,
// finished, or been given up on. Past this point the application may already
// be running again.
func IsPodPastPause(phase longhorn.HookPhase) bool {
	return phase == longhorn.HookPhasePostRunning || IsPodHookReleased(phase)
}

// UnreleasedHookPods returns every pod recorded in status.hooks[] that is
// not yet released.
func UnreleasedHookPods(group *longhorn.SnapshotGroup) []longhorn.HookPodStatus {
	var pods []longhorn.HookPodStatus
	for _, member := range group.Status.Hooks {
		for _, pod := range member.Pods {
			if !IsPodHookReleased(pod.Phase) {
				pods = append(pods, pod)
			}
		}
	}
	return pods
}

// ReleaseStarted reports whether the executor has begun releasing the group:
// at least one recorded pod is past its pause.
func ReleaseStarted(group *longhorn.SnapshotGroup) bool {
	for _, member := range group.Status.Hooks {
		for _, pod := range member.Pods {
			if IsPodPastPause(pod.Phase) {
				return true
			}
		}
	}
	return false
}

// CountMembersWithHookPods returns how many members have at least one
// recorded pod. Released members still count, so the value does not shrink
// while a release is in progress.
func CountMembersWithHookPods(group *longhorn.SnapshotGroup) int {
	count := 0
	for _, member := range group.Status.Hooks {
		if len(member.Pods) > 0 {
			count++
		}
	}
	return count
}

// IsGroupUnreleased reports whether a hooked group may still be holding a
// member application in its pre-command state: the group is not terminal,
// or a pod it ran a pre-command in is not yet released. Two unreleased
// groups must never share a volume, because releasing one would run its
// post-command while the other is still taking snapshots.
func IsGroupUnreleased(group *longhorn.SnapshotGroup) bool {
	if !IsHooked(group) {
		return false
	}
	if IsTerminal(group) {
		return len(UnreleasedHookPods(group)) > 0
	}
	return true
}

// GroupHasUnreleasedMember reports whether the named volume is a member of
// an unreleased group.
func GroupHasUnreleasedMember(group *longhorn.SnapshotGroup, volumeName string) bool {
	return IsGroupUnreleased(group) && FindMemberByVolume(group, volumeName) != nil
}

// GroupHasUnreleasedPod reports whether the group ran its pre-command in
// the pod and has not yet released it. Until the release, the pod stays
// paused and no other group may run hooks in it. A caller that does not
// know the pod's UID, such as the admission webhook, may pass an empty uid;
// the pod is then matched by namespace and name only.
func GroupHasUnreleasedPod(group *longhorn.SnapshotGroup, namespace, name, uid string) bool {
	for _, pod := range UnreleasedHookPods(group) {
		if pod.Namespace != namespace || pod.Name != name {
			continue
		}
		if uid == "" || pod.UID == uid {
			return true
		}
	}
	return false
}

// VolumeConflictMessage is the shared message for a volume that an
// unreleased hooked group still includes.
func VolumeConflictMessage(volumeName, groupName string) string {
	return fmt.Sprintf("volume %v is a member of snapshot group %v, which has not released it", volumeName, groupName)
}

// PodConflictMessage is the shared message for a pod that an unreleased
// hooked group still holds through another volume.
func PodConflictMessage(namespace, podName, groupName string) string {
	return fmt.Sprintf("pod %v/%v is still paused by snapshot group %v through another volume, which has not released it", namespace, podName, groupName)
}

// PostFailedHookPodWarnings returns one warning for each PostFailed pod
// the groups recorded for the volume's member. A PostFailed pod counts as
// released yet may still be paused, so admission accepts a new group on
// the volume with these warnings instead of refusing it.
func PostFailedHookPodWarnings(groups []*longhorn.SnapshotGroup, volumeName string) []string {
	var warnings []string
	for _, group := range groups {
		for _, pod := range postFailedHookPods(group, volumeName) {
			warnings = append(warnings, fmt.Sprintf("pod %v/%v on member volume %v may still be paused: its post-command failed in snapshot group %v and is not retried", pod.Namespace, pod.Name, volumeName, group.Name))
		}
	}
	return warnings
}

// postFailedHookPods returns the PostFailed pods the group recorded for
// the volume's member.
func postFailedHookPods(group *longhorn.SnapshotGroup, volumeName string) []longhorn.HookPodStatus {
	member := FindMemberByVolume(group, volumeName)
	if member == nil {
		return nil
	}
	hook := FindHookStatus(group, member.SnapshotName)
	if hook == nil {
		return nil
	}
	var pods []longhorn.HookPodStatus
	for _, pod := range hook.Pods {
		if pod.Phase == longhorn.HookPhasePostFailed {
			pods = append(pods, pod)
		}
	}
	return pods
}

// RunningWorkloadPods returns the names of the pods the volume's
// kubernetesStatus reports as Running.
func RunningWorkloadPods(volume *longhorn.Volume) []string {
	ks := volume.Status.KubernetesStatus
	if ks.LastPodRefAt != "" {
		// The workload list is history: the last pod detached.
		return nil
	}
	var pods []string
	for _, workload := range ks.WorkloadsStatus {
		if workload.PodStatus == string(corev1.PodRunning) {
			pods = append(pods, workload.PodName)
		}
	}
	return pods
}
