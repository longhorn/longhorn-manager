package runner

import (
	"fmt"

	"github.com/longhorn/longhorn-manager/hookexecutor/snapshothook"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// hookStatusFor returns the member's status.hooks record, creating a
// Pending one when it does not exist yet.
func hookStatusFor(group *longhorn.SnapshotGroup, snapshotName string) *longhorn.HookStatus {
	if hook := snapshothook.FindHookStatus(group, snapshotName); hook != nil {
		return hook
	}
	group.Status.Hooks = append(group.Status.Hooks, longhorn.HookStatus{
		SnapshotName: snapshotName,
		Phase:        longhorn.HookPhasePending,
	})
	return &group.Status.Hooks[len(group.Status.Hooks)-1]
}

// refreshHookProgress recomputes the HookProgress printer column. Pre
// progress is counted over members, post progress over recorded pods.
func refreshHookProgress(group *longhorn.SnapshotGroup) {
	if !snapshothook.IsHooked(group) {
		group.Status.HookProgress = ""
		return
	}
	total := len(group.Spec.Members)
	preDone, preFailed := 0, 0
	podsTotal, postDone, postFailed := 0, 0, 0
	for _, hook := range group.Status.Hooks {
		switch hook.Phase {
		case longhorn.HookPhasePreDone, longhorn.HookPhasePostRunning, longhorn.HookPhasePostDone, longhorn.HookPhasePostFailed:
			preDone++
		case longhorn.HookPhasePreFailed:
			preFailed++
		}
		for _, pod := range hook.Pods {
			podsTotal++
			switch pod.Phase {
			case longhorn.HookPhasePostDone:
				postDone++
			case longhorn.HookPhasePostFailed:
				postFailed++
			}
		}
	}

	releasing := group.Status.PostHooksDoneAt != nil || snapshothook.ReleaseStarted(group) || snapshothook.IsTerminal(group) || !group.DeletionTimestamp.IsZero()
	switch {
	case postFailed > 0 && postDone+postFailed == podsTotal:
		group.Status.HookProgress = fmt.Sprintf("post failed %d/%d", postFailed, podsTotal)
	case releasing && podsTotal > 0 && postDone == podsTotal:
		group.Status.HookProgress = "post done"
	case releasing && podsTotal > 0:
		group.Status.HookProgress = fmt.Sprintf("post %d/%d", postDone+postFailed, podsTotal)
	case preFailed > 0:
		group.Status.HookProgress = "pre failed"
	case group.Status.PreHooksDone:
		group.Status.HookProgress = "pre done"
	default:
		group.Status.HookProgress = fmt.Sprintf("pre %d/%d", preDone, total)
	}
}
