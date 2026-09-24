package runner

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/hookexecutor"
	"github.com/longhorn/longhorn-manager/hookexecutor/podexec"
	"github.com/longhorn/longhorn-manager/hookexecutor/snapshothook"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// release runs the post-commands of a group whose pause is over: the group
// is terminal, deleting, or past its deadline. It takes the group over from
// any pause still running, then works through the recorded pods until every
// one is PostDone or PostFailed.
func (r *Runner) release(ctx context.Context, name string) (bool, error) {
	group, err := r.getGroup(ctx, name)
	if err != nil || group == nil || !snapshothook.IsHooked(group) || !r.releaseDue(group) {
		return false, err
	}
	log := r.logger.WithField("snapshotGroup", name)

	newlyGated, pauseRunning := r.gateRelease(name, group.UID)
	if newlyGated && !snapshothook.IsTerminal(group) && group.DeletionTimestamp.IsZero() {
		log.Warnf("Group is past its %ds deadline and not terminal; releasing", group.Spec.DeadlineSeconds)
	}
	if pauseRunning {
		log.Info("Taking the group over from its pause")
	}

	// A running pause holds the group lock until its cancelled exec returns.
	// Acquiring the lock therefore blocks until the pause has fully exited.
	defer r.lockGroup(name)()

	// The read above is stale once the lock is held: the pause may have
	// recorded more pods before it exited. Re-read so none are missed.
	group, err = r.getGroup(ctx, name)
	if err != nil || group == nil {
		return false, err
	}
	if len(snapshothook.UnreleasedHookPods(group)) == 0 {
		return false, r.finishRelease(ctx, group)
	}

	budget, err := r.releaseBudget()
	if err != nil {
		return false, err
	}

	// Reverse pause order: the last member paused is the first released.
	members := snapshothook.MembersInPauseOrder(group)
	for _, member := range slices.Backward(members) {
		// A member the pause never reached has no record, and the release
		// must not create one for it.
		hook := snapshothook.FindHookStatus(group, member.SnapshotName)
		if hook == nil || !hasUnreleasedPods(hook) {
			continue
		}
		group, err = r.releaseMember(ctx, group, member.SnapshotName, budget)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// The group is gone. Whatever ran was released, and there is
				// nothing left to record on.
				log.Info("Group deleted during release; dropping its remaining work")
				return false, nil
			}
			return false, err
		}
	}

	// A round that leaves pods unreleased asks the queue for another round
	// with backoff.
	if len(snapshothook.UnreleasedHookPods(group)) > 0 {
		return true, nil
	}
	return false, r.finishRelease(ctx, group)
}

// releaseDue reports whether the pause of this group is over.
func (r *Runner) releaseDue(group *longhorn.SnapshotGroup) bool {
	return snapshothook.IsTerminal(group) || !group.DeletionTimestamp.IsZero() || r.now().After(snapshothook.GroupDeadline(group))
}

// releaseBudget is the settings' limit on one pod's release.
type releaseBudget struct {
	maxAttempts int64
	timeout     time.Duration
}

// releaseBudget reads the budget from the settings.
func (r *Runner) releaseBudget() (releaseBudget, error) {
	maxAttempts, err := r.store.GetSettingAsInt(types.SettingNameHookReleaseAttempts)
	if err != nil {
		return releaseBudget{}, errors.Wrapf(err, "failed to read setting %v", types.SettingNameHookReleaseAttempts)
	}
	timeoutSeconds, err := r.store.GetSettingAsInt(types.SettingNameHookReleaseTimeout)
	if err != nil {
		return releaseBudget{}, errors.Wrapf(err, "failed to read setting %v", types.SettingNameHookReleaseTimeout)
	}
	return releaseBudget{maxAttempts: maxAttempts, timeout: time.Duration(timeoutSeconds) * time.Second}, nil
}

func hasUnreleasedPods(hook *longhorn.HookStatus) bool {
	for _, pod := range hook.Pods {
		if !snapshothook.IsPodHookReleased(pod.Phase) {
			return true
		}
	}
	return false
}

// releaseRound is one release round for one member.
type releaseRound struct {
	log          logrus.FieldLogger
	snapshotName string
	policy       *longhorn.VolumeHookPolicy
	failure      *memberFailure
	// policyChanged reports that the policy has changed since the pause
	// recorded it. The round still runs the recorded hooks and notes the
	// drift on the outcome.
	policyChanged bool
	budget        releaseBudget

	// group is the latest written group. member and hook are this member's
	// entries in it.
	group  *longhorn.SnapshotGroup
	member *longhorn.SnapshotGroupMember
	hook   *longhorn.HookStatus

	// targets holds the exec target of every pod that gets an attempt this
	// round. alreadyFailed names the pods that gave up in an earlier round,
	// so their failure is not reported twice. Both are keyed by pod name.
	targets       map[string]runTarget
	alreadyFailed map[string]bool
}

// refresh points the round at the given group and re-finds this member's
// entries in it.
func (round *releaseRound) refresh(group *longhorn.SnapshotGroup) {
	round.group = group
	round.member = snapshothook.FindMember(group, round.snapshotName)
	round.hook = hookStatusFor(group, round.snapshotName)
}

// write persists the round's group with the given write and refreshes the
// round from the result. On error the round still points at the unpersisted
// state; callers return the error without reading the round again, so this
// is harmless.
func (round *releaseRound) write(write func(*longhorn.SnapshotGroup) (*longhorn.SnapshotGroup, error)) error {
	group, err := write(round.group)
	if err != nil {
		return err
	}
	round.refresh(group)
	return nil
}

// policyNamespacedName renders the member's recorded policy reference for
// log and event messages.
func (round *releaseRound) policyNamespacedName() string {
	return snapshothook.PolicyRefNamespacedName(round.member.HookPolicyRef)
}

// releaseMember runs one release round for one member: it plans which
// recorded pods get an attempt, persists the attempt counts, runs the
// post-commands in parallel, and persists the outcomes. The attempt counts
// are persisted before any command runs, so a crash mid-round wastes at
// most one attempt per pod and never runs a command without counting it.
// A crash before the outcomes are persisted runs the post-commands again
// on the next round, which is why they must be safe to repeat.
func (r *Runner) releaseMember(ctx context.Context, group *longhorn.SnapshotGroup, snapshotName string, budget releaseBudget) (*longhorn.SnapshotGroup, error) {
	round := &releaseRound{
		log:           r.logger.WithField("snapshotGroup", group.Name).WithField("member", snapshotName),
		snapshotName:  snapshotName,
		budget:        budget,
		targets:       map[string]runTarget{},
		alreadyFailed: map[string]bool{},
	}
	round.refresh(group)

	resolved, failure, err := r.policyForMember(ctx, round.member)
	if err != nil {
		if apierrors.IsForbidden(err) {
			r.warnReleaseBlocked(group, resolved.ref.Namespace, fmt.Sprintf("release of member %v cannot read hook policy %v: the hook executor is denied get on volumehookpolicies", snapshotName, snapshothook.PolicyRefNamespacedName(resolved.ref)))
		}
		return nil, err
	}
	round.policy, round.failure, round.policyChanged = resolved.policy, failure, resolved.changed

	// Decide and persist before any command can run.
	if err := r.planReleaseRound(ctx, round); err != nil {
		return nil, err
	}
	round.hook.Phase = memberReleasePhase(round.hook)
	if err := round.write(func(group *longhorn.SnapshotGroup) (*longhorn.SnapshotGroup, error) {
		return r.writeStatus(ctx, group)
	}); err != nil {
		return nil, err
	}
	before := round.hook.DeepCopy()

	// Run what was persisted, then record the outcomes.
	r.runReleaseRound(ctx, round)
	round.hook.Phase = memberReleasePhase(round.hook)
	if err := round.write(func(group *longhorn.SnapshotGroup) (*longhorn.SnapshotGroup, error) {
		return r.writeOutcomes(ctx, group, before, r.writeStatus)
	}); err != nil {
		return nil, err
	}
	r.reportPostFailures(round)
	return round.group, nil
}

// planReleaseRound settles every unreleased pod of the member for this
// round without running anything. A pod that is gone, replaced, or out of
// attempts is closed out in place. Every other pod is resolved to an exec
// target and uses up one of its attempts. A failed pod read is returned
// for the queue to retry.
func (r *Runner) planReleaseRound(ctx context.Context, round *releaseRound) error {
	for i := range round.hook.Pods {
		record := &round.hook.Pods[i]
		if record.Phase == longhorn.HookPhasePostFailed {
			round.alreadyFailed[record.Name] = true
		}
		if snapshothook.IsPodHookReleased(record.Phase) {
			continue
		}
		if round.failure != nil {
			settleWithoutRun(record, round.failure.reason)
			continue
		}
		target, reason, err := r.resolveTarget(ctx, round.group, round.policy, record, postCommand)
		if err != nil {
			if apierrors.IsForbidden(err) {
				r.warnReleaseBlocked(round.group, record.Namespace, fmt.Sprintf("release of hook %v in policy %v cannot read pod %v/%v: the hook executor is denied list on pods", record.HookName, round.policyNamespacedName(), record.Namespace, record.Name))
			}
			return err
		}
		if reason != "" {
			settleWithoutRun(record, reason)
			continue
		}
		if int64(record.ReleaseAttempts) >= round.budget.maxAttempts {
			record.Phase = longhorn.HookPhasePostFailed
			if record.Reason == "" {
				record.Reason = longhorn.HookReasonExecFailed
			}
			continue
		}
		record.ReleaseAttempts++
		record.Phase = longhorn.HookPhasePostRunning
		round.targets[record.Name] = target
	}
	return nil
}

// settleWithoutRun closes out a pod that gets no post-command this round.
// A pod that is gone took its paused process with it, leaving nothing to
// release. Every other reason is a failure.
func settleWithoutRun(record *longhorn.HookPodStatus, reason longhorn.HookReason) {
	record.Phase = longhorn.HookPhasePostFailed
	if reason == longhorn.HookReasonPodNotFound {
		record.Phase = longhorn.HookPhasePostDone
	}
	record.Reason = reason
}

// runReleaseRound runs the post-command in every pod the plan gave an
// attempt, in parallel, each bounded by the round's timeout, and applies
// the outcomes to the records in place.
func (r *Runner) runReleaseRound(ctx context.Context, round *releaseRound) {
	var wg sync.WaitGroup
	for i := range round.hook.Pods {
		record := &round.hook.Pods[i]
		target, run := round.targets[record.Name]
		if !run || record.Phase != longhorn.HookPhasePostRunning {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			runCtx, cancel := context.WithTimeout(ctx, round.budget.timeout)
			defer cancel()
			err := r.podExec.Exec(runCtx, target.namespace, target.pod, target.container, target.command)
			r.recordReleaseOutcome(runCtx, round, record, err)
		}()
	}
	wg.Wait()
}

// reportPostFailures logs and events every pod that gave up in this round,
// once the outcomes are written.
func (r *Runner) reportPostFailures(round *releaseRound) {
	for _, record := range round.hook.Pods {
		if record.Phase != longhorn.HookPhasePostFailed || round.alreadyFailed[record.Name] {
			continue
		}
		message := snapshothook.FailureMessage(record.Reason, record.ExitCode)
		container := round.targets[record.Name].container
		if container == "" {
			container = "unknown"
		}
		round.log.Warnf("Post-command of hook %v failed in pod %v/%v after %d attempts: %v", record.HookName, record.Namespace, record.Name, record.ReleaseAttempts, message)
		r.warn(round.group, constant.EventReasonPostHookFailed,
			"post-command of hook %v in policy %v failed in pod %v/%v (container %v): %v; the application may still be paused; run that hook's postCommand by hand with kubectl exec",
			record.HookName, round.policyNamespacedName(), record.Namespace, record.Name, container, message)
	}
}

// recordReleaseOutcome applies one exec result to a pod record. runCtx is
// the context the exec ran under; its deadline tells whether a failed exec
// timed out or hit a transport error.
func (r *Runner) recordReleaseOutcome(runCtx context.Context, round *releaseRound, record *longhorn.HookPodStatus, err error) {
	if err == nil {
		record.Phase = longhorn.HookPhasePostDone
		record.ExitCode = nil
		record.Reason = ""
		if round.policyChanged {
			// The command came from a newer policy than the pre-command. The
			// release still happened, but the pair was not the recorded one.
			record.Reason = longhorn.HookReasonPolicyChanged
		}
		return
	}
	if apierrors.IsNotFound(err) {
		record.Phase = longhorn.HookPhasePostDone
		record.Reason = longhorn.HookReasonPodNotFound
		return
	}
	if podexec.ClassifyExecError(err) == podexec.ExecFailureCouldNotStart {
		// No process ran, so the attempt is given back and retried next
		// round. The retry is unbounded; the event is what makes the wait
		// visible.
		record.ReleaseAttempts--
		round.log.Infof("Post-command could not start in pod %v/%v: %v; retrying", record.Namespace, record.Name, err)
		if apierrors.IsForbidden(err) {
			r.warnReleaseBlocked(round.group, record.Namespace, fmt.Sprintf("post-command of hook %v in policy %v cannot start in pod %v/%v: the hook executor is denied pods/exec", record.HookName, round.policyNamespacedName(), record.Namespace, record.Name))
			return
		}
		r.warn(round.group, constant.EventReasonHookReleaseBlocked,
			"post-command of hook %v in policy %v could not start in pod %v/%v: %v; the release retries until it can start",
			record.HookName, round.policyNamespacedName(), record.Namespace, record.Name, err)
		return
	}
	outcome := classifyRunOutcome(runCtx, err)
	record.Reason = outcome.reason
	record.ExitCode = outcome.exitCode
	if int64(record.ReleaseAttempts) >= round.budget.maxAttempts {
		record.Phase = longhorn.HookPhasePostFailed
		return
	}
	round.log.Infof("Post-command failed in pod %v/%v (%v), attempt %d of %d; retrying", record.Namespace, record.Name, outcome.reason, record.ReleaseAttempts, round.budget.maxAttempts)
}

// warnReleaseBlocked reports a Forbidden from anywhere on the release path:
// the policy read, the pod read, or the exec. Retrying alone cannot restore
// the lost access, so the event tells the operator what to put back.
func (r *Runner) warnReleaseBlocked(group *longhorn.SnapshotGroup, namespace, blocked string) {
	r.warn(group, constant.EventReasonHookReleaseBlocked,
		"%v in namespace %v; the release retries until access is restored; grant it by applying this RoleBinding:\n%s",
		blocked, namespace, hookexecutor.RoleBindingManifest(namespace, group.Namespace))
}

// memberReleasePhase folds the pod phases into the member's.
func memberReleasePhase(hook *longhorn.HookStatus) longhorn.HookPhase {
	phase := longhorn.HookPhasePostDone
	for _, pod := range hook.Pods {
		if !snapshothook.IsPodHookReleased(pod.Phase) {
			return longhorn.HookPhasePostRunning
		}
		if pod.Phase == longhorn.HookPhasePostFailed {
			phase = longhorn.HookPhasePostFailed
		}
	}
	return phase
}

// finishRelease sets postHooksDoneAt and reports the result once every
// recorded pod is released. Called with the group lock held.
func (r *Runner) finishRelease(ctx context.Context, group *longhorn.SnapshotGroup) error {
	if group.Status.PostHooksDoneAt != nil {
		return nil
	}
	pods, failed := 0, 0
	for _, hook := range group.Status.Hooks {
		for _, pod := range hook.Pods {
			pods++
			if pod.Phase == longhorn.HookPhasePostFailed {
				failed++
			}
		}
	}
	if pods == 0 {
		// Nothing was ever paused, so there is no window to close.
		return nil
	}
	now := metav1.NewTime(r.now())
	group.Status.PostHooksDoneAt = &now
	written, err := r.writeStatus(ctx, group)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if failed > 0 {
		return nil
	}
	r.logger.WithField("snapshotGroup", group.Name).Infof("Post-hooks done on %d pods", pods)
	r.event(written, corev1.EventTypeNormal, constant.EventReasonPostHooksSucceeded, "Post-hooks succeeded in all %d recorded pods", pods)
	return nil
}
