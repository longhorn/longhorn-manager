package runner

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/hookexecutor/snapshothook"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// runTarget holds everything one exec needs: where to run the command and
// what to run.
type runTarget struct {
	namespace string
	pod       string
	container string
	command   []string
}

// pause walks the members of a RunningPreHooks group in spec order. Each
// member is claimed (its pods recorded in status) before its pre-command
// runs, so even after a crash between the two steps, status still shows
// which pods may be paused. A member still PreRunning on entry means a
// previous attempt crashed there: it is failed as interrupted, because the
// command may already have run and must not run twice. Pausing stops at
// the first failed member. The controller then fails the group, which
// triggers the release.
func (r *Runner) pause(ctx context.Context, name string) (bool, error) {
	defer r.lockGroup(name)()

	ctx, endPause := r.beginPause(ctx, name)
	defer endPause()

	group, err := r.getGroup(ctx, name)
	if err != nil {
		return false, err
	}
	if group == nil || !r.pauseEligible(group) {
		return false, nil
	}
	log := r.logger.WithField("snapshotGroup", name)

	for _, member := range snapshothook.MembersInPauseOrder(group) {
		if ctx.Err() != nil {
			return false, nil
		}
		hook := hookStatusFor(group, member.SnapshotName)
		switch hook.Phase {
		case "", longhorn.HookPhasePending:
			group, err = r.pauseMember(ctx, group, member.SnapshotName)
			if err != nil || group == nil {
				return false, err
			}
		case longhorn.HookPhasePreRunning:
			_, err = r.failInterruptedMember(ctx, group, member.SnapshotName)
			return false, err
		case longhorn.HookPhasePreDone:
			continue
		default:
			// PreFailed, or already releasing: nothing more to pause.
			return false, nil
		}
	}

	if group.Status.PreHooksDone {
		return false, nil
	}
	now := metav1.NewTime(r.now())
	group.Status.PreHooksDone = true
	group.Status.PreHooksDoneAt = &now
	written, err := r.writeGuarded(ctx, group)
	if err != nil || written == nil {
		return false, err
	}
	log.Infof("Pre-hooks done on all %d members", len(group.Spec.Members))
	r.event(written, corev1.EventTypeNormal, constant.EventReasonPreHooksSucceeded,
		"Pre-hooks succeeded on all %d members", len(group.Spec.Members))
	return false, nil
}

// pauseMember claims one unclaimed member, then runs its pre-command with
// the policy validated during the claim. It returns the group as last
// written, or nil when the caller must stop pausing more members.
func (r *Runner) pauseMember(ctx context.Context, group *longhorn.SnapshotGroup, snapshotName string) (*longhorn.SnapshotGroup, error) {
	group, policy, err := r.claimMember(ctx, group, snapshotName)
	if err != nil || group == nil || hookStatusFor(group, snapshotName).Phase == longhorn.HookPhasePreFailed {
		return nil, err
	}
	group, err = r.runPreCommands(ctx, group, snapshotName, policy)
	if err != nil || group == nil || hookStatusFor(group, snapshotName).Phase == longhorn.HookPhasePreFailed {
		return nil, err
	}
	return group, nil
}

// pauseEligible reports whether pausing may start. The release check reads
// both the in-memory gate and the recorded pods, because a new leader has
// no gate for a release the old leader began.
func (r *Runner) pauseEligible(group *longhorn.SnapshotGroup) bool {
	if r.isReleaseGated(group.UID) || snapshothook.ReleaseStarted(group) || !group.DeletionTimestamp.IsZero() {
		return false
	}
	if r.now().After(snapshothook.GroupDeadline(group)) {
		return false
	}
	return group.Status.Phase == longhorn.SnapshotGroupPhaseRunningPreHooks && !group.Status.PreHooksDone
}

// writeGuarded writes status, unless the release took the group over since
// the caller read it. In that case it returns nil, nil and the caller must
// stop writing.
func (r *Runner) writeGuarded(ctx context.Context, group *longhorn.SnapshotGroup) (*longhorn.SnapshotGroup, error) {
	if ctx.Err() != nil || r.isReleaseGated(group.UID) {
		return nil, nil
	}
	return r.writeStatus(ctx, group)
}

// memberFailure is why a member failed, with the detail for its event.
type memberFailure struct {
	reason longhorn.HookReason
	detail string
}

// claimMember records the member's pods in status, or records a
// member-level failure. On success it also returns the policy the pods
// were matched against. On a write conflict it re-reads and retries, as
// long as the group is still pausable and the member unclaimed. If the
// re-read shows the member already claimed, another writer owns it, so
// this caller stops and the pre-command runs only once.
func (r *Runner) claimMember(ctx context.Context, group *longhorn.SnapshotGroup, snapshotName string) (*longhorn.SnapshotGroup, *longhorn.VolumeHookPolicy, error) {
	log := r.logger.WithField("snapshotGroup", group.Name)
	for range writeAttempts {
		member := snapshothook.FindMember(group, snapshotName)
		if member == nil {
			return nil, nil, fmt.Errorf("member %v is not in the group spec", snapshotName)
		}
		hook := hookStatusFor(group, snapshotName)
		if hook.Phase != "" && hook.Phase != longhorn.HookPhasePending {
			log.Warnf("Member %v was already claimed by another writer; leaving it to that writer", snapshotName)
			return nil, nil, nil
		}

		resolved, err := r.resolveMember(ctx, group, member)
		if err != nil {
			return nil, nil, err
		}
		records, pvcRef, failure := resolved.records, resolved.pvcRef, resolved.failure
		if failure != nil {
			hook.Phase = longhorn.HookPhasePreFailed
			hook.Reason = failure.reason
			hook.Pods = nil
		} else {
			hook.Phase = longhorn.HookPhasePreRunning
			hook.Reason = ""
			hook.PVCRef = pvcRef
			hook.Pods = records
			if group.Status.HooksStartedAt == nil {
				now := metav1.NewTime(r.now())
				group.Status.HooksStartedAt = &now
			}
		}

		written, err := r.writeGuarded(ctx, group)
		if apierrors.IsConflict(err) {
			group, err = r.reloadPausableGroup(ctx, group.Name)
			if group == nil || err != nil {
				return group, nil, err
			}
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		if written == nil {
			return nil, nil, nil
		}

		if failure != nil {
			log.Warnf("Pre-hook failed on member %v: %v: %v", snapshotName, failure.reason, failure.detail)
			r.warn(written, constant.EventReasonPreHookFailed, "Pre-hook failed on member %v (policy %v): %v: %v", snapshotName, snapshothook.PolicyRefNamespacedName(member.HookPolicyRef), failure.reason, failure.detail)
			commandFailures.WithLabelValues("pre", string(failure.reason), string(group.Spec.HookOperation)).Inc()
			return written, nil, nil
		}
		log.Infof("Claimed member %v: %d pods on PVC %v", snapshotName, len(records), snapshothook.PVCRefNamespacedName(pvcRef))
		return written, resolved.policy, nil
	}
	return nil, nil, fmt.Errorf("failed to claim member %v after %d conflicts", snapshotName, writeAttempts)
}

// reloadPausableGroup reloads the group after a Conflict. It returns nil
// when the group is gone or no longer pausable.
func (r *Runner) reloadPausableGroup(ctx context.Context, name string) (*longhorn.SnapshotGroup, error) {
	group, err := r.getGroup(ctx, name)
	if err != nil {
		return nil, err
	}
	if group == nil || !r.pauseEligible(group) {
		return nil, nil
	}
	return group, nil
}

// resolvedMember is the outcome of resolving one member. It holds either a
// failure, or the pod records and the policy that assigned their hooks.
type resolvedMember struct {
	records []longhorn.HookPodStatus
	pvcRef  *longhorn.PVCRef
	policy  *longhorn.VolumeHookPolicy
	failure *memberFailure
}

// resolveMember finds the pods to pause for one member. A member that
// cannot be paused is recorded as a member-level failure; API errors are
// returned for the queue to retry, since they say nothing about the member.
func (r *Runner) resolveMember(ctx context.Context, group *longhorn.SnapshotGroup, member *longhorn.SnapshotGroupMember) (resolvedMember, error) {
	fail := func(reason longhorn.HookReason, detail string) (resolvedMember, error) {
		return resolvedMember{failure: &memberFailure{reason, detail}}, nil
	}

	other, err := r.olderUnreleasedGroup(group, member.VolumeName)
	if err != nil {
		return resolvedMember{}, err
	}
	if other != "" {
		return fail(longhorn.HookReasonVolumeConflict, snapshothook.VolumeConflictMessage(member.VolumeName, other))
	}

	volume, err := r.store.GetVolumeRO(member.VolumeName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return fail(longhorn.HookReasonPodNotFound, fmt.Sprintf("volume %v does not exist", member.VolumeName))
		}
		return resolvedMember{}, errors.Wrapf(err, "failed to get volume %v", member.VolumeName)
	}
	namespace, pvcName, bound := snapshothook.BoundPVC(volume)
	if !bound {
		return fail(longhorn.HookReasonPodNotFound, fmt.Sprintf("volume %v has no bound PVC", member.VolumeName))
	}
	// Admission refuses PVCs in Longhorn's namespace, but the PVC is
	// resolved live here and the volume may have been re-bound since. Refuse
	// again so no hook command ever runs in Longhorn's own pods.
	if namespace == r.store.Namespace() {
		return fail(longhorn.HookReasonPolicyNotFound, snapshothook.LonghornNamespaceMessage(namespace))
	}
	pvcRef := &longhorn.PVCRef{Namespace: namespace, Name: pvcName}
	pvc := snapshothook.PVCRefNamespacedName(pvcRef)

	resolved, failure, err := r.policyForMember(ctx, member)
	if err != nil {
		return resolvedMember{}, err
	}
	if failure != nil {
		return resolvedMember{failure: failure}, nil
	}
	if resolved.changed {
		return fail(longhorn.HookReasonPolicyChanged,
			fmt.Sprintf("hook policy %v changed since admission (generation %d, was %d)", snapshothook.PolicyRefNamespacedName(resolved.ref), resolved.policy.Generation, resolved.ref.Generation))
	}
	policy := resolved.policy

	pods, err := r.store.ListPods(ctx, namespace)
	if err != nil {
		return resolvedMember{}, errors.Wrapf(err, "failed to list pods in namespace %v", namespace)
	}
	if terminating := snapshothook.TerminatingPodsUsingPVC(pods, pvcName); len(terminating) > 0 {
		// A terminating pod may still write until its containers stop, so
		// wait for it to leave: the queue retries with backoff.
		return resolvedMember{}, fmt.Errorf("pod %v/%v mounting PVC %v is shutting down: %w",
			terminating[0].Namespace, terminating[0].Name, pvc, errWaiting)
	}
	consumers := snapshothook.ConsumerPodsUsingPVC(pods, pvcName)
	for _, pod := range consumers {
		// A starting pod may already write through its init containers,
		// but only a Running pod can take an exec: wait for it.
		if pod.Status.Phase != corev1.PodRunning {
			return resolvedMember{}, fmt.Errorf("pod %v/%v mounting PVC %v is still starting: %w",
				pod.Namespace, pod.Name, pvc, errWaiting)
		}
	}
	running := consumers
	if len(running) == 0 {
		return fail(longhorn.HookReasonPodNotFound, fmt.Sprintf("no running pod mounts PVC %v", pvc))
	}

	// A pod already paused by another group must not be paused again, even
	// when the two groups share no volume. When that group releases the
	// pod, the application resumes while this one is still taking
	// snapshots.
	holder, heldPod, err := r.otherGroupHoldingPod(group, running)
	if err != nil {
		return resolvedMember{}, err
	}
	if holder != "" {
		return fail(longhorn.HookReasonPodConflict, snapshothook.PodConflictMessage(heldPod.Namespace, heldPod.Name, holder))
	}

	var records []longhorn.HookPodStatus
	for _, pod := range running {
		hook, _, err := snapshothook.MatchHookForPod(policy, pod, group.Spec.HookOperation)
		if err != nil {
			return fail(longhorn.HookReasonHookNotMatched, err.Error())
		}
		records = append(records, longhorn.HookPodStatus{
			Namespace:    pod.Namespace,
			Name:         pod.Name,
			UID:          string(pod.UID),
			ContainerIDs: util.PodContainerIDs(pod),
			HookName:     snapshothook.HookName(hook),
			Phase:        longhorn.HookPhasePreRunning,
		})
	}
	return resolvedMember{records: records, pvcRef: pvcRef, policy: policy}, nil
}

// otherGroupHoldingPod finds a pod that another group paused and has not
// yet released. It returns that group's name and the pod.
func (r *Runner) otherGroupHoldingPod(group *longhorn.SnapshotGroup, pods []*corev1.Pod) (string, *corev1.Pod, error) {
	others, err := r.store.ListSnapshotGroupsRO()
	if err != nil {
		return "", nil, errors.Wrap(err, "failed to list snapshot groups for the pod conflict check")
	}
	for _, other := range others {
		if other.UID == group.UID {
			continue
		}
		for _, pod := range pods {
			if snapshothook.GroupHasUnreleasedPod(other, pod.Namespace, pod.Name, string(pod.UID)) {
				return other.Name, pod, nil
			}
		}
	}
	return "", nil, nil
}

// olderUnreleasedGroup returns the name of an older unreleased hooked
// group that includes the volume, or "" when there is none. The group
// with the earlier creationTimestamp is older; on a tie, the one whose
// name sorts first. This total order means exactly one of two concurrent
// groups backs off, which keeps two releases off one volume.
func (r *Runner) olderUnreleasedGroup(group *longhorn.SnapshotGroup, volumeName string) (string, error) {
	others, err := r.store.ListSnapshotGroupsRO()
	if err != nil {
		return "", errors.Wrap(err, "failed to list snapshot groups for the volume conflict check")
	}
	for _, other := range others {
		if other.UID == group.UID || !snapshothook.GroupHasUnreleasedMember(other, volumeName) {
			continue
		}
		if other.CreationTimestamp.Before(&group.CreationTimestamp) ||
			(other.CreationTimestamp.Equal(&group.CreationTimestamp) && other.Name < group.Name) {
			return other.Name, nil
		}
	}
	return "", nil
}

// runPreCommands runs the pre-command in every recorded pod of a claimed
// member, in parallel, bounded by the group deadline, then records the
// outcomes. It uses the policy from the claim, not a fresh read: a fresh
// read could disagree with the hook names recorded on the pods.
func (r *Runner) runPreCommands(ctx context.Context, group *longhorn.SnapshotGroup, snapshotName string, policy *longhorn.VolumeHookPolicy) (*longhorn.SnapshotGroup, error) {
	member := snapshothook.FindMember(group, snapshotName)
	hook := hookStatusFor(group, snapshotName)
	before := hook.DeepCopy()
	log := r.logger.WithField("snapshotGroup", group.Name).WithField("member", snapshotName)

	runCtx, cancel := context.WithDeadline(ctx, snapshothook.GroupDeadline(group))
	defer cancel()

	var wg sync.WaitGroup
	for i := range hook.Pods {
		record := &hook.Pods[i]
		if record.Phase != longhorn.HookPhasePreRunning {
			continue
		}
		wg.Go(func() {
			target, reason, err := r.resolveTarget(runCtx, group, policy, record, preCommand)
			if err != nil {
				// The pod could not be confirmed, so no command runs. With
				// the deadline running, that is a pre-command that could not
				// start.
				log.WithError(err).Warnf("Pre-command could not start in pod %v/%v", record.Namespace, record.Name)
				reason = longhorn.HookReasonExecFailed
			}
			if reason != "" {
				record.Phase = longhorn.HookPhasePreFailed
				record.Reason = reason
				return
			}
			outcome := classifyRunOutcome(runCtx, r.podExec.Exec(runCtx, target.namespace, target.pod, target.container, target.command))
			if outcome.ok {
				record.Phase = longhorn.HookPhasePreDone
				record.Reason = ""
				record.ExitCode = nil
				return
			}
			record.Phase = longhorn.HookPhasePreFailed
			record.Reason = outcome.reason
			record.ExitCode = outcome.exitCode
		})
	}
	wg.Wait()

	hook.Phase = longhorn.HookPhasePreDone
	for _, record := range hook.Pods {
		if record.Phase != longhorn.HookPhasePreDone {
			hook.Phase = longhorn.HookPhasePreFailed
			break
		}
	}

	written, err := r.writeOutcomes(ctx, group, before, r.writeGuarded)
	if err != nil || written == nil {
		return written, err
	}
	if hook.Phase == longhorn.HookPhasePreDone {
		log.Infof("Pre-command succeeded in %d pods", len(hook.Pods))
		return written, nil
	}
	r.reportPreFailures(log, written, member, hook)
	return written, nil
}

// failInterruptedMember fails a member found still PreRunning on entry.
// The writer that claimed it stopped between running the pre-command and
// recording the outcome: the command may not have started, may have
// finished, or may still be running, so it must not run again. Every
// PreRunning pod is failed as interrupted. The pods stay recorded, and
// the release runs the post-command in each.
func (r *Runner) failInterruptedMember(ctx context.Context, group *longhorn.SnapshotGroup, snapshotName string) (*longhorn.SnapshotGroup, error) {
	member := snapshothook.FindMember(group, snapshotName)
	hook := hookStatusFor(group, snapshotName)
	log := r.logger.WithField("snapshotGroup", group.Name).WithField("member", snapshotName)

	for i := range hook.Pods {
		record := &hook.Pods[i]
		if record.Phase != longhorn.HookPhasePreRunning {
			continue
		}
		record.Phase = longhorn.HookPhasePreFailed
		record.Reason = longhorn.HookReasonInterrupted
	}
	hook.Phase = longhorn.HookPhasePreFailed

	written, err := r.writeGuarded(ctx, group)
	if err != nil || written == nil {
		return written, err
	}
	r.reportPreFailures(log, written, member, hook)
	return written, nil
}

// reportPreFailures logs and events every PreFailed pod of a member after
// its outcomes have been written.
func (r *Runner) reportPreFailures(log logrus.FieldLogger, group *longhorn.SnapshotGroup, member *longhorn.SnapshotGroupMember, hook *longhorn.HookStatus) {
	for _, record := range hook.Pods {
		if record.Phase != longhorn.HookPhasePreFailed {
			continue
		}
		message := snapshothook.FailureMessage(record.Reason, record.ExitCode)
		log.Warnf("Pre-command of hook %v failed in pod %v/%v: %v", record.HookName, record.Namespace, record.Name, message)
		r.warn(group, constant.EventReasonPreHookFailed,
			"Pre-command of hook %v in policy %v failed in pod %v/%v on member %v: %v",
			record.HookName, snapshothook.PolicyRefNamespacedName(member.HookPolicyRef), record.Namespace, record.Name, hook.SnapshotName, message)
		commandFailures.WithLabelValues("pre", string(record.Reason), string(group.Spec.HookOperation)).Inc()
	}
}

// memberPolicy is the result of reading a member's recorded hook policy.
type memberPolicy struct {
	// ref is set as long as the member has a reference, even when the
	// read failed, so failures can still name the policy.
	ref *longhorn.VolumeHookPolicyRef
	// policy is set only when the read succeeded.
	policy *longhorn.VolumeHookPolicy
	// changed means the live policy generation differs from the one
	// recorded at admission.
	changed bool
}

// policyForMember reads a member's recorded hook policy. Both the claim
// and the release use it. A missing reference or policy is reported as a
// member failure, not an error. It detects policy drift; what to do
// about it is the caller's call.
func (r *Runner) policyForMember(ctx context.Context, member *longhorn.SnapshotGroupMember) (memberPolicy, *memberFailure, error) {
	ref := member.HookPolicyRef
	if ref == nil {
		return memberPolicy{}, &memberFailure{longhorn.HookReasonPolicyNotFound, fmt.Sprintf("member %v has no hook policy reference; the group was admitted by a longhorn-manager webhook that predates hooks, recreate it", member.SnapshotName)}, nil
	}
	resolved := memberPolicy{ref: ref}
	policy, err := r.store.GetVolumeHookPolicy(ctx, ref.Namespace, ref.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return resolved, &memberFailure{longhorn.HookReasonPolicyNotFound, fmt.Sprintf("hook policy %v does not exist", snapshothook.PolicyRefNamespacedName(ref))}, nil
		}
		return resolved, nil, errors.Wrapf(err, "failed to get hook policy %v", snapshothook.PolicyRefNamespacedName(ref))
	}
	resolved.policy = policy
	// A different UID means the policy was deleted and recreated; its
	// generation restarts at 1 and cannot be trusted on its own.
	resolved.changed = policy.UID != ref.UID || policy.Generation != ref.Generation
	return resolved, nil, nil
}

// commandPhase names which of a hook's two commands a target runs.
type commandPhase int

const (
	preCommand commandPhase = iota
	postCommand
)

// resolveTarget turns a recorded pod into an exec target for the hook
// recorded on it.
func (r *Runner) resolveTarget(ctx context.Context, group *longhorn.SnapshotGroup, policy *longhorn.VolumeHookPolicy, record *longhorn.HookPodStatus, phase commandPhase) (runTarget, longhorn.HookReason, error) {
	hook := snapshothook.FindHook(policy, record.HookName)
	if hook == nil {
		return runTarget{}, longhorn.HookReasonHookNotFound, nil
	}
	pod, err := r.store.FindPod(ctx, record.Namespace, record.Name)
	if err != nil {
		return runTarget{}, "", errors.Wrapf(err, "failed to read pod %v/%v", record.Namespace, record.Name)
	}
	// The command must only run in the pod that ran the pre-command: a pod
	// that is gone or has a new UID is PodNotFound. So is a pod in a
	// terminal phase - its containers have stopped and cannot exec anymore.
	if pod == nil || string(pod.UID) != record.UID ||
		pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return runTarget{}, longhorn.HookReasonPodNotFound, nil
	}
	target := runTarget{namespace: record.Namespace, pod: record.Name, container: hook.Exec.Container, command: hook.Exec.PostCommand}
	if phase == preCommand {
		target.command = hook.Exec.PreCommand
	}
	// The container comes from the hook, or is matched from the pod.
	if target.container == "" {
		_, container, err := snapshothook.MatchHookForPod(policy, pod, group.Spec.HookOperation)
		if err != nil {
			return runTarget{}, longhorn.HookReasonHookNotMatched, nil
		}
		target.container = container
	}
	return target, "", nil
}
