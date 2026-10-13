package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/hookexecutor/snapshothook"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	// snapshotGroupMemberLostError is recorded in a member's status entry when
	// its Snapshot CR disappears after the group became Ready.
	snapshotGroupMemberLostError = "member snapshot deleted"
	// snapshotGroupMemberReplacedError is recorded when an unrelated Snapshot
	// CR occupies a lost member's name after the group became Ready.
	snapshotGroupMemberReplacedError = "member snapshot replaced by an unrelated snapshot"
)

// SnapshotGroupController reconciles SnapshotGroups: it fans the fixed member
// set out into ordinary Snapshot CRs through the existing per-volume path,
// mirrors member status, and drives the phase InProgress -> Ready | Failed
// within the deadline. A hooked group first passes through RunningPreHooks,
// where the hook executor pauses the application.
type SnapshotGroupController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds         *datastore.DataStore
	cacheSyncs []cache.InformerSynced

	// for unit test
	nowHandler func() time.Time
}

func NewSnapshotGroupController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	namespace string,
	controllerID string,
) (*SnapshotGroupController, error) {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	ctrl := &SnapshotGroupController{
		baseController: newBaseController("longhorn-snapshot-group", logger),

		namespace:    namespace,
		controllerID: controllerID,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-snapshot-group-controller"}),

		ds:         ds,
		nowHandler: time.Now,
	}

	var err error
	if _, err = ds.SnapshotGroupInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.enqueueSnapshotGroup,
		UpdateFunc: func(old, cur interface{}) { ctrl.enqueueSnapshotGroup(cur) },
		DeleteFunc: ctrl.enqueueSnapshotGroup,
	}, 0); err != nil {
		return nil, err
	}
	ctrl.cacheSyncs = append(ctrl.cacheSyncs, ds.SnapshotGroupInformer.HasSynced)

	if _, err = ds.SnapshotInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.enqueueOwningSnapshotGroup,
		UpdateFunc: func(old, cur interface{}) { ctrl.enqueueOwningSnapshotGroup(cur) },
		DeleteFunc: ctrl.enqueueOwningSnapshotGroup,
	}, 0); err != nil {
		return nil, err
	}
	ctrl.cacheSyncs = append(ctrl.cacheSyncs, ds.SnapshotInformer.HasSynced)

	return ctrl, nil
}

func (ctrl *SnapshotGroupController) enqueueSnapshotGroup(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object %#v: %v", obj, err))
		return
	}

	ctrl.queue.Add(key)
}

func (ctrl *SnapshotGroupController) enqueueSnapshotGroupAfter(obj interface{}, duration time.Duration) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("enqueueSnapshotGroupAfter: failed to get key for object %#v: %v", obj, err))
		return
	}

	ctrl.queue.AddAfter(key, duration)
}

// enqueueOwningSnapshotGroup routes a member Snapshot event to its group,
// keyed on the longhorn.io/snapshot-group metadata label the controller
// stamps at member creation.
func (ctrl *SnapshotGroupController) enqueueOwningSnapshotGroup(obj interface{}) {
	snapshot, ok := obj.(*longhorn.Snapshot)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}
		snapshot, ok = deletedState.Obj.(*longhorn.Snapshot)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	groupName, exists := snapshot.Labels[types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroup)]
	if !exists || groupName == "" {
		return
	}

	ctrl.queue.Add(ctrl.namespace + "/" + groupName)
}

func (ctrl *SnapshotGroupController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer ctrl.queue.ShutDown()

	ctrl.logger.Info("Starting Longhorn Snapshot Group Controller")
	defer ctrl.logger.Info("Shut down Longhorn Snapshot Group Controller")

	if !cache.WaitForNamedCacheSync(ctrl.name, stopCh, ctrl.cacheSyncs...) {
		return
	}

	ctrl.initReconcileMetrics(workers)
	for range workers {
		go wait.Until(ctrl.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (ctrl *SnapshotGroupController) worker() {
	for ctrl.processNextWorkItem() {
	}
}

func (ctrl *SnapshotGroupController) processNextWorkItem() bool {
	key, quit := ctrl.queue.Get()
	if quit {
		return false
	}
	defer ctrl.queue.Done(key)
	err := ctrl.syncWithMetrics(func() error { return ctrl.syncHandler(key.(string)) })
	ctrl.handleErr(err, key)
	return true
}

func (ctrl *SnapshotGroupController) handleErr(err error, key interface{}) {
	if err == nil {
		ctrl.queue.Forget(key)
		return
	}

	log := ctrl.logger.WithField("snapshotGroup", key)
	handleReconcileErrorLogging(log, err, "Failed to sync Longhorn snapshot group")
	ctrl.queue.AddRateLimited(key)
}

func (ctrl *SnapshotGroupController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: failed to sync snapshot group %v", ctrl.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != ctrl.namespace {
		return nil
	}
	return ctrl.reconcile(name)
}

func (ctrl *SnapshotGroupController) isResponsibleFor(snapshotGroup *longhorn.SnapshotGroup) bool {
	return isControllerResponsibleFor(ctrl.controllerID, ctrl.ds, snapshotGroup.Name, "", snapshotGroup.Status.OwnerID)
}

func (ctrl *SnapshotGroupController) reconcile(name string) (err error) {
	snapshotGroup, err := ctrl.ds.GetSnapshotGroup(name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	if !ctrl.isResponsibleFor(snapshotGroup) {
		return nil
	}

	if snapshotGroup.Status.OwnerID != ctrl.controllerID {
		snapshotGroup.Status.OwnerID = ctrl.controllerID
		snapshotGroup, err = ctrl.ds.UpdateSnapshotGroupStatus(snapshotGroup)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		ctrl.logger.WithField("snapshotGroup", name).Infof("Snapshot group got new owner %v", ctrl.controllerID)
	}

	if !snapshotGroup.DeletionTimestamp.IsZero() {
		return ctrl.handleDeletion(snapshotGroup)
	}

	existingSnapshotGroup := snapshotGroup.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if reflect.DeepEqual(existingSnapshotGroup.Status, snapshotGroup.Status) {
			return
		}
		_, err = ctrl.ds.UpdateSnapshotGroupStatus(snapshotGroup)
	}()

	ctrl.restoreTerminalPhaseFromAnnotation(snapshotGroup)

	// First reconcile: initialize the member entries from the spec (already
	// resolved at admission) and enter InProgress, or RunningPreHooks for a
	// hooked group. There is no Pending phase - it would carry no information
	// a watcher could act on.
	if snapshotGroup.Status.Phase == "" {
		ctrl.initializeMemberStatuses(snapshotGroup)
		if snapshothook.IsHooked(snapshotGroup) {
			snapshotGroup.Status.Phase = longhorn.SnapshotGroupPhaseRunningPreHooks
			ctrl.eventRecorder.Eventf(snapshotGroup, corev1.EventTypeNormal, constant.EventReasonStart,
				"Snapshot group started running pre-hooks on %v members with deadline %vs", len(snapshotGroup.Spec.Members), snapshotGroup.Spec.DeadlineSeconds)
		} else {
			snapshotGroup.Status.Phase = longhorn.SnapshotGroupPhaseInProgress
			ctrl.eventRecorder.Eventf(snapshotGroup, corev1.EventTypeNormal, constant.EventReasonStart,
				"Snapshot group started taking %v member snapshots with deadline %vs", len(snapshotGroup.Spec.Members), snapshotGroup.Spec.DeadlineSeconds)
		}
	}

	switch snapshotGroup.Status.Phase {
	case longhorn.SnapshotGroupPhaseRunningPreHooks:
		return ctrl.reconcileRunningPreHooks(snapshotGroup)
	case longhorn.SnapshotGroupPhaseInProgress:
		return ctrl.reconcileInProgress(snapshotGroup)
	default:
		return ctrl.reconcileTerminal(snapshotGroup)
	}
}

// restoreTerminalPhaseFromAnnotation applies the terminal-phase annotation
// when the status does not show the recorded outcome: an empty phase is a
// restored copy of a finished group, and a non-terminal phase is a crash
// between the annotation write and the status persist. Either way no members
// are created and no pause is started.
func (ctrl *SnapshotGroupController) restoreTerminalPhaseFromAnnotation(snapshotGroup *longhorn.SnapshotGroup) {
	switch snapshotGroup.Status.Phase {
	case "", longhorn.SnapshotGroupPhaseRunningPreHooks, longhorn.SnapshotGroupPhaseInProgress:
	default:
		return
	}
	annotatedPhase, recorded := types.GetSnapshotGroupTerminalPhase(snapshotGroup)
	if !recorded {
		return
	}
	snapshotGroup.Status.Phase = annotatedPhase
	snapshotGroup.Status.ReadyToUse = annotatedPhase == longhorn.SnapshotGroupPhaseReady
	// The annotation records only the phase; a restored Failed group must
	// still say why it shows no failure reason.
	if annotatedPhase == longhorn.SnapshotGroupPhaseFailed && snapshotGroup.Status.Error == "" {
		snapshotGroup.Status.Error = "original failure reason was not preserved across the restore"
	}
	ctrl.initializeMemberStatuses(snapshotGroup)
}

// initializeMemberStatuses creates one status entry per spec member; entries
// that already exist are kept.
func (ctrl *SnapshotGroupController) initializeMemberStatuses(snapshotGroup *longhorn.SnapshotGroup) {
	if len(snapshotGroup.Status.Members) == len(snapshotGroup.Spec.Members) {
		return
	}
	members := make([]longhorn.SnapshotGroupMemberStatus, 0, len(snapshotGroup.Spec.Members))
	for _, member := range snapshotGroup.Spec.Members {
		members = append(members, longhorn.SnapshotGroupMemberStatus{
			VolumeName:   member.VolumeName,
			SnapshotName: member.SnapshotName,
		})
	}
	snapshotGroup.Status.Members = members
}

// scheduleDeadlineReconcile queues a reconcile for right after the deadline.
// It runs before any phase work: an error return later in the phase would
// skip a schedule placed at the end, leaving only the retry backoff, which
// can grow past the deadline. Rescheduling on every reconcile is safe: the
// workqueue deduplicates by key, so a group has at most one pending wake-up.
func (ctrl *SnapshotGroupController) scheduleDeadlineReconcile(snapshotGroup *longhorn.SnapshotGroup, deadline time.Time) {
	if now := ctrl.nowHandler(); !now.After(deadline) {
		ctrl.enqueueSnapshotGroupAfter(snapshotGroup, deadline.Sub(now)+time.Second)
	}
}

// reconcileRunningPreHooks waits for the hook executor to pause every
// member's application; the executor owns status.hooks and this function
// only reads it. A recorded pre-hook failure or a passed deadline fails the
// group. Once preHooksDone is set, the member snapshots are taken in the
// same pass to minimize the pause. Any failure changes the phase, which
// wakes the executor to run the release.
func (ctrl *SnapshotGroupController) reconcileRunningPreHooks(snapshotGroup *longhorn.SnapshotGroup) error {
	deadline := snapshothook.GroupDeadline(snapshotGroup)
	ctrl.scheduleDeadlineReconcile(snapshotGroup, deadline)

	if message := preHookFailureMessage(snapshotGroup); message != "" {
		return ctrl.transitionToFailed(snapshotGroup, message)
	}

	if snapshotGroup.Status.PreHooksDone {
		snapshotGroup.Status.Phase = longhorn.SnapshotGroupPhaseInProgress
		ctrl.eventRecorder.Eventf(snapshotGroup, corev1.EventTypeNormal, constant.EventReasonStart,
			"Pre-hooks done on all %v members; taking member snapshots", len(snapshotGroup.Spec.Members))
		return ctrl.reconcileInProgress(snapshotGroup)
	}

	if ctrl.nowHandler().After(deadline) {
		message := preHookDeadlineExceededError(snapshotGroup)
		ctrl.eventRecorder.Eventf(snapshotGroup, corev1.EventTypeWarning, constant.EventReasonHookDeadlineExceeded,
			"Snapshot group missed its %vs deadline before the pre-hooks finished: %v", snapshotGroup.Spec.DeadlineSeconds, message)
		return ctrl.transitionToFailed(snapshotGroup, message)
	}
	return nil
}

// preHookFailureMessage builds the group failure reason from the executor's
// record, or returns "" when no pre-hook has failed. A pod-level failure
// names the pod the command ran in; a member-level failure has no pod
// entries, meaning no command ran.
func preHookFailureMessage(snapshotGroup *longhorn.SnapshotGroup) string {
	var failures []string
	for _, hook := range snapshotGroup.Status.Hooks {
		if hook.Phase != longhorn.HookPhasePreFailed {
			continue
		}
		member := fmt.Sprintf("member %v (%v)", hook.SnapshotName, memberHookPolicyKey(snapshotGroup, hook.SnapshotName))
		failedPods := 0
		for _, pod := range hook.Pods {
			if pod.Phase != longhorn.HookPhasePreFailed {
				continue
			}
			failedPods++
			failures = append(failures, fmt.Sprintf("pre-hook failed on %v in pod %v/%v: %v", member, pod.Namespace, pod.Name, snapshothook.FailureMessage(pod.Reason, pod.ExitCode)))
		}
		if failedPods == 0 {
			failures = append(failures, fmt.Sprintf("pre-hook failed on %v: %v", member, hook.Reason))
		}
	}
	return strings.Join(failures, "; ")
}

// memberHookPolicyKey returns the hook policy recorded on the member with
// the given snapshot name, for use in the group failure message.
func memberHookPolicyKey(snapshotGroup *longhorn.SnapshotGroup, snapshotName string) string {
	if member := snapshothook.FindMember(snapshotGroup, snapshotName); member != nil {
		return snapshothook.PolicyRefNamespacedName(member.HookPolicyRef)
	}
	return ""
}

// preHookDeadlineExceededError builds the failure reason for a deadline
// that passed in RunningPreHooks. An unset hooksStartedAt means the
// executor never started on the group and nothing was paused.
func preHookDeadlineExceededError(snapshotGroup *longhorn.SnapshotGroup) string {
	if snapshotGroup.Status.HooksStartedAt == nil {
		return "deadline passed while queued: paused nothing " + snapshothook.QueuedPastDeadlineMarker
	}
	done := 0
	for _, hook := range snapshotGroup.Status.Hooks {
		if hook.Phase == longhorn.HookPhasePreDone {
			done++
		}
	}
	return fmt.Sprintf("deadline passed while running pre-hooks: %v/%v members done", done, len(snapshotGroup.Spec.Members))
}

// reconcileInProgress creates missing member Snapshots, mirrors member state,
// and drives the phase to Ready or Failed. Once the deadline has passed,
// nothing is created anymore - a snapshot would auto-attach a detached member
// volume - and a creation error can no longer hold off the Failed transition;
// the group only settles on its outcome.
func (ctrl *SnapshotGroupController) reconcileInProgress(snapshotGroup *longhorn.SnapshotGroup) error {
	// A hooked group reaches InProgress only after the executor records the
	// pre-hooks done. A group arriving without that record skipped the pause,
	// for example driven by an older longhorn-manager; fail it before
	// creating any member snapshot.
	if snapshothook.IsHooked(snapshotGroup) && !snapshotGroup.Status.PreHooksDone {
		return ctrl.transitionToFailed(snapshotGroup,
			"group entered InProgress before the pre-hooks were done "+snapshothook.PreHooksSkippedMarker)
	}

	if snapshothook.IsHooked(snapshotGroup) && snapshothook.ReleaseStarted(snapshotGroup) {
		return ctrl.transitionToFailed(snapshotGroup, snapshothook.ReleasedBeforeReadyMessage(snapshotGroup))
	}

	deadline := snapshothook.GroupDeadline(snapshotGroup)
	ctrl.scheduleDeadlineReconcile(snapshotGroup, deadline)

	memberSnapshots := make(map[string]*longhorn.Snapshot, len(snapshotGroup.Spec.Members))
	for _, member := range snapshotGroup.Spec.Members {
		snapshot, err := ctrl.ds.GetSnapshotRO(member.SnapshotName)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			// Re-read the clock: every create below can wait for lister
			// visibility, so the deadline can pass inside this loop, and no
			// member request may start once it has. Existing members are
			// still collected for the mirror.
			if ctrl.nowHandler().After(deadline) {
				continue
			}
			if err := ctrl.createMemberSnapshot(snapshotGroup, member); err != nil {
				return err
			}
			continue
		}
		// An existing Snapshot with a member's name is adopted only if it
		// carries this group's label and volume; anything else is a foreign
		// snapshot and fails the group. This check must also run past the
		// deadline: mirrorMemberStatuses trusts it and would otherwise read
		// a foreign snapshot as a member.
		if !ctrl.isOwnMemberSnapshot(snapshotGroup, member.VolumeName, snapshot) {
			return ctrl.transitionToFailed(snapshotGroup,
				fmt.Sprintf("member snapshot name %v for volume %v collides with an existing foreign snapshot", member.SnapshotName, member.VolumeName))
		}
		// Member snapshots of a hooked group are labeled with the hooked
		// marker at creation. A member without the marker was taken outside
		// the pause, for example by an older longhorn-manager, so it is not
		// application-consistent. The snapshot still belongs to the group,
		// so deleting the group cleans it up.
		if snapshothook.IsHooked(snapshotGroup) && !hasHookedMarker(snapshot) {
			return ctrl.transitionToFailed(snapshotGroup,
				fmt.Sprintf("member snapshot %v for volume %v was taken before the application was paused, by a longhorn-manager that predates hooks %v", member.SnapshotName, member.VolumeName, snapshothook.PreHooksSkippedMarker))
		}
		memberSnapshots[member.SnapshotName] = snapshot
	}

	ctrl.mirrorMemberStatuses(snapshotGroup, memberSnapshots)

	// The pause time, preHooksDoneAt, is deliberately not used as lowerBound:
	// comparing it against snapshot creation times means comparing timestamps
	// from two different clocks, and a clock running a few seconds ahead of
	// the other could wrongly fail a valid group.
	lowerBound := snapshotGroup.CreationTimestamp.Time
	if allCreated, latestCreationTime := allMembersCreatedByDeadline(snapshotGroup.Status.Members, lowerBound, deadline); allCreated {
		if snapshothook.IsHooked(snapshotGroup) {
			change, err := ctrl.hookPodChangeMessage(snapshotGroup)
			if err != nil {
				return err
			}
			if change != "" {
				return ctrl.transitionToFailed(snapshotGroup, change)
			}
		}
		return ctrl.transitionToReady(snapshotGroup, latestCreationTime)
	}

	// Read the clock after the loop, not before: the creates can overrun the
	// deadline, and a pre-loop sample would defer the failure by the overrun.
	if ctrl.nowHandler().After(deadline) {
		message := deadlineExceededError(snapshotGroup.Status.Members, lowerBound, deadline)
		if snapshothook.IsHooked(snapshotGroup) {
			ctrl.eventRecorder.Eventf(snapshotGroup, corev1.EventTypeWarning, constant.EventReasonHookDeadlineExceeded,
				"Snapshot group missed its %vs deadline before every member snapshot was taken: %v", snapshotGroup.Spec.DeadlineSeconds, message)
		}
		return ctrl.transitionToFailed(snapshotGroup, message)
	}

	return nil
}

// hookPodChangeMessage compares the pods recorded at pause time with the
// pods consuming the claims now, listed live from the API server. A pod
// gone or replaced, a restarted container, a new pod, or a terminating pod
// means a writer may have run outside the pause. It returns "" when every
// member still matches.
func (ctrl *SnapshotGroupController) hookPodChangeMessage(snapshotGroup *longhorn.SnapshotGroup) (string, error) {
	podsByNamespace := map[string][]corev1.Pod{}
	for _, member := range snapshotGroup.Spec.Members {
		hook := snapshothook.FindHookStatus(snapshotGroup, member.SnapshotName)
		if hook == nil || hook.PVCRef == nil {
			return snapshothook.PodChangedMessage(member, "no pause record to verify the pods against"), nil
		}
		namespace, pvcName := hook.PVCRef.Namespace, hook.PVCRef.Name
		pods, listed := podsByNamespace[namespace]
		if !listed {
			podList, err := ctrl.kubeClient.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{})
			if err != nil {
				return "", errors.Wrapf(err, "failed to list pods in namespace %v to check the paused pods of snapshot group %v", namespace, snapshotGroup.Name)
			}
			pods = podList.Items
			podsByNamespace[namespace] = pods
		}
		// A terminating pod keeps writing until its containers stop, so it
		// may write outside the pause whether recorded or not.
		if terminating := snapshothook.TerminatingPodsUsingPVC(pods, pvcName); len(terminating) > 0 {
			return snapshothook.PodChangedMessage(member,
				fmt.Sprintf("pod %v/%v mounting the claim is terminating and may write outside the pause", terminating[0].Namespace, terminating[0].Name)), nil
		}
		if change := snapshothook.PodIdentityChange(hook.Pods, snapshothook.ConsumerPodsUsingPVC(pods, pvcName)); change != "" {
			return snapshothook.PodChangedMessage(member, change), nil
		}
		// A short-lived pod, such as a Job, can start, write, and finish
		// within the hook window without ever being paused.
		for _, pod := range snapshothook.TerminalPodsUsingPVC(pods, pvcName) {
			if pod.CreationTimestamp.After(snapshotGroup.CreationTimestamp.Time) {
				return snapshothook.PodChangedMessage(member,
					fmt.Sprintf("pod %v/%v mounted the claim and finished during the hook window", pod.Namespace, pod.Name)), nil
			}
		}
	}
	return "", nil
}

func (ctrl *SnapshotGroupController) createMemberSnapshot(snapshotGroup *longhorn.SnapshotGroup, member longhorn.SnapshotGroupMember) error {
	labels := map[string]string{
		types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroup): snapshotGroup.Name,
	}
	// The marker records that the snapshot was taken after the pause.
	if snapshothook.IsHooked(snapshotGroup) {
		labels[types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupHooked)] = "true"
	}
	snapshot := &longhorn.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:   member.SnapshotName,
			Labels: labels,
		},
		Spec: longhorn.SnapshotSpec{
			Volume:         member.VolumeName,
			CreateSnapshot: true,
			Labels:         snapshotGroup.Spec.Labels,
		},
	}
	// The group sets no ownerReference on members: they keep the volume
	// ownerReference the snapshot mutator stamps, and cleanup goes through
	// the finalizer.
	// AlreadyExists is an error here: the snapshot is either our own create
	// the lister has not seen yet, or a foreign one that took the name. The
	// retry reads it through the lister and the ownership check decides.
	if _, err := ctrl.ds.CreateSnapshot(snapshot); err != nil {
		return errors.Wrapf(err, "failed to create member snapshot %v for volume %v of snapshot group %v",
			member.SnapshotName, member.VolumeName, snapshotGroup.Name)
	}
	return nil
}

func (ctrl *SnapshotGroupController) isOwnMemberSnapshot(snapshotGroup *longhorn.SnapshotGroup, volumeName string, snapshot *longhorn.Snapshot) bool {
	return snapshot.Labels[types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroup)] == snapshotGroup.Name &&
		snapshot.Spec.Volume == volumeName
}

// hasHookedMarker reports whether the member snapshot was created by a
// longhorn-manager that knows spec.hooks, and so after the pre-hooks.
func hasHookedMarker(snapshot *longhorn.Snapshot) bool {
	return snapshot.Labels[types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupHooked)] == "true"
}

// mirrorMemberStatuses copies every member Snapshot's readyToUse, creation
// time, and error into the status entries. It mirrors the objects the caller
// already validated: a fresh read here could see a foreign snapshot that
// took a deleted member's name. Member errors are not treated as final: the
// per-volume path routinely reports and then clears errors on its own.
func (ctrl *SnapshotGroupController) mirrorMemberStatuses(snapshotGroup *longhorn.SnapshotGroup, memberSnapshots map[string]*longhorn.Snapshot) {
	for i := range snapshotGroup.Status.Members {
		member := &snapshotGroup.Status.Members[i]
		snapshot := memberSnapshots[member.SnapshotName]
		if snapshot == nil {
			// A missing member CR must not keep a stale ready entry, or the
			// Ready evaluation could pass on old data.
			member.ReadyToUse = false
			continue
		}
		// A member being deleted must not count as ready, or the group could
		// turn Ready and immediately degrade.
		if !snapshot.DeletionTimestamp.IsZero() {
			member.ReadyToUse = false
			continue
		}
		member.ReadyToUse = snapshot.Status.ReadyToUse
		member.CreationTime = snapshot.Status.CreationTime
		member.Error = snapshot.Status.Error
	}
}

// allMembersCreatedByDeadline reports whether every member snapshot is ready
// and was created between windowStart and deadline, and returns the latest
// creation time. It reads the recorded creation times, not the clock. Pass a
// zero windowStart to accept members created before the group; the restore
// recovery needs this, because a restored group is recreated after its member
// snapshots already exist.
func allMembersCreatedByDeadline(members []longhorn.SnapshotGroupMemberStatus, windowStart, deadline time.Time) (bool, string) {
	var latest time.Time
	latestCreationTime := ""
	for _, member := range members {
		if memberFailureReason(member, windowStart, deadline) != "" {
			return false, ""
		}
		// An empty reason guarantees the creation time parses.
		creationTime, _ := time.Parse(time.RFC3339, member.CreationTime)
		if creationTime.After(latest) {
			latest = creationTime
			latestCreationTime = member.CreationTime
		}
	}
	return true, latestCreationTime
}

// deadlineExceededError builds the group failure message for an expired
// deadline: it names every member that kept the group from becoming Ready,
// each with the reason.
func deadlineExceededError(members []longhorn.SnapshotGroupMemberStatus, windowStart, deadline time.Time) string {
	var failed []string
	for _, member := range members {
		reason := memberFailureReason(member, windowStart, deadline)
		if reason == "" {
			continue
		}
		failed = append(failed, fmt.Sprintf("volume %v (snapshot %v): %v", member.VolumeName, member.SnapshotName, reason))
	}
	return fmt.Sprintf("deadline exceeded before every member snapshot was taken; failed members: %v", strings.Join(failed, "; "))
}

// memberFailureReason returns why the member kept the group from becoming
// Ready, or empty when it did not. windowStart is normally the group's
// creation time: a member created earlier is a leftover from an earlier
// group with the same name, not one of ours.
func memberFailureReason(member longhorn.SnapshotGroupMemberStatus, windowStart, deadline time.Time) string {
	if !member.ReadyToUse || member.CreationTime == "" {
		reason := "not taken"
		if member.Error != "" {
			reason += ": " + member.Error
		}
		return reason
	}
	creationTime, err := time.Parse(time.RFC3339, member.CreationTime)
	if err != nil {
		return fmt.Sprintf("invalid creation time %v", member.CreationTime)
	}
	if creationTime.Before(windowStart) {
		return fmt.Sprintf("taken at %v, before the group's window opened at %v", member.CreationTime, windowStart.UTC().Format(time.RFC3339))
	}
	if creationTime.After(deadline) {
		return fmt.Sprintf("taken at %v, after the deadline", member.CreationTime)
	}
	return ""
}

func (ctrl *SnapshotGroupController) transitionToReady(snapshotGroup *longhorn.SnapshotGroup, latestCreationTime string) error {
	// The annotation is persisted here, before the deferred status update.
	// Either crash order recovers: an annotation without status is restored
	// on the next reconcile, and status without an annotation is re-stamped
	// once the group is terminal.
	//
	// The group is re-read live because this is the last chance to catch a
	// release the executor started since the readiness evaluation: once the
	// annotation is written, the next reconcile restores Ready from it
	// without further checks.
	latest, err := ctrl.ds.GetSnapshotGroupUncached(snapshotGroup.Name)
	if err != nil {
		return errors.Wrapf(err, "failed to read snapshot group %v before marking it Ready", snapshotGroup.Name)
	}
	if snapshothook.IsHooked(snapshotGroup) && snapshothook.ReleaseStarted(latest) {
		return ctrl.transitionToFailed(snapshotGroup, snapshothook.ReleasedBeforeReadyMessage(latest))
	}

	snapshotGroup.Status.Phase = longhorn.SnapshotGroupPhaseReady
	snapshotGroup.Status.ReadyToUse = true
	snapshotGroup.Status.CreationTime = latestCreationTime
	if err := ctrl.stampTerminalPhaseAnnotationOn(snapshotGroup, latest); err != nil {
		return err
	}
	ctrl.eventRecorder.Eventf(snapshotGroup, corev1.EventTypeNormal, constant.EventReasonReady,
		"Snapshot group is ready: all %v member snapshots taken within the deadline", len(snapshotGroup.Spec.Members))
	ctrl.warnIfHookDeadlineNearlyExceeded(snapshotGroup, latestCreationTime)
	return nil
}

// hookDeadlineWarningFraction is how much of its deadline a Ready hooked
// group may use before the controller warns that the next run may miss it.
const hookDeadlineWarningFraction = 0.8

// warnIfHookDeadlineNearlyExceeded records a HookDeadlineNearlyExceeded
// warning event when a Ready hooked group used over 80% of its deadline.
func (ctrl *SnapshotGroupController) warnIfHookDeadlineNearlyExceeded(snapshotGroup *longhorn.SnapshotGroup, latestCreationTime string) {
	if !snapshothook.IsHooked(snapshotGroup) || snapshotGroup.Status.HooksStartedAt == nil || snapshotGroup.Status.PreHooksDoneAt == nil {
		return
	}
	latest, err := time.Parse(time.RFC3339, latestCreationTime)
	if err != nil {
		return
	}
	start := snapshotGroup.CreationTimestamp.Time
	used := latest.Sub(start)
	budget := time.Duration(snapshotGroup.Spec.DeadlineSeconds) * time.Second
	if float64(used) <= hookDeadlineWarningFraction*float64(budget) {
		return
	}
	queued := snapshotGroup.Status.HooksStartedAt.Sub(start)
	preHooks := snapshotGroup.Status.PreHooksDoneAt.Sub(snapshotGroup.Status.HooksStartedAt.Time)
	snapshots := latest.Sub(snapshotGroup.Status.PreHooksDoneAt.Time)
	ctrl.eventRecorder.Eventf(snapshotGroup, corev1.EventTypeWarning, constant.EventReasonHookDeadlineNearlyExceeded,
		"Snapshot group used %.0fs of its %ds deadline (queued %.0fs, pre-hooks %.0fs, snapshots %.0fs); raise the deadline or shorten the pre-command before the next run misses it",
		used.Seconds(), snapshotGroup.Spec.DeadlineSeconds, queued.Seconds(), preHooks.Seconds(), snapshots.Seconds())
}

func (ctrl *SnapshotGroupController) transitionToFailed(snapshotGroup *longhorn.SnapshotGroup, message string) error {
	snapshotGroup.Status.Phase = longhorn.SnapshotGroupPhaseFailed
	snapshotGroup.Status.ReadyToUse = false
	snapshotGroup.Status.Error = message
	if err := ctrl.ensureTerminalPhaseAnnotation(snapshotGroup); err != nil {
		return err
	}
	ctrl.eventRecorder.Eventf(snapshotGroup, corev1.EventTypeWarning, constant.EventReasonFailed,
		"Snapshot group failed: %v", message)
	return nil
}

// ensureTerminalPhaseAnnotation records the outcome into the
// longhorn.io/snapshot-group-terminal-phase annotation - the restore guard:
// a restored or crash-interrupted group regains the recorded outcome instead
// of taking snapshots again.
func (ctrl *SnapshotGroupController) ensureTerminalPhaseAnnotation(snapshotGroup *longhorn.SnapshotGroup) error {
	phase := string(snapshotGroup.Status.Phase)
	if snapshotGroup.Annotations[types.SnapshotGroupAnnotationTerminalPhase] == phase {
		return nil
	}
	updated, err := ctrl.ds.GetSnapshotGroup(snapshotGroup.Name)
	if err != nil {
		return err
	}
	return ctrl.stampTerminalPhaseAnnotationOn(snapshotGroup, updated)
}

// stampTerminalPhaseAnnotationOn writes snapshotGroup's phase as the
// terminal-phase annotation onto updated, a fresh copy of the same group,
// then carries the new resourceVersion and the executor-owned status back
// into snapshotGroup.
func (ctrl *SnapshotGroupController) stampTerminalPhaseAnnotationOn(snapshotGroup, updated *longhorn.SnapshotGroup) error {
	phase := string(snapshotGroup.Status.Phase)
	if updated.Annotations == nil {
		updated.Annotations = map[string]string{}
	}
	updated.Annotations[types.SnapshotGroupAnnotationTerminalPhase] = phase
	result, err := ctrl.ds.UpdateSnapshotGroup(updated)
	if err != nil {
		return errors.Wrapf(err, "failed to stamp terminal phase annotation on snapshot group %v", snapshotGroup.Name)
	}
	if snapshotGroup.Annotations == nil {
		snapshotGroup.Annotations = map[string]string{}
	}
	snapshotGroup.Annotations[types.SnapshotGroupAnnotationTerminalPhase] = phase
	// Adopt the new resourceVersion so the caller's pending status update
	// does not conflict with the annotation write above. That update writes
	// the whole status object, so carry over the executor-owned fields too;
	// otherwise it would silently revert a hook record written since this
	// reconcile started, and the deletion hold could release a group whose
	// pods are still paused.
	snapshothook.AdoptExecutorStatus(&snapshotGroup.Status, &result.Status)
	snapshotGroup.ResourceVersion = result.ResourceVersion
	return nil
}

// reconcileTerminal handles a group after Ready or Failed: it re-stamps the
// terminal-phase annotation if needed and, on a Ready group, records member
// losses, maintains the Degraded condition, and keeps ReadyToUse in step
// with member readiness. It never creates another member snapshot.
func (ctrl *SnapshotGroupController) reconcileTerminal(snapshotGroup *longhorn.SnapshotGroup) error {
	if err := ctrl.ensureTerminalPhaseAnnotation(snapshotGroup); err != nil {
		return err
	}

	// Failed already says the set is not trusted; only a Ready group tracks
	// completeness.
	if snapshotGroup.Status.Phase != longhorn.SnapshotGroupPhaseReady {
		return nil
	}

	memberSnapshots, err := ctrl.mirrorMemberStatusesAfterReady(snapshotGroup)
	if err != nil {
		return err
	}

	// A restored Ready group has no group creation time; recover it from the
	// re-mirrored member creation times. The original deadline instant is not
	// reconstructable, so pass now: every recorded creation time is in the
	// past, leaving only member readiness and time validity to decide.
	if snapshotGroup.Status.CreationTime == "" {
		if allCreated, latestCreationTime := allMembersCreatedByDeadline(snapshotGroup.Status.Members, time.Time{}, ctrl.nowHandler()); allCreated {
			snapshotGroup.Status.CreationTime = latestCreationTime
		}
	}

	ctrl.warnIfPreHooksSkipped(snapshotGroup, memberSnapshots)
	ctrl.updateDegradedCondition(snapshotGroup)

	// Group readiness is the AND of member readiness: a group with a lost or
	// unusable member is not restorable as a set, so ReadyToUse follows the
	// member mirror and recovers with it once every member is whole again.
	readyToUse := true
	for _, member := range snapshotGroup.Status.Members {
		if !member.ReadyToUse {
			readyToUse = false
			break
		}
	}
	snapshotGroup.Status.ReadyToUse = readyToUse
	return nil
}

// warnIfPreHooksSkipped warns when a Ready hooked group has a member
// snapshot without the hooked marker, meaning it was taken outside the
// pause, for example by an older longhorn-manager. A terminal phase is never
// rewritten; instead, a warning event tells the user that the snapshots are
// not application-consistent and the group should be deleted and recreated.
func (ctrl *SnapshotGroupController) warnIfPreHooksSkipped(snapshotGroup *longhorn.SnapshotGroup, memberSnapshots map[string]*longhorn.Snapshot) {
	if !snapshothook.IsHooked(snapshotGroup) || snapshotGroup.Status.PreHooksDone {
		return
	}
	for _, snapshot := range memberSnapshots {
		if !hasHookedMarker(snapshot) {
			ctrl.eventRecorder.Eventf(snapshotGroup, corev1.EventTypeWarning, constant.EventReasonPreHooksSkipped,
				"Member snapshot %v was taken before the application was paused, by a longhorn-manager that predates hooks; the group is not application-consistent, delete it and create a new one", snapshot.Name)
			return
		}
	}
}

// mirrorMemberStatusesAfterReady refreshes the member entries of a Ready
// group and returns the member snapshots that are present and the group's
// own. Mirroring goes both ways: a healthy member is mirrored back to true,
// so a restored Ready group recovers its member state on its own.
func (ctrl *SnapshotGroupController) mirrorMemberStatusesAfterReady(snapshotGroup *longhorn.SnapshotGroup) (map[string]*longhorn.Snapshot, error) {
	memberSnapshots := make(map[string]*longhorn.Snapshot, len(snapshotGroup.Status.Members))
	for i := range snapshotGroup.Status.Members {
		member := &snapshotGroup.Status.Members[i]
		snapshot, err := ctrl.ds.GetSnapshotRO(member.SnapshotName)
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, err
		}
		lost := apierrors.IsNotFound(err) || !snapshot.DeletionTimestamp.IsZero()
		// An unrelated snapshot reusing a lost member's name must not be
		// mirrored into the group as the member.
		replaced := !lost && !ctrl.isOwnMemberSnapshot(snapshotGroup, member.VolumeName, snapshot)
		if !lost && !replaced {
			memberSnapshots[member.SnapshotName] = snapshot
		}

		switch {
		case lost:
			// The creation time is kept.
			member.ReadyToUse = false
			member.Error = snapshotGroupMemberLostError
		case replaced:
			// The creation time is kept.
			member.ReadyToUse = false
			member.Error = snapshotGroupMemberReplacedError
		case !snapshot.Status.ReadyToUse:
			// An unusable member keeps its own mirrored error.
			member.ReadyToUse = false
			member.Error = snapshot.Status.Error
		default:
			member.ReadyToUse = true
			member.CreationTime = snapshot.Status.CreationTime
			member.Error = ""
		}
	}
	return memberSnapshots, nil
}

// updateDegradedCondition sets Degraded from the member entries and emits an
// event when the group first degrades. Degraded clears once every member is
// whole again.
func (ctrl *SnapshotGroupController) updateDegradedCondition(snapshotGroup *longhorn.SnapshotGroup) {
	var degradedMessages []string
	for _, member := range snapshotGroup.Status.Members {
		if member.ReadyToUse {
			continue
		}
		message := fmt.Sprintf("member snapshot %v of volume %v", member.SnapshotName, member.VolumeName)
		if member.Error != "" {
			message += ": " + member.Error
		}
		degradedMessages = append(degradedMessages, message)
	}
	if len(degradedMessages) == 0 {
		snapshotGroup.Status.Conditions = types.SetCondition(snapshotGroup.Status.Conditions,
			longhorn.SnapshotGroupConditionTypeDegraded, longhorn.ConditionStatusFalse, "", "")
		return
	}

	degradedMessage := strings.Join(degradedMessages, "; ")
	alreadyDegraded := types.GetCondition(snapshotGroup.Status.Conditions, longhorn.SnapshotGroupConditionTypeDegraded).Status == longhorn.ConditionStatusTrue
	snapshotGroup.Status.Conditions = types.SetCondition(snapshotGroup.Status.Conditions,
		longhorn.SnapshotGroupConditionTypeDegraded, longhorn.ConditionStatusTrue,
		constant.EventReasonDegraded, degradedMessage)
	if !alreadyDegraded {
		ctrl.eventRecorder.Eventf(snapshotGroup, corev1.EventTypeWarning, constant.EventReasonDegraded,
			"Snapshot group no longer represents a complete set: %v", degradedMessage)
	}
}

// handleDeletion deletes the member Snapshots through the existing per-volume
// deletion path, then removes the finalizer once every member deletion has
// been requested. Waiting for the DeletionTimestamp instead of the CR
// disappearing matches the per-volume CSI DeleteSnapshot semantics: once
// deletion is requested it is irrevocable, and the snapshot controller owns
// the deferred purge.
func (ctrl *SnapshotGroupController) handleDeletion(snapshotGroup *longhorn.SnapshotGroup) error {
	allDeletionsRequested := true
	for _, member := range snapshotGroup.Spec.Members {
		snapshot, err := ctrl.ds.GetSnapshotRO(member.SnapshotName)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			continue
		}
		// Never delete a foreign snapshot that happens to carry a member's
		// name.
		if !ctrl.isOwnMemberSnapshot(snapshotGroup, member.VolumeName, snapshot) {
			continue
		}
		if !snapshot.DeletionTimestamp.IsZero() {
			// Deletion requested; the purge is the snapshot controller's job.
			continue
		}
		if err := ctrl.ds.DeleteSnapshotWithUIDPrecondition(snapshot.Name, snapshot.UID); err != nil {
			// Already gone: nothing to wait for.
			if apierrors.IsNotFound(err) {
				continue
			}
			// A Conflict means the name no longer holds the observed
			// snapshot: it is foreign now, stays, and blocks nothing.
			if apierrors.IsConflict(err) {
				continue
			}
			return errors.Wrapf(err, "failed to delete member snapshot %v of snapshot group %v", snapshot.Name, snapshotGroup.Name)
		}
		// Deletion was requested this pass; the next pass observes it.
		allDeletionsRequested = false
	}
	if !allDeletionsRequested {
		return nil
	}
	return ctrl.ds.RemoveFinalizerForSnapshotGroup(snapshotGroup)
}
