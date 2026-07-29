package controller

import (
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
// within the deadline. It calls no engine code.
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
	err := ctrl.syncHandler(key.(string))
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
	// resolved at admission) and enter InProgress. There is no Pending phase -
	// it would carry no information a watcher could act on.
	if snapshotGroup.Status.Phase == "" {
		ctrl.initializeMemberStatuses(snapshotGroup)
		snapshotGroup.Status.Phase = longhorn.SnapshotGroupPhaseInProgress
		ctrl.eventRecorder.Eventf(snapshotGroup, corev1.EventTypeNormal, constant.EventReasonStart,
			"Snapshot group started taking %v member snapshots with deadline %vs", len(snapshotGroup.Spec.Members), snapshotGroup.Spec.DeadlineSeconds)
	}

	if snapshotGroup.Status.Phase == longhorn.SnapshotGroupPhaseInProgress {
		return ctrl.reconcileInProgress(snapshotGroup)
	}
	return ctrl.reconcileTerminal(snapshotGroup)
}

// restoreTerminalPhaseFromAnnotation applies the terminal-phase annotation
// when the status does not show the recorded outcome: an empty phase is a
// restored copy of a finished group, and an InProgress phase is a crash
// between the annotation write and the status persist. Either way no members
// are created.
func (ctrl *SnapshotGroupController) restoreTerminalPhaseFromAnnotation(snapshotGroup *longhorn.SnapshotGroup) {
	if snapshotGroup.Status.Phase != "" && snapshotGroup.Status.Phase != longhorn.SnapshotGroupPhaseInProgress {
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

// reconcileInProgress creates missing member Snapshots, mirrors member state,
// and drives the phase to Ready or Failed. Once the deadline has passed,
// nothing is created anymore - a snapshot would auto-attach a detached member
// volume - and a creation error can no longer hold off the Failed transition;
// the group only settles on its outcome.
func (ctrl *SnapshotGroupController) reconcileInProgress(snapshotGroup *longhorn.SnapshotGroup) error {
	deadline := snapshotGroup.CreationTimestamp.Add(time.Duration(snapshotGroup.Spec.DeadlineSeconds) * time.Second)

	// Schedule a reconcile at the deadline before doing any work: the error
	// returns below would skip a schedule placed at the end, leaving only
	// the retry backoff, which can grow past the deadline. The queue keeps
	// one timer per group, so scheduling again on every reconcile is free.
	if now := ctrl.nowHandler(); !now.After(deadline) {
		ctrl.enqueueSnapshotGroupAfter(snapshotGroup, deadline.Sub(now)+time.Second)
	}

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
		memberSnapshots[member.SnapshotName] = snapshot
	}

	ctrl.mirrorMemberStatuses(snapshotGroup, memberSnapshots)

	if allCreated, latestCreationTime := allMembersCreatedByDeadline(snapshotGroup.Status.Members, snapshotGroup.CreationTimestamp.Time, deadline); allCreated {
		return ctrl.transitionToReady(snapshotGroup, latestCreationTime)
	}

	// Read the clock after the loop, not before: the creates can overrun the
	// deadline, and a pre-loop sample would defer the failure by the overrun.
	if ctrl.nowHandler().After(deadline) {
		return ctrl.transitionToFailed(snapshotGroup, deadlineExceededError(snapshotGroup.Status.Members, snapshotGroup.CreationTimestamp.Time, deadline))
	}

	return nil
}

func (ctrl *SnapshotGroupController) createMemberSnapshot(snapshotGroup *longhorn.SnapshotGroup, member longhorn.SnapshotGroupMember) error {
	snapshot := &longhorn.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name: member.SnapshotName,
			Labels: map[string]string{
				types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroup): snapshotGroup.Name,
			},
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
// and was created between groupCreationTime and deadline, and returns the
// latest creation time. It reads the recorded creation times, not the clock.
// A zero groupCreationTime accepts members older than the group; the restore
// recovery needs this, because a restored group is created after its members.
func allMembersCreatedByDeadline(members []longhorn.SnapshotGroupMemberStatus, groupCreationTime, deadline time.Time) (bool, string) {
	var latest time.Time
	latestCreationTime := ""
	for _, member := range members {
		if memberFailureReason(member, groupCreationTime, deadline) != "" {
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
func deadlineExceededError(members []longhorn.SnapshotGroupMemberStatus, groupCreationTime, deadline time.Time) string {
	var failed []string
	for _, member := range members {
		reason := memberFailureReason(member, groupCreationTime, deadline)
		if reason == "" {
			continue
		}
		failed = append(failed, fmt.Sprintf("volume %v (snapshot %v): %v", member.VolumeName, member.SnapshotName, reason))
	}
	return fmt.Sprintf("deadline exceeded before every member snapshot was taken; failed members: %v", strings.Join(failed, "; "))
}

// memberFailureReason returns why the member kept the group from
// becoming Ready, or empty when it did not. The creation time must fall
// between the group's creation and the deadline: an earlier time belongs
// to a survivor of an earlier group with the same name.
func memberFailureReason(member longhorn.SnapshotGroupMemberStatus, groupCreationTime, deadline time.Time) string {
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
	if creationTime.Before(groupCreationTime) {
		return fmt.Sprintf("taken at %v, before the group was requested", member.CreationTime)
	}
	if creationTime.After(deadline) {
		return fmt.Sprintf("taken at %v, after the deadline", member.CreationTime)
	}
	return ""
}

func (ctrl *SnapshotGroupController) transitionToReady(snapshotGroup *longhorn.SnapshotGroup, latestCreationTime string) error {
	// The annotation is persisted here, before the deferred status update.
	// Either crash order recovers: an annotation without status is restored by
	// the terminal-annotation guard, and status without an annotation is
	// re-stamped by reconcileTerminal.
	snapshotGroup.Status.Phase = longhorn.SnapshotGroupPhaseReady
	snapshotGroup.Status.ReadyToUse = true
	snapshotGroup.Status.CreationTime = latestCreationTime
	if err := ctrl.stampTerminalPhaseAnnotation(snapshotGroup); err != nil {
		return err
	}
	ctrl.eventRecorder.Eventf(snapshotGroup, corev1.EventTypeNormal, constant.EventReasonReady,
		"Snapshot group is ready: all %v member snapshots taken within the deadline", len(snapshotGroup.Spec.Members))
	return nil
}

func (ctrl *SnapshotGroupController) transitionToFailed(snapshotGroup *longhorn.SnapshotGroup, message string) error {
	snapshotGroup.Status.Phase = longhorn.SnapshotGroupPhaseFailed
	snapshotGroup.Status.ReadyToUse = false
	snapshotGroup.Status.Error = message
	if err := ctrl.stampTerminalPhaseAnnotation(snapshotGroup); err != nil {
		return err
	}
	ctrl.eventRecorder.Eventf(snapshotGroup, corev1.EventTypeWarning, constant.EventReasonFailed,
		"Snapshot group failed: %v", message)
	return nil
}

// stampTerminalPhaseAnnotation records the outcome into the
// longhorn.io/snapshot-group-terminal-phase annotation - the restore guard:
// a restored or crash-interrupted group regains the recorded outcome instead
// of taking snapshots again.
func (ctrl *SnapshotGroupController) stampTerminalPhaseAnnotation(snapshotGroup *longhorn.SnapshotGroup) error {
	phase := string(snapshotGroup.Status.Phase)
	if snapshotGroup.Annotations[types.SnapshotGroupAnnotationTerminalPhase] == phase {
		return nil
	}
	updated, err := ctrl.ds.GetSnapshotGroup(snapshotGroup.Name)
	if err != nil {
		return err
	}
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
	// Adopt the new resourceVersion so the caller's pending status update does
	// not conflict with the annotation write above.
	snapshotGroup.ResourceVersion = result.ResourceVersion
	return nil
}

// reconcileTerminal handles a group after Ready or Failed: it re-stamps the
// terminal-phase annotation if needed and, on a Ready group, records member
// losses and maintains the Degraded condition. It never creates another
// member snapshot.
func (ctrl *SnapshotGroupController) reconcileTerminal(snapshotGroup *longhorn.SnapshotGroup) error {
	if err := ctrl.stampTerminalPhaseAnnotation(snapshotGroup); err != nil {
		return err
	}

	// Failed already says the set is not trusted; only a Ready group tracks
	// completeness.
	if snapshotGroup.Status.Phase != longhorn.SnapshotGroupPhaseReady {
		return nil
	}

	if err := ctrl.mirrorMemberStatusesAfterReady(snapshotGroup); err != nil {
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

	ctrl.updateDegradedCondition(snapshotGroup)
	return nil
}

// mirrorMemberStatusesAfterReady refreshes the member entries of a Ready
// group. Mirroring goes both ways: a healthy member is mirrored back to true,
// so a restored Ready group recovers its member state on its own.
func (ctrl *SnapshotGroupController) mirrorMemberStatusesAfterReady(snapshotGroup *longhorn.SnapshotGroup) error {
	for i := range snapshotGroup.Status.Members {
		member := &snapshotGroup.Status.Members[i]
		snapshot, err := ctrl.ds.GetSnapshotRO(member.SnapshotName)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		lost := apierrors.IsNotFound(err) || !snapshot.DeletionTimestamp.IsZero()
		// An unrelated snapshot reusing a lost member's name must not be
		// mirrored into the group as the member.
		replaced := !lost && !ctrl.isOwnMemberSnapshot(snapshotGroup, member.VolumeName, snapshot)

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
	return nil
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
