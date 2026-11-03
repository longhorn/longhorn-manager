package controller

import (
	"context"
	"fmt"
	"hash/crc32"
	"regexp"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/hookexecutor/snapshothook"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	testSnapshotGroupName            = "test-snapshot-group"
	testSnapshotGroupDeadlineSeconds = int64(300)
)

// testVolumeHookPolicyRef renders as app/policy in failure reasons.
func testVolumeHookPolicyRef() *longhorn.VolumeHookPolicyRef {
	return &longhorn.VolumeHookPolicyRef{Namespace: "app", Name: "policy", Generation: 1}
}

// testSnapshotGroupBaseTime is the group's creationTimestamp in every test;
// deadlines and creation times are expressed relative to it.
var testSnapshotGroupBaseTime = time.Date(2026, time.July, 30, 0, 0, 0, 0, time.UTC)

func creationTimeAt(offset time.Duration) string {
	return testSnapshotGroupBaseTime.Add(offset).UTC().Format(time.RFC3339)
}

// snapshotGroupControllerFixture bundles everything needed to drive
// snapshot group controller reconciles in unit tests. Reads go through the
// lister indexers; writes land in the fake clientset.
type snapshotGroupControllerFixture struct {
	controller *SnapshotGroupController

	lhClient   *lhfake.Clientset
	kubeClient *fake.Clientset

	snapshotGroupIndexer cache.Indexer
	snapshotIndexer      cache.Indexer
}

func newSnapshotGroupControllerFixture(c *C) *snapshotGroupControllerFixture {
	datastore.SkipListerCheck = true

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	ctrl, err := NewSnapshotGroupController(logrus.StandardLogger(), ds, scheme.Scheme, kubeClient, TestNamespace, TestOwnerID1)
	c.Assert(err, IsNil)

	ctrl.eventRecorder = record.NewFakeRecorder(eventRecorderBufferSize)
	ctrl.nowHandler = func() time.Time { return testSnapshotGroupBaseTime.Add(time.Minute) }
	for i := range ctrl.cacheSyncs {
		ctrl.cacheSyncs[i] = alwaysReady
	}

	lhFactory := informerFactories.LhInformerFactory.Longhorn().V1beta2()
	return &snapshotGroupControllerFixture{
		controller:           ctrl,
		lhClient:             lhClient,
		kubeClient:           kubeClient,
		snapshotGroupIndexer: lhFactory.SnapshotGroups().Informer().GetIndexer(),
		snapshotIndexer:      lhFactory.Snapshots().Informer().GetIndexer(),
	}
}

func newTestSnapshotGroup(volumes ...string) *longhorn.SnapshotGroup {
	members := make([]longhorn.SnapshotGroupMember, 0, len(volumes))
	for _, volume := range volumes {
		members = append(members, longhorn.SnapshotGroupMember{
			VolumeName:   volume,
			SnapshotName: fmt.Sprintf("%s-%08x", testSnapshotGroupName, crc32.ChecksumIEEE([]byte(volume))),
		})
	}
	return &longhorn.SnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testSnapshotGroupName,
			Namespace:         TestNamespace,
			CreationTimestamp: metav1.NewTime(testSnapshotGroupBaseTime),
			Finalizers:        []string{longhorn.SchemeGroupVersion.Group},
		},
		Spec: longhorn.SnapshotGroupSpec{
			Volumes:         volumes,
			DeadlineSeconds: testSnapshotGroupDeadlineSeconds,
			Members:         members,
		},
	}
}

// newTestHookedSnapshotGroup builds a hooks: command group as admission
// stamps it: every member carries its policy and generation.
func newTestHookedSnapshotGroup(volumes ...string) *longhorn.SnapshotGroup {
	group := newTestSnapshotGroup(volumes...)
	group.Spec.Hooks = longhorn.SnapshotGroupHooksCommand
	for i := range group.Spec.Members {
		group.Spec.Members[i].HookPolicyRef = testVolumeHookPolicyRef()
	}
	return group
}

// initializedMemberStatuses returns the member entries the first reconcile
// creates, for tests that start a group mid-flight.
func initializedMemberStatuses(group *longhorn.SnapshotGroup) []longhorn.SnapshotGroupMemberStatus {
	members := make([]longhorn.SnapshotGroupMemberStatus, 0, len(group.Spec.Members))
	for _, member := range group.Spec.Members {
		members = append(members, longhorn.SnapshotGroupMemberStatus{VolumeName: member.VolumeName, SnapshotName: member.SnapshotName})
	}
	return members
}

const (
	testHookPodNamespace = "app"
	testHookPVCName      = "data"
	testHookPodName      = "mongo-0"
	testHookPodUID       = "pod-uid-1"
	testHookContainerID  = "containerd://c1"
)

// testHookedGroupPausedAt is when the executor reports the pre-hooks done in
// the paused-group tests: 20s into a 300s budget.
const testHookedGroupPausedAt = 20 * time.Second

// newTestPausedSnapshotGroup builds a one-member hooked group as it looks
// after the executor paused it and the controller moved it to InProgress:
// the record names the running pod, its UID, and its container.
func newTestPausedSnapshotGroup() *longhorn.SnapshotGroup {
	group := newTestHookedSnapshotGroup("vol-1")
	startedAt := metav1.NewTime(testSnapshotGroupBaseTime.Add(5 * time.Second))
	pausedAt := metav1.NewTime(testSnapshotGroupBaseTime.Add(testHookedGroupPausedAt))
	group.Status = longhorn.SnapshotGroupStatus{
		OwnerID:        TestOwnerID1,
		Phase:          longhorn.SnapshotGroupPhaseInProgress,
		Members:        initializedMemberStatuses(group),
		PreHooksDone:   true,
		HooksStartedAt: &startedAt,
		PreHooksDoneAt: &pausedAt,
		Hooks: []longhorn.HookStatus{{
			SnapshotName: group.Spec.Members[0].SnapshotName,
			Phase:        longhorn.HookPhasePreDone,
			PVCRef:       &longhorn.PVCRef{Namespace: testHookPodNamespace, Name: testHookPVCName},
			Pods: []longhorn.HookPodStatus{{
				Namespace: testHookPodNamespace, Name: testHookPodName, UID: testHookPodUID,
				ContainerIDs: []string{testHookContainerID}, HookName: "default", Phase: longhorn.HookPhasePreDone,
			}},
		}},
	}
	return group
}

// registerRunningPod puts a Running pod mounting the test PVC into the fake
// kube client, where the identity gate's live LIST finds it.
func (f *snapshotGroupControllerFixture) registerRunningPod(c *C, name, uid string, containerIDs ...string) {
	f.registerPod(c, name, uid, nil, containerIDs...)
}

func (f *snapshotGroupControllerFixture) registerTerminatingPod(c *C, name, uid string, containerIDs ...string) {
	now := metav1.Now()
	f.registerPod(c, name, uid, &now, containerIDs...)
}

// registerFinishedPod puts a retained Succeeded pod mounting the test PVC
// into the fake kube client, created at the given time, like a completed
// Job pod kept for log inspection.
func (f *snapshotGroupControllerFixture) registerFinishedPod(c *C, name, uid string, createdAt time.Time, containerIDs ...string) {
	pod := newTestHookPod(name, uid, nil, containerIDs...)
	pod.CreationTimestamp = metav1.NewTime(createdAt)
	pod.Status.Phase = corev1.PodSucceeded
	f.createPod(c, pod)
}

func (f *snapshotGroupControllerFixture) registerPod(c *C, name, uid string, deletedAt *metav1.Time, containerIDs ...string) {
	f.createPod(c, newTestHookPod(name, uid, deletedAt, containerIDs...))
}

func newTestHookPod(name, uid string, deletedAt *metav1.Time, containerIDs ...string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testHookPodNamespace, UID: k8stypes.UID(uid), DeletionTimestamp: deletedAt},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
			Name: "data",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: testHookPVCName},
			},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, id := range containerIDs {
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{ContainerID: id})
	}
	return pod
}

func (f *snapshotGroupControllerFixture) createPod(c *C, pod *corev1.Pod) {
	_, err := f.kubeClient.CoreV1().Pods(testHookPodNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
}

// recordedEvents drains the fake recorder and returns every event emitted
// so far.
func (f *snapshotGroupControllerFixture) recordedEvents() []string {
	recorder := f.controller.eventRecorder.(*record.FakeRecorder)
	var events []string
	for {
		select {
		case event := <-recorder.Events:
			events = append(events, event)
		default:
			return events
		}
	}
}

// annotateTerminalPhase stamps the terminal-phase annotation, as the
// controller records it when a group reaches Ready or Failed.
func annotateTerminalPhase(group *longhorn.SnapshotGroup, phase longhorn.SnapshotGroupPhase) {
	group.Annotations = map[string]string{
		types.SnapshotGroupAnnotationTerminalPhase: string(phase),
	}
}

// registerSnapshotGroup persists the group in the fake clientset and the
// lister indexer, as if the informer had already delivered it.
func (f *snapshotGroupControllerFixture) registerSnapshotGroup(c *C, snapshotGroup *longhorn.SnapshotGroup) {
	created, err := f.lhClient.LonghornV1beta2().SnapshotGroups(TestNamespace).Create(
		context.TODO(), snapshotGroup, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(f.snapshotGroupIndexer.Add(created), IsNil)
}

// registerMemberSnapshot persists a member Snapshot with the labels the
// controller stamps at create: the group label, and the hooked marker when
// the registered group runs hooks.
func (f *snapshotGroupControllerFixture) registerMemberSnapshot(c *C, member longhorn.SnapshotGroupMember, readyToUse bool, creationTime string) {
	labels := map[string]string{
		types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroup): testSnapshotGroupName,
	}
	if obj, exists, err := f.snapshotGroupIndexer.GetByKey(TestNamespace + "/" + testSnapshotGroupName); err == nil && exists && snapshothook.IsHooked(obj.(*longhorn.SnapshotGroup)) {
		labels[types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupHooked)] = "true"
	}
	f.registerMemberSnapshotWithLabels(c, member, readyToUse, creationTime, labels)
}

// registerUnhookedMemberSnapshot persists a member Snapshot as a manager
// that does not know spec.hooks creates it: group label only, no hooked
// marker.
func (f *snapshotGroupControllerFixture) registerUnhookedMemberSnapshot(c *C, member longhorn.SnapshotGroupMember, readyToUse bool, creationTime string) {
	f.registerMemberSnapshotWithLabels(c, member, readyToUse, creationTime, map[string]string{
		types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroup): testSnapshotGroupName,
	})
}

func (f *snapshotGroupControllerFixture) registerMemberSnapshotWithLabels(c *C, member longhorn.SnapshotGroupMember, readyToUse bool, creationTime string, labels map[string]string) {
	snapshot := &longhorn.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      member.SnapshotName,
			Namespace: TestNamespace,
			Labels:    labels,
		},
		Spec: longhorn.SnapshotSpec{
			Volume:         member.VolumeName,
			CreateSnapshot: true,
		},
		Status: longhorn.SnapshotStatus{
			ReadyToUse:   readyToUse,
			CreationTime: creationTime,
		},
	}
	created, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Create(
		context.TODO(), snapshot, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(f.snapshotIndexer.Add(created), IsNil)
}

// registerForeignSnapshot persists a snapshot that occupies the given name
// but does not belong to the group: no group label, different volume.
func (f *snapshotGroupControllerFixture) registerForeignSnapshot(c *C, name string, readyToUse bool, creationTime string) {
	foreign := &longhorn.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
		},
		Spec: longhorn.SnapshotSpec{Volume: "some-other-volume"},
		Status: longhorn.SnapshotStatus{
			ReadyToUse:   readyToUse,
			CreationTime: creationTime,
		},
	}
	created, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Create(
		context.TODO(), foreign, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(f.snapshotIndexer.Add(created), IsNil)
}

// markMemberSnapshotTerminating stamps a DeletionTimestamp on the member's
// snapshot in the lister indexer: deletion was requested and the CR lingers
// in Terminating.
func (f *snapshotGroupControllerFixture) markMemberSnapshotTerminating(c *C, snapshotName string) {
	obj, ok, err := f.snapshotIndexer.GetByKey(TestNamespace + "/" + snapshotName)
	c.Assert(err, IsNil)
	c.Assert(ok, Equals, true)
	snapshot := obj.(*longhorn.Snapshot).DeepCopy()
	deletionTime := metav1.NewTime(testSnapshotGroupBaseTime.Add(time.Minute))
	snapshot.DeletionTimestamp = &deletionTime
	c.Assert(f.snapshotIndexer.Update(snapshot), IsNil)
}

// syncGroupFromClient refreshes the lister indexer from the fake clientset so
// the next reconcile observes the previous reconcile's writes.
func (f *snapshotGroupControllerFixture) syncGroupFromClient(c *C) {
	updated, err := f.lhClient.LonghornV1beta2().SnapshotGroups(TestNamespace).Get(
		context.TODO(), testSnapshotGroupName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(f.snapshotGroupIndexer.Add(updated), IsNil)
}

// syncSnapshotsFromClient refreshes the snapshot lister indexer from the fake
// clientset, as a restarted informer would list the existing Snapshots.
func (f *snapshotGroupControllerFixture) syncSnapshotsFromClient(c *C) {
	snapshots, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	for i := range snapshots.Items {
		c.Assert(f.snapshotIndexer.Add(&snapshots.Items[i]), IsNil)
	}
}

func (f *snapshotGroupControllerFixture) getGroup(c *C) *longhorn.SnapshotGroup {
	snapshotGroup, err := f.lhClient.LonghornV1beta2().SnapshotGroups(TestNamespace).Get(
		context.TODO(), testSnapshotGroupName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	return snapshotGroup
}

// assertSnapshotCount verifies how many Snapshot CRs exist in the fake
// clientset.
func (f *snapshotGroupControllerFixture) assertSnapshotCount(c *C, count int) {
	snapshots, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(snapshots.Items, HasLen, count)
}

type SnapshotGroupControllerSuite struct{}

var _ = Suite(&SnapshotGroupControllerSuite{})

// TestFirstReconcileCreatesMembersAndEntersInProgress verifies the first sync:
// ownership is claimed, one labeled Snapshot per member is created, and the
// phase enters InProgress.
func (s *SnapshotGroupControllerSuite) TestFirstReconcileCreatesMembersAndEntersInProgress(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1", "vol-2")
	f.registerSnapshotGroup(c, group)

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.OwnerID, Equals, TestOwnerID1)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseInProgress)
	c.Assert(updated.Status.Members, HasLen, 2)

	for _, member := range group.Spec.Members {
		snapshot, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(
			context.TODO(), member.SnapshotName, metav1.GetOptions{})
		c.Assert(err, IsNil)
		c.Assert(snapshot.Spec.Volume, Equals, member.VolumeName)
		c.Assert(snapshot.Spec.CreateSnapshot, Equals, true)
		c.Assert(snapshot.Labels[types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroup)], Equals, testSnapshotGroupName)
	}
}

// TestReconcileIsIdempotentAcrossRestart verifies that a repeated reconcile
// does not create duplicate member snapshots or disturb recorded state.
func (s *SnapshotGroupControllerSuite) TestReconcileIsIdempotentAcrossRestart(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1")
	f.registerSnapshotGroup(c, group)

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)
	f.syncGroupFromClient(c)
	f.syncSnapshotsFromClient(c)
	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	f.assertSnapshotCount(c, 1)
	c.Assert(f.getGroup(c).Status.Phase, Equals, longhorn.SnapshotGroupPhaseInProgress)
}

// TestAdoptsExistingMemberSnapshot verifies that a member snapshot already
// carrying the group label and volume is adopted: no duplicate is created and
// the group does not fail with a collision.
func (s *SnapshotGroupControllerSuite) TestAdoptsExistingMemberSnapshot(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1")
	f.registerSnapshotGroup(c, group)
	f.registerMemberSnapshot(c, group.Spec.Members[0], false, "")

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseInProgress)
	f.assertSnapshotCount(c, 1)
}

// TestReadyWhenAllMembersTakenWithinDeadline verifies the happy path: all
// member snapshots taken within the deadline make the group Ready, with the
// latest creation time recorded as the group creation time and the
// terminal-phase annotation set.
func (s *SnapshotGroupControllerSuite) TestReadyWhenAllMembersTakenWithinDeadline(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1", "vol-2")
	f.registerSnapshotGroup(c, group)
	f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(10*time.Second))
	f.registerMemberSnapshot(c, group.Spec.Members[1], true, creationTimeAt(30*time.Second))

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseReady)
	c.Assert(updated.Status.ReadyToUse, Equals, true)
	c.Assert(updated.Status.CreationTime, Equals, creationTimeAt(30*time.Second))
	c.Assert(updated.Annotations[types.SnapshotGroupAnnotationTerminalPhase], Equals, string(longhorn.SnapshotGroupPhaseReady))
}

// TestFailedWhenMemberNotReadyByDeadline verifies that a member still unready
// past the deadline fails the group, with the error naming the volume.
func (s *SnapshotGroupControllerSuite) TestFailedWhenMemberNotReadyByDeadline(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1", "vol-2")
	f.registerSnapshotGroup(c, group)
	f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(10*time.Second))
	f.registerMemberSnapshot(c, group.Spec.Members[1], false, "")

	f.controller.nowHandler = func() time.Time {
		return testSnapshotGroupBaseTime.Add(time.Duration(testSnapshotGroupDeadlineSeconds+1) * time.Second)
	}
	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
	c.Assert(updated.Status.ReadyToUse, Equals, false)
	c.Assert(updated.Status.Error, Matches, ".*deadline exceeded.*vol-2.*")
	c.Assert(updated.Annotations[types.SnapshotGroupAnnotationTerminalPhase], Equals, string(longhorn.SnapshotGroupPhaseFailed))
	for _, event := range f.recordedEvents() {
		c.Assert(event, Not(Matches), ".*HookDeadlineExceeded.*")
	}
}

// TestFailedWhenMemberTakenAfterDeadline verifies that a member snapshot taken after the
// deadline fails the group even though all members are ready at reconcile
// time.
func (s *SnapshotGroupControllerSuite) TestFailedWhenMemberTakenAfterDeadline(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1", "vol-2")
	f.registerSnapshotGroup(c, group)
	f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(10*time.Second))
	f.registerMemberSnapshot(c, group.Spec.Members[1], true,
		creationTimeAt(time.Duration(testSnapshotGroupDeadlineSeconds+30)*time.Second))

	f.controller.nowHandler = func() time.Time {
		return testSnapshotGroupBaseTime.Add(time.Duration(testSnapshotGroupDeadlineSeconds+60) * time.Second)
	}
	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
	// The failure message must name the late member: at reconcile time it
	// looks ready, and only its creation time shows it missed the deadline.
	c.Assert(updated.Status.Error, Matches, ".*deadline exceeded.*vol-2.*after the deadline.*")
}

// TestFailedWhenReconciledAfterDeadline verifies a group first reconciled
// after its deadline fails without creating any member snapshot: a snapshot
// taken past the deadline would be discarded anyway and would auto-attach a
// detached member volume.
func (s *SnapshotGroupControllerSuite) TestFailedWhenReconciledAfterDeadline(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1", "vol-2")
	f.registerSnapshotGroup(c, group)

	f.controller.nowHandler = func() time.Time {
		return testSnapshotGroupBaseTime.Add(time.Duration(testSnapshotGroupDeadlineSeconds+1) * time.Second)
	}
	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
	c.Assert(updated.Status.Error, Matches, ".*deadline exceeded.*")
	f.assertSnapshotCount(c, 0)
}

// TestNoMemberCreateStartsAfterDeadline verifies the clock reads around the
// creates: when the deadline passes between two member creates, the second
// member is not created, and the post-loop read fails the group in the same
// pass instead of deferring to the next one.
func (s *SnapshotGroupControllerSuite) TestNoMemberCreateStartsAfterDeadline(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1", "vol-2")
	f.registerSnapshotGroup(c, group)

	// The reconcile reads the clock once to arm the deadline, before each
	// create, and once after the loop: the third read lands past the
	// deadline, between the two creates.
	clockReads := 0
	f.controller.nowHandler = func() time.Time {
		clockReads++
		if clockReads <= 2 {
			return testSnapshotGroupBaseTime.Add(time.Second)
		}
		return testSnapshotGroupBaseTime.Add(time.Duration(testSnapshotGroupDeadlineSeconds+1) * time.Second)
	}

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	f.assertSnapshotCount(c, 1)
	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
	c.Assert(updated.Status.Error, Matches, ".*vol-2.*not taken.*")
}

// TestDeletionUIDConflictDoesNotBlockFinalizer verifies that a member name
// taken over by a different snapshot between the read and the delete does
// not wedge the group deletion: the conflict counts as foreign, the
// replacement survives, and the finalizer is released.
func (s *SnapshotGroupControllerSuite) TestDeletionUIDConflictDoesNotBlockFinalizer(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1")
	deletionTime := metav1.NewTime(testSnapshotGroupBaseTime.Add(time.Minute))
	group.DeletionTimestamp = &deletionTime
	group.Finalizers = []string{longhorn.SchemeGroupVersion.Group}
	f.registerSnapshotGroup(c, group)
	f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(10*time.Second))

	f.lhClient.PrependReactor("delete", "snapshots", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(longhorn.Resource("snapshots"), group.Spec.Members[0].SnapshotName, fmt.Errorf("uid mismatch"))
	})

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Finalizers, HasLen, 0)
	_, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(context.TODO(), group.Spec.Members[0].SnapshotName, metav1.GetOptions{})
	c.Assert(err, IsNil)
}

// TestFailedAtDeadlineDespiteMemberCreateErrors verifies that a group whose
// member creates keep failing still fails at the deadline: the deadline
// requeue is armed at reconcile entry, before the fallible create, so the
// rate limiter's growing backoff cannot defer the failure.
func (s *SnapshotGroupControllerSuite) TestFailedAtDeadlineDespiteMemberCreateErrors(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1")
	f.registerSnapshotGroup(c, group)
	f.lhClient.PrependReactor("create", "snapshots", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("member volume is gone")
	})

	c.Assert(f.controller.reconcile(testSnapshotGroupName), NotNil)

	// The armed deadline fires this reconcile; the create error must not
	// keep the group InProgress past it.
	f.controller.nowHandler = func() time.Time {
		return testSnapshotGroupBaseTime.Add(time.Duration(testSnapshotGroupDeadlineSeconds+1) * time.Second)
	}
	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
	c.Assert(updated.Status.Error, Matches, ".*not taken.*")
}

// TestFailedWhenAdoptedMemberPredatesGroup verifies that a surviving member
// snapshot of an earlier same-name group cannot make the new group Ready:
// its creation time is before the group was requested, so the group stays
// InProgress and fails at the deadline naming the member.
func (s *SnapshotGroupControllerSuite) TestFailedWhenAdoptedMemberPredatesGroup(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1")
	f.registerSnapshotGroup(c, group)
	// The survivor carries the group label and volume, so it is adopted, but
	// it was taken an hour before this group existed.
	f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(-time.Hour))

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)
	c.Assert(f.getGroup(c).Status.Phase, Equals, longhorn.SnapshotGroupPhaseInProgress)

	f.syncGroupFromClient(c)
	f.controller.nowHandler = func() time.Time {
		return testSnapshotGroupBaseTime.Add(time.Duration(testSnapshotGroupDeadlineSeconds+1) * time.Second)
	}
	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
	c.Assert(updated.Status.Error, Matches, ".*before the group's window opened.*")
}

// TestFailedOnForeignSnapshotCollision verifies that an unrelated snapshot
// occupying a member's name fails the group and is left untouched.
func (s *SnapshotGroupControllerSuite) TestFailedOnForeignSnapshotCollision(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1")
	f.registerSnapshotGroup(c, group)
	f.registerForeignSnapshot(c, group.Spec.Members[0].SnapshotName, false, "")

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
	c.Assert(updated.Status.Error, Matches, ".*collides.*")

	survivor, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(
		context.TODO(), group.Spec.Members[0].SnapshotName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(survivor.Spec.Volume, Equals, "some-other-volume")
}

// TestFailedOnForeignSnapshotCollisionAfterDeadline verifies the foreign-name
// check still runs past the deadline: an expired group must fail on the
// collision, never read the foreign snapshot as a member.
func (s *SnapshotGroupControllerSuite) TestFailedOnForeignSnapshotCollisionAfterDeadline(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1")
	f.registerSnapshotGroup(c, group)
	f.registerForeignSnapshot(c, group.Spec.Members[0].SnapshotName, false, "")

	f.controller.nowHandler = func() time.Time {
		return testSnapshotGroupBaseTime.Add(time.Duration(testSnapshotGroupDeadlineSeconds+1) * time.Second)
	}
	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
	c.Assert(updated.Status.Error, Matches, ".*collides.*")
}

// TestFailedReconcileOnCreateNameConflict verifies that a snapshot taking the
// member name between the lister read and the create fails the reconcile
// instead of being silently adopted: the retry re-reads it through the lister
// and runs the ownership check on it.
func (s *SnapshotGroupControllerSuite) TestFailedReconcileOnCreateNameConflict(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1")
	f.registerSnapshotGroup(c, group)

	// In the fake clientset but not in the lister indexer: the reconcile
	// reads NotFound, then collides on create.
	foreign := &longhorn.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      group.Spec.Members[0].SnapshotName,
			Namespace: TestNamespace,
		},
		Spec: longhorn.SnapshotSpec{Volume: "some-other-volume"},
	}
	_, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Create(
		context.TODO(), foreign, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	c.Assert(f.controller.reconcile(testSnapshotGroupName), ErrorMatches, ".*already exists.*")

	c.Assert(f.getGroup(c).Status.Phase, Not(Equals), longhorn.SnapshotGroupPhaseReady)
	survivor, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(
		context.TODO(), group.Spec.Members[0].SnapshotName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(survivor.Spec.Volume, Equals, "some-other-volume")
}

// TestRestoreGuardRestoresTerminalPhaseWithoutTakingSnapshots verifies that a group
// with an empty status but a terminal-phase annotation regains the annotated
// phase without creating member snapshots. With no member snapshot present,
// a restored Ready group is degraded and not ReadyToUse.
func (s *SnapshotGroupControllerSuite) TestRestoreGuardRestoresTerminalPhaseWithoutTakingSnapshots(c *C) {
	for _, phase := range []longhorn.SnapshotGroupPhase{longhorn.SnapshotGroupPhaseReady, longhorn.SnapshotGroupPhaseFailed} {
		f := newSnapshotGroupControllerFixture(c)
		group := newTestSnapshotGroup("vol-1")
		annotateTerminalPhase(group, phase)
		f.registerSnapshotGroup(c, group)

		c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

		updated := f.getGroup(c)
		c.Assert(updated.Status.Phase, Equals, phase)
		c.Assert(updated.Status.ReadyToUse, Equals, false)

		f.assertSnapshotCount(c, 0)
	}
}

// TestRestoredReadyGroupWithIntactMembersIsNotDegraded verifies the restore
// variant with all member snapshots present: the terminal reconcile re-mirrors
// each healthy member back to true, recovers the member creation times and the
// group creation time, and does not raise the Degraded condition.
func (s *SnapshotGroupControllerSuite) TestRestoredReadyGroupWithIntactMembersIsNotDegraded(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1", "vol-2")
	annotateTerminalPhase(group, longhorn.SnapshotGroupPhaseReady)
	f.registerSnapshotGroup(c, group)
	f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(10*time.Second))
	f.registerMemberSnapshot(c, group.Spec.Members[1], true, creationTimeAt(30*time.Second))

	// First reconcile restores the annotated phase; the second runs the
	// terminal path against the restored status.
	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)
	f.syncGroupFromClient(c)
	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseReady)
	c.Assert(updated.Status.ReadyToUse, Equals, true)
	c.Assert(updated.Status.CreationTime, Equals, creationTimeAt(30*time.Second))

	for i, member := range updated.Status.Members {
		c.Assert(member.ReadyToUse, Equals, true)
		c.Assert(member.Error, Equals, "")
		c.Assert(member.CreationTime, Equals, creationTimeAt(time.Duration(10+20*i)*time.Second))
	}

	degraded := types.GetCondition(updated.Status.Conditions, longhorn.SnapshotGroupConditionTypeDegraded)
	c.Assert(degraded.Status, Not(Equals), longhorn.ConditionStatusTrue)

	// No replacement snapshots: only the two pre-existing member snapshots exist.
	f.assertSnapshotCount(c, 2)
}

// TestRestoredFailedGroupStaysFailed verifies the recorded outcome wins over
// re-derivation: a Failed group whose member survived ready within the
// deadline stays Failed. The restored phase is empty for a restored copy, or
// InProgress for a crash between the annotation write and the status persist.
func (s *SnapshotGroupControllerSuite) TestRestoredFailedGroupStaysFailed(c *C) {
	for _, restoredPhase := range []longhorn.SnapshotGroupPhase{"", longhorn.SnapshotGroupPhaseInProgress} {
		f := newSnapshotGroupControllerFixture(c)
		group := newTestSnapshotGroup("vol-1")
		annotateTerminalPhase(group, longhorn.SnapshotGroupPhaseFailed)
		group.Status.Phase = restoredPhase
		f.registerSnapshotGroup(c, group)
		f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(10*time.Second))

		c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)
		f.syncGroupFromClient(c)
		c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

		updated := f.getGroup(c)
		c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
		c.Assert(updated.Status.ReadyToUse, Equals, false)
		// The original failure reason is not restorable; the group must still
		// say why it has none.
		c.Assert(updated.Status.Error, Not(Equals), "")
	}
}

// TestDegradedAfterMemberLossClearsReadyToUse verifies that losing a member
// snapshot after Ready marks the member lost, raises the Degraded condition,
// and clears the group's ReadyToUse: the group stays Ready in phase, but the
// set is no longer restorable as a whole.
func (s *SnapshotGroupControllerSuite) TestDegradedAfterMemberLossClearsReadyToUse(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1", "vol-2")
	annotateTerminalPhase(group, longhorn.SnapshotGroupPhaseReady)
	group.Status = longhorn.SnapshotGroupStatus{
		OwnerID:      TestOwnerID1,
		Phase:        longhorn.SnapshotGroupPhaseReady,
		ReadyToUse:   true,
		CreationTime: creationTimeAt(30 * time.Second),
		Members: []longhorn.SnapshotGroupMemberStatus{
			{VolumeName: "vol-1", SnapshotName: group.Spec.Members[0].SnapshotName, ReadyToUse: true, CreationTime: creationTimeAt(10 * time.Second)},
			{VolumeName: "vol-2", SnapshotName: group.Spec.Members[1].SnapshotName, ReadyToUse: true, CreationTime: creationTimeAt(30 * time.Second)},
		},
	}
	f.registerSnapshotGroup(c, group)
	// Only vol-1's member snapshot survives; vol-2's is gone.
	f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(10*time.Second))

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseReady)
	c.Assert(updated.Status.ReadyToUse, Equals, false)

	c.Assert(updated.Status.Members[0].ReadyToUse, Equals, true)
	c.Assert(updated.Status.Members[1].ReadyToUse, Equals, false)
	c.Assert(updated.Status.Members[1].Error, Equals, snapshotGroupMemberLostError)
	// The lost member keeps its recorded creation time.
	c.Assert(updated.Status.Members[1].CreationTime, Equals, creationTimeAt(30*time.Second))

	degraded := types.GetCondition(updated.Status.Conditions, longhorn.SnapshotGroupConditionTypeDegraded)
	c.Assert(degraded.Status, Equals, longhorn.ConditionStatusTrue)
	c.Assert(degraded.Message, Matches, ".*vol-2.*")
}

// TestDegradedWhenMemberSnapshotIsTerminating verifies that a member whose
// deletion was requested after Ready is recorded as lost while its CR still
// lingers, raising the Degraded condition and clearing the group's
// ReadyToUse.
func (s *SnapshotGroupControllerSuite) TestDegradedWhenMemberSnapshotIsTerminating(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1")
	annotateTerminalPhase(group, longhorn.SnapshotGroupPhaseReady)
	group.Status = longhorn.SnapshotGroupStatus{
		OwnerID:      TestOwnerID1,
		Phase:        longhorn.SnapshotGroupPhaseReady,
		ReadyToUse:   true,
		CreationTime: creationTimeAt(10 * time.Second),
		Members: []longhorn.SnapshotGroupMemberStatus{
			{VolumeName: "vol-1", SnapshotName: group.Spec.Members[0].SnapshotName, ReadyToUse: true, CreationTime: creationTimeAt(10 * time.Second)},
		},
	}
	f.registerSnapshotGroup(c, group)
	f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(10*time.Second))

	// Request the member's deletion; the CR lingers in Terminating.
	f.markMemberSnapshotTerminating(c, group.Spec.Members[0].SnapshotName)

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseReady)
	c.Assert(updated.Status.ReadyToUse, Equals, false)
	c.Assert(updated.Status.Members[0].ReadyToUse, Equals, false)
	c.Assert(updated.Status.Members[0].Error, Equals, snapshotGroupMemberLostError)

	degraded := types.GetCondition(updated.Status.Conditions, longhorn.SnapshotGroupConditionTypeDegraded)
	c.Assert(degraded.Status, Equals, longhorn.ConditionStatusTrue)
}

// TestMemberNotHealedByForeignSnapshotAfterReady verifies a lost member stays
// lost when an unrelated snapshot reuses its name: the foreign snapshot must
// not be mirrored into the group as the member.
func (s *SnapshotGroupControllerSuite) TestMemberNotHealedByForeignSnapshotAfterReady(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1")
	annotateTerminalPhase(group, longhorn.SnapshotGroupPhaseReady)
	group.Status = longhorn.SnapshotGroupStatus{
		OwnerID:      TestOwnerID1,
		Phase:        longhorn.SnapshotGroupPhaseReady,
		ReadyToUse:   true,
		CreationTime: creationTimeAt(10 * time.Second),
		Members: []longhorn.SnapshotGroupMemberStatus{
			{VolumeName: "vol-1", SnapshotName: group.Spec.Members[0].SnapshotName, ReadyToUse: false, CreationTime: creationTimeAt(10 * time.Second), Error: snapshotGroupMemberLostError},
		},
	}
	f.registerSnapshotGroup(c, group)
	// A ready unrelated snapshot reuses the lost member's name.
	f.registerForeignSnapshot(c, group.Spec.Members[0].SnapshotName, true, creationTimeAt(60*time.Second))

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Members[0].ReadyToUse, Equals, false)
	c.Assert(updated.Status.Members[0].Error, Equals, snapshotGroupMemberReplacedError)
	// The lost member keeps its recorded creation time.
	c.Assert(updated.Status.Members[0].CreationTime, Equals, creationTimeAt(10*time.Second))

	degraded := types.GetCondition(updated.Status.Conditions, longhorn.SnapshotGroupConditionTypeDegraded)
	c.Assert(degraded.Status, Equals, longhorn.ConditionStatusTrue)
}

// TestDeletionRemovesOwnMembersThenFinalizer verifies teardown: the first
// reconcile deletes the group's own member snapshots and holds the finalizer;
// once every member deletion is requested (DeletionTimestamp set) the
// finalizer is removed even though the member CRs may linger while the
// snapshot controller defers the purge. A foreign snapshot occupying a member
// name is never deleted.
func (s *SnapshotGroupControllerSuite) TestDeletionRemovesOwnMembersThenFinalizer(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestSnapshotGroup("vol-1", "vol-2")
	now := metav1.NewTime(testSnapshotGroupBaseTime.Add(time.Minute))
	group.DeletionTimestamp = &now
	group.Status = longhorn.SnapshotGroupStatus{OwnerID: TestOwnerID1, Phase: longhorn.SnapshotGroupPhaseReady}
	f.registerSnapshotGroup(c, group)

	f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(10*time.Second))
	f.registerForeignSnapshot(c, group.Spec.Members[1].SnapshotName, false, "")

	// First pass: own member deleted, foreign one untouched, finalizer held.
	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	_, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(
		context.TODO(), group.Spec.Members[0].SnapshotName, metav1.GetOptions{})
	c.Assert(apierrors.IsNotFound(err), Equals, true)
	_, err = f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(
		context.TODO(), group.Spec.Members[1].SnapshotName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(f.getGroup(c).Finalizers, HasLen, 1)

	// Member deletion requested: the member CR lingers in Terminating
	// (deferred purge). The finalizer must come off without waiting for the
	// CR to disappear.
	f.markMemberSnapshotTerminating(c, group.Spec.Members[0].SnapshotName)

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)
	c.Assert(f.getGroup(c).Finalizers, HasLen, 0)
}

// TestHookedGroupEntersRunningPreHooksWithoutTakingSnapshots verifies the
// first reconcile of a hooked group: the phase is RunningPreHooks and no
// member snapshot exists until the executor reports the pause.
func (s *SnapshotGroupControllerSuite) TestHookedGroupEntersRunningPreHooksWithoutTakingSnapshots(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	f.registerSnapshotGroup(c, newTestHookedSnapshotGroup("vol-1", "vol-2"))

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseRunningPreHooks)
	c.Assert(updated.Status.Members, HasLen, 2)
	f.assertSnapshotCount(c, 0)

	// A second reconcile with nothing reported keeps waiting.
	f.syncGroupFromClient(c)
	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)
	c.Assert(f.getGroup(c).Status.Phase, Equals, longhorn.SnapshotGroupPhaseRunningPreHooks)
	f.assertSnapshotCount(c, 0)
}

// TestHookedGroupTakesSnapshotsOncePreHooksDone verifies that preHooksDone
// moves the group to InProgress and creates the member snapshots in the
// same pass, so the pause is not held for an extra reconcile.
func (s *SnapshotGroupControllerSuite) TestHookedGroupTakesSnapshotsOncePreHooksDone(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestHookedSnapshotGroup("vol-1", "vol-2")
	preHooksDoneAt := metav1.NewTime(testSnapshotGroupBaseTime.Add(20 * time.Second))
	group.Status = longhorn.SnapshotGroupStatus{
		OwnerID:        TestOwnerID1,
		Phase:          longhorn.SnapshotGroupPhaseRunningPreHooks,
		Members:        initializedMemberStatuses(group),
		PreHooksDone:   true,
		HooksStartedAt: &preHooksDoneAt,
		PreHooksDoneAt: &preHooksDoneAt,
	}
	f.registerSnapshotGroup(c, group)

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseInProgress)
	// The executor's fields round-trip through the controller's status write.
	c.Assert(updated.Status.PreHooksDone, Equals, true)
	c.Assert(updated.Status.PreHooksDoneAt, NotNil)
	f.assertSnapshotCount(c, 2)
}

// TestHookedGroupFailsOnPreHookFailure verifies that a PreFailed member in
// the executor's record fails the group, with status.error composed from
// the member policy and the recorded reason: at the pod level with the exit
// code, or at the member level when no command could run.
func (s *SnapshotGroupControllerSuite) TestHookedGroupFailsOnPreHookFailure(c *C) {
	exitCode := int32(1)
	cases := []struct {
		hook          longhorn.HookStatus
		expectedError string
	}{
		{
			hook: longhorn.HookStatus{
				Phase: longhorn.HookPhasePreFailed,
				Pods: []longhorn.HookPodStatus{{
					Namespace: "app", Name: "mongo-0", Phase: longhorn.HookPhasePreFailed,
					Reason: longhorn.HookReasonExitNonZero, ExitCode: &exitCode,
				}},
			},
			expectedError: "pre-hook failed on member .* \\(app/policy\\) in pod app/mongo-0: ExitNonZero \\(exit 1\\)",
		},
		{
			hook: longhorn.HookStatus{
				Phase:  longhorn.HookPhasePreFailed,
				Reason: longhorn.HookReasonVolumeConflict,
			},
			expectedError: "pre-hook failed on member .* \\(app/policy\\): VolumeConflict",
		},
	}
	for _, tc := range cases {
		f := newSnapshotGroupControllerFixture(c)
		group := newTestHookedSnapshotGroup("vol-1")
		startedAt := metav1.NewTime(testSnapshotGroupBaseTime.Add(5 * time.Second))
		tc.hook.SnapshotName = group.Spec.Members[0].SnapshotName
		group.Status = longhorn.SnapshotGroupStatus{
			OwnerID:        TestOwnerID1,
			Phase:          longhorn.SnapshotGroupPhaseRunningPreHooks,
			Members:        initializedMemberStatuses(group),
			HooksStartedAt: &startedAt,
			Hooks:          []longhorn.HookStatus{tc.hook},
		}
		f.registerSnapshotGroup(c, group)

		c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

		updated := f.getGroup(c)
		c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
		c.Assert(updated.Status.Error, Matches, tc.expectedError)
		c.Assert(updated.Annotations[types.SnapshotGroupAnnotationTerminalPhase], Equals, string(longhorn.SnapshotGroupPhaseFailed))
		f.assertSnapshotCount(c, 0)
	}
}

// TestHookedGroupFailsAtDeadlineWhileRunningPreHooks verifies the deadline
// rule in RunningPreHooks: the error tells a group the executor never
// claimed apart from one whose pre-hooks were still running, with the
// member count, and no snapshot is ever created.
func (s *SnapshotGroupControllerSuite) TestHookedGroupFailsAtDeadlineWhileRunningPreHooks(c *C) {
	startedAt := metav1.NewTime(testSnapshotGroupBaseTime.Add(5 * time.Second))
	cases := []struct {
		hooksStartedAt *metav1.Time
		preDoneMembers int
		expectedError  string
		expectedMarker string
	}{
		{hooksStartedAt: nil, expectedError: "deadline passed while queued: paused nothing .*", expectedMarker: snapshothook.QueuedPastDeadlineMarker},
		{hooksStartedAt: &startedAt, preDoneMembers: 1, expectedError: "deadline passed while running pre-hooks: 1/2 members done"},
	}
	for _, tc := range cases {
		f := newSnapshotGroupControllerFixture(c)
		group := newTestHookedSnapshotGroup("vol-1", "vol-2")
		group.Status = longhorn.SnapshotGroupStatus{
			OwnerID:        TestOwnerID1,
			Phase:          longhorn.SnapshotGroupPhaseRunningPreHooks,
			Members:        initializedMemberStatuses(group),
			HooksStartedAt: tc.hooksStartedAt,
		}
		for i := 0; i < tc.preDoneMembers; i++ {
			group.Status.Hooks = append(group.Status.Hooks, longhorn.HookStatus{
				SnapshotName: group.Spec.Members[i].SnapshotName,
				Phase:        longhorn.HookPhasePreDone,
			})
		}
		f.registerSnapshotGroup(c, group)

		f.controller.nowHandler = func() time.Time {
			return testSnapshotGroupBaseTime.Add(time.Duration(testSnapshotGroupDeadlineSeconds+1) * time.Second)
		}
		c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

		updated := f.getGroup(c)
		c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
		c.Assert(updated.Status.Error, Matches, tc.expectedError)
		if tc.expectedMarker != "" {
			c.Assert(strings.HasSuffix(updated.Status.Error, tc.expectedMarker), Equals, true)
		}
		f.assertSnapshotCount(c, 0)
		f.assertHookDeadlineExceededEvent(c)
	}
}

// TestHookedGroupInProgressWithoutPreHooksDoneFails verifies the check for a
// hooked group that a longhorn-manager predating hooks moved straight to
// InProgress. The new controller fails it with the pre-hooks-skipped marker
// and creates no member snapshot, even though every other condition for
// Ready would hold.
func (s *SnapshotGroupControllerSuite) TestHookedGroupInProgressWithoutPreHooksDoneFails(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestHookedSnapshotGroup("vol-1")
	group.Status = longhorn.SnapshotGroupStatus{
		OwnerID: TestOwnerID1,
		Phase:   longhorn.SnapshotGroupPhaseInProgress,
		Members: initializedMemberStatuses(group),
	}
	f.registerSnapshotGroup(c, group)

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
	c.Assert(updated.Status.Error, Matches, ".*before the pre-hooks were done.*")
	c.Assert(strings.HasSuffix(updated.Status.Error, snapshothook.PreHooksSkippedMarker), Equals, true)
	c.Assert(updated.Annotations[types.SnapshotGroupAnnotationTerminalPhase], Equals, string(longhorn.SnapshotGroupPhaseFailed))
	f.assertSnapshotCount(c, 0)
}

// TestHookedGroupFailsOnMemberWithoutHookedMarker verifies the second check
// against a longhorn-manager that predates hooks. Such a manager creates
// member snapshots before its refused status write, and those carry no
// hooked marker. The new controller fails the paused group instead of
// calling the unpaused snapshot consistent, and the snapshot stays the
// group's own so deletion still removes it.
func (s *SnapshotGroupControllerSuite) TestHookedGroupFailsOnMemberWithoutHookedMarker(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestPausedSnapshotGroup()
	f.registerSnapshotGroup(c, group)
	f.registerUnhookedMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(testHookedGroupPausedAt+10*time.Second))
	f.registerRunningPod(c, testHookPodName, testHookPodUID, testHookContainerID)

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
	c.Assert(updated.Status.Error, Matches, "member snapshot .* was taken before the application was paused.*")
	c.Assert(strings.HasSuffix(updated.Status.Error, snapshothook.PreHooksSkippedMarker), Equals, true)
	f.assertSnapshotCount(c, 1)

	snapshot, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(context.TODO(), group.Spec.Members[0].SnapshotName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(f.controller.isOwnMemberSnapshot(updated, group.Spec.Members[0].VolumeName, snapshot), Equals, true)
}

// TestHookedGroupStampsHookedMarkerOnMembers verifies that the member
// snapshots a hooked group creates carry the hooked marker, and that an
// unhooked group's do not.
func (s *SnapshotGroupControllerSuite) TestHookedGroupStampsHookedMarkerOnMembers(c *C) {
	marker := types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupHooked)
	cases := []struct {
		name         string
		hooked       bool
		expectMarker bool
	}{
		{name: "hooked group stamps the marker", hooked: true, expectMarker: true},
		{name: "unhooked group does not", hooked: false, expectMarker: false},
	}
	for _, tc := range cases {
		f := newSnapshotGroupControllerFixture(c)
		var group *longhorn.SnapshotGroup
		if tc.hooked {
			group = newTestHookedSnapshotGroup("vol-1")
			group.Status = longhorn.SnapshotGroupStatus{
				OwnerID:      TestOwnerID1,
				Phase:        longhorn.SnapshotGroupPhaseRunningPreHooks,
				Members:      initializedMemberStatuses(group),
				PreHooksDone: true,
			}
		} else {
			group = newTestSnapshotGroup("vol-1")
		}
		f.registerSnapshotGroup(c, group)

		c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil, Commentf(tc.name))

		snapshots, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).List(context.TODO(), metav1.ListOptions{})
		c.Assert(err, IsNil)
		c.Assert(snapshots.Items, HasLen, 1, Commentf(tc.name))
		value, hasMarker := snapshots.Items[0].Labels[marker]
		c.Assert(hasMarker, Equals, tc.expectMarker, Commentf(tc.name))
		if tc.expectMarker {
			c.Assert(value, Equals, "true", Commentf(tc.name))
		}
	}
}

// TestReadyHookedGroupWithoutPreHooksWarns verifies the companion to the
// InProgress check. A hooked group that a longhorn-manager predating hooks
// already drove to Ready gets a PreHooksSkipped warning and keeps its phase,
// while a restored Ready group, whose members carry the hooked marker, does
// not.
func (s *SnapshotGroupControllerSuite) TestReadyHookedGroupWithoutPreHooksWarns(c *C) {
	cases := []struct {
		name       string
		hooked     bool
		expectWarn bool
	}{
		{name: "member taken by a manager that predates hooks, no marker", hooked: false, expectWarn: true},
		{name: "restored: member carries the marker", hooked: true, expectWarn: false},
	}
	creationTime := testSnapshotGroupBaseTime.Add(10 * time.Second).UTC().Format(time.RFC3339)
	for _, tc := range cases {
		f := newSnapshotGroupControllerFixture(c)
		group := newTestHookedSnapshotGroup("vol-1")
		annotateTerminalPhase(group, longhorn.SnapshotGroupPhaseReady)
		group.Status = longhorn.SnapshotGroupStatus{
			OwnerID:    TestOwnerID1,
			Phase:      longhorn.SnapshotGroupPhaseReady,
			ReadyToUse: true,
			Members:    initializedMemberStatuses(group),
		}
		f.registerSnapshotGroup(c, group)
		if tc.hooked {
			f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTime)
		} else {
			f.registerUnhookedMemberSnapshot(c, group.Spec.Members[0], true, creationTime)
		}

		c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil, Commentf(tc.name))

		updated := f.getGroup(c)
		c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseReady, Commentf(tc.name))
		warned := false
		for _, event := range f.recordedEvents() {
			if strings.Contains(event, constant.EventReasonPreHooksSkipped) {
				warned = true
			}
		}
		c.Assert(warned, Equals, tc.expectWarn, Commentf(tc.name))
	}
}

// TestTerminalWriteKeepsExecutorHookRecord verifies that the controller's
// terminal status write does not revert a hook record the executor wrote
// after this reconcile read the group. The annotation stamp returns the
// server copy, which carries the executor's paused-pod record; the deferred
// whole-status write must carry it too, or the deletion hold would see no
// unreleased pods and let the group go while the application is paused.
func (s *SnapshotGroupControllerSuite) TestTerminalWriteKeepsExecutorHookRecord(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestHookedSnapshotGroup("vol-1")
	group.Status = longhorn.SnapshotGroupStatus{
		OwnerID: TestOwnerID1,
		Phase:   longhorn.SnapshotGroupPhaseRunningPreHooks,
		Members: initializedMemberStatuses(group),
	}
	f.registerSnapshotGroup(c, group)

	paused := newTestPausedSnapshotGroup().Status
	f.lhClient.PrependReactor("update", "snapshotgroups", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "" {
			return false, nil, nil
		}
		// The executor claimed the member and paused its pod between the
		// controller's lister read and this annotation write; the server
		// copy the write returns carries that record.
		stored := action.(k8stesting.UpdateAction).GetObject().(*longhorn.SnapshotGroup)
		stored.Status.HooksStartedAt = paused.HooksStartedAt
		stored.Status.Hooks = paused.Hooks
		return false, nil, nil
	})

	f.controller.nowHandler = func() time.Time {
		return testSnapshotGroupBaseTime.Add(time.Duration(testSnapshotGroupDeadlineSeconds+1) * time.Second)
	}
	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
	c.Assert(updated.Status.HooksStartedAt, DeepEquals, paused.HooksStartedAt)
	c.Assert(updated.Status.Hooks, DeepEquals, paused.Hooks)
	c.Assert(snapshothook.UnreleasedHookPods(updated), HasLen, 1)
}

// TestHookedGroupFailsAtDeadlineWhileInProgress verifies that a hooked group
// whose deadline passes after the pre-hooks succeeded but before every member
// snapshot was taken fails with the HookDeadlineExceeded event, the same as a
// deadline missed during the pre-hooks: both ends of the hook window report
// under one reason.
func (s *SnapshotGroupControllerSuite) TestHookedGroupFailsAtDeadlineWhileInProgress(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestPausedSnapshotGroup()
	f.registerSnapshotGroup(c, group)

	f.controller.nowHandler = func() time.Time {
		return testSnapshotGroupBaseTime.Add(time.Duration(testSnapshotGroupDeadlineSeconds+1) * time.Second)
	}
	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
	c.Assert(updated.Status.Error, Matches, "deadline exceeded before every member snapshot was taken; failed members: volume vol-1 .*")
	f.assertSnapshotCount(c, 0)
	f.assertHookDeadlineExceededEvent(c)
}

// assertHookDeadlineExceededEvent drains the recorder and asserts that the
// hook-specific deadline event was emitted alongside the terminal Failed one.
func (f *snapshotGroupControllerFixture) assertHookDeadlineExceededEvent(c *C) {
	var hookDeadline, failed bool
	for _, event := range f.recordedEvents() {
		switch {
		case strings.Contains(event, constant.EventReasonHookDeadlineExceeded):
			hookDeadline = true
		case strings.Contains(event, " "+constant.EventReasonFailed+" "):
			failed = true
		}
	}
	c.Assert(hookDeadline, Equals, true)
	c.Assert(failed, Equals, true)
}

// TestRestoreGuardCoversRunningPreHooks verifies that a hooked group crashed
// between the terminal annotation write and the status persist regains the
// recorded outcome from RunningPreHooks and starts no pause: no snapshot is
// created and the phase is terminal on the first reconcile.
func (s *SnapshotGroupControllerSuite) TestRestoreGuardCoversRunningPreHooks(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestHookedSnapshotGroup("vol-1")
	annotateTerminalPhase(group, longhorn.SnapshotGroupPhaseFailed)
	group.Status = longhorn.SnapshotGroupStatus{OwnerID: TestOwnerID1, Phase: longhorn.SnapshotGroupPhaseRunningPreHooks, Members: initializedMemberStatuses(group), PreHooksDone: true}
	f.registerSnapshotGroup(c, group)

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	c.Assert(f.getGroup(c).Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
	f.assertSnapshotCount(c, 0)
}

// TestPausedGroupReadyWhenRecordedPodsUnchanged verifies the identity gate
// passes when the live running pods match the pause record exactly: the
// group becomes Ready and the member taken under the pause is accepted.
func (s *SnapshotGroupControllerSuite) TestPausedGroupReadyWhenRecordedPodsUnchanged(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestPausedSnapshotGroup()
	f.registerSnapshotGroup(c, group)
	f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(testHookedGroupPausedAt+10*time.Second))
	f.registerRunningPod(c, testHookPodName, testHookPodUID, testHookContainerID)

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseReady)
	c.Assert(updated.Status.ReadyToUse, Equals, true)
	for _, event := range f.recordedEvents() {
		c.Assert(event, Not(Matches), ".*HookDeadlineNearlyExceeded.*")
	}
}

// TestPausedGroupFailsWhenRecordedPodChanged verifies the identity gate
// fails the group, naming the member and pod, when the live set differs from
// the pause record: a restarted container, a replaced pod, a gone pod, or a
// new pod that arrived mid-window. Every snapshot was taken, yet the group
// is never Ready.
func (s *SnapshotGroupControllerSuite) TestPausedGroupFailsWhenRecordedPodChanged(c *C) {
	cases := []struct {
		name     string
		livePods func(f *snapshotGroupControllerFixture, c *C)
		expected string
	}{
		{
			name: "container restarted",
			livePods: func(f *snapshotGroupControllerFixture, c *C) {
				f.registerRunningPod(c, testHookPodName, testHookPodUID, "containerd://c2")
			},
			expected: "mongo-0 restarted a container",
		},
		{
			name: "pod replaced",
			livePods: func(f *snapshotGroupControllerFixture, c *C) {
				f.registerRunningPod(c, testHookPodName, "pod-uid-2", testHookContainerID)
			},
			expected: "mongo-0 was replaced",
		},
		{
			name:     "pod gone",
			livePods: func(f *snapshotGroupControllerFixture, c *C) {},
			expected: "mongo-0 recorded at pause time is no longer running",
		},
		{
			name: "new pod arrived",
			livePods: func(f *snapshotGroupControllerFixture, c *C) {
				f.registerRunningPod(c, testHookPodName, testHookPodUID, testHookContainerID)
				f.registerRunningPod(c, "mongo-1", "pod-uid-9", "containerd://c9")
			},
			expected: "mongo-1 started during the hook window",
		},
		{
			name: "new terminating pod",
			livePods: func(f *snapshotGroupControllerFixture, c *C) {
				f.registerRunningPod(c, testHookPodName, testHookPodUID, testHookContainerID)
				f.registerTerminatingPod(c, "mongo-1", "pod-uid-9", "containerd://c9")
			},
			expected: "mongo-1 mounting the claim is terminating",
		},
		{
			// A Job pod that mounted the claim, wrote, and completed inside
			// the hook window is retained as Succeeded: the gate must see it
			// even though it can no longer be listed as a consumer.
			name: "finished pod retained",
			livePods: func(f *snapshotGroupControllerFixture, c *C) {
				f.registerRunningPod(c, testHookPodName, testHookPodUID, testHookContainerID)
				f.registerFinishedPod(c, "job-1", "pod-uid-7", testSnapshotGroupBaseTime.Add(10*time.Second), "containerd://c7")
			},
			expected: "job-1 mounted the claim and finished during the hook window",
		},
	}
	for _, tc := range cases {
		f := newSnapshotGroupControllerFixture(c)
		group := newTestPausedSnapshotGroup()
		f.registerSnapshotGroup(c, group)
		f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(testHookedGroupPausedAt+10*time.Second))
		tc.livePods(f, c)

		c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil, Commentf(tc.name))

		updated := f.getGroup(c)
		c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed, Commentf(tc.name))
		c.Assert(updated.Status.Error, Matches, "pod changed during the hook window on member .* \\(app/policy\\): .*"+tc.expected+".* "+regexp.QuoteMeta(snapshothook.PodChangedMarker), Commentf(tc.name))
	}
}

// TestPausedGroupReadyDespiteFinishedPodFromBeforeTheGroup verifies the
// retained-pod check scopes itself to the hook window: a Succeeded pod
// created before the group existed, such as an old completed Job, does not
// fail the gate.
func (s *SnapshotGroupControllerSuite) TestPausedGroupReadyDespiteFinishedPodFromBeforeTheGroup(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestPausedSnapshotGroup()
	f.registerSnapshotGroup(c, group)
	f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(testHookedGroupPausedAt+10*time.Second))
	f.registerRunningPod(c, testHookPodName, testHookPodUID, testHookContainerID)
	f.registerFinishedPod(c, "old-job", "pod-uid-8", testSnapshotGroupBaseTime.Add(-time.Hour), "containerd://c8")

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseReady)
	c.Assert(updated.Status.ReadyToUse, Equals, true)
}

// TestPausedGroupFailsWhenPauseRecordIsUnusable verifies the gate refuses
// to pass a member it cannot verify: a hook record without a PVC is a
// pod-changed failure even though the live pods are untouched. That is
// executor-side corruption that should not happen; the gate fails closed
// on it.
func (s *SnapshotGroupControllerSuite) TestPausedGroupFailsWhenPauseRecordIsUnusable(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestPausedSnapshotGroup()
	group.Status.Hooks[0].PVCRef = nil
	f.registerSnapshotGroup(c, group)
	f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(testHookedGroupPausedAt+10*time.Second))
	f.registerRunningPod(c, testHookPodName, testHookPodUID, testHookContainerID)

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
	c.Assert(updated.Status.Error, Matches, "pod changed during the hook window on member .* \\(app/policy\\): no pause record to verify the pods against "+regexp.QuoteMeta(snapshothook.PodChangedMarker))
}

// TestPausedGroupFailsWhenReleaseStartedBeforeSnapshots verifies the early
// exit: the executor already recorded a pod PostRunning - its deadline
// clock ran ahead of the controller's - so no member snapshot is created
// and the group fails naming the released pod, even though the controller
// still sees time left.
func (s *SnapshotGroupControllerSuite) TestPausedGroupFailsWhenReleaseStartedBeforeSnapshots(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestPausedSnapshotGroup()
	group.Status.Hooks[0].Pods[0].Phase = longhorn.HookPhasePostRunning
	f.registerSnapshotGroup(c, group)
	f.registerRunningPod(c, testHookPodName, testHookPodUID, testHookContainerID)

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
	c.Assert(updated.Status.Error, Matches, "the hook executor released pod app/mongo-0 of member .* before every member snapshot was taken.* "+regexp.QuoteMeta(snapshothook.ReleasedBeforeReadyMarker))
	f.assertSnapshotCount(c, 0)
}

// TestPausedGroupFailsWhenReleaseStartsBeforeReadyStamp verifies the live
// read at the Ready stamp: the lister still shows the pod paused and every
// snapshot is in, but the executor's PostRunning write has landed on the
// API server since the reconcile read the group. The stamp must see it and
// fail the group rather than persist Ready beside a released pod.
func (s *SnapshotGroupControllerSuite) TestPausedGroupFailsWhenReleaseStartsBeforeReadyStamp(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestPausedSnapshotGroup()
	f.registerSnapshotGroup(c, group)
	f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(testHookedGroupPausedAt+10*time.Second))
	f.registerRunningPod(c, testHookPodName, testHookPodUID, testHookContainerID)

	// The executor's write reaches the API server but not the lister.
	released := f.getGroup(c)
	released.Status.Hooks[0].Pods[0].Phase = longhorn.HookPhasePostRunning
	_, err := f.lhClient.LonghornV1beta2().SnapshotGroups(TestNamespace).UpdateStatus(context.TODO(), released, metav1.UpdateOptions{})
	c.Assert(err, IsNil)

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	updated := f.getGroup(c)
	c.Assert(updated.Status.Phase, Equals, longhorn.SnapshotGroupPhaseFailed)
	c.Assert(updated.Status.Error, Matches, ".* "+regexp.QuoteMeta(snapshothook.ReleasedBeforeReadyMarker))
	c.Assert(updated.Annotations[types.SnapshotGroupAnnotationTerminalPhase], Equals, string(longhorn.SnapshotGroupPhaseFailed))
}

// TestPausedGroupWarnsWhenDeadlineNearlyExceeded verifies that a Ready
// hooked group that used over 80% of its budget gets the warning with the
// consumption split into queue, pre-hook, and snapshot time.
func (s *SnapshotGroupControllerSuite) TestPausedGroupWarnsWhenDeadlineNearlyExceeded(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestPausedSnapshotGroup()
	f.registerSnapshotGroup(c, group)
	// Deadline 300s: queued 5s, pre-hooks 15s, snapshots up to 250s = 83%.
	f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(250*time.Second))
	f.registerRunningPod(c, testHookPodName, testHookPodUID, testHookContainerID)

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	c.Assert(f.getGroup(c).Status.Phase, Equals, longhorn.SnapshotGroupPhaseReady)
	warned := false
	for _, event := range f.recordedEvents() {
		if strings.Contains(event, "HookDeadlineNearlyExceeded") && strings.Contains(event, "used 250s of its 300s deadline (queued 5s, pre-hooks 15s, snapshots 230s)") {
			warned = true
		}
	}
	c.Assert(warned, Equals, true)
}

// TestDeletionOfPausedGroupWaitsForRelease verifies the finalizer of a
// hooked group stays while a recorded pod is still paused, proceeds once the
// executor reports it released, and never touches the member snapshot in
// between.
func (s *SnapshotGroupControllerSuite) TestDeletionOfPausedGroupWaitsForRelease(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestPausedSnapshotGroup()
	deletionTime := metav1.NewTime(testSnapshotGroupBaseTime.Add(time.Minute))
	group.DeletionTimestamp = &deletionTime
	f.registerSnapshotGroup(c, group)
	f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(30*time.Second))

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	c.Assert(f.getGroup(c).Finalizers, HasLen, 1)
	f.assertSnapshotCount(c, 1)

	// The executor releases the pod; deletion proceeds.
	released := f.getGroup(c)
	released.Status.Hooks[0].Pods[0].Phase = longhorn.HookPhasePostDone
	_, err := f.lhClient.LonghornV1beta2().SnapshotGroups(TestNamespace).UpdateStatus(context.TODO(), released, metav1.UpdateOptions{})
	c.Assert(err, IsNil)
	f.syncGroupFromClient(c)

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)
	f.assertSnapshotCount(c, 0)
}

// TestDeletionOfPausedGroupAbandonsReleaseAfterBound verifies the escape:
// past max(deadline, deletion) plus the worst case of the release attempts
// over every paused member, the finalizer stops waiting,
// HookReleaseAbandoned names the never-released pods, and the member
// snapshots are deleted. Two members: the release runs them one at a time,
// so the bound must leave room for both, or the second member's remaining
// attempts would be cut off while its pods are still paused.
func (s *SnapshotGroupControllerSuite) TestDeletionOfPausedGroupAbandonsReleaseAfterBound(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestPausedSnapshotGroup()
	group.Spec.Volumes = append(group.Spec.Volumes, "vol-2")
	group.Spec.Members = append(group.Spec.Members, longhorn.SnapshotGroupMember{
		VolumeName: "vol-2", SnapshotName: testSnapshotGroupName + "-vol-2", HookPolicyRef: testVolumeHookPolicyRef(),
	})
	group.Status.Members = initializedMemberStatuses(group)
	group.Status.Hooks = append(group.Status.Hooks, longhorn.HookStatus{
		SnapshotName: group.Spec.Members[1].SnapshotName,
		Phase:        longhorn.HookPhasePreDone,
		PVCRef:       &longhorn.PVCRef{Namespace: testHookPodNamespace, Name: "data-2"},
		Pods: []longhorn.HookPodStatus{{
			Namespace: testHookPodNamespace, Name: "mongo-1", UID: "pod-uid-2",
			ContainerIDs: []string{"containerd://c2"}, HookName: "default", Phase: longhorn.HookPhasePreDone,
		}},
	})
	deletionTime := metav1.NewTime(testSnapshotGroupBaseTime.Add(time.Minute))
	group.DeletionTimestamp = &deletionTime
	f.registerSnapshotGroup(c, group)
	f.registerMemberSnapshot(c, group.Spec.Members[0], true, creationTimeAt(30*time.Second))
	f.registerMemberSnapshot(c, group.Spec.Members[1], true, creationTimeAt(30*time.Second))

	// Defaults: 3 attempts x 2 members x 60s + 2 x 32s backoff cap past the
	// 300s deadline.
	bound := testSnapshotGroupBaseTime.Add(time.Duration(testSnapshotGroupDeadlineSeconds)*time.Second + 3*2*60*time.Second + 2*32*time.Second)
	f.controller.nowHandler = func() time.Time { return bound.Add(-time.Second) }
	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)
	c.Assert(f.getGroup(c).Finalizers, HasLen, 1)
	f.recordedEvents()

	f.controller.nowHandler = func() time.Time { return bound.Add(time.Second) }
	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	f.assertSnapshotCount(c, 0)
	abandoned := false
	for _, event := range f.recordedEvents() {
		if strings.Contains(event, "HookReleaseAbandoned") && strings.Contains(event, "app/mongo-0") && strings.Contains(event, "app/mongo-1") {
			abandoned = true
		}
	}
	c.Assert(abandoned, Equals, true)
}

// TestDeletionOfUnclaimedHookedGroupProceeds verifies a hooked group deleted
// before the executor recorded any pod waits for nothing: there is no pause
// to release.
func (s *SnapshotGroupControllerSuite) TestDeletionOfUnclaimedHookedGroupProceeds(c *C) {
	f := newSnapshotGroupControllerFixture(c)
	group := newTestHookedSnapshotGroup("vol-1")
	deletionTime := metav1.NewTime(testSnapshotGroupBaseTime.Add(time.Minute))
	group.DeletionTimestamp = &deletionTime
	group.Status = longhorn.SnapshotGroupStatus{OwnerID: TestOwnerID1, Phase: longhorn.SnapshotGroupPhaseRunningPreHooks, Members: initializedMemberStatuses(group)}
	f.registerSnapshotGroup(c, group)

	c.Assert(f.controller.reconcile(testSnapshotGroupName), IsNil)

	c.Assert(f.getGroup(c).Finalizers, HasLen, 0)
}
