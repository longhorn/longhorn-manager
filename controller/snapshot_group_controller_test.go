package controller

import (
	"context"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	testSnapshotGroupName            = "test-snapshot-group"
	testSnapshotGroupDeadlineSeconds = int64(300)
)

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

	lhClient *lhfake.Clientset

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

// registerMemberSnapshot persists a member Snapshot with the group label set,
// as the controller creates it.
func (f *snapshotGroupControllerFixture) registerMemberSnapshot(c *C, member longhorn.SnapshotGroupMember, readyToUse bool, creationTime string) {
	snapshot := &longhorn.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      member.SnapshotName,
			Namespace: TestNamespace,
			Labels: map[string]string{
				types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroup): testSnapshotGroupName,
			},
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
	c.Assert(updated.Status.Error, Matches, ".*before the group was requested.*")
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
