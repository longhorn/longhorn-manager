package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func TestShouldUpdateObject(t *testing.T) {
	var reconcileErr1, reconcileErr2 error
	reconcileErr1 = reconcileError{
		error:              fmt.Errorf("test error message"),
		shouldUpdateObject: true,
	}
	reconcileErr2 = reconcileError{
		error:              fmt.Errorf("test error message"),
		shouldUpdateObject: false,
	}

	if !shouldUpdateObject(reconcileErr1) {
		t.Fatal("reconcileErr1 must be updatable error")
	}
	if shouldUpdateObject(reconcileErr2) {
		t.Fatal("reconcileErr1 must be non-updatable error")
	}

	err := fmt.Errorf("test error message")
	if shouldUpdateObject(err) {
		t.Fatal("reconcileErr1 must be non-updatable error")
	}
}

// snapshotControllerFixture bundles everything needed to drive a single
// snapshot-controller sync inside a unit test.
type snapshotControllerFixture struct {
	sc              *SnapshotController
	lhClient        *lhfake.Clientset
	snapshotIndexer cache.Indexer
	volumeIndexer   cache.Indexer
	engineIndexer   cache.Indexer
	vaIndexer       cache.Indexer
}

func newSnapshotControllerFixture(c *C) *snapshotControllerFixture {
	datastore.SkipListerCheck = true

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	logger := logrus.StandardLogger()
	sc, err := NewSnapshotController(
		logger, ds, scheme.Scheme, kubeClient,
		TestNamespace, TestOwnerID1, nil,
		util.NewAtomicCounter(), NewSnapshotConcurrentLimiter(),
	)
	c.Assert(err, IsNil)

	sc.nowHandler = getTestNow
	for i := range sc.cacheSyncs {
		sc.cacheSyncs[i] = alwaysReady
	}

	lhFactory := informerFactories.LhInformerFactory.Longhorn().V1beta2()
	return &snapshotControllerFixture{
		sc:              sc,
		lhClient:        lhClient,
		snapshotIndexer: lhFactory.Snapshots().Informer().GetIndexer(),
		volumeIndexer:   lhFactory.Volumes().Informer().GetIndexer(),
		engineIndexer:   lhFactory.Engines().Informer().GetIndexer(),
		vaIndexer:       lhFactory.VolumeAttachments().Informer().GetIndexer(),
	}
}

// seedVolume creates a volume owned by TestOwnerID1 (the controller node) in
// both the fake clientset and the lister indexer.
func (f *snapshotControllerFixture) seedVolume(c *C) *longhorn.Volume {
	vol := newVolume(TestVolumeName, 1)
	vol.Status.OwnerID = TestOwnerID1

	created, err := f.lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(
		context.TODO(), vol, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(f.volumeIndexer.Add(created), IsNil)
	return created
}

// seedEngine creates an engine for vol in both the fake clientset and the
// lister indexer.  Status.CurrentImage is left empty to make isEngineUpgrading
// return true (Spec.Image = TestEngineImage != ""), which drives the
// engine-upgrading branch without needing a real engine process.
func (f *snapshotControllerFixture) seedEngine(c *C, vol *longhorn.Volume) *longhorn.Engine {
	engine := newEngineForVolume(vol)

	created, err := f.lhClient.LonghornV1beta2().Engines(TestNamespace).Create(
		context.TODO(), engine, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(f.engineIndexer.Add(created), IsNil)
	return created
}

// seedSnapshotForDeletion creates a snapshot CR that looks like the bug
// scenario: DeletionTimestamp set, Spec.CreateSnapshot=true, and
// Status.RequestedTime controlled by the caller.
func (f *snapshotControllerFixture) seedSnapshotForDeletion(c *C, requestedTime string) *longhorn.Snapshot {
	now := metav1.Now()
	snap := newSnapshot("test-snap-12489")
	snap.Spec.Volume = TestVolumeName
	snap.Spec.CreateSnapshot = true
	snap.Finalizers = []string{longhorn.SchemeGroupVersion.Group}
	snap.DeletionTimestamp = &now
	snap.Status.RequestedTime = requestedTime

	created, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Create(
		context.TODO(), snap, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(f.snapshotIndexer.Add(created), IsNil)
	return created
}

// seedVAWithTicket creates a VolumeAttachment that already has the snapshot's
// attachment ticket, simulating the leaked-ticket scenario.
func (f *snapshotControllerFixture) seedVAWithTicket(c *C, snapName string) (*longhorn.VolumeAttachment, string) {
	ticketID := longhorn.GetAttachmentTicketID(longhorn.AttacherTypeSnapshotController, snapName)
	vaName := types.GetLHVolumeAttachmentNameFromVolumeName(TestVolumeName)

	va := &longhorn.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vaName,
			Namespace: TestNamespace,
		},
		Spec: longhorn.VolumeAttachmentSpec{
			Volume: TestVolumeName,
			AttachmentTickets: map[string]*longhorn.AttachmentTicket{
				ticketID: {
					ID:         ticketID,
					Type:       longhorn.AttacherTypeSnapshotController,
					NodeID:     TestOwnerID1,
					Parameters: map[string]string{},
				},
			},
		},
	}

	created, err := f.lhClient.LonghornV1beta2().VolumeAttachments(TestNamespace).Create(
		context.TODO(), va, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(f.vaIndexer.Add(created), IsNil)
	return created, ticketID
}

// TestSnapshotDeletionShortcut_FiresWhenNoRequestedTime verifies the core fix:
// a snapshot with CreateSnapshot=true and an empty RequestedTime (never
// reached the engine) bypasses the normal deletion path, removes the
// attachment ticket and strips its own finalizer immediately.
func (s *TestSuite) TestSnapshotDeletionShortcut_FiresWhenNoRequestedTime(c *C) {
	f := newSnapshotControllerFixture(c)

	vol := f.seedVolume(c)
	f.seedEngine(c, vol)
	snap := f.seedSnapshotForDeletion(c, "" /* RequestedTime empty */)
	va, ticketID := f.seedVAWithTicket(c, snap.Name)
	vaName := va.Name

	c.Assert(f.sc.syncHandler(getKey(snap, c)), IsNil)

	// Finalizer must be removed so Kubernetes can garbage-collect the CR.
	retSnap, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(
		context.TODO(), snap.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(util.FinalizerExists(longhorn.SchemeGroupVersion.Group, retSnap), Equals, false)

	// Attachment ticket must be gone so the volume stops auto-attaching.
	retVA, err := f.lhClient.LonghornV1beta2().VolumeAttachments(TestNamespace).Get(
		context.TODO(), vaName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	_, hasTicket := retVA.Spec.AttachmentTickets[ticketID]
	c.Assert(hasTicket, Equals, false)
}

// TestSnapshotDeletionShortcut_SkippedWhenRequestedTimeSet verifies that a
// snapshot whose RequestedTime is already populated (the create RPC was
// issued) is NOT fast-deleted: the normal flow is used instead, and the
// finalizer is left in place.
func (s *TestSuite) TestSnapshotDeletionShortcut_SkippedWhenRequestedTimeSet(c *C) {
	f := newSnapshotControllerFixture(c)

	vol := f.seedVolume(c)
	f.seedEngine(c, vol)
	// RequestedTime is set, so the snapshot previously reached the engine.
	snap := f.seedSnapshotForDeletion(c, TestTimeNow)
	_, ticketID := f.seedVAWithTicket(c, snap.Name)

	// The engine has Status.CurrentImage="" vs Spec.Image=TestEngineImage, so
	// isEngineUpgrading=true.  The controller re-enqueues and returns nil.
	c.Assert(f.sc.syncHandler(getKey(snap, c)), IsNil)

	// Finalizer must still be present: the shortcut did not fire.
	retSnap, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(
		context.TODO(), snap.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(util.FinalizerExists(longhorn.SchemeGroupVersion.Group, retSnap), Equals, true)

	// Ticket must still be present.
	vaName := types.GetLHVolumeAttachmentNameFromVolumeName(TestVolumeName)
	retVA, err := f.lhClient.LonghornV1beta2().VolumeAttachments(TestNamespace).Get(
		context.TODO(), vaName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	_, hasTicket := retVA.Spec.AttachmentTickets[ticketID]
	c.Assert(hasTicket, Equals, true)
}

// TestSnapshotDeletionShortcut_SkippedWhenCreateSnapshotFalse verifies that
// the shortcut only fires when Spec.CreateSnapshot=true.  A snapshot whose
// CreateSnapshot=false (e.g. one synced from an existing engine snapshot)
// must go through the normal deletion path.
func (s *TestSuite) TestSnapshotDeletionShortcut_SkippedWhenCreateSnapshotFalse(c *C) {
	f := newSnapshotControllerFixture(c)

	vol := f.seedVolume(c)
	f.seedEngine(c, vol)

	// Snapshot with CreateSnapshot=false and empty RequestedTime.
	now := metav1.Now()
	snap := newSnapshot("test-snap-existing")
	snap.Spec.Volume = TestVolumeName
	snap.Spec.CreateSnapshot = false // <-- key difference
	snap.Finalizers = []string{longhorn.SchemeGroupVersion.Group}
	snap.DeletionTimestamp = &now
	// Status.RequestedTime intentionally left empty

	created, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Create(
		context.TODO(), snap, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(f.snapshotIndexer.Add(created), IsNil)

	_, ticketID := f.seedVAWithTicket(c, snap.Name)

	c.Assert(f.sc.syncHandler(getKey(created, c)), IsNil)

	// Finalizer still present: shortcut did not fire.
	retSnap, err := f.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(
		context.TODO(), snap.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(util.FinalizerExists(longhorn.SchemeGroupVersion.Group, retSnap), Equals, true)

	// Ticket still present.
	vaName := types.GetLHVolumeAttachmentNameFromVolumeName(TestVolumeName)
	retVA, err := f.lhClient.LonghornV1beta2().VolumeAttachments(TestNamespace).Get(
		context.TODO(), vaName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	_, hasTicket := retVA.Spec.AttachmentTickets[ticketID]
	c.Assert(hasTicket, Equals, true)
}

// TestSyncSnapshotWithSnapshotInfo_BackfillsRequestedTime verifies that
// syncSnapshotWithSnapshotInfo sets RequestedTime from snapInfo.Created when
// the snapshot has no RequestedTime yet.  This backfill path is only reached
// when the engine has already detected the snapshot in its Status.Snapshots,
// which means the snapshot was not freshly created by handleSnapshotCreate
// (that path sets RequestedTime before calling the RPC).  Consequently the
// snapshot is not user-created; UserCreated=false reflects engine-detected
// system/auto snapshots.
func (s *TestSuite) TestSyncSnapshotWithSnapshotInfo_BackfillsRequestedTime(c *C) {
	snap := newSnapshot("existing-snap")
	// RequestedTime is empty: engine detected a snapshot the controller has
	// not tracked yet (e.g. pre-v1.13.0 upgrade, or auto-snapshot).
	c.Assert(snap.Status.RequestedTime, Equals, "")

	snapInfo := &longhorn.SnapshotInfo{
		Name:        "existing-snap",
		Created:     TestTimeNow,
		Size:        "0",
		Children:    map[string]bool{},
		Labels:      map[string]string{},
		UserCreated: false, // engine-detected snapshot, not user-API-initiated
	}

	err := syncSnapshotWithSnapshotInfo(logrus.StandardLogger(), snap, snapInfo, 0)
	c.Assert(err, IsNil)
	c.Assert(snap.Status.RequestedTime, Equals, TestTimeNow)
}

// TestSyncSnapshotWithSnapshotInfo_PreservesExistingRequestedTime verifies
// that syncSnapshotWithSnapshotInfo does not overwrite a RequestedTime that
// is already set (i.e. the snapshot was previously tracked by the controller).
func (s *TestSuite) TestSyncSnapshotWithSnapshotInfo_PreservesExistingRequestedTime(c *C) {
	snap := newSnapshot("existing-snap")
	snap.Status.RequestedTime = "2010-01-01T00:00:00Z"

	snapInfo := &longhorn.SnapshotInfo{
		Name:        "existing-snap",
		Created:     TestTimeNow, // different timestamp — must not win
		Size:        "0",
		Children:    map[string]bool{},
		Labels:      map[string]string{},
		UserCreated: false,
	}

	err := syncSnapshotWithSnapshotInfo(logrus.StandardLogger(), snap, snapInfo, 0)
	c.Assert(err, IsNil)
	// Must not be overwritten.
	c.Assert(snap.Status.RequestedTime, Equals, "2010-01-01T00:00:00Z")
}
