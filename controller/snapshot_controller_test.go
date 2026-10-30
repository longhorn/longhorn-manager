package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/hookexecutor/snapshothook"
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
	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

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

// hookPolicyBypassFixture holds a datastore with one PVC for each
// policy-resolution outcome, for the hook policy bypass warning tests.
type hookPolicyBypassFixture struct {
	ds            *datastore.DataStore
	volumeIndexer cache.Indexer
	recorder      *record.FakeRecorder
	sc            *SnapshotController
	bc            *BackupController
}

func newHookPolicyBypassFixture(t *testing.T) *hookPolicyBypassFixture {
	datastore.SkipListerCheck = true

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	pvcIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer()
	for _, pvc := range []*corev1.PersistentVolumeClaim{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "mongo-data", Labels: map[string]string{"app": "mongo"}}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "cache-data", Labels: map[string]string{"app": "cache"}}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "plain-data"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "shared-data", Labels: map[string]string{"app": "mongo", "tier": "shared"}}},
	} {
		require.NoError(t, pvcIndexer.Add(pvc))
	}

	policyIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().VolumeHookPolicies().Informer().GetIndexer()
	for _, policy := range []*longhorn.VolumeHookPolicy{
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "mongo"},
			Spec: longhorn.VolumeHookPolicySpec{
				PVCSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "mongo"}},
				Hooks:       []longhorn.VolumeHookPolicyHook{{}},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "cache"},
			Spec: longhorn.VolumeHookPolicySpec{
				PVCSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "cache"}},
				Hooks:       []longhorn.VolumeHookPolicyHook{{Operations: []longhorn.HookOperation{longhorn.HookOperationBackup}}},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "shared"},
			Spec: longhorn.VolumeHookPolicySpec{
				PVCSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "shared"}},
				Hooks:       []longhorn.VolumeHookPolicyHook{{}},
			},
		},
	} {
		require.NoError(t, policyIndexer.Add(policy))
	}

	recorder := record.NewFakeRecorder(10)
	sc, err := NewSnapshotController(logrus.StandardLogger(), ds, scheme.Scheme, kubeClient, TestNamespace, TestOwnerID1, nil, util.NewAtomicCounter(), NewSnapshotConcurrentLimiter())
	require.NoError(t, err)
	sc.eventRecorder = recorder
	bc, err := NewBackupController(logrus.StandardLogger(), ds, scheme.Scheme, kubeClient, TestOwnerID1, TestNamespace, util.NewAtomicCounter())
	require.NoError(t, err)
	bc.eventRecorder = recorder

	return &hookPolicyBypassFixture{
		ds:            ds,
		volumeIndexer: informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer(),
		recorder:      recorder,
		sc:            sc,
		bc:            bc,
	}
}

// volumeBoundTo returns a volume whose PVC is namespace app / pvcName.
func volumeBoundTo(pvcName string) *longhorn.Volume {
	return &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "vol-" + pvcName, Namespace: TestNamespace},
		Status: longhorn.VolumeStatus{KubernetesStatus: longhorn.KubernetesStatus{
			Namespace: "app", PVCName: pvcName,
		}},
	}
}

// events drains the recorder and returns what was recorded.
func (f *hookPolicyBypassFixture) events() []string {
	var events []string
	for {
		select {
		case event := <-f.recorder.Events:
			events = append(events, event)
		default:
			return events
		}
	}
}

// bypassWarning resolves the volume hook policy and reports the bypass
// warning, as the controllers do.
func bypassWarning(ds *datastore.DataStore, volume *longhorn.Volume, operation longhorn.HookOperation) (string, error) {
	policy, resolveErr := ds.ResolveVolumeHookPolicyForVolume(volume)
	return snapshothook.BypassWarning(policy, resolveErr, volume.Name, operation)
}

func TestHookPolicyBypassWarning(t *testing.T) {
	f := newHookPolicyBypassFixture(t)

	t.Run("policy covering the operation names the policy and operation", func(t *testing.T) {
		message, err := bypassWarning(f.ds, volumeBoundTo("mongo-data"), longhorn.HookOperationSnapshot)
		require.NoError(t, err)
		assert.Contains(t, message, "app/mongo")
		assert.Contains(t, message, "this snapshot was taken outside a SnapshotGroup")
	})

	t.Run("policy whose hooks skip the operation is silent", func(t *testing.T) {
		message, err := bypassWarning(f.ds, volumeBoundTo("cache-data"), longhorn.HookOperationSnapshot)
		require.NoError(t, err)
		assert.Empty(t, message)

		message, err = bypassWarning(f.ds, volumeBoundTo("cache-data"), longhorn.HookOperationBackup)
		require.NoError(t, err)
		assert.Contains(t, message, "app/cache")
	})

	t.Run("no matching policy is silent", func(t *testing.T) {
		message, err := bypassWarning(f.ds, volumeBoundTo("plain-data"), longhorn.HookOperationSnapshot)
		require.NoError(t, err)
		assert.Empty(t, message)
	})

	t.Run("several matching policies warn, naming every policy", func(t *testing.T) {
		message, err := bypassWarning(f.ds, volumeBoundTo("shared-data"), longhorn.HookOperationSnapshot)
		require.NoError(t, err)
		assert.Contains(t, message, "app/mongo, app/shared")
		assert.Contains(t, message, "this snapshot was taken outside a SnapshotGroup")
		assert.Contains(t, message, "exactly one")
	})

	t.Run("volume without a PVC is silent", func(t *testing.T) {
		message, err := bypassWarning(f.ds, &longhorn.Volume{ObjectMeta: metav1.ObjectMeta{Name: "detached"}}, longhorn.HookOperationSnapshot)
		require.NoError(t, err)
		assert.Empty(t, message)
	})
}

func TestSnapshotControllerWarnIfHookPolicyBypassed(t *testing.T) {
	f := newHookPolicyBypassFixture(t)
	volume := volumeBoundTo("mongo-data")
	require.NoError(t, f.volumeIndexer.Add(volume))

	snapshot := &longhorn.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snap", Namespace: TestNamespace},
		Spec:       longhorn.SnapshotSpec{Volume: volume.Name, CreateSnapshot: true},
	}

	t.Run("per-volume snapshot on a covered volume warns once", func(t *testing.T) {
		f.sc.warnIfHookPolicyBypassed(snapshot)
		events := f.events()
		require.Len(t, events, 1)
		assert.Contains(t, events[0], corev1.EventTypeWarning+" "+constant.EventReasonHookPolicyBypassed)
		assert.Contains(t, events[0], "app/mongo")
	})

	t.Run("snapshot group member is silent", func(t *testing.T) {
		member := snapshot.DeepCopy()
		member.Labels = map[string]string{types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroup): "group"}
		f.sc.warnIfHookPolicyBypassed(member)
		assert.Empty(t, f.events())
	})

	t.Run("volume missing from the cache is silent", func(t *testing.T) {
		orphan := snapshot.DeepCopy()
		orphan.Spec.Volume = "gone"
		f.sc.warnIfHookPolicyBypassed(orphan)
		assert.Empty(t, f.events())
	})
}
