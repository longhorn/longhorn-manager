package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	backupLifecycleTestSnapshotName = "missing-snapshot"
	backupLifecycleTestBackupURL    = "backup://monitor"
)

type backupLifecycleTestEngineClientProxy struct {
	*engineapi.EngineSimulator
}

func (p *backupLifecycleTestEngineClientProxy) Close() {}

func (p *backupLifecycleTestEngineClientProxy) SnapshotBackupStatus(obj engineapi.DataEngineObject, backupName, replicaAddress, replicaName string) (*longhorn.EngineBackupStatus, error) {
	return &longhorn.EngineBackupStatus{
		Progress:     100,
		BackupURL:    backupLifecycleTestBackupURL,
		SnapshotName: backupLifecycleTestSnapshotName,
		State:        "complete",
	}, nil
}

var _ engineapi.EngineClientProxy = (*backupLifecycleTestEngineClientProxy)(nil)

func newBackupLifecycleTestController(t *testing.T) (*BackupController, *datastore.DataStore, *lhfake.Clientset, *longhorn.Backup) {
	t.Helper()

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	defaultEngineImageSetting := newSetting(string(types.SettingNameDefaultEngineImage), TestEngineImage)
	createdSetting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
		context.Background(), defaultEngineImageSetting, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to seed default engine image setting: %v", err)
	}
	settingIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	if err := settingIndexer.Add(createdSetting); err != nil {
		t.Fatalf("failed to seed setting indexer: %v", err)
	}

	engineImage := newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed)
	engineImage.Status.NodeDeploymentMap = map[string]bool{TestNode1: true}
	createdEngineImage, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(
		context.Background(), engineImage, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to seed engine image: %v", err)
	}
	engineImageIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
	if err := engineImageIndexer.Add(createdEngineImage); err != nil {
		t.Fatalf("failed to seed engine image indexer: %v", err)
	}

	node := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	createdNode, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(
		context.Background(), node, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to seed node: %v", err)
	}
	nodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	if err := nodeIndexer.Add(createdNode); err != nil {
		t.Fatalf("failed to seed node indexer: %v", err)
	}
	volume := newVolume(TestBackupVolumeName, 1)
	volume.Namespace = TestNamespace
	createdVolume, err := lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(
		context.Background(), volume, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to seed volume: %v", err)
	}
	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	if err := volumeIndexer.Add(createdVolume); err != nil {
		t.Fatalf("failed to seed volume indexer: %v", err)
	}

	volumeAttachment := newVolumeAttachment(TestBackupVolumeName)
	volumeAttachment.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{}
	createdVolumeAttachment, err := lhClient.LonghornV1beta2().VolumeAttachments(TestNamespace).Create(
		context.Background(), volumeAttachment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to seed volume attachment: %v", err)
	}
	volumeAttachmentIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().VolumeAttachments().Informer().GetIndexer()
	if err := volumeAttachmentIndexer.Add(createdVolumeAttachment); err != nil {
		t.Fatalf("failed to seed volume attachment indexer: %v", err)
	}

	cleanupSnapshotSetting := newSetting(string(types.SettingNameAutoCleanupSnapshotAfterOnDemandBackupCompleted), "false")
	createdCleanupSnapshotSetting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
		context.Background(), cleanupSnapshotSetting, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to seed snapshot cleanup setting: %v", err)
	}
	if err := settingIndexer.Add(createdCleanupSnapshotSetting); err != nil {
		t.Fatalf("failed to seed snapshot cleanup setting indexer: %v", err)
	}

	backupTarget := &longhorn.BackupTarget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestBackupTargetName,
			Namespace: TestNamespace,
		},
		Spec: longhorn.BackupTargetSpec{
			BackupTargetURL: TestBackupTarget,
		},
	}
	createdBackupTarget, err := lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Create(
		context.Background(), backupTarget, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to seed backup target: %v", err)
	}
	backupTargetIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackupTargets().Informer().GetIndexer()
	if err := backupTargetIndexer.Add(createdBackupTarget); err != nil {
		t.Fatalf("failed to seed backup target indexer: %v", err)
	}

	backup := &longhorn.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "backup-monitor-lifecycle",
			Namespace: TestNamespace,
			Labels: map[string]string{
				types.LonghornLabelBackupTarget: TestBackupTargetName,
				types.LonghornLabelBackupVolume: TestBackupVolumeName,
			},
		},
		Spec: longhorn.BackupSpec{
			SnapshotName: backupLifecycleTestSnapshotName,
		},
		Status: longhorn.BackupStatus{
			OwnerID: TestNode1,
			State:   longhorn.BackupStateInProgress,
		},
	}
	created, err := lhClient.LonghornV1beta2().Backups(TestNamespace).Create(
		context.Background(), backup, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to seed backup tracker: %v", err)
	}

	backupIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Backups().Informer().GetIndexer()
	if err := backupIndexer.Add(created); err != nil {
		t.Fatalf("failed to seed backup indexer: %v", err)
	}

	bc := &BackupController{
		baseController: newBaseController("longhorn-backup-test", logrus.New()),
		namespace:      TestNamespace,
		controllerID:   TestNode1,
		monitors:       map[string]*engineapi.BackupMonitor{},
		ds:             ds,
		eventRecorder:  record.NewFakeRecorder(10),
	}
	t.Cleanup(bc.queue.ShutDown)
	return bc, ds, lhClient, created
}

func newBackupLifecycleTestMonitor(t *testing.T, bc *BackupController, ds *datastore.DataStore,
	backup *longhorn.Backup) *engineapi.BackupMonitor {
	t.Helper()

	proxy := &backupLifecycleTestEngineClientProxy{EngineSimulator: &engineapi.EngineSimulator{}}
	monitor, err := engineapi.NewBackupMonitor(
		logrus.New(), ds, backup, nil, nil, "", "", 0, "", nil, proxy, func(string) {})
	if err != nil {
		t.Fatalf("failed to construct backup monitor: %v", err)
	}
	t.Cleanup(monitor.Close)
	bc.monitorLock.Lock()
	bc.monitors[backup.Name] = monitor
	bc.monitorLock.Unlock()

	if err := wait.PollUntilContextTimeout(context.Background(), 20*time.Millisecond, 5*time.Second, true,
		func(context.Context) (bool, error) {
			status := monitor.GetBackupStatus()
			return status.State == longhorn.BackupStateCompleted || status.State == longhorn.BackupStateError, nil
		}); err != nil {
		t.Fatalf("backup monitor did not reach a terminal status: %v", err)
	}
	monitorStatus := monitor.GetBackupStatus()
	if got, want := monitorStatus.State, longhorn.BackupStateCompleted; got != want {
		t.Fatalf("monitor status state = %v, want %v", got, want)
	}
	if got, want := monitorStatus.SnapshotName, backupLifecycleTestSnapshotName; got != want {
		t.Fatalf("monitor status snapshot name = %q, want %q", got, want)
	}
	if got, want := monitorStatus.URL, backupLifecycleTestBackupURL; got != want {
		t.Fatalf("monitor status URL = %q, want %q", got, want)
	}

	return monitor
}

func TestBackupReconcileRetainsMonitorOnStatusError(t *testing.T) {
	bc, ds, lhClient, seededBackup := newBackupLifecycleTestController(t)
	monitor := newBackupLifecycleTestMonitor(t, bc, ds, seededBackup)

	injectedErr := fmt.Errorf("injected status update error")
	var updateActions []k8stesting.Action
	lhClient.PrependReactor("update", "backups", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updateActions = append(updateActions, action)
		return true, nil, injectedErr
	})

	err := bc.reconcile(seededBackup.Name)
	if !errors.Is(err, injectedErr) {
		t.Fatalf("reconcile error = %v, want %v", err, injectedErr)
	}
	if got := bc.hasMonitor(seededBackup.Name); got != monitor {
		t.Fatalf("status error replaced monitor: got %p, want %p", got, monitor)
	}
	if got, want := len(updateActions), 1; got != want {
		t.Fatalf("backup status update call count = %d, want %d", got, want)
	}
	if got, want := updateActions[0].GetSubresource(), "status"; got != want {
		t.Fatalf("backup status update subresource = %q, want %q", got, want)
	}
}

func TestBackupReconcileRetainsMonitorUntilStatusPersists(t *testing.T) {
	bc, ds, lhClient, seededBackup := newBackupLifecycleTestController(t)
	backup := seededBackup.DeepCopy()
	monitor := newBackupLifecycleTestMonitor(t, bc, ds, backup)

	conflictErr := apierrors.NewConflict(
		schema.GroupResource{Group: "longhorn.io", Resource: "backups"}, backup.Name, fmt.Errorf("status conflict"))
	var updateActions []k8stesting.Action
	lhClient.PrependReactor("update", "backups", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updateActions = append(updateActions, action)
		if len(updateActions) == 1 {
			return true, nil, conflictErr
		}
		return false, nil, nil
	})

	if err := bc.reconcile(backup.Name); err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	if got := bc.hasMonitor(backup.Name); got != monitor {
		t.Fatalf("conflict replaced monitor: got %p, want %p", got, monitor)
	}
	if got, want := bc.queue.Len(), 1; got != want {
		t.Fatalf("conflict queue length = %d, want %d", got, want)
	}
	queuedKey, quit := bc.queue.Get()
	if quit {
		t.Fatal("backup queue shut down unexpectedly")
	}
	if got, want := queuedKey, TestNamespace+"/"+backup.Name; got != want {
		t.Fatalf("queued key = %v, want %v", got, want)
	}
	bc.queue.Done(queuedKey)

	if err := bc.reconcile(backup.Name); err != nil {
		t.Fatalf("retry reconcile failed: %v", err)
	}
	if got := bc.hasMonitor(backup.Name); got != nil {
		t.Fatalf("successful reconcile retained monitor %p", got)
	}
	storedBackup, err := lhClient.LonghornV1beta2().Backups(TestNamespace).Get(
		context.Background(), backup.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get persisted backup: %v", err)
	}
	if got, want := storedBackup.Status.State, longhorn.BackupStateCompleted; got != want {
		t.Fatalf("persisted backup state = %v, want %v", got, want)
	}
	if got, want := storedBackup.Status.SnapshotName, backupLifecycleTestSnapshotName; got != want {
		t.Fatalf("persisted backup snapshot name = %q, want %q", got, want)
	}
	if got, want := storedBackup.Status.URL, backupLifecycleTestBackupURL; got != want {
		t.Fatalf("persisted backup URL = %q, want %q", got, want)
	}
	if got, want := len(updateActions), 2; got != want {
		t.Fatalf("backup status update call count = %d, want %d", got, want)
	}
	for i, action := range updateActions {
		if got, want := action.GetSubresource(), "status"; got != want {
			t.Errorf("backup status update action %d subresource = %q, want %q", i+1, got, want)
		}
	}

}

func TestBackupReconcileRemovesStaleMonitorForPersistedFinalStatus(t *testing.T) {
	bc, ds, lhClient, seededBackup := newBackupLifecycleTestController(t)
	terminalBackup := seededBackup.DeepCopy()
	terminalBackup.Status.State = longhorn.BackupStateCompleted
	terminalBackup.Status.LastSyncedAt = metav1.Now()
	terminalBackup.Status.Messages = map[string]string{}
	persistedBackup, err := lhClient.LonghornV1beta2().Backups(TestNamespace).UpdateStatus(
		context.Background(), terminalBackup, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("failed to seed terminal backup status: %v", err)
	}
	if err := bc.ds.BackupInformer.GetStore().Update(persistedBackup); err != nil {
		t.Fatalf("failed to update backup indexer: %v", err)
	}

	proxy := &backupLifecycleTestEngineClientProxy{EngineSimulator: &engineapi.EngineSimulator{}}
	monitor, err := engineapi.NewBackupMonitor(
		logrus.New(), ds, terminalBackup, nil, nil, "", "", 0, "", nil, proxy, func(string) {})
	if err != nil {
		t.Fatalf("failed to construct backup monitor: %v", err)
	}
	t.Cleanup(monitor.Close)
	bc.monitorLock.Lock()
	bc.monitors[terminalBackup.Name] = monitor
	bc.monitorLock.Unlock()

	if err := bc.reconcile(terminalBackup.Name); err != nil {
		t.Fatalf("idempotent reconcile failed: %v", err)
	}
	if got := bc.hasMonitor(terminalBackup.Name); got != nil {
		t.Fatalf("idempotent reconcile retained stale monitor %p", got)
	}
	storedBackup, err := lhClient.LonghornV1beta2().Backups(TestNamespace).Get(
		context.Background(), terminalBackup.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get persisted terminal backup: %v", err)
	}
	if got, want := storedBackup.Status.State, longhorn.BackupStateCompleted; got != want {
		t.Fatalf("persisted terminal backup state = %v, want %v", got, want)
	}

}
