package controller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clienttesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func newTestBackupController(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, controllerID string) (*BackupController, error) {
	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	logger := logrus.StandardLogger()
	proxyConnCounter := util.NewAtomicCounter()
	bc, err := NewBackupController(logger, ds, scheme.Scheme, kubeClient, controllerID, TestNamespace, proxyConnCounter)
	if err != nil {
		return nil, err
	}
	return bc, nil
}

// TestBackupControllerGetBackupTargetName verifies the backup target name
// resolution order: the backup-target label, then the synced
// status.backupTargetName, then the snapshot/volume lookup. Backups pulled
// from the backup target have an empty spec.snapshotName by design, so the
// snapshot lookup must not be attempted with an empty name; the resulting
// "snapshot \"\" not found" error used to wedge the backup controller in a
// permanent reconcile loop that also blocked Backup CR deletion on the
// longhorn.io finalizer.
func TestBackupControllerGetBackupTargetName(t *testing.T) {
	const (
		testBackupName = "test-backup"
		testSnapshot   = "test-snapshot"
		testVolumeName = "test-volume"
		labelTarget    = "label-target"
		statusTarget   = "status-target"
		snapshotTarget = "snapshot-target"
	)

	testCases := []struct {
		name          string
		backup        *longhorn.Backup
		withSnapshot  bool
		expectedName  string
		expectError   bool
		errorContains string
	}{
		{
			name: "label wins when present",
			backup: &longhorn.Backup{
				ObjectMeta: metav1.ObjectMeta{
					Name:   testBackupName,
					Labels: map[string]string{types.LonghornLabelBackupTarget: labelTarget},
				},
				Status: longhorn.BackupStatus{BackupTargetName: statusTarget},
			},
			withSnapshot: true,
			expectedName: labelTarget,
		},
		{
			name: "empty label value falls back to the synced status backup target",
			backup: &longhorn.Backup{
				ObjectMeta: metav1.ObjectMeta{
					Name:   testBackupName,
					Labels: map[string]string{types.LonghornLabelBackupTarget: ""},
				},
				Status: longhorn.BackupStatus{BackupTargetName: statusTarget},
			},
			expectedName: statusTarget,
		},
		{
			name: "missing label falls back to the synced status backup target",
			backup: &longhorn.Backup{
				ObjectMeta: metav1.ObjectMeta{Name: testBackupName},
				Status:     longhorn.BackupStatus{BackupTargetName: statusTarget},
			},
			expectedName: statusTarget,
		},
		{
			name: "missing label and status resolve through the snapshot",
			backup: &longhorn.Backup{
				ObjectMeta: metav1.ObjectMeta{Name: testBackupName},
				Spec:       longhorn.BackupSpec{SnapshotName: testSnapshot},
			},
			withSnapshot: true,
			expectedName: snapshotTarget,
		},
		{
			name: "pulled backup without label errors clearly instead of looking up an empty snapshot name",
			backup: &longhorn.Backup{
				ObjectMeta: metav1.ObjectMeta{Name: testBackupName},
			},
			expectError:   true,
			errorContains: "cannot resolve the backup target",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
			lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
			extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
			informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)

			snapshotIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Snapshots().Informer().GetIndexer()
			volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()

			bc, err := newTestBackupController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
			if err != nil {
				t.Fatalf("failed to create the test backup controller: %v", err)
			}

			if tc.withSnapshot {
				snapshot := newSnapshot(testSnapshot)
				snapshot.Spec.Volume = testVolumeName
				if _, err = lhClient.LonghornV1beta2().Snapshots(TestNamespace).Create(context.TODO(), snapshot, metav1.CreateOptions{}); err != nil {
					t.Fatalf("failed to create the snapshot: %v", err)
				}
				if err = snapshotIndexer.Add(snapshot); err != nil {
					t.Fatalf("failed to add the snapshot to the indexer: %v", err)
				}

				volume := newVolume(testVolumeName, 1)
				volume.Namespace = TestNamespace
				volume.Spec.BackupTargetName = snapshotTarget
				if _, err = lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(context.TODO(), volume, metav1.CreateOptions{}); err != nil {
					t.Fatalf("failed to create the volume: %v", err)
				}
				if err = volumeIndexer.Add(volume); err != nil {
					t.Fatalf("failed to add the volume to the indexer: %v", err)
				}
			}

			name, err := bc.getBackupTargetName(tc.backup)
			if tc.expectError {
				if err == nil {
					t.Fatalf("expected an error, got name %v", name)
				}
				if !strings.Contains(err.Error(), tc.errorContains) {
					t.Fatalf("expected the error to contain %q, got %q", tc.errorContains, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if name != tc.expectedName {
				t.Fatalf("expected backup target name %q, got %q", tc.expectedName, name)
			}
		})
	}
}

// backupStatusProxy reports the backup as completed and counts Close calls. Other
// EngineClientProxy methods panic on the nil embedded interface.
type backupStatusProxy struct {
	engineapi.EngineClientProxy

	lock   sync.Mutex
	status *longhorn.EngineBackupStatus
	closed int
}

func (p *backupStatusProxy) SnapshotBackupStatus(engineapi.DataEngineObject, string, string, string) (*longhorn.EngineBackupStatus, error) {
	return p.status, nil
}

func (p *backupStatusProxy) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.closed++
}

func (p *backupStatusProxy) closeCount() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.closed
}

// TestBackupControllerKeepsMonitorUntilFinalStatePersisted covers longhorn/longhorn#14226.
// When the Completed status write loses a conflict, the controller must keep the monitor that
// holds the result. If it closed the monitor first, the next reconcile would build a new one
// for the still in-progress Backup, and the recurring job could see a blank status. The monitor
// must be closed once the final state is persisted.
func TestBackupControllerKeepsMonitorUntilFinalStatePersisted(t *testing.T) {
	const (
		testBackupName   = "test-backup"
		testSnapshotName = "test-snapshot"
	)

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)
	lhInformers := informerFactories.LhInformerFactory.Longhorn().V1beta2()

	bc, err := newTestBackupController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	if err != nil {
		t.Fatalf("failed to create the test backup controller: %v", err)
	}

	engineImage := newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed)
	engineImage.Status.NodeDeploymentMap[TestNode1] = true

	volume := newVolume(TestVolumeName, 1)
	volume.Namespace = TestNamespace
	volume.Status.OwnerID = TestNode1

	volumeAttachment := &longhorn.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      types.GetLHVolumeAttachmentNameFromVolumeName(TestVolumeName),
			Namespace: TestNamespace,
		},
		Spec: longhorn.VolumeAttachmentSpec{
			Volume:            TestVolumeName,
			AttachmentTickets: map[string]*longhorn.AttachmentTicket{},
		},
	}

	backupTarget := &longhorn.BackupTarget{
		ObjectMeta: metav1.ObjectMeta{Name: TestBackupTargetName, Namespace: TestNamespace},
		Spec:       longhorn.BackupTargetSpec{BackupTargetURL: TestBackupTarget},
	}

	backup := newBackup(testBackupName)
	backup.Labels = map[string]string{
		types.LonghornLabelBackupVolume: TestVolumeName,
		types.LonghornLabelBackupTarget: TestBackupTargetName,
	}
	backup.Spec.SnapshotName = testSnapshotName
	backup.Status = longhorn.BackupStatus{
		OwnerID:           TestNode1,
		State:             longhorn.BackupStateInProgress,
		Progress:          90,
		SnapshotName:      testSnapshotName,
		SnapshotCreatedAt: "2026-10-22T00:00:00Z",
		VolumeSize:        "1073741824",
		Messages:          map[string]string{},
	}

	add := func(obj runtime.Object, indexer cache.Indexer, create func() error) {
		if err := create(); err != nil {
			t.Fatalf("failed to create %T: %v", obj, err)
		}
		if err := indexer.Add(obj); err != nil {
			t.Fatalf("failed to add %T to the indexer: %v", obj, err)
		}
	}
	lh := lhClient.LonghornV1beta2()
	setting := newSetting(string(types.SettingNameDefaultEngineImage), TestEngineImage)
	add(setting, lhInformers.Settings().Informer().GetIndexer(), func() error {
		_, err := lh.Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
		return err
	})
	imImageSetting := newSetting(string(types.SettingNameDefaultInstanceManagerImage), TestInstanceManagerImage)
	add(imImageSetting, lhInformers.Settings().Informer().GetIndexer(), func() error {
		_, err := lh.Settings(TestNamespace).Create(context.TODO(), imImageSetting, metav1.CreateOptions{})
		return err
	})
	add(engineImage, lhInformers.EngineImages().Informer().GetIndexer(), func() error {
		_, err := lh.EngineImages(TestNamespace).Create(context.TODO(), engineImage, metav1.CreateOptions{})
		return err
	})
	node := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	add(node, lhInformers.Nodes().Informer().GetIndexer(), func() error {
		_, err := lh.Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
		return err
	})
	add(volume, lhInformers.Volumes().Informer().GetIndexer(), func() error {
		_, err := lh.Volumes(TestNamespace).Create(context.TODO(), volume, metav1.CreateOptions{})
		return err
	})
	add(volumeAttachment, lhInformers.VolumeAttachments().Informer().GetIndexer(), func() error {
		_, err := lh.VolumeAttachments(TestNamespace).Create(context.TODO(), volumeAttachment, metav1.CreateOptions{})
		return err
	})
	add(backupTarget, lhInformers.BackupTargets().Informer().GetIndexer(), func() error {
		_, err := lh.BackupTargets(TestNamespace).Create(context.TODO(), backupTarget, metav1.CreateOptions{})
		return err
	})
	add(backup, lhInformers.Backups().Informer().GetIndexer(), func() error {
		_, err := lh.Backups(TestNamespace).Create(context.TODO(), backup, metav1.CreateOptions{})
		return err
	})

	// The first Backup status write loses a conflict, as when another controller updates the
	// Backup at the moment the backup completes.
	statusUpdates := 0
	lhClient.PrependReactor("update", "backups", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "status" {
			return false, nil, nil
		}
		statusUpdates++
		if statusUpdates == 1 {
			return true, nil, apierrors.NewConflict(longhorn.Resource("backups"), testBackupName, fmt.Errorf("the object has been modified"))
		}
		return false, nil, nil
	})

	// A monitor that has already observed the completed backup.
	completedBackup := backup.DeepCopy()
	completedBackup.Status.State = longhorn.BackupStateCompleted
	completedBackup.Status.Progress = 100
	proxy := &backupStatusProxy{status: &longhorn.EngineBackupStatus{
		Progress:     100,
		SnapshotName: testSnapshotName,
		State:        "complete",
	}}
	monitor, err := engineapi.NewBackupMonitor(bc.logger, bc.ds, completedBackup, volume, nil, "", "", 0, "",
		&longhorn.Engine{}, proxy, func(string) {})
	if err != nil {
		t.Fatalf("failed to create the backup monitor: %v", err)
	}
	defer monitor.Close()
	bc.monitors[testBackupName] = monitor

	// Reconcile 1: the Completed status write conflicts. The monitor must be kept.
	if err := bc.reconcile(testBackupName); err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	if statusUpdates != 1 {
		t.Fatalf("expected 1 status update attempt, got %v", statusUpdates)
	}
	if bc.hasMonitor(testBackupName) != monitor {
		t.Fatalf("the monitor must be kept while its final state is not persisted")
	}
	if proxy.closeCount() != 0 {
		t.Fatalf("the monitor must not be closed while its final state is not persisted")
	}

	// Reconcile 2: the cached Backup is still InProgress. The kept monitor publishes Completed,
	// the write succeeds, and the monitor is closed.
	if err := bc.reconcile(testBackupName); err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	persisted, err := lh.Backups(TestNamespace).Get(context.TODO(), testBackupName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get the backup: %v", err)
	}
	if persisted.Status.State != longhorn.BackupStateCompleted || persisted.Status.SnapshotName != testSnapshotName {
		t.Fatalf("expected Completed with snapshot %v, got state %q and snapshot %q",
			testSnapshotName, persisted.Status.State, persisted.Status.SnapshotName)
	}
	if bc.hasMonitor(testBackupName) != nil {
		t.Fatalf("the monitor must be removed after its final state is persisted")
	}
	if proxy.closeCount() != 1 {
		t.Fatalf("expected the monitor to be closed once, got %v", proxy.closeCount())
	}
}
