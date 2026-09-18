package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"

	"github.com/longhorn/longhorn-manager/datastore"
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
