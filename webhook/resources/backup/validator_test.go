package backup

import (
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	testNamespace = "longhorn-system"

	testVolumeName   = "test-volume"
	testSnapshotName = "test-snapshot"
	testBackupName   = "test-backup"

	testVolumeBackupTargetName = "default"
	testOtherBackupTargetName  = "secondary"
	testThirdBackupTargetName  = "tertiary"

	testRecurringJobName = "test-recurring-job"
)

func newTestBackupTarget(name string) *longhorn.BackupTarget {
	return &longhorn.BackupTarget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Spec: longhorn.BackupTargetSpec{
			BackupTargetURL: "nfs://longhorn-test-nfs-svc.default:/opt/backupstore",
		},
		Status: longhorn.BackupTargetStatus{
			Available: true,
		},
	}
}

func newTestVolume() *longhorn.Volume {
	return &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testVolumeName,
			Namespace: testNamespace,
		},
		Spec: longhorn.VolumeSpec{
			// A multiple of the backup block size below; the validator rejects
			// backups whose block size does not evenly divide the volume.
			Size:             20 * types.BackupBlockSizeMi,
			BackupTargetName: testVolumeBackupTargetName,
		},
	}
}

func newTestSnapshot() *longhorn.Snapshot {
	return &longhorn.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testSnapshotName,
			Namespace: testNamespace,
		},
		Spec: longhorn.SnapshotSpec{
			Volume: testVolumeName,
		},
	}
}

func newTestRecurringJob(backupTargetName string) *longhorn.RecurringJob {
	return &longhorn.RecurringJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testRecurringJobName,
			Namespace: testNamespace,
		},
		Spec: longhorn.RecurringJobSpec{
			Name:         testRecurringJobName,
			Task:         longhorn.RecurringJobTypeBackup,
			Cron:         "0 0 * * *",
			Retain:       1,
			BackupTarget: backupTargetName,
		},
	}
}

// newTestBackup builds a backup that is valid in every respect other than the
// backup target it is labelled for, so each case below isolates the backup
// target check. recurringJobName is the value of the RecurringJob spec label,
// empty for a backup that was not created by a recurring job.
func newTestBackup(backupTargetName, recurringJobName string) *longhorn.Backup {
	backup := &longhorn.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testBackupName,
			Namespace: testNamespace,
			Labels: map[string]string{
				types.LonghornLabelBackupTarget: backupTargetName,
				types.LonghornLabelBackupVolume: testVolumeName,
			},
		},
		Spec: longhorn.BackupSpec{
			SnapshotName:    testSnapshotName,
			BackupMode:      longhorn.BackupModeIncremental,
			BackupBlockSize: types.BackupBlockSize2Mi,
		},
	}
	if recurringJobName != "" {
		backup.Spec.Labels = map[string]string{types.RecurringJobLabel: recurringJobName}
	}
	return backup
}

func newTestValidator(t *testing.T, stopCh chan struct{}, objects ...runtime.Object) *backupValidator {
	t.Helper()

	lhClient := lhfake.NewSimpleClientset(objects...)          // nolint: staticcheck
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(testNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStore(testNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	informerFactories.Start(stopCh)
	require.True(t, cache.WaitForCacheSync(stopCh,
		ds.BackupTargetInformer.HasSynced,
		ds.VolumeInformer.HasSynced,
		ds.SnapshotInformer.HasSynced,
		ds.RecurringJobInformer.HasSynced,
	))

	return &backupValidator{ds: ds}
}

// TestValidatorCreateBackupTarget covers the relaxed backup target check added
// for the per-recurring-job backup target (longhorn/longhorn#11421). Before it,
// a backup had to be labelled for the backup target of its own volume. A
// recurring job may now direct its backups elsewhere, so the mismatch is only
// tolerated when the backup really came from a recurring job that is configured
// for that target -- anything looser would let a backup be written to an
// arbitrary target just by setting a label.
func TestValidatorCreateBackupTarget(t *testing.T) {
	baseObjects := func(extra ...runtime.Object) []runtime.Object {
		objects := []runtime.Object{
			newTestBackupTarget(testVolumeBackupTargetName),
			newTestBackupTarget(testOtherBackupTargetName),
			newTestVolume(),
			newTestSnapshot(),
		}
		return append(objects, extra...)
	}

	tests := map[string]struct {
		existingObjects []runtime.Object
		backup          *longhorn.Backup
		expectError     string
	}{
		"backup for the volume backup target is accepted": {
			existingObjects: baseObjects(),
			backup:          newTestBackup(testVolumeBackupTargetName, ""),
		},
		"mismatch without a recurring job label is rejected": {
			existingObjects: baseObjects(),
			backup:          newTestBackup(testOtherBackupTargetName, ""),
			expectError:     "volume backup target default and label backup target secondary does not match",
		},
		"mismatch naming an unknown recurring job is rejected": {
			existingObjects: baseObjects(),
			backup:          newTestBackup(testOtherBackupTargetName, testRecurringJobName),
			expectError:     "failed to get recurring job " + testRecurringJobName,
		},
		"mismatch from a recurring job without its own backup target is rejected": {
			// The job defers to the volume backup target, so the label cannot
			// legitimately name a different one.
			existingObjects: baseObjects(newTestRecurringJob("")),
			backup:          newTestBackup(testOtherBackupTargetName, testRecurringJobName),
			expectError:     "volume backup target default and label backup target secondary does not match",
		},
		"mismatch from a recurring job pointing at a third backup target is rejected": {
			existingObjects: baseObjects(newTestRecurringJob(testThirdBackupTargetName)),
			backup:          newTestBackup(testOtherBackupTargetName, testRecurringJobName),
			expectError:     "recurring job backup target tertiary and label backup target secondary does not match",
		},
		"mismatch from a recurring job pointing at that backup target is accepted": {
			existingObjects: baseObjects(newTestRecurringJob(testOtherBackupTargetName)),
			backup:          newTestBackup(testOtherBackupTargetName, testRecurringJobName),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			stopCh := make(chan struct{})
			defer close(stopCh)
			validator := newTestValidator(t, stopCh, tc.existingObjects...)

			err := validator.Create(nil, tc.backup)
			if tc.expectError == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectError)
			}
		})
	}
}
