package recurringjob

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/types"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	cleanupTestNamespace = "longhorn-system"
	cleanupTestJobName   = "daily-backup"
	cleanupTestVolume    = "test-volume"
)

type cleanupVolumeOperations struct {
	longhornclient.VolumeOperations
	snapshots []longhornclient.SnapshotCR
	deleted   []string
}

func (o *cleanupVolumeOperations) ById(name string) (*longhornclient.Volume, error) {
	return &longhornclient.Volume{Name: name}, nil
}

func (o *cleanupVolumeOperations) ActionSnapshotCRList(*longhornclient.Volume) (*longhornclient.SnapshotCRListOutput, error) {
	return &longhornclient.SnapshotCRListOutput{Data: o.snapshots}, nil
}

func (o *cleanupVolumeOperations) ActionSnapshotCRDelete(_ *longhornclient.Volume, input *longhornclient.SnapshotCRInput) (*longhornclient.Empty, error) {
	o.deleted = append(o.deleted, input.Name)
	return &longhornclient.Empty{}, nil
}

func newCleanupTestJob(task longhorn.RecurringJobType, retain int, snapshots []longhornclient.SnapshotCR, objects ...runtime.Object) (*VolumeJob, *cleanupVolumeOperations) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	operations := &cleanupVolumeOperations{snapshots: snapshots}
	job := &Job{
		api:       &longhornclient.RancherClient{Volume: operations},
		lhClient:  lhfake.NewSimpleClientset(objects...), // nolint: staticcheck
		logger:    logger,
		name:      cleanupTestJobName,
		namespace: cleanupTestNamespace,
		retain:    retain,
		task:      task,
	}
	return &VolumeJob{
		Job:          job,
		logger:       logger.WithField("volume", cleanupTestVolume),
		volumeName:   cleanupTestVolume,
		snapshotName: "current-snapshot",
		specLabels: map[string]string{
			types.RecurringJobLabel: cleanupTestJobName,
		},
	}, operations
}

func cleanupTestSnapshot(name string, offset time.Duration) longhornclient.SnapshotCR {
	return longhornclient.SnapshotCR{
		Name:           name,
		CrCreationTime: time.Unix(1, 0).Add(offset).UTC().Format(time.RFC3339),
		CreateSnapshot: true,
		CreationTime:   time.Unix(1, 0).Add(offset).UTC().Format(time.RFC3339),
		Labels:         map[string]string{types.RecurringJobLabel: cleanupTestJobName},
		ReadyToUse:     true,
	}
}

func cleanupTestBackup(name, volumeName, snapshotName string, state longhorn.BackupState) *longhorn.Backup {
	return &longhorn.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cleanupTestNamespace,
			Labels:    types.GetBackupVolumeLabels(volumeName),
		},
		Spec: longhorn.BackupSpec{SnapshotName: snapshotName},
		Status: longhorn.BackupStatus{
			State: state,
		},
	}
}

func TestSnapshotCleanupProtectsActiveBackupSnapshots(t *testing.T) {
	snapshotNames := []string{
		"new-snapshot",
		"pending-snapshot",
		"in-progress-snapshot",
		"completed-snapshot",
		"error-snapshot",
		"unknown-snapshot",
		"deleting-state-snapshot",
		"terminating-snapshot",
		"other-volume-snapshot",
		"unreferenced-snapshot",
	}
	snapshots := make([]longhornclient.SnapshotCR, 0, len(snapshotNames))
	for i, name := range snapshotNames {
		snapshots = append(snapshots, cleanupTestSnapshot(name, time.Duration(i)*time.Second))
	}

	deletingBackup := cleanupTestBackup("deleting-backup", cleanupTestVolume, "terminating-snapshot", longhorn.BackupStateInProgress)
	now := metav1.Now()
	deletingBackup.DeletionTimestamp = &now
	job, operations := newCleanupTestJob(
		longhorn.RecurringJobTypeSnapshotDelete,
		0,
		snapshots,
		cleanupTestBackup("new-backup", cleanupTestVolume, "new-snapshot", longhorn.BackupStateNew),
		cleanupTestBackup("pending-backup", cleanupTestVolume, "pending-snapshot", longhorn.BackupStatePending),
		cleanupTestBackup("in-progress-backup", cleanupTestVolume, "in-progress-snapshot", longhorn.BackupStateInProgress),
		cleanupTestBackup("completed-backup", cleanupTestVolume, "completed-snapshot", longhorn.BackupStateCompleted),
		cleanupTestBackup("error-backup", cleanupTestVolume, "error-snapshot", longhorn.BackupStateError),
		cleanupTestBackup("unknown-backup", cleanupTestVolume, "unknown-snapshot", longhorn.BackupStateUnknown),
		cleanupTestBackup("deleting-state-backup", cleanupTestVolume, "deleting-state-snapshot", longhorn.BackupStateDeleting),
		deletingBackup,
		cleanupTestBackup("other-volume-backup", "other-volume", "other-volume-snapshot", longhorn.BackupStateInProgress),
	)

	err := job.doSnapshotCleanup(false)

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{
		"completed-snapshot",
		"error-snapshot",
		"unknown-snapshot",
		"deleting-state-snapshot",
		"other-volume-snapshot",
		"unreferenced-snapshot",
	}, operations.deleted)
}

func TestBackupAutoCleanupProtectsActiveBackupSnapshot(t *testing.T) {
	snapshots := []longhornclient.SnapshotCR{
		cleanupTestSnapshot("current-snapshot", 0),
		cleanupTestSnapshot("active-snapshot", time.Second),
		cleanupTestSnapshot("completed-snapshot", 2*time.Second),
	}
	setting := &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(types.SettingNameAutoCleanupRecurringJobBackupSnapshot),
			Namespace: cleanupTestNamespace,
		},
		Value: "true",
	}
	job, operations := newCleanupTestJob(
		longhorn.RecurringJobTypeBackup,
		1,
		snapshots,
		setting,
		cleanupTestBackup("active-backup", cleanupTestVolume, "active-snapshot", longhorn.BackupStateInProgress),
		cleanupTestBackup("completed-backup", cleanupTestVolume, "completed-snapshot", longhorn.BackupStateCompleted),
	)

	err := job.doSnapshotCleanup(true)

	require.NoError(t, err)
	assert.Equal(t, []string{"completed-snapshot"}, operations.deleted)
}

func TestRetainCountCleanupProtectsActiveBackupSnapshot(t *testing.T) {
	snapshots := []longhornclient.SnapshotCR{
		cleanupTestSnapshot("active-snapshot", 0),
		cleanupTestSnapshot("unreferenced-snapshot", time.Second),
		cleanupTestSnapshot("retained-snapshot", 2*time.Second),
	}
	setting := &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(types.SettingNameAutoCleanupRecurringJobBackupSnapshot),
			Namespace: cleanupTestNamespace,
		},
		Value: "false",
	}
	job, operations := newCleanupTestJob(
		longhorn.RecurringJobTypeBackup,
		1,
		snapshots,
		setting,
		cleanupTestBackup("active-backup", cleanupTestVolume, "active-snapshot", longhorn.BackupStateInProgress),
	)

	err := job.doSnapshotCleanup(false)

	require.NoError(t, err)
	assert.Equal(t, []string{"unreferenced-snapshot"}, operations.deleted)
}

func TestSnapshotCleanupFailsClosedWhenBackupsCannotBeListed(t *testing.T) {
	expectedErr := errors.New("Kubernetes API unavailable")
	job, operations := newCleanupTestJob(
		longhorn.RecurringJobTypeSnapshotDelete,
		0,
		[]longhornclient.SnapshotCR{cleanupTestSnapshot("candidate-snapshot", 0)},
	)
	client := job.lhClient.(*lhfake.Clientset)
	client.PrependReactor("list", "backups", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, expectedErr
	})

	err := job.doSnapshotCleanup(false)

	assert.ErrorIs(t, err, expectedErr)
	assert.Empty(t, operations.deleted)
}

func TestSnapshotCleanupDeletesSnapshotAfterBackupBecomesFinal(t *testing.T) {
	backup := cleanupTestBackup("backup", cleanupTestVolume, "backup-snapshot", longhorn.BackupStateInProgress)
	job, operations := newCleanupTestJob(
		longhorn.RecurringJobTypeSnapshotDelete,
		0,
		[]longhornclient.SnapshotCR{cleanupTestSnapshot("backup-snapshot", 0)},
		backup,
	)

	require.NoError(t, job.doSnapshotCleanup(false))
	assert.Empty(t, operations.deleted)

	backup.Status.State = longhorn.BackupStateCompleted
	_, err := job.lhClient.LonghornV1beta2().Backups(cleanupTestNamespace).UpdateStatus(t.Context(), backup, metav1.UpdateOptions{})
	require.NoError(t, err)

	require.NoError(t, job.doSnapshotCleanup(false))
	assert.Equal(t, []string{"backup-snapshot"}, operations.deleted)
}
