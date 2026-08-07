package recurringjob

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	testVolumeJobName = "daily-backup"
	testVolumeName    = "test-volume"
	testSnapshotName  = "daily-ba-snapshot"

	testVolumeBackupTargetName = "volume-backup-target"
	testJobBackupTargetName    = "job-backup-target"
)

// fakeVolumeOperations implements only the VolumeOperations calls a volume job
// makes. The embedded interface is nil, so any other call panics: an unintended
// code path fails the test loudly instead of quietly returning a zero value.
type fakeVolumeOperations struct {
	longhornclient.VolumeOperations

	volume      *longhornclient.Volume
	snapshotCRs []longhornclient.SnapshotCR

	backupInput      *longhornclient.SnapshotInput // captured from ActionSnapshotBackup
	deletedSnapshots []string
}

func (f *fakeVolumeOperations) ById(id string) (*longhornclient.Volume, error) {
	return f.volume, nil
}

func (f *fakeVolumeOperations) ActionSnapshotCRList(*longhornclient.Volume) (*longhornclient.SnapshotCRListOutput, error) {
	return &longhornclient.SnapshotCRListOutput{Data: f.snapshotCRs}, nil
}

func (f *fakeVolumeOperations) ActionSnapshotCRCreate(_ *longhornclient.Volume, input *longhornclient.SnapshotCRInput) (*longhornclient.SnapshotCR, error) {
	return &longhornclient.SnapshotCR{Name: input.Name}, nil
}

func (f *fakeVolumeOperations) ActionSnapshotCRGet(_ *longhornclient.Volume, input *longhornclient.SnapshotCRInput) (*longhornclient.SnapshotCR, error) {
	return &longhornclient.SnapshotCR{Name: input.Name, ReadyToUse: true}, nil
}

func (f *fakeVolumeOperations) ActionSnapshotCRDelete(_ *longhornclient.Volume, input *longhornclient.SnapshotCRInput) (*longhornclient.Empty, error) {
	f.deletedSnapshots = append(f.deletedSnapshots, input.Name)
	return &longhornclient.Empty{}, nil
}

func (f *fakeVolumeOperations) ActionSnapshotBackup(_ *longhornclient.Volume, input *longhornclient.SnapshotInput) (*longhornclient.Volume, error) {
	f.backupInput = input
	return f.volume, nil
}

// fakeBackupVolumeOperations serves the post-backup retention pass. It reports
// one backup volume per backup target so the lookup resolves, and no backups, so
// nothing is pruned; backup retention itself is covered by
// TestListBackupsForCleanup.
type fakeBackupVolumeOperations struct {
	longhornclient.BackupVolumeOperations

	backupVolumes []longhornclient.BackupVolume
}

func (f *fakeBackupVolumeOperations) List(*longhornclient.ListOpts) (*longhornclient.BackupVolumeCollection, error) {
	return &longhornclient.BackupVolumeCollection{Data: f.backupVolumes}, nil
}

func (f *fakeBackupVolumeOperations) ById(id string) (*longhornclient.BackupVolume, error) {
	for _, bv := range f.backupVolumes {
		if bv.Name == id {
			return &bv, nil
		}
	}
	return nil, errors.Errorf("backup volume %v not found", id)
}

func (f *fakeBackupVolumeOperations) ActionBackupList(*longhornclient.BackupVolume) (*longhornclient.BackupListOutput, error) {
	return &longhornclient.BackupListOutput{}, nil
}

// newVolumeJobForTest wires a VolumeJob to fake Longhorn API and clientset so
// the real code path runs without a cluster. autoCleanupBackupSnapshot seeds the
// setting doSnapshotCleanup reads after a backup.
func newVolumeJobForTest(t *testing.T, task longhorn.RecurringJobType, retain int, backupTarget string, volumeOps *fakeVolumeOperations, backupVolumeOps *fakeBackupVolumeOperations, settings ...runtime.Object) *VolumeJob {
	t.Helper()

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	return &VolumeJob{
		Job: &Job{
			api: &longhornclient.RancherClient{
				Volume:       volumeOps,
				BackupVolume: backupVolumeOps,
			},
			lhClient:     lhfake.NewSimpleClientset(settings...), // nolint: staticcheck
			logger:       logger,
			name:         testVolumeJobName,
			namespace:    testNamespace,
			retain:       retain,
			task:         task,
			backupTarget: backupTarget,
		},
		logger:       logrus.NewEntry(logger),
		volumeName:   testVolumeName,
		snapshotName: testSnapshotName,
		specLabels:   map[string]string{types.RecurringJobLabel: testVolumeJobName},
	}
}

func newAutoCleanupBackupSnapshotSetting(value string) *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(types.SettingNameAutoCleanupRecurringJobBackupSnapshot),
			Namespace: testNamespace,
		},
		Value: value,
	}
}

// newSnapshotCR builds a snapshot CR as the volume job sees it. Retention sorts
// on CrCreationTime, and only snapshots carrying the RecurringJob label of the
// running job are candidates for deletion.
func newSnapshotCR(name, jobName string, crCreationTime time.Time) longhornclient.SnapshotCR {
	snapshotCR := longhornclient.SnapshotCR{
		Name:           name,
		CreateSnapshot: true,
		CrCreationTime: crCreationTime.Format(time.RFC3339),
		ReadyToUse:     true,
	}
	if jobName != "" {
		snapshotCR.Labels = map[string]string{types.RecurringJobLabel: jobName}
	}
	return snapshotCR
}

// TestNewVolumeJob covers the per-volume job setup. StartVolumeJobs fans out one
// volume job per volume through an errgroup, all sharing one RecurringJob, so
// the label map must be copied rather than stamped in place -- otherwise the
// jobs race on the shared spec map -- and each job needs its own snapshot name.
func TestNewVolumeJob(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	job := &Job{
		logger: logger,
		name:   testVolumeJobName,
		task:   longhorn.RecurringJobTypeSnapshot,
		retain: 3,
	}
	recurringJob := &longhorn.RecurringJob{
		ObjectMeta: metav1.ObjectMeta{Name: testVolumeJobName, Namespace: testNamespace},
		Spec: longhorn.RecurringJobSpec{
			Name:        testVolumeJobName,
			Task:        longhorn.RecurringJobTypeSnapshot,
			Concurrency: 2,
			Labels:      map[string]string{"user-label": "user-value"},
		},
	}

	volumeJob, err := newVolumeJob(job, recurringJob, testVolumeName, []string{"default"})
	require.NoError(t, err)

	assert.Equal(t, testVolumeJobName, volumeJob.specLabels[types.RecurringJobLabel],
		"snapshots must be stamped with the owning job so retention only prunes its own")
	assert.Equal(t, "user-value", volumeJob.specLabels["user-label"],
		"labels configured on the RecurringJob must survive onto the snapshot")
	assert.NotContains(t, recurringJob.Spec.Labels, types.RecurringJobLabel,
		"the RecurringJob spec label map is shared across concurrent volume jobs and must not be mutated")

	otherVolumeJob, err := newVolumeJob(job, recurringJob, "other-volume", []string{"default"})
	require.NoError(t, err)
	assert.NotEqual(t, volumeJob.snapshotName, otherVolumeJob.snapshotName,
		"concurrent volume jobs must not collide on one snapshot name")

	// The name is prefixed with the cron job name so an operator can tell which
	// recurring job produced a snapshot from its name alone.
	prefix := sliceStringSafely(types.GetCronJobNameForRecurringJob(testVolumeJobName), 0, 8)
	assert.True(t, strings.HasPrefix(volumeJob.snapshotName, prefix+"-"),
		"snapshot name %v should start with the cron job name prefix %v", volumeJob.snapshotName, prefix)
}

// TestListSnapshotNamesToCleanup covers the task dispatch that decides which
// snapshots a run deletes. The three arms behave differently on purpose:
// snapshot-delete is a cluster-wide count-based prune, snapshot-cleanup only
// purges already-removed snapshots and must never delete CRs, and the default
// arm is scoped to the running job's own snapshots.
func TestListSnapshotNamesToCleanup(t *testing.T) {
	base := time.Date(2026, 5, 20, 1, 0, 0, 0, time.UTC)

	// Two snapshots from this job, one from another job, oldest first.
	snapshotCRs := []longhornclient.SnapshotCR{
		newSnapshotCR("own-old", testVolumeJobName, base),
		newSnapshotCR("other-job", "weekly-backup", base.Add(time.Hour)),
		newSnapshotCR("own-new", testVolumeJobName, base.Add(2*time.Hour)),
	}

	t.Run("snapshot-delete prunes by count across every snapshot", func(t *testing.T) {
		// The snapshot-delete task is not scoped to its own snapshots: it keeps
		// the newest `retain` snapshots of the volume whoever created them.
		job := newVolumeJobForTest(t, longhorn.RecurringJobTypeSnapshotDelete, 2, "", &fakeVolumeOperations{}, &fakeBackupVolumeOperations{})

		assert.Equal(t, []string{"own-old"}, job.listSnapshotNamesToCleanup(snapshotCRs, false),
			"retain=2 over three snapshots expires only the oldest")
	})

	t.Run("snapshot-cleanup never deletes snapshot CRs", func(t *testing.T) {
		// snapshot-cleanup exists to trigger a purge of snapshots already marked
		// removed. Deleting CRs here would destroy data the user still retains.
		job := newVolumeJobForTest(t, longhorn.RecurringJobTypeSnapshotCleanup, 0, "", &fakeVolumeOperations{}, &fakeBackupVolumeOperations{})

		assert.Empty(t, job.listSnapshotNamesToCleanup(snapshotCRs, false),
			"retain=0 must still delete nothing for the cleanup task")
	})

	t.Run("snapshot task only prunes its own snapshots", func(t *testing.T) {
		// The default arm filters by the RecurringJob label first, so another
		// job's snapshot is never counted toward, or evicted by, this job's
		// retain.
		job := newVolumeJobForTest(t, longhorn.RecurringJobTypeSnapshot, 1, "", &fakeVolumeOperations{}, &fakeBackupVolumeOperations{},
			newAutoCleanupBackupSnapshotSetting("false"))

		assert.Equal(t, []string{"own-old"}, job.listSnapshotNamesToCleanup(snapshotCRs, false),
			"retain=1 over this job's two snapshots expires its oldest and leaves the other job's alone")
	})
}

// TestListBackupsForCleanup covers backup retention. Backups are shared cluster
// state: a manual backup, or one from another recurring job, must survive this
// job's retention no matter how low its retain is.
func TestListBackupsForCleanup(t *testing.T) {
	base := time.Date(2026, 5, 20, 1, 0, 0, 0, time.UTC)

	newBackup := func(name, jobName, created string) longhornclient.Backup {
		backup := longhornclient.Backup{Name: name, Created: created}
		if jobName != "" {
			backup.Labels = map[string]string{types.RecurringJobLabel: jobName}
		}
		return backup
	}

	backups := []longhornclient.Backup{
		newBackup("own-old", testVolumeJobName, base.Format(time.RFC3339)),
		newBackup("own-new", testVolumeJobName, base.Add(2*time.Hour).Format(time.RFC3339)),
		newBackup("other-job", "weekly-backup", base.Add(-time.Hour).Format(time.RFC3339)),
		newBackup("manual", "", base.Add(-2*time.Hour).Format(time.RFC3339)),
	}

	t.Run("only this job's backups are expired", func(t *testing.T) {
		job := newVolumeJobForTest(t, longhorn.RecurringJobTypeBackup, 1, "", &fakeVolumeOperations{}, &fakeBackupVolumeOperations{})

		assert.Equal(t, []string{"own-old"}, job.listBackupsForCleanup(backups),
			"the unlabeled and other-job backups are older than retain would allow, but are not this job's to delete")
	})

	t.Run("nothing is expired within retain", func(t *testing.T) {
		job := newVolumeJobForTest(t, longhorn.RecurringJobTypeBackup, 2, "", &fakeVolumeOperations{}, &fakeBackupVolumeOperations{})

		assert.Empty(t, job.listBackupsForCleanup(backups))
	})

	t.Run("backups with an unparsable timestamp are skipped", func(t *testing.T) {
		// A backup whose Created cannot be parsed cannot be placed in the
		// retention order, so it is left alone rather than guessed at.
		job := newVolumeJobForTest(t, longhorn.RecurringJobTypeBackup, 0, "", &fakeVolumeOperations{}, &fakeBackupVolumeOperations{})

		withBroken := append([]longhornclient.Backup{}, backups...)
		withBroken = append(withBroken, newBackup("own-broken", testVolumeJobName, "not-a-timestamp"))

		assert.Equal(t, []string{"own-old", "own-new"}, job.listBackupsForCleanup(withBroken),
			"retain=0 expires both parsable backups of this job and skips the unparsable one")
	})
}

// TestDoRecurringBackupBackupTarget covers the per-job backup target
// (longhorn/longhorn#11421): doRecurringBackup now passes a backup target to
// ActionSnapshotBackup instead of letting the manager fall back to the volume's.
// The empty case has to keep resolving to the volume's backup target, because
// that is what every recurring job predating the field carries.
//
// Each subtest runs the real doRecurringBackup, which polls for the backup to
// start on a WaitInterval (5s) tick, so it takes a few seconds.
func TestDoRecurringBackupBackupTarget(t *testing.T) {
	tests := map[string]struct {
		jobBackupTarget      string
		expectedBackupTarget string
	}{
		"job backup target is used when set": {
			jobBackupTarget:      testJobBackupTargetName,
			expectedBackupTarget: testJobBackupTargetName,
		},
		"volume backup target is used when the job has none": {
			jobBackupTarget:      "",
			expectedBackupTarget: testVolumeBackupTargetName,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			volumeOps := &fakeVolumeOperations{
				volume: &longhornclient.Volume{
					Name:             testVolumeName,
					State:            string(longhorn.VolumeStateAttached),
					BackupTargetName: testVolumeBackupTargetName,
					// The backup of our snapshot is already complete, so the
					// wait loop makes a single pass.
					BackupStatus: []longhornclient.BackupStatus{
						{
							Snapshot: testSnapshotName,
							State:    string(longhorn.BackupStateCompleted),
						},
					},
				},
			}
			backupVolumeOps := &fakeBackupVolumeOperations{
				backupVolumes: []longhornclient.BackupVolume{
					{
						Name:             "bv-1",
						VolumeName:       testVolumeName,
						BackupTargetName: testVolumeBackupTargetName,
					},
				},
			}
			job := newVolumeJobForTest(t, longhorn.RecurringJobTypeBackup, 1, tc.jobBackupTarget, volumeOps, backupVolumeOps,
				newAutoCleanupBackupSnapshotSetting("false"))

			require.NoError(t, job.doRecurringBackup())

			require.NotNil(t, volumeOps.backupInput, "the backup was never requested")
			assert.Equal(t, tc.expectedBackupTarget, volumeOps.backupInput.BackupTarget)
			assert.Equal(t, testSnapshotName, volumeOps.backupInput.Name)
			assert.Equal(t, string(longhorn.BackupModeIncremental), volumeOps.backupInput.BackupMode,
				"without a full-backup-interval parameter every run is incremental")
		})
	}
}

// TestDoRecurringBackupFullBackupInterval covers the full-backup-interval
// parameter, which is evaluated on the same call the backup target rides on. The
// interval counts job executions, so run N is full when N is a multiple of it.
func TestDoRecurringBackupFullBackupInterval(t *testing.T) {
	tests := map[string]struct {
		executionCount int
		interval       string
		expectedMode   longhorn.BackupMode
	}{
		"execution on the interval is a full backup": {
			executionCount: 4,
			interval:       "2",
			expectedMode:   longhorn.BackupModeFull,
		},
		"execution off the interval stays incremental": {
			executionCount: 5,
			interval:       "2",
			expectedMode:   longhorn.BackupModeIncremental,
		},
		"a zero interval disables full backups": {
			executionCount: 0,
			interval:       "0",
			expectedMode:   longhorn.BackupModeIncremental,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			volumeOps := &fakeVolumeOperations{
				volume: &longhornclient.Volume{
					Name:             testVolumeName,
					State:            string(longhorn.VolumeStateAttached),
					BackupTargetName: testVolumeBackupTargetName,
					BackupStatus: []longhornclient.BackupStatus{
						{
							Snapshot: testSnapshotName,
							State:    string(longhorn.BackupStateCompleted),
						},
					},
				},
			}
			backupVolumeOps := &fakeBackupVolumeOperations{
				backupVolumes: []longhornclient.BackupVolume{
					{
						Name:             "bv-1",
						VolumeName:       testVolumeName,
						BackupTargetName: testVolumeBackupTargetName,
					},
				},
			}
			job := newVolumeJobForTest(t, longhorn.RecurringJobTypeBackup, 1, "", volumeOps, backupVolumeOps,
				newAutoCleanupBackupSnapshotSetting("false"))
			job.executionCount = tc.executionCount
			job.parameters = map[string]string{types.RecurringJobParameterFullBackupInterval: tc.interval}

			require.NoError(t, job.doRecurringBackup())

			require.NotNil(t, volumeOps.backupInput, "the backup was never requested")
			assert.Equal(t, string(tc.expectedMode), volumeOps.backupInput.BackupMode)
		})
	}
}
