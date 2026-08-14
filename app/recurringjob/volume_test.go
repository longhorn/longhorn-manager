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
	// backups the backup volume of the same name holds, keyed by backup volume name.
	backups map[string][]longhornclient.Backup
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

func (f *fakeBackupVolumeOperations) ActionBackupGet(backupVolume *longhornclient.BackupVolume, input *longhornclient.BackupInput) (*longhornclient.Backup, error) {
	for _, backup := range f.backups[backupVolume.Name] {
		if backup.Name == input.Name {
			return &backup, nil
		}
	}
	return nil, errors.Errorf("backup %v not found in backup volume %v", input.Name, backupVolume.Name)
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
			// One backup volume per backup target: the post-backup retention pass
			// looks up the one belonging to the target the backup went to, and
			// would otherwise take the "no backup volume" path in both subtests.
			backupVolumeOps := &fakeBackupVolumeOperations{
				backupVolumes: []longhornclient.BackupVolume{
					{
						Name:             "bv-1",
						VolumeName:       testVolumeName,
						BackupTargetName: testVolumeBackupTargetName,
					},
					{
						Name:             "bv-2",
						VolumeName:       testVolumeName,
						BackupTargetName: testJobBackupTargetName,
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

// TestGetBackupVolume covers the backup volume lookup. A job with its own backup
// target (longhorn/longhorn#11421) has no backup volume on that target until it
// has completed its first backup, so "not found" has to be reported as such
// rather than looked up by an empty name -- an ById("") request resolves to the
// collection endpoint and decodes into an empty BackupVolume, which reads as a
// real one to every caller.
func TestGetBackupVolume(t *testing.T) {
	backupVolumeOps := &fakeBackupVolumeOperations{
		backupVolumes: []longhornclient.BackupVolume{
			{Name: "bv-1", VolumeName: testVolumeName, BackupTargetName: testVolumeBackupTargetName},
			{Name: "bv-2", VolumeName: "other-volume", BackupTargetName: testJobBackupTargetName},
		},
	}
	job := newVolumeJobForTest(t, longhorn.RecurringJobTypeBackup, 1, "", &fakeVolumeOperations{}, backupVolumeOps)

	backupVolume, err := job.getBackupVolume(testVolumeBackupTargetName)
	require.NoError(t, err)
	require.NotNil(t, backupVolume)
	assert.Equal(t, "bv-1", backupVolume.Name)

	// The only backup volume on this target belongs to another volume.
	backupVolume, err = job.getBackupVolume(testJobBackupTargetName)
	require.NoError(t, err)
	assert.Nil(t, backupVolume, "a volume with no backup volume on the target must not resolve to one")
}

// TestGetLastBackup covers which backup a job treats as its last one. It used to
// be Volume.Status.LastBackup, but that field only ever tracks the volume's own
// backup target (VolumeController.ReconcileBackupVolumeState scopes it to
// Volume.Spec.BackupTargetName). Once a job can back up somewhere else
// (longhorn/longhorn#11421) that value is wrong for it in both directions: it
// names a backup absent from the job's target, and it is empty even when the
// job's target holds backups. The last backup has to come from the backup volume
// of the target the job actually uses.
func TestGetLastBackup(t *testing.T) {
	volumeTargetBackup := longhornclient.Backup{Name: "backup-on-volume-target", SnapshotName: "snapshot-on-volume-target"}
	jobTargetBackup := longhornclient.Backup{Name: "backup-on-job-target", SnapshotName: "snapshot-on-job-target"}

	newBackupVolumeOps := func(jobTargetLastBackupName string) *fakeBackupVolumeOperations {
		return &fakeBackupVolumeOperations{
			backupVolumes: []longhornclient.BackupVolume{
				{
					Name:             "bv-volume-target",
					VolumeName:       testVolumeName,
					BackupTargetName: testVolumeBackupTargetName,
					LastBackupName:   volumeTargetBackup.Name,
				},
				{
					Name:             "bv-job-target",
					VolumeName:       testVolumeName,
					BackupTargetName: testJobBackupTargetName,
					LastBackupName:   jobTargetLastBackupName,
				},
			},
			backups: map[string][]longhornclient.Backup{
				"bv-volume-target": {volumeTargetBackup},
				"bv-job-target":    {jobTargetBackup},
			},
		}
	}

	// Volume.Status.LastBackup deliberately points at the volume target's backup
	// in every case, so a job reading it instead of its own target's backup
	// volume picks the wrong one.
	newVolumeOps := func() *fakeVolumeOperations {
		return &fakeVolumeOperations{
			volume: &longhornclient.Volume{
				Name:             testVolumeName,
				BackupTargetName: testVolumeBackupTargetName,
				LastBackup:       volumeTargetBackup.Name,
			},
		}
	}

	t.Run("a job without its own backup target uses the volume's", func(t *testing.T) {
		job := newVolumeJobForTest(t, longhorn.RecurringJobTypeBackup, 1, "", newVolumeOps(), newBackupVolumeOps(jobTargetBackup.Name))

		lastBackup, err := job.getLastBackup()
		require.NoError(t, err)
		require.NotNil(t, lastBackup)
		assert.Equal(t, volumeTargetBackup.Name, lastBackup.Name)
	})

	t.Run("a job with its own backup target uses that target's last backup", func(t *testing.T) {
		job := newVolumeJobForTest(t, longhorn.RecurringJobTypeBackup, 1, testJobBackupTargetName, newVolumeOps(), newBackupVolumeOps(jobTargetBackup.Name))

		lastBackup, err := job.getLastBackup()
		require.NoError(t, err)
		require.NotNil(t, lastBackup)
		assert.Equal(t, jobTargetBackup.Name, lastBackup.Name,
			"the volume's last backup lives on another backup target and is not this job's")
	})

	t.Run("no backup on the job's backup target yet", func(t *testing.T) {
		// The volume has a backup on its own target, so reading
		// Volume.Status.LastBackup here would claim a last backup that this job's
		// target does not hold.
		job := newVolumeJobForTest(t, longhorn.RecurringJobTypeBackup, 1, testJobBackupTargetName, newVolumeOps(), newBackupVolumeOps(""))

		lastBackup, err := job.getLastBackup()
		require.NoError(t, err)
		assert.Nil(t, lastBackup)
	})

	t.Run("no backup volume on the job's backup target yet", func(t *testing.T) {
		// The first run of a job with its own backup target: nothing has created
		// a backup volume there.
		backupVolumeOps := &fakeBackupVolumeOperations{
			backupVolumes: []longhornclient.BackupVolume{
				{
					Name:             "bv-volume-target",
					VolumeName:       testVolumeName,
					BackupTargetName: testVolumeBackupTargetName,
					LastBackupName:   volumeTargetBackup.Name,
				},
			},
		}
		job := newVolumeJobForTest(t, longhorn.RecurringJobTypeBackup, 1, testJobBackupTargetName, newVolumeOps(), backupVolumeOps)

		lastBackup, err := job.getLastBackup()
		require.NoError(t, err)
		assert.Nil(t, lastBackup)
	})

	t.Run("the volume has no backup anywhere", func(t *testing.T) {
		volumeOps := newVolumeOps()
		volumeOps.volume.LastBackup = ""
		backupVolumeOps := newBackupVolumeOps("")
		backupVolumeOps.backupVolumes[0].LastBackupName = ""

		job := newVolumeJobForTest(t, longhorn.RecurringJobTypeBackup, 1, "", volumeOps, backupVolumeOps)

		lastBackup, err := job.getLastBackup()
		require.NoError(t, err)
		assert.Nil(t, lastBackup)
	})
}

// TestFilterExpiredSnapshotsRetainsLastBackupSnapshot covers what the last backup
// is for: with auto-cleanup-recurring-job-backup-snapshot enabled a backup job
// deletes every one of its snapshots except the current one and the one its last
// backup was taken from, which the next incremental backup builds on. For a job
// with its own backup target (longhorn/longhorn#11421) that snapshot has to be
// found through that target -- resolving it through the volume's target keeps
// the wrong snapshot and deletes the one the next backup needs.
func TestFilterExpiredSnapshotsRetainsLastBackupSnapshot(t *testing.T) {
	base := time.Date(2026, 5, 20, 1, 0, 0, 0, time.UTC)

	snapshotCRs := []longhornclient.SnapshotCR{
		newSnapshotCR("snapshot-on-volume-target", testVolumeJobName, base),
		newSnapshotCR("snapshot-on-job-target", testVolumeJobName, base.Add(time.Hour)),
		newSnapshotCR(testSnapshotName, testVolumeJobName, base.Add(2*time.Hour)),
	}

	volumeOps := &fakeVolumeOperations{
		volume: &longhornclient.Volume{
			Name:             testVolumeName,
			BackupTargetName: testVolumeBackupTargetName,
			LastBackup:       "backup-on-volume-target",
		},
	}
	backupVolumeOps := &fakeBackupVolumeOperations{
		backupVolumes: []longhornclient.BackupVolume{
			{
				Name:             "bv-volume-target",
				VolumeName:       testVolumeName,
				BackupTargetName: testVolumeBackupTargetName,
				LastBackupName:   "backup-on-volume-target",
			},
			{
				Name:             "bv-job-target",
				VolumeName:       testVolumeName,
				BackupTargetName: testJobBackupTargetName,
				LastBackupName:   "backup-on-job-target",
			},
		},
		backups: map[string][]longhornclient.Backup{
			"bv-volume-target": {{Name: "backup-on-volume-target", SnapshotName: "snapshot-on-volume-target"}},
			"bv-job-target":    {{Name: "backup-on-job-target", SnapshotName: "snapshot-on-job-target"}},
		},
	}

	job := newVolumeJobForTest(t, longhorn.RecurringJobTypeBackup, 1, testJobBackupTargetName, volumeOps, backupVolumeOps,
		newAutoCleanupBackupSnapshotSetting("true"))

	assert.Equal(t, []string{"snapshot-on-volume-target"}, job.listSnapshotNamesToCleanup(snapshotCRs, false),
		"the snapshot of the last backup on the job's own backup target must survive, and the volume target's must not be mistaken for it")
}
