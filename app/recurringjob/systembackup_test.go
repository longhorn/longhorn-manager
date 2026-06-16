package recurringjob

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/runtime"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	testNamespace       = "longhorn-system"
	testSystemBackupJob = "daily-system-backup"
)

// systemBackupRecurringJobLabels returns the label set the system-backup
// RecurringJob stamps on the SystemBackup CRs it creates. cleanup() lists by
// this exact label (see (*Job).ListSystemBackup), so seeded fixtures must carry
// it to be considered for retention.
func systemBackupRecurringJobLabels(jobName string) map[string]string {
	return map[string]string{
		types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJob, string(longhorn.RecurringJobTypeSystemBackup)): jobName,
	}
}

// newLabeledSystemBackup builds a SystemBackup owned by jobName (carrying the
// recurring-job label) in testNamespace, reusing the shared fixture builder.
func newLabeledSystemBackup(name, jobName string, creationTime, statusCreatedAt time.Time, state longhorn.SystemBackupState) *longhorn.SystemBackup {
	sb := newSystemBackup(name, creationTime, statusCreatedAt, state)
	sb.Namespace = testNamespace
	sb.Labels = systemBackupRecurringJobLabels(jobName)
	return &sb
}

// newSystemBackupJobForTest wires a SystemBackupJob to a fake Longhorn clientset
// seeded with the given SystemBackups. This exercises the real cleanup() path
// (ListSystemBackup -> filter retention-eligible states ->
// systemBackupsToNameWithTimestamps -> filterExpiredItems -> DeleteSystemBackup)
// without needing a cluster.
func newSystemBackupJobForTest(retain int, objs ...*longhorn.SystemBackup) *SystemBackupJob {
	runtimeObjs := make([]runtime.Object, 0, len(objs))
	for _, o := range objs {
		runtimeObjs = append(runtimeObjs, o)
	}
	lhClient := lhfake.NewSimpleClientset(runtimeObjs...) // nolint: staticcheck

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	return &SystemBackupJob{
		Job: &Job{
			lhClient:  lhClient,
			logger:    logger,
			name:      testSystemBackupJob,
			namespace: testNamespace,
			retain:    retain,
		},
		logger: logrus.NewEntry(logger),
	}
}

// remainingSystemBackupNames returns the names of SystemBackups still present in
// the fake cluster after a cleanup pass.
func remainingSystemBackupNames(t *testing.T, job *SystemBackupJob) []string {
	t.Helper()
	list, err := job.lhClient.LonghornV1beta2().SystemBackups(testNamespace).List(context.TODO(), metav1.ListOptions{})
	assert.NoError(t, err)
	names := make([]string, 0, len(list.Items))
	for _, sb := range list.Items {
		names = append(names, sb.Name)
	}
	return names
}

func TestSystemBackupJobCleanup(t *testing.T) {
	base := time.Date(2026, 5, 20, 1, 0, 0, 0, time.UTC)

	t.Run("prunes_error_backup_before_ready", func(t *testing.T) {
		// longhorn/longhorn#13203, resolved per @derekbit's guidance: only Ready
		// and Error are eligible for retention, and an Error backup (zero
		// Status.CreatedAt) sorts first, so it is pruned before any successful
		// Ready backup. With retain=2 and two Ready + one newest Error, the Error
		// CR is removed and both Ready CRs are kept.
		oldReady := newLabeledSystemBackup("daily-old-ready", testSystemBackupJob,
			base, base.Add(8*time.Minute), longhorn.SystemBackupStateReady)
		midReady := newLabeledSystemBackup("daily-mid-ready", testSystemBackupJob,
			base.Add(24*time.Hour), base.Add(24*time.Hour+8*time.Minute), longhorn.SystemBackupStateReady)
		newError := newLabeledSystemBackup("daily-new-error", testSystemBackupJob,
			base.Add(48*time.Hour), time.Time{}, longhorn.SystemBackupStateError)

		job := newSystemBackupJobForTest(2, oldReady, midReady, newError)

		job.cleanup()

		assert.ElementsMatch(t, []string{"daily-old-ready", "daily-mid-ready"},
			remainingSystemBackupNames(t, job),
			"the Error CR is pruned before either Ready CR; successful backups are kept")
	})

	t.Run("skips_in_flight_system_backups", func(t *testing.T) {
		// In-flight backups (anything not Ready/Error) must never be deleted, and
		// must not count toward retain. With retain=1 and two Ready + one
		// Generating, retention applies only to the two terminal Ready CRs (the
		// oldest is pruned); the Generating CR is left untouched.
		oldReady := newLabeledSystemBackup("daily-old-ready", testSystemBackupJob,
			base, base.Add(8*time.Minute), longhorn.SystemBackupStateReady)
		newReady := newLabeledSystemBackup("daily-new-ready", testSystemBackupJob,
			base.Add(24*time.Hour), base.Add(24*time.Hour+8*time.Minute), longhorn.SystemBackupStateReady)
		inFlight := newLabeledSystemBackup("daily-in-flight", testSystemBackupJob,
			base.Add(48*time.Hour), time.Time{}, longhorn.SystemBackupStateGenerating)

		job := newSystemBackupJobForTest(1, oldReady, newReady, inFlight)

		job.cleanup()

		assert.ElementsMatch(t, []string{"daily-new-ready", "daily-in-flight"},
			remainingSystemBackupNames(t, job),
			"in-flight CR is skipped (kept, not counted); only the oldest terminal "+
				"Ready CR is pruned under retain=1")
	})

	t.Run("repeated_failures_do_not_evict_ready_backups", func(t *testing.T) {
		// Safety property behind dropping the creationTimestamp fallback: during a
		// backup-target outage, consecutive failed runs produce Error CRs. They
		// must be pruned before successful backups so retention never erodes the
		// usable (Ready) backups. retain=2 with two Ready + two Error keeps both
		// Ready CRs and prunes both Error CRs.
		ready1 := newLabeledSystemBackup("daily-ready-1", testSystemBackupJob,
			base, base.Add(8*time.Minute), longhorn.SystemBackupStateReady)
		ready2 := newLabeledSystemBackup("daily-ready-2", testSystemBackupJob,
			base.Add(24*time.Hour), base.Add(24*time.Hour+8*time.Minute), longhorn.SystemBackupStateReady)
		error1 := newLabeledSystemBackup("daily-error-1", testSystemBackupJob,
			base.Add(48*time.Hour), time.Time{}, longhorn.SystemBackupStateError)
		error2 := newLabeledSystemBackup("daily-error-2", testSystemBackupJob,
			base.Add(72*time.Hour), time.Time{}, longhorn.SystemBackupStateError)

		job := newSystemBackupJobForTest(2, ready1, ready2, error1, error2)

		job.cleanup()

		assert.ElementsMatch(t, []string{"daily-ready-1", "daily-ready-2"},
			remainingSystemBackupNames(t, job),
			"both Error CRs are pruned; successful Ready backups are never evicted by failures")
	})

	t.Run("ignores_system_backups_owned_by_other_jobs", func(t *testing.T) {
		// cleanup() lists by the recurring-job label, so CRs created by a
		// different recurring job must never be pruned by this job — even when
		// this job's retain would otherwise expire them.
		ownOld := newLabeledSystemBackup("own-old", testSystemBackupJob,
			base, base.Add(8*time.Minute), longhorn.SystemBackupStateReady)
		ownNew := newLabeledSystemBackup("own-new", testSystemBackupJob,
			base.Add(24*time.Hour), base.Add(24*time.Hour+8*time.Minute), longhorn.SystemBackupStateReady)
		otherJob := newLabeledSystemBackup("other-job-old", "weekly-system-backup",
			base.Add(-24*time.Hour), base.Add(-24*time.Hour+8*time.Minute), longhorn.SystemBackupStateReady)

		job := newSystemBackupJobForTest(1, ownOld, ownNew, otherJob)

		job.cleanup()

		assert.ElementsMatch(t, []string{"own-new", "other-job-old"},
			remainingSystemBackupNames(t, job),
			"with retain=1 only this job's oldest CR (own-old) is pruned; another "+
				"job's CR (other-job-old) is left untouched")
	})

	t.Run("no_deletion_when_within_retain", func(t *testing.T) {
		// retain >= number of owned terminal CRs: nothing is expired. The Error CR
		// with a zero Status.CreatedAt must not trip an accidental deletion here.
		ready := newLabeledSystemBackup("a-ready", testSystemBackupJob,
			base, base.Add(8*time.Minute), longhorn.SystemBackupStateReady)
		errored := newLabeledSystemBackup("b-error", testSystemBackupJob,
			base.Add(24*time.Hour), time.Time{}, longhorn.SystemBackupStateError)

		job := newSystemBackupJobForTest(2, ready, errored)

		job.cleanup()

		assert.ElementsMatch(t, []string{"a-ready", "b-error"}, remainingSystemBackupNames(t, job),
			"retain=2 with two owned CRs must delete nothing")
	})
}
