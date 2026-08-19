package recurringjob

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func newSystemBackup(name string, creationTime time.Time, statusCreatedAt time.Time, state longhorn.SystemBackupState) longhorn.SystemBackup {
	return longhorn.SystemBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(creationTime),
		},
		Status: longhorn.SystemBackupStatus{
			State:     state,
			CreatedAt: metav1.NewTime(statusCreatedAt),
		},
	}
}

func TestSystemBackupsToNameWithTimestamps(t *testing.T) {
	base := time.Date(2026, 5, 20, 1, 0, 0, 0, time.UTC)

	t.Run("uses_status_created_at", func(t *testing.T) {
		// Ready backups have Status.CreatedAt populated by the controller; the
		// returned timestamps should match Status.CreatedAt for each entry.
		systemBackups := []longhorn.SystemBackup{
			newSystemBackup("daily-1", base, base.Add(8*time.Minute), longhorn.SystemBackupStateReady),
			newSystemBackup("daily-2", base.Add(24*time.Hour), base.Add(24*time.Hour+6*time.Minute), longhorn.SystemBackupStateReady),
		}

		got := systemBackupsToNameWithTimestamps(systemBackups)

		assert.Len(t, got, 2)
		byName := map[string]time.Time{}
		for _, n := range got {
			byName[n.Name] = n.Timestamp
		}
		assert.Equal(t, base.Add(8*time.Minute), byName["daily-1"])
		assert.Equal(t, base.Add(24*time.Hour+6*time.Minute), byName["daily-2"])
	})

	t.Run("error_backup_keeps_zero_timestamp", func(t *testing.T) {
		// Status.CreatedAt is only set on the successful upload path, so an Error
		// backup carries a zero timestamp. We deliberately do not fall back to
		// metadata.creationTimestamp: the zero value sorts ahead of successful
		// (Ready) backups in filterExpiredItems, so failed backups are pruned
		// first and never evict a successful one (longhorn/longhorn#13203).
		errored := newSystemBackup("daily-stuck", base, time.Time{}, longhorn.SystemBackupStateError)

		got := systemBackupsToNameWithTimestamps([]longhorn.SystemBackup{errored})

		if assert.Len(t, got, 1) {
			assert.True(t, got[0].Timestamp.IsZero(),
				"Error backup must keep a zero timestamp so it sorts (and prunes) ahead of Ready backups")
		}
	})

	t.Run("error_is_pruned_before_ready", func(t *testing.T) {
		// With retain=2 and two Ready + one Error, the Error backup (zero
		// Status.CreatedAt) sorts first and is the one pruned; both successful
		// Ready backups are retained.
		oldReady := newSystemBackup("daily-old-ready",
			base, base.Add(8*time.Minute), longhorn.SystemBackupStateReady)
		midReady := newSystemBackup("daily-mid-ready",
			base.Add(24*time.Hour), base.Add(24*time.Hour+8*time.Minute), longhorn.SystemBackupStateReady)
		newError := newSystemBackup("daily-new-error",
			base.Add(48*time.Hour), time.Time{}, longhorn.SystemBackupStateError)

		expired := filterExpiredItems(systemBackupsToNameWithTimestamps(
			[]longhorn.SystemBackup{oldReady, midReady, newError}), 2, 0, longhorn.RecurringJobRetentionPolicyCountBased, base.Add(100*time.Hour))

		assert.Equal(t, []string{"daily-new-error"}, expired,
			"the Error backup is pruned before either Ready backup")
	})

	t.Run("empty", func(t *testing.T) {
		assert.Empty(t, systemBackupsToNameWithTimestamps(nil))
	})
}

func TestFilterExpiredItems(t *testing.T) {
	base := time.Date(2026, 5, 20, 1, 0, 0, 0, time.UTC)
	// These cases cover count-only retention, so retainAge is 0 (disabled) and
	// the clock must not influence the result no matter how far past the items
	// it sits. The retention policy is left empty on purpose: that is what jobs
	// created before the field existed carry, and they must keep behaving exactly
	// as they did.
	now := base.Add(100 * time.Hour)

	t.Run("retains_n_newest", func(t *testing.T) {
		nts := []NameWithTimestamp{
			{Name: "a", Timestamp: base},
			{Name: "b", Timestamp: base.Add(24 * time.Hour)},
			{Name: "c", Timestamp: base.Add(48 * time.Hour)},
			{Name: "d", Timestamp: base.Add(72 * time.Hour)},
		}

		expired := filterExpiredItems(nts, 2, 0, longhorn.RecurringJobRetentionPolicyCountBased, now)

		// Expect the two oldest (a, b) to be pruned; c and d retained.
		assert.ElementsMatch(t, []string{"a", "b"}, expired)
	})

	t.Run("zero_retain_count_expires_everything", func(t *testing.T) {
		// retain 0 is a supported configuration, not an unset field:
		// validateRetentionPolicy admits it for the snapshot-delete,
		// snapshot-cleanup and filesystem-trim tasks, and a snapshot-delete job
		// relies on it to clear every snapshot of the volume. Treating a
		// non-positive count as "retain everything" would silently turn those jobs
		// into no-ops.
		nts := []NameWithTimestamp{
			{Name: "a", Timestamp: base},
			{Name: "b", Timestamp: base.Add(24 * time.Hour)},
		}

		expired := filterExpiredItems(nts, 0, 0, longhorn.RecurringJobRetentionPolicyCountBased, now)

		assert.ElementsMatch(t, []string{"a", "b"}, expired)
	})

	t.Run("retain_count_equals_len_returns_empty", func(t *testing.T) {
		nts := []NameWithTimestamp{
			{Name: "a", Timestamp: base},
			{Name: "b", Timestamp: base.Add(24 * time.Hour)},
		}

		expired := filterExpiredItems(nts, 2, 0, longhorn.RecurringJobRetentionPolicyCountBased, now)

		assert.Empty(t, expired)
	})

	t.Run("retain_count_greater_than_len_returns_empty", func(t *testing.T) {
		nts := []NameWithTimestamp{
			{Name: "a", Timestamp: base},
		}

		expired := filterExpiredItems(nts, 5, 0, longhorn.RecurringJobRetentionPolicyCountBased, now)

		assert.Empty(t, expired)
	})

	t.Run("zero_timestamp_sorts_first", func(t *testing.T) {
		// A zero time.Time sorts before any real timestamp, so an entry with a
		// zero Timestamp is always treated as the oldest and pruned first. For
		// system backups this is intentional: Error backups carry a zero
		// Status.CreatedAt and must be pruned ahead of successful (Ready) ones.
		nts := []NameWithTimestamp{
			{Name: "newest-real", Timestamp: base.Add(48 * time.Hour)},
			{Name: "older-real", Timestamp: base},
			{Name: "zero-time", Timestamp: time.Time{}},
		}

		expired := filterExpiredItems(nts, 2, 0, longhorn.RecurringJobRetentionPolicyCountBased, now)

		// Zero timestamp sorts to position 0 — gets pruned.
		assert.Equal(t, []string{"zero-time"}, expired)
	})
}

// TestFilterExpiredItemsByAge covers age-based retention (longhorn/longhorn#12060)
// under the "age-based" policy. The rule under test: an item is expired when it has
// existed for longer than retainAge as of now, and the retain count is not consulted
// at all. The window is rolling — measured back from the moment the job runs — not a
// fixed cutoff instant, so the same item can be kept by one run and deleted by the
// next. See TestFilterExpiredItemsByRetentionPolicy for the choice between this
// policy and "count-based".
func TestFilterExpiredItemsByAge(t *testing.T) {
	base := time.Date(2026, 5, 20, 1, 0, 0, 0, time.UTC)
	// a < b < c < d, one day apart.
	items := func() []NameWithTimestamp {
		return []NameWithTimestamp{
			{Name: "a", Timestamp: base},
			{Name: "b", Timestamp: base.Add(24 * time.Hour)},
			{Name: "c", Timestamp: base.Add(48 * time.Hour)},
			{Name: "d", Timestamp: base.Add(72 * time.Hour)},
		}
	}
	// Runs one hour after the newest item, so the ages are a=73h, b=49h, c=25h,
	// d=1h.
	now := base.Add(73 * time.Hour)

	t.Run("worked_example_from_the_issue", func(t *testing.T) {
		// The concrete case the feature was specified against: a backup taken at
		// 07:50 with retainAge 10m must be deleted once the job runs at 08:01,
		// because it has existed for 11m. retain is 10 and there is a single
		// backup, so this only passes if age alone can expire an item.
		created := time.Date(2026, 7, 28, 7, 50, 0, 0, time.UTC)
		runAt := time.Date(2026, 7, 28, 8, 1, 0, 0, time.UTC)

		expired := filterExpiredItems(
			[]NameWithTimestamp{{Name: "backup-1", Timestamp: created}}, 10, 10*time.Minute, longhorn.RecurringJobRetentionPolicyAgeBased, runAt)

		assert.Equal(t, []string{"backup-1"}, expired)
	})

	t.Run("same_item_survives_an_earlier_run", func(t *testing.T) {
		// The mirror of the case above, and the reason retainAge cannot be stored
		// as a fixed instant: at 07:59 the same backup is only 9m old and must be
		// kept. The verdict depends on when the job runs.
		created := time.Date(2026, 7, 28, 7, 50, 0, 0, time.UTC)
		runAt := time.Date(2026, 7, 28, 7, 59, 0, 0, time.UTC)

		expired := filterExpiredItems(
			[]NameWithTimestamp{{Name: "backup-1", Timestamp: created}}, 10, 10*time.Minute, longhorn.RecurringJobRetentionPolicyAgeBased, runAt)

		assert.Empty(t, expired)
	})

	t.Run("deletes_the_items_past_the_window", func(t *testing.T) {
		// The whole point of the feature: enforce a time-based policy even when a
		// count threshold would never be reached (e.g. backups generated rarely).
		// 37h window: a (73h) and b (49h) are over it; c (25h) and d (1h) are not.
		expired := filterExpiredItems(items(), 10, 37*time.Hour, longhorn.RecurringJobRetentionPolicyAgeBased, now)
		assert.ElementsMatch(t, []string{"a", "b"}, expired)
	})

	t.Run("being_within_the_retain_count_does_not_protect_an_old_item", func(t *testing.T) {
		// retain=4 would keep all four by count. Under age-based the count is not
		// read at all, so the items past the window still go.
		expired := filterExpiredItems(items(), 4, 37*time.Hour, longhorn.RecurringJobRetentionPolicyAgeBased, now)
		assert.ElementsMatch(t, []string{"a", "b"}, expired)
	})

	t.Run("being_beyond_the_retain_count_does_not_expire_a_young_item", func(t *testing.T) {
		// The other direction: retain=1 puts a, b and c beyond the count, but a
		// 100h window covers every item. Nothing expires, because surplus by count
		// is not a reason to delete under this policy.
		expired := filterExpiredItems(items(), 1, 100*time.Hour, longhorn.RecurringJobRetentionPolicyAgeBased, now)
		assert.Empty(t, expired)
	})

	t.Run("window_shorter_than_every_item_deletes_everything", func(t *testing.T) {
		// No floor: when every item is older than the window all of them go, even
		// with retain=2 set. An operator who must always keep something to restore
		// from wants count-based, not a smaller window.
		expired := filterExpiredItems(items(), 2, 30*time.Minute, longhorn.RecurringJobRetentionPolicyAgeBased, now)
		assert.ElementsMatch(t, []string{"a", "b", "c", "d"}, expired)
	})

	t.Run("window_longer_than_every_item_deletes_nothing", func(t *testing.T) {
		// Window wider than the oldest item's age, so nothing has expired yet.
		expired := filterExpiredItems(items(), 10, 100*time.Hour, longhorn.RecurringJobRetentionPolicyAgeBased, now)
		assert.Empty(t, expired)
	})

	t.Run("item_exactly_at_the_window_is_kept", func(t *testing.T) {
		// Boundary: the comparison is strictly greater-than, so an item whose age
		// exactly equals retainAge is not yet "over" it and must be retained. Here
		// a is exactly 73h old; only a strictly older item would expire.
		expired := filterExpiredItems(items(), 10, 73*time.Hour, longhorn.RecurringJobRetentionPolicyAgeBased, now)
		assert.Empty(t, expired,
			"an item whose age equals retainAge has not exceeded the window")
	})

	t.Run("zero_age_expires_nothing", func(t *testing.T) {
		// A zero window is an unconfigured job, not an instruction to delete
		// everything. The webhook rejects the combination, but if one ever reaches
		// the helper it must fail towards keeping data.
		expired := filterExpiredItems(items(), 2, 0, longhorn.RecurringJobRetentionPolicyAgeBased, now)
		assert.Empty(t, expired,
			"a non-positive window must not be read as 'every item is past it'")
	})

	t.Run("negative_age_expires_nothing", func(t *testing.T) {
		// Same reasoning as above; a negative window would otherwise put the
		// boundary in the future and make every item "over age".
		expired := filterExpiredItems(items(), 10, -1*time.Hour, longhorn.RecurringJobRetentionPolicyAgeBased, now)
		assert.Empty(t, expired)
	})
}

// TestFilterExpiredItemsByRetentionPolicy covers spec.retentionPolicy as a selector
// between two independent retention modes: "count-based" cleans up by retain and never
// reads retainAge, "age-based" cleans up by retainAge and never reads retain. Exactly
// one bound is ever in force, so the field the policy does not select must have no
// effect at all — every case below sets both fields to values that disagree, which is
// the only way that claim is observable.
func TestFilterExpiredItemsByRetentionPolicy(t *testing.T) {
	base := time.Date(2026, 5, 20, 1, 0, 0, 0, time.UTC)
	items := func() []NameWithTimestamp {
		return []NameWithTimestamp{
			{Name: "a", Timestamp: base},
			{Name: "b", Timestamp: base.Add(24 * time.Hour)},
			{Name: "c", Timestamp: base.Add(48 * time.Hour)},
			{Name: "d", Timestamp: base.Add(72 * time.Hour)},
		}
	}
	// Ages as of now: a=73h, b=49h, c=25h, d=1h.
	now := base.Add(73 * time.Hour)

	t.Run("the_same_spec_gives_different_results_per_policy", func(t *testing.T) {
		// retain=3 keeps the newest three; a 13h window keeps only d. One spec, two
		// answers — proof that the policy, not the field values, decides.
		byCount := filterExpiredItems(items(), 3, 13*time.Hour, longhorn.RecurringJobRetentionPolicyCountBased, now)
		byAge := filterExpiredItems(items(), 3, 13*time.Hour, longhorn.RecurringJobRetentionPolicyAgeBased, now)

		assert.ElementsMatch(t, []string{"a"}, byCount)
		assert.ElementsMatch(t, []string{"a", "b", "c"}, byAge)
	})

	t.Run("count_based_ignores_the_window_even_when_every_item_is_past_it", func(t *testing.T) {
		// Every item is past a 30m window, so a job that consulted the age would
		// delete all four and leave nothing to restore from. count-based must keep
		// the newest two regardless.
		expired := filterExpiredItems(items(), 2, 30*time.Minute, longhorn.RecurringJobRetentionPolicyCountBased, now)
		assert.ElementsMatch(t, []string{"a", "b"}, expired,
			"retainAge must have no effect under count-based")
	})

	t.Run("count_based_with_no_window_is_the_pre_feature_behavior", func(t *testing.T) {
		// Regression guard for the upgrade path: retainAge unset plus the default
		// policy has to delete exactly what the old count-only code deleted.
		expired := filterExpiredItems(items(), 2, 0, longhorn.RecurringJobRetentionPolicyCountBased, now)
		assert.ElementsMatch(t, []string{"a", "b"}, expired)
	})

	t.Run("age_based_ignores_a_retain_count_that_would_keep_everything", func(t *testing.T) {
		// retain=4 covers all four items, so a count-based job would delete none.
		// age-based does not read it and prunes by the 13h window instead.
		expired := filterExpiredItems(items(), 4, 13*time.Hour, longhorn.RecurringJobRetentionPolicyAgeBased, now)
		assert.ElementsMatch(t, []string{"a", "b", "c"}, expired,
			"retain must have no effect under age-based")
	})

	t.Run("empty_policy_behaves_as_count-based", func(t *testing.T) {
		// Jobs created before the field existed carry no policy, and the CRD
		// default is count-based. They must keep deleting by count alone; reading
		// the (unset, therefore zero) retainAge instead would stop them cleaning up
		// entirely.
		expired := filterExpiredItems(items(), 2, 30*time.Minute, "", now)
		assert.ElementsMatch(t, []string{}, expired)
	})

	t.Run("unrecognized_policy_behaves_as_count_based", func(t *testing.T) {
		// The helper only special-cases age-based, so anything else lands on the
		// default. Falling back to the pre-feature behavior is the conservative
		// choice; validateRetentionPolicy rejects unknown values at admission so
		// this fallback is never what a user actually gets.
		expired := filterExpiredItems(items(), 2, 30*time.Minute, longhorn.RecurringJobRetentionPolicy("age_based"), now)
		assert.ElementsMatch(t, []string{}, expired)
	})
}
