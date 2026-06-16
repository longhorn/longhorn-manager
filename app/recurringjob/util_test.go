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
			[]longhorn.SystemBackup{oldReady, midReady, newError}), 2)

		assert.Equal(t, []string{"daily-new-error"}, expired,
			"the Error backup is pruned before either Ready backup")
	})

	t.Run("empty", func(t *testing.T) {
		assert.Empty(t, systemBackupsToNameWithTimestamps(nil))
	})
}

func TestFilterExpiredItems(t *testing.T) {
	base := time.Date(2026, 5, 20, 1, 0, 0, 0, time.UTC)

	t.Run("retains_n_newest", func(t *testing.T) {
		nts := []NameWithTimestamp{
			{Name: "a", Timestamp: base},
			{Name: "b", Timestamp: base.Add(24 * time.Hour)},
			{Name: "c", Timestamp: base.Add(48 * time.Hour)},
			{Name: "d", Timestamp: base.Add(72 * time.Hour)},
		}

		expired := filterExpiredItems(nts, 2)

		// Expect the two oldest (a, b) to be pruned; c and d retained.
		assert.ElementsMatch(t, []string{"a", "b"}, expired)
	})

	t.Run("retain_count_equals_len_returns_empty", func(t *testing.T) {
		nts := []NameWithTimestamp{
			{Name: "a", Timestamp: base},
			{Name: "b", Timestamp: base.Add(24 * time.Hour)},
		}

		expired := filterExpiredItems(nts, 2)

		assert.Empty(t, expired)
	})

	t.Run("retain_count_greater_than_len_returns_empty", func(t *testing.T) {
		nts := []NameWithTimestamp{
			{Name: "a", Timestamp: base},
		}

		expired := filterExpiredItems(nts, 5)

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

		expired := filterExpiredItems(nts, 2)

		// Zero timestamp sorts to position 0 — gets pruned.
		assert.Equal(t, []string{"zero-time"}, expired)
	})
}
