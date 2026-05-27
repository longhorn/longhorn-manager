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

	t.Run("all_status_created_at_set_uses_status", func(t *testing.T) {
		// Happy path: every SystemBackup reached Ready and has Status.CreatedAt
		// populated by the controller. The returned timestamps should match
		// Status.CreatedAt for each entry (not metadata.creationTimestamp).
		list := &longhorn.SystemBackupList{
			Items: []longhorn.SystemBackup{
				newSystemBackup("daily-1", base, base.Add(8*time.Minute), longhorn.SystemBackupStateReady),
				newSystemBackup("daily-2", base.Add(24*time.Hour), base.Add(24*time.Hour+6*time.Minute), longhorn.SystemBackupStateReady),
			},
		}

		got := systemBackupsToNameWithTimestamps(list)

		assert.Len(t, got, 2)
		byName := map[string]time.Time{}
		for _, n := range got {
			byName[n.Name] = n.Timestamp
		}
		assert.Equal(t, base.Add(8*time.Minute), byName["daily-1"], "should use Status.CreatedAt when set")
		assert.Equal(t, base.Add(24*time.Hour+6*time.Minute), byName["daily-2"], "should use Status.CreatedAt when set")
	})

	t.Run("zero_status_created_at_falls_back_to_metadata_creation_timestamp", func(t *testing.T) {
		// Regression test for longhorn/longhorn#13203.
		// A SystemBackup in Error state, or one whose status write has not
		// yet landed when ListSystemBackup runs, has a zero
		// Status.CreatedAt. Without the fallback, this CR would sort to
		// position 0 (year-1 < any real time) in filterExpiredItems, and
		// `retain: N` would prune it instead of an older successful CR.
		errored := newSystemBackup("daily-stuck", base, time.Time{}, longhorn.SystemBackupStateError)
		list := &longhorn.SystemBackupList{Items: []longhorn.SystemBackup{errored}}

		got := systemBackupsToNameWithTimestamps(list)

		if assert.Len(t, got, 1) {
			assert.False(t, got[0].Timestamp.IsZero(),
				"Timestamp must not be zero when Status.CreatedAt is unset; "+
					"otherwise the CR sorts to the front in filterExpiredItems "+
					"and gets pruned ahead of older successful CRs (longhorn/longhorn#13203)")
			assert.Equal(t, base, got[0].Timestamp,
				"should fall back to metadata.creationTimestamp when Status.CreatedAt is zero")
		}
	})

	t.Run("mixed_zero_and_set_status_created_at_sorts_correctly", func(t *testing.T) {
		// End-to-end demonstration that filterExpiredItems pairs correctly
		// with the fallback. With retain=2 and three CRs (two old/Ready, one
		// new/Error), the Error CR must be retained — it is the newest.
		//
		// Pre-fix behavior: the Error CR with zero Status.CreatedAt sorted to
		// position 0 and was pruned, leaving the two older Ready CRs.
		// Post-fix behavior: the Error CR uses its metadata.creationTimestamp
		// (newest), sorts to the end, and is retained — the oldest Ready CR
		// is pruned instead.
		oldReady := newSystemBackup("daily-old-ready",
			base, base.Add(8*time.Minute), longhorn.SystemBackupStateReady)
		midReady := newSystemBackup("daily-mid-ready",
			base.Add(24*time.Hour), base.Add(24*time.Hour+8*time.Minute), longhorn.SystemBackupStateReady)
		newError := newSystemBackup("daily-new-error",
			base.Add(48*time.Hour), time.Time{}, longhorn.SystemBackupStateError)
		list := &longhorn.SystemBackupList{
			Items: []longhorn.SystemBackup{oldReady, midReady, newError},
		}

		expired := filterExpiredItems(systemBackupsToNameWithTimestamps(list), 2)

		assert.Equal(t, []string{"daily-old-ready"}, expired,
			"retain=2 must prune the oldest CR by creation order, not the "+
				"Error CR with zero Status.CreatedAt (longhorn/longhorn#13203)")
	})

	t.Run("empty_list", func(t *testing.T) {
		got := systemBackupsToNameWithTimestamps(&longhorn.SystemBackupList{})
		assert.Empty(t, got)
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

	t.Run("zero_timestamp_sorts_first_demonstrating_pre_fix_bug", func(t *testing.T) {
		// Demonstrates the underlying sort behavior that motivated the fix:
		// a zero time.Time sorts before any real timestamp, so a NameWithTimestamp
		// with zero Timestamp is always considered "oldest" by filterExpiredItems.
		// This is correct for filterExpiredItems in isolation — the bug was in
		// the caller, which fed it zero timestamps for new-but-Error CRs.
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
