package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestIsBackupTargetSyncRequired(t *testing.T) {
	now := time.Now().UTC()

	testCases := map[string]struct {
		backupTarget *longhorn.BackupTarget
		expected     bool
	}{
		"nil backup target": {
			backupTarget: nil,
			expected:     false,
		},
		"empty backup target URL bypasses stale sync gate": {
			backupTarget: &longhorn.BackupTarget{
				Spec: longhorn.BackupTargetSpec{
					BackupTargetURL: "",
					SyncRequestedAt: metav1.Time{Time: now.Add(-2 * time.Minute)},
				},
				Status: longhorn.BackupTargetStatus{
					LastSyncedAt: metav1.Time{Time: now.Add(-1 * time.Minute)},
				},
			},
			expected: true,
		},
		"stale sync request is skipped for configured target": {
			backupTarget: &longhorn.BackupTarget{
				Spec: longhorn.BackupTargetSpec{
					BackupTargetURL: TestBackupTarget,
					SyncRequestedAt: metav1.Time{Time: now.Add(-2 * time.Minute)},
				},
				Status: longhorn.BackupTargetStatus{
					Available:    true,
					LastSyncedAt: metav1.Time{Time: now.Add(-1 * time.Minute)},
				},
			},
			expected: false,
		},
		"newer sync request is processed": {
			backupTarget: &longhorn.BackupTarget{
				Spec: longhorn.BackupTargetSpec{
					BackupTargetURL: TestBackupTarget,
					SyncRequestedAt: metav1.Time{Time: now},
				},
				Status: longhorn.BackupTargetStatus{
					LastSyncedAt: metav1.Time{Time: now.Add(-1 * time.Minute)},
				},
			},
			expected: true,
		},
		"unavailable target with non-empty URL defers to timer": {
			backupTarget: &longhorn.BackupTarget{
				Spec: longhorn.BackupTargetSpec{
					BackupTargetURL: TestBackupTarget,
					SyncRequestedAt: metav1.Time{Time: now.Add(-2 * time.Minute)},
				},
				Status: longhorn.BackupTargetStatus{
					Available:    false,
					LastSyncedAt: metav1.Time{Time: now.Add(-1 * time.Minute)},
				},
			},
			expected: false,
		},
		"first sync after URL transition from empty": {
			backupTarget: &longhorn.BackupTarget{
				Spec: longhorn.BackupTargetSpec{
					BackupTargetURL: TestBackupTarget,
				},
				Status: longhorn.BackupTargetStatus{
					Available: false,
				},
			},
			expected: true,
		},
		"available target with stale sync is skipped": {
			backupTarget: &longhorn.BackupTarget{
				Spec: longhorn.BackupTargetSpec{
					BackupTargetURL: TestBackupTarget,
					SyncRequestedAt: metav1.Time{Time: now.Add(-2 * time.Minute)},
				},
				Status: longhorn.BackupTargetStatus{
					Available:    true,
					LastSyncedAt: metav1.Time{Time: now.Add(-1 * time.Minute)},
				},
			},
			expected: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := isBackupTargetSyncRequired(tc.backupTarget)
			if actual != tc.expected {
				t.Fatalf("unexpected sync requirement: got %v, want %v", actual, tc.expected)
			}
		})
	}
}
