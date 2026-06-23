package snapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestCountExistingSnapshots(t *testing.T) {
	now := metav1.Now()

	tests := []struct {
		name      string
		snapshots map[string]*longhorn.Snapshot
		expected  int
	}{
		{
			name: "counts user-created snapshots including in-progress",
			snapshots: map[string]*longhorn.Snapshot{
				"snap-ready":       {Status: longhorn.SnapshotStatus{UserCreated: true, ReadyToUse: true}},
				"snap-in-progress": {Status: longhorn.SnapshotStatus{UserCreated: true, ReadyToUse: false}},
			},
			expected: 2,
		},
		{
			name:      "empty map returns zero",
			snapshots: map[string]*longhorn.Snapshot{},
			expected:  0,
		},
		{
			name: "nil entry is excluded",
			snapshots: map[string]*longhorn.Snapshot{
				"snap-normal": {Status: longhorn.SnapshotStatus{UserCreated: true}},
				"nil-entry":   nil,
			},
			expected: 1,
		},
		{
			name: "snapshot with DeletionTimestamp is excluded",
			snapshots: map[string]*longhorn.Snapshot{
				"snap-normal": {Status: longhorn.SnapshotStatus{UserCreated: true}},
				"snap-deleting": {
					ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now},
					Status:     longhorn.SnapshotStatus{UserCreated: true},
				},
			},
			expected: 1,
		},
		{
			name: "snapshot with MarkRemoved is excluded",
			snapshots: map[string]*longhorn.Snapshot{
				"snap-normal":  {Status: longhorn.SnapshotStatus{UserCreated: true}},
				"snap-removed": {Status: longhorn.SnapshotStatus{UserCreated: true, MarkRemoved: true}},
			},
			expected: 1,
		},
		{
			name: "system snapshot is excluded",
			snapshots: map[string]*longhorn.Snapshot{
				"snap-user":   {Status: longhorn.SnapshotStatus{UserCreated: true}},
				"snap-system": {Status: longhorn.SnapshotStatus{UserCreated: false}},
			},
			expected: 1,
		},
		{
			name: "all nil entries return zero",
			snapshots: map[string]*longhorn.Snapshot{
				"nil-1": nil,
				"nil-2": nil,
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, countExistingSnapshots(tt.snapshots))
		})
	}
}
