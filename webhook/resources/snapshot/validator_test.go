package snapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// TestSnapshotValidatorUpdateSnapshotGroupLabelImmutable verifies that the
// snapshot-group label cannot be added, changed, or removed on an update: the
// label routes member snapshot events to the owning group.
func TestSnapshotValidatorUpdateSnapshotGroupLabelImmutable(t *testing.T) {
	v := &snapshotValidator{}
	groupLabelKey := types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroup)

	newSnapshot := func(labels map[string]string) *longhorn.Snapshot {
		return &longhorn.Snapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "snap-1", Labels: labels},
			Spec:       longhorn.SnapshotSpec{Volume: "vol-1"},
		}
	}

	testCases := []struct {
		name      string
		oldLabels map[string]string
		newLabels map[string]string
		expectErr bool
	}{
		{"unchanged label is allowed", map[string]string{groupLabelKey: "group-a"}, map[string]string{groupLabelKey: "group-a"}, false},
		{"no label on either side is allowed", nil, nil, false},
		{"changing the label is rejected", map[string]string{groupLabelKey: "group-a"}, map[string]string{groupLabelKey: "group-b"}, true},
		{"removing the label is rejected", map[string]string{groupLabelKey: "group-a"}, nil, true},
		{"adding the label is rejected", nil, map[string]string{groupLabelKey: "group-a"}, true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := v.Update(nil, newSnapshot(tc.oldLabels), newSnapshot(tc.newLabels))
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
