package v113xto1140

import (
	"testing"

	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// TestUpdateNodesStatusBackfillsDiskInitializedCondition covers a node that is down during the upgrade: its manager
// does not sync the disks, so the upgrade is the only place its existing disks get the Initialized condition.
func TestUpdateNodesStatusBackfillsDiskInitializedCondition(t *testing.T) {
	node := &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Status: longhorn.NodeStatus{
			DiskStatus: map[string]*longhorn.DiskStatus{
				"recorded-disk": {
					DiskUUID: "recorded-disk-uuid",
					DiskPath: "/var/lib/longhorn",
					Conditions: []longhorn.Condition{
						{Type: longhorn.DiskConditionTypeReady, Status: longhorn.ConditionStatusFalse,
							Reason: string(longhorn.DiskConditionReasonNodeNotReady)},
					},
				},
				"new-disk": {DiskPath: "/mnt/new"},
				// Already has the condition, e.g. the upgrade runs again or the node controller got there first.
				"synced-disk": {
					DiskPath: "/mnt/synced",
					Conditions: []longhorn.Condition{
						{Type: longhorn.DiskConditionTypeInitialized, Status: longhorn.ConditionStatusTrue,
							Reason: "keep-reason", Message: "keep-message"},
					},
				},
				"nil-disk": nil,
			},
		},
	}
	resourceMaps := map[string]interface{}{
		types.LonghornKindNode: map[string]*longhorn.Node{node.Name: node},
	}

	require.NoError(t, UpgradeResourcesStatus("longhorn-system", nil, nil, resourceMaps))

	recorded := node.Status.DiskStatus["recorded-disk"]
	initialized := types.GetCondition(recorded.Conditions, longhorn.DiskConditionTypeInitialized)
	require.Equal(t, longhorn.ConditionStatusTrue, initialized.Status)
	require.Equal(t, "", initialized.Reason)
	require.Equal(t, "Disk recorded-disk(/var/lib/longhorn) on node node-1 is initialized with disk UUID recorded-disk-uuid", initialized.Message)
	// The backfill must not touch Ready, which the node controller owns.
	ready := types.GetCondition(recorded.Conditions, longhorn.DiskConditionTypeReady)
	require.Equal(t, longhorn.ConditionStatusFalse, ready.Status)
	require.Equal(t, string(longhorn.DiskConditionReasonNodeNotReady), ready.Reason)

	uninitialized := types.GetCondition(node.Status.DiskStatus["new-disk"].Conditions, longhorn.DiskConditionTypeInitialized)
	require.Equal(t, longhorn.ConditionStatusFalse, uninitialized.Status)
	require.Equal(t, string(longhorn.DiskConditionReasonDiskUninitialized), uninitialized.Reason)
	require.Equal(t, "Disk new-disk(/mnt/new) on node node-1 has no disk UUID recorded yet", uninitialized.Message)

	// A disk that already has the condition is left alone, even though its UUID is empty.
	synced := types.GetCondition(node.Status.DiskStatus["synced-disk"].Conditions, longhorn.DiskConditionTypeInitialized)
	require.Equal(t, longhorn.ConditionStatusTrue, synced.Status)
	require.Equal(t, "keep-reason", synced.Reason)
	require.Equal(t, "keep-message", synced.Message)

	require.Nil(t, node.Status.DiskStatus["nil-disk"])
}
