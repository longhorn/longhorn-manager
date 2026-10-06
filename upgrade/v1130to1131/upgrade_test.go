package v1130to1131

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
	// The backfill must not touch Ready, which the node controller owns.
	ready := types.GetCondition(recorded.Conditions, longhorn.DiskConditionTypeReady)
	require.Equal(t, longhorn.ConditionStatusFalse, ready.Status)
	require.Equal(t, string(longhorn.DiskConditionReasonNodeNotReady), ready.Reason)

	uninitialized := types.GetCondition(node.Status.DiskStatus["new-disk"].Conditions, longhorn.DiskConditionTypeInitialized)
	require.Equal(t, longhorn.ConditionStatusFalse, uninitialized.Status)
	require.Equal(t, string(longhorn.DiskConditionReasonDiskUninitialized), uninitialized.Reason)

	require.Nil(t, node.Status.DiskStatus["nil-disk"])
}
