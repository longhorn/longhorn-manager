package snapshothook

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// fullyPopulatedStatus has every field set to a non-zero value, so a copy
// can be checked field by field. Adding a status field without extending it
// fails TestStatusFieldOwnership, which is the point.
func fullyPopulatedStatus() longhorn.SnapshotGroupStatus {
	now := metav1.Now()
	return longhorn.SnapshotGroupStatus{
		OwnerID:         "node-1",
		Phase:           longhorn.SnapshotGroupPhaseInProgress,
		Members:         []longhorn.SnapshotGroupMemberStatus{{SnapshotName: "g-1"}},
		ReadyToUse:      true,
		CreationTime:    now.Format(metav1.RFC3339Micro),
		Error:           "err",
		Conditions:      []longhorn.Condition{{Type: "Degraded"}},
		PreHooksDone:    true,
		HooksStartedAt:  &now,
		PreHooksDoneAt:  &now,
		PostHooksDoneAt: &now,
		HookProgress:    "pre done",
		Hooks:           []longhorn.HookStatus{{SnapshotName: "g-1"}},
	}
}

func TestStatusFieldOwnership(t *testing.T) {
	owner := map[string]string{}
	for _, name := range controllerOwnedStatusFields {
		owner[name] = "controller"
	}
	for _, name := range executorOwnedStatusFields {
		_, dup := owner[name]
		require.Falsef(t, dup, "field %v is in both owner lists", name)
		owner[name] = "executor"
	}

	statusType := reflect.TypeFor[longhorn.SnapshotGroupStatus]()
	for i := 0; i < statusType.NumField(); i++ {
		name := statusType.Field(i).Name
		_, listed := owner[name]
		assert.Truef(t, listed, "status field %v has no owner; add it to one list in ownership.go", name)
	}
	for name := range owner {
		_, exists := statusType.FieldByName(name)
		assert.Truef(t, exists, "owner list names %v, which SnapshotGroupStatus does not have", name)
	}

	// AdoptExecutorStatus copies exactly the executor-owned fields.
	src := fullyPopulatedStatus()
	full := reflect.ValueOf(src)
	for i := 0; i < statusType.NumField(); i++ {
		require.Falsef(t, full.Field(i).IsZero(), "fullyPopulatedStatus leaves %v zero", statusType.Field(i).Name)
	}
	var dst longhorn.SnapshotGroupStatus
	AdoptExecutorStatus(&dst, &src)
	copied := reflect.ValueOf(dst)
	for i := 0; i < statusType.NumField(); i++ {
		name := statusType.Field(i).Name
		switch owner[name] {
		case "executor":
			assert.Falsef(t, copied.Field(i).IsZero(), "AdoptExecutorStatus did not copy executor-owned %v", name)
		default:
			assert.Truef(t, copied.Field(i).IsZero(), "AdoptExecutorStatus copied controller-owned %v", name)
		}
	}
}
