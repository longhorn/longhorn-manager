package runner

import (
	"context"

	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// Store is the executor's view of the cluster. Reads that decide whether
// a command runs go to the API server, because a stale answer could run
// or skip a command in the wrong pod. The rest may come from cache: those
// methods carry the RO suffix, take no ctx, and return objects that must
// not be mutated.
type Store interface {
	// Namespace is the Longhorn namespace.
	Namespace() string
	// GetSnapshotGroup reads the group live.
	GetSnapshotGroup(ctx context.Context, name string) (*longhorn.SnapshotGroup, error)
	// UpdateSnapshotGroupStatus writes status only and returns the stored
	// object. A Conflict error is returned as is.
	UpdateSnapshotGroupStatus(ctx context.Context, group *longhorn.SnapshotGroup) (*longhorn.SnapshotGroup, error)
	// ListSnapshotGroupsRO lists every group from cache.
	ListSnapshotGroupsRO() ([]*longhorn.SnapshotGroup, error)
	// GetVolumeRO reads the volume from cache.
	GetVolumeRO(name string) (*longhorn.Volume, error)
	// GetVolumeHookPolicy reads the policy live.
	GetVolumeHookPolicy(ctx context.Context, namespace, name string) (*longhorn.VolumeHookPolicy, error)
	// ListPods lists the pods of a namespace live. The API server cannot
	// filter pods by the PVC they mount, so the caller filters.
	ListPods(ctx context.Context, namespace string) ([]corev1.Pod, error)
	// FindPod reads one pod live. It uses a LIST filtered by name rather
	// than a GET, so the executor's RBAC role only needs list on pods. A
	// pod that does not exist is nil, nil.
	FindPod(ctx context.Context, namespace, name string) (*corev1.Pod, error)
	// GetSettingAsInt reads a setting, falling back to its default when the
	// Setting object does not exist.
	GetSettingAsInt(name types.SettingName) (int64, error)
}
