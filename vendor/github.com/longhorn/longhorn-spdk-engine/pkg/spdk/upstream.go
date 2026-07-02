package spdk

import (
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// Upstream represents one peer SPDK process - a replica or a shardgroup -
// that provides a base bdev for the engine's bdev_raid1. The engine talks to
// that peer only through this interface.
//
// Name and Address are read-only; Mode and BdevName are local getters/setters.
// None make RPC. The other methods call the peer's SPDK service, each opening
// a fresh client for the call.
//
// **Delete-path invariant.** This interface declares NO Delete() method by
// design. Engine teardown calls bdev_raid_delete + bdev_nvme_detach_controller
// directly without going through Upstream. Adding a Delete method here would
// open a layout-aware path through the engine's cleanup code - exactly the
// class of bug the EC volume detach data-loss issue exposed. Any future
// upstream-aware cleanup needs to add the method deliberately and review the
// blast radius.
type Upstream interface {
	// Identity (immutable for the life of the upstream).
	Name() string
	Address() string

	// Mutable state. The engine reads/writes these directly during its
	// reconciliation loop (e.g., marking ERR on RPC failure during
	// ValidateAndUpdate, recording the local nvme bdev name after
	// connectNVMfBdev).
	Mode() types.Mode
	SetMode(m types.Mode)
	BdevName() string
	SetBdevName(name string)

	// RPC methods.

	// Get returns a snapshot of the upstream's current state. For
	// replicaUpstream this issues ReplicaGet; for shardGroupUpstream it
	// issues ShardGroupGet (the engine never queries individual shards).
	Get() (*UpstreamView, error)

	// Snapshot operations forward to the upstream's snapshot RPCs.
	SnapshotCreate(snapshotName string, opts *api.SnapshotOptions) error
	SnapshotDelete(snapshotName string) error
	SnapshotRevert(snapshotName string) error
	SnapshotPurge() error
	SnapshotHash(snapshotName string, rehash bool) error

	// BackingImageGet resolves a backing-image record on the upstream.
	// RAID1 dispatches to BackingImageGet on the replica's SPDK service;
	// shardGroupUpstream returns (nil, nil) - EC volumes do not surface
	// backing-image state at the engine layer.
	BackingImageGet(name, lvsUUID string) (*api.BackingImage, error)
}

// UpstreamFactory builds an Upstream for a given (name, address) pair. The
// engine receives the factory at Create time from the server layer, which
// picks newReplicaUpstream or newShardGroupUpstream keyed on
// EngineCreateRequest.DataLayoutType. The engine itself never reads the
// layout - it just calls the factory once per replica_address_map entry.
//
// This is the only place layout enters the engine on the create path - no
// dataLayoutType field on Engine, no `if dataLayout == ...` branches in
// engine.go. The rebuild path (replicaAddStart) is separate: it constructs
// newReplicaUpstream directly and only handles RAID1; EC engines are
// rejected at the server boundary by isShardedEngine.
type UpstreamFactory func(name, address string) Upstream

// UpstreamView is the engine's view of an upstream's current state, returned
// by Upstream.Get(). Both implementations populate the same fields from their
// respective RPC responses (ReplicaGet vs. ShardGroupGet); the engine reads
// the unified shape without knowing which topology produced it.
type UpstreamView struct {
	SpecSize         uint64
	ActualSize       uint64
	Head             *api.Lvol
	Snapshots        map[string]*api.Lvol
	BackingImageName string
	LvsUUID          string
}

// isUpstreamDispatchable reports whether the engine should issue a per-upstream
// RPC to u: it must be RW or WO and have a known address.
func isUpstreamDispatchable(u Upstream) bool {
	return (u.Mode() == types.ModeRW || u.Mode() == types.ModeWO) && u.Address() != ""
}

// closeServiceClient closes a per-call SPDK service client opened by
// GetServiceClient, logging any error. name identifies the upstream.
func closeServiceClient(serviceClient *client.SPDKClient, name string) {
	if err := serviceClient.Close(); err != nil {
		logrus.WithError(err).Warnf("Failed to close SPDK service client for %s", name)
	}
}
