package spdk

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// shardGroupUpstream is the Upstream for EC-mode engines. The engine holds
// exactly one: the ShardGroup process owns the lvstore and head lvol on
// bdev_ec, so the engine sees a single base bdev no matter how many shards
// are behind it.
//
// mu guards Mode and BdevName: the engine's reconciliation loop writes them
// (e.g., marking ERR on an RPC failure) while other methods read them. The
// address is set when the upstream is created and never changes, so nothing
// guards or rewrites it; unlike a replica, a ShardGroup is never rebuilt with
// a new port, which is why replicaUpstream has SetAddress and this type does
// not. As with replicaUpstream, no long-lived gRPC client is held; each RPC
// method opens a fresh SPDK client for the call and closes it on return. The
// client dials the ShardGroup's IM-pod gRPC port, derived from its NVMe-oF
// transport address.
//
// The delete-path invariant applies here too: this type implements no Delete
// method. EC teardown lives in the ShardGroup process, gated by its own
// cleanupRequired flag.
type shardGroupUpstream struct {
	mu sync.RWMutex

	name    string // immutable; ShardGroup name (= upstream key in Engine.upstreams)
	address string // ip:port of the ShardGroup process's NVMe-oF target

	mode     types.Mode
	bdevName string

	newServiceClient ServiceClientFactory
}

// newShardGroupUpstream constructs a shardGroupUpstream. Mode defaults to WO
// until the engine flips it to RW after connectNVMfBdev succeeds, mirroring
// replicaUpstream's lifecycle.
func newShardGroupUpstream(name, address string, newServiceClient ServiceClientFactory) *shardGroupUpstream {
	return &shardGroupUpstream{
		name:             name,
		address:          address,
		mode:             types.ModeWO,
		newServiceClient: newServiceClient,
	}
}

func (u *shardGroupUpstream) Name() string { return u.name }

func (u *shardGroupUpstream) Address() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.address
}

func (u *shardGroupUpstream) Mode() types.Mode {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.mode
}

func (u *shardGroupUpstream) SetMode(m types.Mode) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.mode = m
}

func (u *shardGroupUpstream) BdevName() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.bdevName
}

func (u *shardGroupUpstream) SetBdevName(name string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.bdevName = name
}

// String makes %+v print the useful fields. Without it, the default would
// reflect through the unexported mutex, which is noise.
func (u *shardGroupUpstream) String() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return fmt.Sprintf("{Name:%s Address:%s Mode:%s BdevName:%s}", u.name, u.address, u.mode, u.bdevName)
}

// Get issues a single ShardGroupGet against the ShardGroup process and adapts
// the response into the topology-agnostic UpstreamView. The proto carries
// SpecSize/ActualSize/Head/Snapshots populated by refreshECSnapshotMapNoLock
// against the ShardGroup process's local lvol store, so the engine never has
// to query individual shards.
func (u *shardGroupUpstream) Get() (*UpstreamView, error) {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get service client for shardgroup %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)

	shardGroup, err := serviceClient.ShardGroupGet(u.name)
	if err != nil {
		return nil, err
	}

	snapshots := map[string]*api.Lvol{}
	for snapName, snapProtoLvol := range shardGroup.Snapshots {
		snapshots[snapName] = api.ProtoLvolToLvol(snapProtoLvol)
	}

	return &UpstreamView{
		SpecSize:   shardGroup.SpecSize,
		ActualSize: shardGroup.ActualSize,
		Head:       api.ProtoLvolToLvol(shardGroup.Head),
		Snapshots:  snapshots,
		// EC volumes do not surface backing-image state at the engine layer.
		BackingImageName: "",
		LvsUUID:          shardGroup.LvsUuid,
	}, nil
}

// SnapshotCreate forwards to ShardGroupSnapshotCreate. The lvol-side body
// executes inside the ShardGroup process; the engine-side raid1 head lvol
// UUID is unaffected, so no engine-side bracket is needed.
//
// SnapshotOptions metadata (UserCreated/Timestamp) is intentionally dropped
// in this initial release - ShardGroupSnapshotCreate's RPC does not carry
// these fields. The ShardGroup process records its own timestamp at the
// lvstore layer.
func (u *shardGroupUpstream) SnapshotCreate(snapshotName string, opts *api.SnapshotOptions) error {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return errors.Wrapf(err, "failed to get service client for shardgroup %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)
	_, err = serviceClient.ShardGroupSnapshotCreate(u.name, snapshotName)
	return err
}

func (u *shardGroupUpstream) SnapshotDelete(snapshotName string) error {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return errors.Wrapf(err, "failed to get service client for shardgroup %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)
	return serviceClient.ShardGroupSnapshotDelete(u.name, snapshotName)
}

// SnapshotRevert is the upstream half of the cross-process revert sequence.
// The engine layer brackets this call with raid1 teardown and
// reconnect/raid-recreate; this method issues ShardGroupSnapshotRevert which
// executes the lvol-side body (head delete + clone + namespace swap) inside
// the ShardGroup process.
func (u *shardGroupUpstream) SnapshotRevert(snapshotName string) error {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return errors.Wrapf(err, "failed to get service client for shardgroup %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)
	return serviceClient.ShardGroupSnapshotRevert(u.name, snapshotName)
}

func (u *shardGroupUpstream) SnapshotPurge() error {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return errors.Wrapf(err, "failed to get service client for shardgroup %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)
	return serviceClient.ShardGroupSnapshotPurge(u.name)
}

// SnapshotHash is not supported for EC volumes in the initial release.
// Snapshot content hashing on EC requires reading through bdev_ec to
// reconstruct plaintext data; per-shard hashing would hash erasure-coded
// chunks which is not meaningful.
func (u *shardGroupUpstream) SnapshotHash(snapshotName string, rehash bool) error {
	return grpcstatus.Errorf(grpccodes.Unimplemented, "SnapshotHash is not supported for EC volumes (shardgroup %s)", u.name)
}

// BackingImageGet returns (nil, nil). EC volumes do not surface
// backing-image state at the engine layer; the equivalent lives in the
// ShardGroup process if and when it becomes meaningful.
func (u *shardGroupUpstream) BackingImageGet(name, lvsUUID string) (*api.BackingImage, error) {
	return nil, nil
}

// Compile-time check that shardGroupUpstream satisfies the Upstream interface.
var _ Upstream = (*shardGroupUpstream)(nil)
