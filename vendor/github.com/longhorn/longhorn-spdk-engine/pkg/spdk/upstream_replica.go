package spdk

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// replicaUpstream is the Upstream for RAID1-mode engines. Each one wraps a
// single Replica process; the engine holds one per replica backing the volume.
//
// mu guards the mutable fields (Mode, BdevName): the engine's reconciliation
// loop writes them (e.g., marking ERR on an RPC failure) while other methods
// read them concurrently. Write them through SetMode/SetBdevName, not directly.
//
// No long-lived gRPC client is held: each RPC method opens a fresh SPDK client
// for the call and closes it on return. This needs no reconnect logic - if the
// replica restarts, the next call just dials its current address.
type replicaUpstream struct {
	mu sync.RWMutex

	name    string // immutable; replica name (= upstream key in Engine.upstreams)
	address string // mutable: ip:port of the replica's NVMe-oF target; engine writes this on rebuild (replica gets a new port)

	mode     types.Mode
	bdevName string

	newServiceClient ServiceClientFactory
}

// newReplicaUpstream constructs a replicaUpstream. Initial mode/bdevName are
// the engine's responsibility to populate after connectNVMfBdev succeeds.
func newReplicaUpstream(name, address string, newServiceClient ServiceClientFactory) *replicaUpstream {
	return &replicaUpstream{
		name:             name,
		address:          address,
		mode:             types.ModeWO, // default until validated; engine flips to RW after connectNVMfBdev
		newServiceClient: newServiceClient,
	}
}

func (u *replicaUpstream) Name() string { return u.name }

func (u *replicaUpstream) Address() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.address
}

// SetAddress is replicaUpstream-specific (not on the Upstream interface). The
// engine calls this when a replica is rebuilt/replaced and gets a new
// transport address. shardGroupUpstream does not need this - the ShardGroup
// process address is set once at engine start and tracked via reconcile, not
// updated per-call.
func (u *replicaUpstream) SetAddress(address string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.address = address
}

func (u *replicaUpstream) Mode() types.Mode {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.mode
}

func (u *replicaUpstream) SetMode(m types.Mode) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.mode = m
}

func (u *replicaUpstream) BdevName() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.bdevName
}

func (u *replicaUpstream) SetBdevName(name string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.bdevName = name
}

// String makes %+v print the useful fields. Without it, the default would
// reflect through the unexported mutex and func pointer, which is noise.
func (u *replicaUpstream) String() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return fmt.Sprintf("{Name:%s Address:%s Mode:%s BdevName:%s}", u.name, u.address, u.mode, u.bdevName)
}

// Get issues ReplicaGet against the replica's SPDK service and adapts the
// response into the topology-agnostic UpstreamView the engine consumes.
func (u *replicaUpstream) Get() (*UpstreamView, error) {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get service client for replica %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)

	r, err := serviceClient.ReplicaGet(u.name)
	if err != nil {
		return nil, err
	}
	return &UpstreamView{
		SpecSize:         r.SpecSize,
		ActualSize:       r.ActualSize,
		Head:             r.Head,
		Snapshots:        r.Snapshots,
		BackingImageName: r.BackingImageName,
		LvsUUID:          r.LvsUUID,
	}, nil
}

func (u *replicaUpstream) SnapshotCreate(snapshotName string, opts *api.SnapshotOptions) error {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return errors.Wrapf(err, "failed to get service client for replica %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)
	return serviceClient.ReplicaSnapshotCreate(u.name, snapshotName, opts)
}

func (u *replicaUpstream) SnapshotDelete(snapshotName string) error {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return errors.Wrapf(err, "failed to get service client for replica %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)
	return serviceClient.ReplicaSnapshotDelete(u.name, snapshotName)
}

func (u *replicaUpstream) SnapshotRevert(snapshotName string) error {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return errors.Wrapf(err, "failed to get service client for replica %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)
	return serviceClient.ReplicaSnapshotRevert(u.name, snapshotName)
}

func (u *replicaUpstream) SnapshotPurge() error {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return errors.Wrapf(err, "failed to get service client for replica %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)
	return serviceClient.ReplicaSnapshotPurge(u.name)
}

func (u *replicaUpstream) SnapshotHash(snapshotName string, rehash bool) error {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return errors.Wrapf(err, "failed to get service client for replica %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)
	return serviceClient.ReplicaSnapshotHash(u.name, snapshotName, rehash)
}

func (u *replicaUpstream) BackingImageGet(name, lvsUUID string) (*api.BackingImage, error) {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get service client for replica %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)
	return serviceClient.BackingImageGet(name, lvsUUID)
}

// Compile-time check that replicaUpstream satisfies the Upstream interface.
var _ Upstream = (*replicaUpstream)(nil)
