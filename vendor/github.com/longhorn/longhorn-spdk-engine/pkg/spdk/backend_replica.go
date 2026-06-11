package spdk

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// replicaBackend is the Backend for RAID1-mode engines. Each one wraps a
// single Replica process; the engine holds one per replica backing the volume.
//
// mu guards the mutable fields (Mode, BdevName): the engine's reconciliation
// loop writes them (e.g., marking ERR on an RPC failure) while other methods
// read them concurrently. Write them through SetMode/SetBdevName, not directly.
//
// No long-lived gRPC client is held: each RPC method opens a fresh SPDK client
// for the call and closes it on return. This needs no reconnect logic - if the
// replica restarts, the next call just dials its current address.
type replicaBackend struct {
	mu sync.RWMutex

	name    string // immutable; replica name (= backend key in Engine.backends)
	address string // mutable: ip:port of the replica's NVMe-oF target; engine writes this on rebuild (replica gets a new port)

	mode     types.Mode
	bdevName string

	newServiceClient ServiceClientFactory
}

// newReplicaBackend constructs a replicaBackend. Initial mode/bdevName are
// the engine's responsibility to populate after connectNVMfBdev succeeds.
func newReplicaBackend(name, address string, newServiceClient ServiceClientFactory) *replicaBackend {
	return &replicaBackend{
		name:             name,
		address:          address,
		mode:             types.ModeWO, // default until validated; engine flips to RW after connectNVMfBdev
		newServiceClient: newServiceClient,
	}
}

func (u *replicaBackend) Name() string { return u.name }

func (u *replicaBackend) Address() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.address
}

// SetAddress is replicaBackend-specific (not on the Backend interface). The
// engine calls this when a replica is rebuilt/replaced and gets a new
// transport address. shardGroupBackend does not need this - the ShardGroup
// process address is set once at engine start and tracked via reconcile, not
// updated per-call.
func (u *replicaBackend) SetAddress(address string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.address = address
}

func (u *replicaBackend) Mode() types.Mode {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.mode
}

func (u *replicaBackend) SetMode(m types.Mode) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.mode = m
}

func (u *replicaBackend) BdevName() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.bdevName
}

func (u *replicaBackend) SetBdevName(name string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.bdevName = name
}

// String makes %+v print the useful fields. Without it, the default would
// reflect through the unexported mutex and func pointer, which is noise.
func (u *replicaBackend) String() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return fmt.Sprintf("{Name:%s Address:%s Mode:%s BdevName:%s}", u.name, u.address, u.mode, u.bdevName)
}

// Get issues ReplicaGet against the replica's SPDK service and adapts the
// response into the topology-agnostic BackendView the engine consumes.
func (u *replicaBackend) Get() (*BackendView, error) {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get service client for replica %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)

	r, err := serviceClient.ReplicaGet(u.name)
	if err != nil {
		return nil, err
	}
	return &BackendView{
		SpecSize:         r.SpecSize,
		ActualSize:       r.ActualSize,
		Head:             r.Head,
		Snapshots:        r.Snapshots,
		BackingImageName: r.BackingImageName,
		LvsUUID:          r.LvsUUID,
	}, nil
}

func (u *replicaBackend) SnapshotCreate(snapshotName string, opts *api.SnapshotOptions) error {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return errors.Wrapf(err, "failed to get service client for replica %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)
	return serviceClient.ReplicaSnapshotCreate(u.name, snapshotName, opts)
}

func (u *replicaBackend) SnapshotDelete(snapshotName string) error {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return errors.Wrapf(err, "failed to get service client for replica %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)
	return serviceClient.ReplicaSnapshotDelete(u.name, snapshotName)
}

func (u *replicaBackend) SnapshotRevert(snapshotName string) error {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return errors.Wrapf(err, "failed to get service client for replica %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)
	return serviceClient.ReplicaSnapshotRevert(u.name, snapshotName)
}

func (u *replicaBackend) SnapshotPurge() error {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return errors.Wrapf(err, "failed to get service client for replica %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)
	return serviceClient.ReplicaSnapshotPurge(u.name)
}

func (u *replicaBackend) SnapshotHash(snapshotName string, rehash bool) error {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return errors.Wrapf(err, "failed to get service client for replica %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)
	return serviceClient.ReplicaSnapshotHash(u.name, snapshotName, rehash)
}

func (u *replicaBackend) BackingImageGet(name, lvsUUID string) (*api.BackingImage, error) {
	serviceClient, err := u.newServiceClient(u.Address())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get service client for replica %s", u.name)
	}
	defer closeServiceClient(serviceClient, u.name)
	return serviceClient.BackingImageGet(name, lvsUUID)
}

// Compile-time check that replicaBackend satisfies the Backend interface.
var _ Backend = (*replicaBackend)(nil)
