package spdk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
)

// ErrSharedBlockFrontendsFreezing is returned when an engine frontend cannot
// register, or another operation cannot suspend the engine frontends, because
// the engine frontends of the shared block engine are being suspended.
var ErrSharedBlockFrontendsFreezing = errors.New("engine frontends of the shared block engine are being suspended")

// sharedBlockEngineState is the state of a shared block engine.
type sharedBlockEngineState struct {
	// metadataDir is the directory the NVMe reservation state is persisted
	// under. It is empty in tests that do not persist the state.
	metadataDir string

	// The following reservation fields are protected by the engine lock.
	//
	// reservationEpoch identifies the generation of the persisted NVMe
	// reservation state of the engine target, and reservationSeed is the state
	// synced to the cluster to restore when the target is exposed if there is
	// no local state of this epoch.
	reservationEpoch uint64
	reservationSeed  *spdkrpc.ReservationState
	// volumeUID identifies the lifetime of the volume, so that the ptpl file
	// of a deleted volume is never restored to a new volume with the same
	// name.
	volumeUID string
	// reservationPrepared indicates the ptpl file is ready for SPDK, so the
	// state in it can be reported. It stays false without a target, e.g., in
	// the maintenance mode, where reporting the missing file as an empty state
	// would overwrite the synced state.
	reservationPrepared bool

	// frontends has its own lock, as the engine lock is not held while the
	// engine calls the engine frontends.
	frontends sharedBlockFrontends
}

type registeredFrontend struct {
	address string
	hostNQN string
}

// sharedBlockFrontends tracks how to suspend each engine frontend of a shared
// block engine, so that the engine can suspend the frontends on all hosts before
// an operation that requires no I/O.
type sharedBlockFrontends struct {
	sync.Mutex

	// registered maps the name of each engine frontend to how to suspend it.
	registered map[string]*registeredFrontend
	isFreezing bool
}

// register records the engine frontend, and returns whether it changed. It is
// rejected while the engine frontends are suspended.
func (f *sharedBlockFrontends) register(name, address, hostNQN string) (changed bool, err error) {
	f.Lock()
	defer f.Unlock()

	if f.isFreezing {
		return false, ErrSharedBlockFrontendsFreezing
	}
	if f.registered == nil {
		f.registered = map[string]*registeredFrontend{}
	}
	old := f.registered[name]
	changed = old == nil || old.address != address || old.hostNQN != hostNQN
	f.registered[name] = &registeredFrontend{address: address, hostNQN: hostNQN}
	return changed, nil
}

// startFreezing marks the engine frontends as being suspended, and returns a
// copy of the registered engine frontends.
func (f *sharedBlockFrontends) startFreezing() (map[string]*registeredFrontend, error) {
	f.Lock()
	defer f.Unlock()

	if f.isFreezing {
		return nil, ErrSharedBlockFrontendsFreezing
	}
	f.isFreezing = true

	frontends := make(map[string]*registeredFrontend, len(f.registered))
	for name, frontend := range f.registered {
		frontends[name] = frontend
	}
	return frontends, nil
}

func (f *sharedBlockFrontends) stopFreezing() {
	f.Lock()
	defer f.Unlock()

	f.isFreezing = false
}

func getConnectedHostNQNs(spdkClient *spdkclient.Client, nqn string) (map[string]bool, error) {
	controllers, err := sharedBlockNvmfSubsystemGetControllers(spdkClient, nqn)
	if err != nil {
		return nil, err
	}
	hosts := map[string]bool{}
	for _, controller := range controllers {
		hosts[controller.Hostnqn] = true
	}
	return hosts, nil
}

// isFrontendGoneError returns true if the engine frontend does not exist or
// its node cannot be reached.
func isFrontendGoneError(err error) bool {
	switch grpcstatus.Code(errors.UnwrapAll(err)) {
	case grpccodes.NotFound, grpccodes.Unavailable, grpccodes.DeadlineExceeded:
		return true
	}
	return false
}

// reservationPtplEmptyFileMinAge is how long a ptpl file must have been empty
// before it is reported as a state without persistence. SPDK truncates the
// file before writing the new state, so a younger empty file may be in the
// middle of a write.
const reservationPtplEmptyFileMinAge = time.Second

// reservationPtplRecord is the NVMe reservation state SPDK persists in a ptpl
// file, see nvmf_ns_reservation_store_json in SPDK lib/nvmf/subsystem.c.
type reservationPtplRecord struct {
	Ptpl        bool                         `json:"ptpl"`
	Rtype       uint32                       `json:"rtype"`
	Crkey       uint64                       `json:"crkey"`
	BdevUUID    *string                      `json:"bdev_uuid"`
	HolderUUID  string                       `json:"holder_uuid"`
	Registrants *[]reservationPtplRegistrant `json:"registrants"`
}

type reservationPtplRegistrant struct {
	Rkey     uint64  `json:"rkey"`
	HostUUID *string `json:"host_uuid"`
}

// parseReservationPtplData parses the ptpl file content and returns why SPDK
// would fail to add the namespace with it, if any. It follows the checks of
// nvmf_ns_reservation_load_json and nvmf_ns_reservation_restore in SPDK
// lib/nvmf/subsystem.c. bdevUUID is the UUID of the namespace bdev.
func parseReservationPtplData(data []byte, bdevUUID string) (*reservationPtplRecord, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("file is empty")
	}

	record := &reservationPtplRecord{}
	if err := json.Unmarshal(data, record); err != nil {
		return nil, errors.Wrap(err, "failed to parse file")
	}
	if record.BdevUUID == nil {
		return nil, fmt.Errorf("bdev_uuid is missing")
	}
	if record.Registrants == nil {
		return nil, fmt.Errorf("registrants is missing")
	}
	registrants := *record.Registrants
	if len(registrants) > sharedBlockMaxFrontends {
		return nil, fmt.Errorf("%d registrants exceed the maximum %d", len(registrants), sharedBlockMaxFrontends)
	}
	for _, reg := range registrants {
		if reg.HostUUID == nil {
			return nil, fmt.Errorf("host_uuid of registrant with key %#x is missing", reg.Rkey)
		}
	}

	// SPDK skips restoring a state without persistence or registrants.
	if !record.Ptpl || len(registrants) == 0 {
		return record, nil
	}

	if record.Crkey != 0 {
		found := false
		for _, reg := range registrants {
			if reg.Rkey == record.Crkey {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("reservation key %#x of the holder is not registered", record.Crkey)
		}
	}
	if !strings.EqualFold(*record.BdevUUID, bdevUUID) {
		return nil, fmt.Errorf("bdev_uuid %s is not the namespace bdev UUID %s", *record.BdevUUID, bdevUUID)
	}

	return record, nil
}

// reservationPtplRecordToState converts the ptpl file content to the
// reservation state of the given epoch. A state without persistence has no
// registrants.
func reservationPtplRecordToState(record *reservationPtplRecord, epoch uint64) *spdkrpc.ReservationState {
	state := &spdkrpc.ReservationState{
		Epoch:       epoch,
		Registrants: []*spdkrpc.ReservationRegistrant{},
	}
	if !record.Ptpl || len(*record.Registrants) == 0 {
		return state
	}

	state.Type = record.Rtype
	state.HolderKey = record.Crkey
	state.HolderHostId = record.HolderUUID
	for _, reg := range *record.Registrants {
		state.Registrants = append(state.Registrants, &spdkrpc.ReservationRegistrant{
			Key:    reg.Rkey,
			HostId: *reg.HostUUID,
		})
	}
	return state
}

// reservationStateToPtplData converts the reservation state to the ptpl file
// content, or returns nil if the state has no registrants to restore.
func reservationStateToPtplData(state *spdkrpc.ReservationState, bdevUUID string) ([]byte, error) {
	if state == nil || len(state.Registrants) == 0 {
		return nil, nil
	}

	registrants := []reservationPtplRegistrant{}
	for _, reg := range state.Registrants {
		hostUUID := reg.HostId
		registrants = append(registrants, reservationPtplRegistrant{
			Rkey:     reg.Key,
			HostUUID: &hostUUID,
		})
	}
	return json.Marshal(&reservationPtplRecord{
		Ptpl:        true,
		Rtype:       state.Type,
		Crkey:       state.HolderKey,
		BdevUUID:    &bdevUUID,
		HolderUUID:  state.HolderHostId,
		Registrants: &registrants,
	})
}

func writeFileAtomically(file string, data []byte) (err error) {
	tmpFile := file + ".tmp"
	f, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmpFile)
		}
	}()

	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpFile, file); err != nil {
		return err
	}

	dir, err := os.Open(filepath.Dir(file))
	if err != nil {
		return err
	}
	defer func() {
		_ = dir.Close()
	}()
	return dir.Sync()
}
