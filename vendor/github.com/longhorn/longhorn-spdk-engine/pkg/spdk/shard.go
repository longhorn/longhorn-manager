package spdk

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	commonnet "github.com/longhorn/go-common-libs/net"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"

	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"
)

type Shard struct {
	sync.RWMutex

	// Name is the external identity used end-to-end across the control plane:
	// Shard CR name, manager -> IM -> SPDK RPC payloads, shardMap key. Format:
	// <volumeName>-<slotIndex>.
	Name string
	// LvolName is the SPDK-internal identifier: lvol name on disk, NVMe-oF
	// subsystem NQN suffix, and NVMe controller name reused on the ShardGroup
	// process side. Format: shard-<volumeName>-<slotIndex>. The prefix
	// disambiguates shard lvols from replica lvols during bdev_examine.
	LvolName   string
	VolumeName string
	SlotIndex  uint32
	Alias      string // <lvsName>/<lvolName>
	LvsName    string
	LvsUUID    string
	Nqn        string

	SizeBytes uint64
	UUID      string // lvol UUID populated after creation

	IP   string
	Port int32

	State    types.InstanceState
	ErrorMsg string

	IsExposed bool

	// UpdateCh should not be protected by the shard lock
	UpdateCh chan interface{}

	log *safelog.SafeLogger
}

// GetShardLvolName returns the SPDK-internal identifier for a shard. It is used as
// both the on-disk lvol name on the shard node and the NVMe-oF controller name
// reused on the ShardGroup process side; the "shard-" prefix disambiguates
// shard lvols from replica lvols during bdev_examine. This is NOT the
// external identifier - Shard CR name and shardMap key use GetShardName.
func GetShardLvolName(volumeName string, slotIndex uint32) string {
	return fmt.Sprintf("shard-%s-%d", volumeName, slotIndex)
}

// GetShardName returns the external shard identity used end-to-end
// across the control plane: Shard CR name, manager -> IM -> SPDK RPC payloads,
// and shardMap key. Format: <volumeName>-<slotIndex>. The SPDK-internal form
// (with the "shard-" prefix) is GetShardLvolName.
func GetShardName(volumeName string, slotIndex uint32) string {
	return fmt.Sprintf("%s-%d", volumeName, slotIndex)
}

func NewShard(volumeName string, slotIndex uint32, lvsName, lvsUUID string, sizeBytes uint64, updateCh chan interface{}) *Shard {
	name := GetShardName(volumeName, slotIndex)
	lvolName := GetShardLvolName(volumeName, slotIndex)

	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"shardName":  name,
		"volumeName": volumeName,
		"slotIndex":  slotIndex,
		"lvsName":    lvsName,
		"lvsUUID":    lvsUUID,
	})

	roundedSize := util.RoundUp(sizeBytes, helpertypes.MiB)
	if roundedSize != sizeBytes {
		log.Infof("Rounded up size from %v to %v since the size should be a multiple of MiB", sizeBytes, roundedSize)
	}
	log = log.WithField("sizeBytes", roundedSize)

	return &Shard{
		Name:       name,
		LvolName:   lvolName,
		VolumeName: volumeName,
		SlotIndex:  slotIndex,
		Alias:      spdktypes.GetLvolAlias(lvsName, lvolName),
		LvsName:    lvsName,
		LvsUUID:    lvsUUID,
		Nqn:        helpertypes.GetNQN(lvolName),

		SizeBytes: roundedSize,

		State: types.InstanceStatePending,

		UpdateCh: updateCh,

		log: safelog.NewSafeLogger(log),
	}
}

func ServiceShardToProtoShard(s *Shard) *spdkrpc.Shard {
	state := spdkrpc.EcSlotState_EC_SLOT_STATE_NORMAL
	if s.State == types.InstanceStateError {
		state = spdkrpc.EcSlotState_EC_SLOT_STATE_FAILED
	}

	return &spdkrpc.Shard{
		Name:       s.Name,
		VolumeName: s.VolumeName,
		SlotIndex:  s.SlotIndex,
		State:      state,
		SizeBytes:  s.SizeBytes,
		LvsName:    s.LvsName,
		LvsUuid:    s.LvsUUID,
		BdevName:   s.Alias,
		NvmfNqn:    s.Nqn,
		Ip:         s.IP,
		Port:       s.Port,
		ErrorMsg:   s.ErrorMsg,
		Uuid:       s.UUID,
	}
}

func (s *Shard) Create(spdkClient *spdkclient.Client, superiorPortAllocator *commonbitmap.Bitmap) (ret *spdkrpc.Shard, err error) {
	updateRequired := true

	s.Lock()
	defer func() {
		s.Unlock()

		if updateRequired {
			s.UpdateCh <- nil
		}
	}()

	if s.State == types.InstanceStateRunning {
		updateRequired = false
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "shard %v already exists and running", s.Name)
	}
	if s.State != types.InstanceStatePending && s.State != types.InstanceStateStopped {
		updateRequired = false
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "invalid state %s for shard %s creation", s.State, s.Name)
	}

	defer func() {
		if err != nil {
			s.log.WithError(err).Errorf("Failed to create shard %s", s.Name)
			s.State = types.InstanceStateError
			s.ErrorMsg = err.Error()
			ret = ServiceShardToProtoShard(s)
			err = nil
		} else {
			s.ErrorMsg = ""
			s.log.Info("Created shard")
		}
	}()

	if err := s.validateAndSyncLvstore(spdkClient); err != nil {
		return nil, err
	}

	if s.UUID == "" {
		uuid, err := spdkClient.BdevLvolCreate("", s.LvsUUID, s.LvolName, util.BytesToMiB(s.SizeBytes), "", true)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create lvol for shard %s", s.Name)
		}
		s.UUID = uuid
	}

	if err := s.prepareIPAndPort(superiorPortAllocator); err != nil {
		return nil, err
	}

	if err := spdkClient.StartExposeBdev(s.Nqn, s.UUID, generateNGUID(s.LvolName), s.IP, strconv.Itoa(int(s.Port))); err != nil {
		return nil, errors.Wrapf(err, "failed to expose bdev for shard %s", s.Name)
	}

	s.IsExposed = true
	s.State = types.InstanceStateRunning

	return ServiceShardToProtoShard(s), nil
}

func (s *Shard) Delete(spdkClient *spdkclient.Client, cleanupRequired bool, superiorPortAllocator *commonbitmap.Bitmap) (err error) {
	updateRequired := false

	s.Lock()
	defer func() {
		if err != nil {
			s.log.WithError(err).Errorf("Failed to delete shard with cleanupRequired flag %v", cleanupRequired)
			if s.State != types.InstanceStateError {
				s.State = types.InstanceStateError
				s.ErrorMsg = err.Error()
			}
		} else if cleanupRequired {
			// Full deletion is terminal regardless of prior state.
			s.State = types.InstanceStateTerminating
			s.ErrorMsg = ""
		} else if s.State != types.InstanceStateError {
			// Healthy detach leaves the shard Stopped so a later ShardCreate
			// re-attaches to it. An Error shard instead keeps its Error state and
			// message: Error is terminal by the Expand contract (recover by
			// replacement, not resurrection). A failed expand can leave the
			// on-disk lvol resized while cached SizeBytes is stale, and the reuse
			// path only checks requested-vs-cached size. Preserving Error makes
			// ShardCreate reject reattach with FailedPrecondition at the boundary
			// instead of laundering that divergence past it.
			s.State = types.InstanceStateStopped
			s.ErrorMsg = ""
		}

		updateRequired = true

		s.Unlock()

		if updateRequired {
			s.UpdateCh <- nil
		}
	}()

	if s.IsExposed {
		s.log.Info("Stopping exposed bdev for shard deletion")
		if err := spdkClient.StopExposeBdev(s.Nqn); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return errors.Wrapf(err, "failed to stop exposing shard %s", s.Name)
		}
		s.IsExposed = false
	}

	if s.Port != 0 {
		if err := superiorPortAllocator.ReleaseRange(s.Port, s.Port); err != nil {
			return errors.Wrapf(err, "failed to release port %d during shard %s deletion", s.Port, s.Name)
		}
		s.Port = 0
	}

	// Detach (cleanupRequired false) stops here: the expose is torn down and the
	// port released, but the lvol and UUID are preserved on disk so a later
	// ShardCreate re-attaches to this cached shard. Only full deletion below
	// destroys the lvol.
	if !cleanupRequired {
		s.log.Info("Detached shard, preserving lvol for re-attach")
		return nil
	}

	if _, err := spdkClient.BdevLvolDelete(s.Alias); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "failed to delete lvol for shard %s", s.Name)
	}

	s.UUID = ""

	s.log.Info("Deleted shard")

	return nil
}

func (s *Shard) Sync(spdkClient *spdkclient.Client) (err error) {
	s.Lock()
	prevState, prevErrorMsg := s.State, s.ErrorMsg
	defer func() {
		updateRequired := s.State != prevState || s.ErrorMsg != prevErrorMsg
		s.Unlock()
		if updateRequired {
			s.UpdateCh <- nil
		}
	}()
	// Only a real state mismatch moves the shard to Error. Transient query
	// failures return the error without touching state, like Replica.Sync.
	failSync := func(e error) error {
		s.log.WithError(e).Errorf("Failed to sync shard %s", s.Name)
		s.State = types.InstanceStateError
		s.ErrorMsg = e.Error()
		return e
	}

	if s.State == types.InstanceStatePending {
		s.State = types.InstanceStateStopped
		s.ErrorMsg = ""
		s.log.Debug("Synced shard")
		return nil
	}

	if s.State != types.InstanceStateRunning {
		return nil
	}

	subsystemMap, err := GetNvmfSubsystemMap(spdkClient)
	if err != nil {
		return err
	}

	exposedPort, exposedPortErr := getExposedPort(subsystemMap[s.Nqn])
	if s.IsExposed {
		if exposedPortErr != nil {
			return failSync(errors.Wrapf(exposedPortErr, "failed to find the actual port in subsystem NQN %s for shard %s, which should be exposed at %d", s.Nqn, s.Name, s.Port))
		}
		if exposedPort != s.Port {
			return failSync(fmt.Errorf("found mismatching between the actual exposed port %d and the recorded port %d for exposed shard %s", exposedPort, s.Port, s.Name))
		}
	} else if exposedPortErr == nil {
		return failSync(fmt.Errorf("found the actual port %d in subsystem NQN %s for shard %s, which should not be exposed", exposedPort, s.Nqn, s.Name))
	}

	s.ErrorMsg = ""
	s.log.Debug("Synced shard")
	return nil
}

func (s *Shard) Get() *spdkrpc.Shard {
	s.RLock()
	defer s.RUnlock()

	return ServiceShardToProtoShard(s)
}

// SetErrorState marks a non-stopped, non-error shard as Error, mirroring
// Replica.SetErrorState.
func (s *Shard) SetErrorState() {
	needUpdate := false

	s.Lock()
	defer func() {
		s.Unlock()

		if needUpdate {
			s.UpdateCh <- nil
		}
	}()

	if s.State != types.InstanceStateStopped && s.State != types.InstanceStateError {
		s.State = types.InstanceStateError
		needUpdate = true
	}
}

// Expand resizes the shard's lvol. It tears down the NVMe-oF subsystem
// first - leaving it up would deadlock the resize on the loopback initiator
// inside the same SPDK process (same workaround as ShardGroup.Expand at the
// head-lvol layer).
//
// Expand only runs when the shard is Running, and any failure moves the
// shard to Error via the defer. To recover, replace the shard; do not
// retry. A retry is unsafe because the stop/resize/re-expose sequence is
// not idempotent: after a failure IsExposed is already false, so a retry
// skips the stop, never re-exposes, and reports success while the NVMe-oF
// target stays down. Moving to Error also keeps the failure visible;
// otherwise Sync sees IsExposed=false and reports the shard healthy.
func (s *Shard) Expand(spdkClient *spdkclient.Client, newSize uint64) (err error) {
	s.Lock()
	if s.State != types.InstanceStateRunning {
		s.Unlock()
		return grpcstatus.Errorf(grpccodes.FailedPrecondition,
			"cannot expand shard %s in state %s (expand requires a running shard)",
			s.Name, s.State)
	}
	prevState, prevErrorMsg, prevSize := s.State, s.ErrorMsg, s.SizeBytes
	defer func() {
		if err != nil {
			s.log.WithError(err).Errorf("Failed to expand shard %s", s.Name)
			s.State = types.InstanceStateError
			s.ErrorMsg = err.Error()
		}
		updateRequired := s.State != prevState || s.ErrorMsg != prevErrorMsg || s.SizeBytes != prevSize
		s.Unlock()
		if updateRequired {
			s.UpdateCh <- nil
		}
	}()

	s.log.Infof("Expanding shard to size %v", newSize)

	roundedSize := util.RoundUp(newSize, helpertypes.MiB)
	if roundedSize != newSize {
		return fmt.Errorf("cannot expand shard %s to %v: size must be a multiple of MiB, round up to %v", s.Name, newSize, roundedSize)
	}

	if s.SizeBytes > newSize {
		return fmt.Errorf("cannot shrink shard %s from %v to %v", s.Name, s.SizeBytes, newSize)
	}
	if s.SizeBytes == newSize {
		s.log.Infof("Shard %s already at size %v", s.Name, newSize)
		return nil
	}

	reExposeBdev := false
	if s.IsExposed {
		if err := spdkClient.StopExposeBdev(s.Nqn); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return errors.Wrapf(err, "failed to stop exposing shard %s before expansion", s.Name)
		}
		s.IsExposed = false
		reExposeBdev = true
	}

	resized, err := spdkClient.BdevLvolResize(s.Alias, util.BytesToMiB(newSize))
	if err != nil {
		return errors.Wrapf(err, "failed to resize shard %s", s.Name)
	}
	if !resized {
		return fmt.Errorf("shard %s was not resized", s.Name)
	}

	if reExposeBdev {
		if err := spdkClient.StartExposeBdev(s.Nqn, s.UUID, generateNGUID(s.LvolName), s.IP, strconv.Itoa(int(s.Port))); err != nil {
			return errors.Wrapf(err, "failed to re-expose shard %s after expansion", s.Name)
		}
		s.IsExposed = true
	}

	s.SizeBytes = newSize

	s.log.Info("Expanding shard complete")
	return nil
}

func (s *Shard) validateAndSyncLvstore(spdkClient *spdkclient.Client) error {
	var (
		lvsList []spdktypes.LvstoreInfo
		err     error
	)

	if s.LvsUUID != "" {
		lvsList, err = spdkClient.BdevLvolGetLvstore("", s.LvsUUID)
	} else if s.LvsName != "" {
		lvsList, err = spdkClient.BdevLvolGetLvstore(s.LvsName, "")
	}
	if err != nil {
		return err
	}
	if len(lvsList) != 1 {
		return fmt.Errorf("found zero or multiple lvstore with name %s and UUID %s during shard %s creation", s.LvsName, s.LvsUUID, s.Name)
	}
	if s.LvsName == "" {
		s.LvsName = lvsList[0].Name
		s.Alias = spdktypes.GetLvolAlias(s.LvsName, s.LvolName)
	}
	if s.LvsUUID == "" {
		s.LvsUUID = lvsList[0].UUID
	}
	if s.LvsName != lvsList[0].Name || s.LvsUUID != lvsList[0].UUID {
		return fmt.Errorf("found mismatching between the actual lvstore name %s with UUID %s and the recorded lvstore name %s with UUID %s during shard %s creation", lvsList[0].Name, lvsList[0].UUID, s.LvsName, s.LvsUUID, s.Name)
	}

	return nil
}

// prepareIPAndPort sets the Shard's IP and allocates its single NVMe-oF port. A
// shard serves one target, so it always uses one port and does not use the
// request's port_count.
func (s *Shard) prepareIPAndPort(superiorPortAllocator *commonbitmap.Bitmap) error {
	podIP, err := commonnet.GetIPForPod()
	if err != nil {
		return err
	}
	s.IP = podIP

	port, _, err := superiorPortAllocator.AllocateRange(1)
	if err != nil {
		return err
	}
	s.Port = port

	s.log.Infof("Prepared IP %s and port %d for shard", s.IP, s.Port)

	return nil
}
