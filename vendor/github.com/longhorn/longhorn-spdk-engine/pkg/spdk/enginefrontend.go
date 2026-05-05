package spdk

import (
	"context"
	"fmt"
	"net"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"

	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"

	"github.com/longhorn/go-spdk-helper/pkg/initiator"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
)

type EngineFrontend struct {
	sync.RWMutex

	Name        string
	EngineName  string
	VolumeName  string
	VolumeNQN   string
	VolumeNGUID string
	SpecSize    uint64
	ActualSize  uint64

	Frontend string
	Endpoint string

	// EngineIP is the IP address of the node running the engine's SPDK service.
	// It is set during Create for all frontend types (including FrontendEmpty)
	// and used to derive the gRPC service address for engine operations like
	// Expand and snapshot.
	EngineIP string

	NvmeTcpFrontend *NvmeTcpFrontend
	NvmeTCPPathMap  map[string]*NvmeTCPPath
	ActivePath      string
	PreferredPath   string
	UblkFrontend    *UblkFrontend

	State    types.InstanceState
	ErrorMsg string

	initiator      *initiator.Initiator
	dmDeviceIsBusy bool

	IsRestoring           bool
	RestoringSnapshotName string

	isCreating            bool
	isSwitchingOver       bool
	isExpanding           bool
	lastExpansionFailedAt string
	lastExpansionError    string

	// UpdateCh should not be protected by the engine lock
	UpdateCh chan interface{}

	// stopCh is closed when the engine frontend is deleted to signal
	// background goroutines to abort early.
	stopCh chan struct{}

	// Test hook for switchover target engine name resolution.
	resolveEngineNameByTargetAddressFn func(targetAddress string) (string, error)
	// Test hook for native multipath path connect during blockdev switchover.
	connectNvmeTCPPathFn func(transportAddress, transportServiceID string) error
	// Test hook for native multipath path reconnect during recovery.
	reconnectNvmeTCPPathFn func(transportAddress, transportServiceID string) error
	// Test hook for initiator NVMe device info loading.
	loadInitiatorNVMeDeviceInfoFn func(transportAddress, transportServiceID, subsystemNQN string) error
	// Test hook for initiator endpoint loading.
	loadInitiatorEndpointFn func(dmDeviceIsBusy bool) error
	// Test hook for endpoint retrieval after switchover.
	getInitiatorEndpointFn func() string
	// Test hook for remote target ANA state synchronization during switchover.
	syncRemoteEngineTargetANAStatesFn func(oldTargetIP, oldEngineName, newTargetIP, newEngineName string) error
	// Test hook for setting a single remote engine target's ANA state.
	setRemoteEngineTargetANAStateFn func(targetIP, engineName string, anaState NvmeTCPANAState) error
	// Test hook for waiting for an NVMe-TCP controller to reach live state.
	waitForNvmeTCPControllerLiveFn func(transportAddress string, transportPort int32) error

	// metadataDir is the base path for persisting engine frontend records.
	// If empty, persistence is disabled.
	metadataDir string

	log *safelog.SafeLogger
}

type NvmeTcpFrontend struct {
	TargetIP   string
	TargetPort int32

	Nqn   string
	Nguid string
}

type NvmeTCPANAState string

type SwitchoverPhase string

const (
	NvmeTCPANAStateOptimized    NvmeTCPANAState = "optimized"
	NvmeTCPANAStateNonOptimized NvmeTCPANAState = "non-optimized"
	NvmeTCPANAStateInaccessible NvmeTCPANAState = "inaccessible"

	SwitchoverPhasePreparing SwitchoverPhase = "preparing"
	SwitchoverPhaseSwitching SwitchoverPhase = "switching"
	SwitchoverPhasePromoting SwitchoverPhase = "promoting"

	anaSyncMaxAttempts   = 5
	anaSyncRetryInterval = 200 * time.Millisecond
)

type NvmeTCPPath struct {
	TargetIP   string
	TargetPort int32
	EngineName string
	Nqn        string
	Nguid      string
	ANAState   NvmeTCPANAState
}

type UblkFrontend struct {
	// spec
	UblkQueueDepth    int32
	UblkNumberOfQueue int32

	// status
	UblkID int32
}

func getUblkQueueDepth(ublkQueueDepth int32) int32 {
	if ublkQueueDepth == 0 {
		return types.DefaultUblkQueueDepth
	}
	return ublkQueueDepth
}

func getUblkNumberOfQueue(ublkNumberOfQueue int32) int32 {
	if ublkNumberOfQueue == 0 {
		return types.DefaultUblkNumberOfQueue
	}
	return ublkNumberOfQueue
}

func NewEngineFrontend(engineFrontendName, engineName, volumeName, frontend string, specSize uint64, ublkQueueDepth, ublkNumberOfQueue int32,
	engineFrontendUpdateCh chan interface{}) *EngineFrontend {
	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"engineFrontendName": engineFrontendName,
		"engineName":         engineName,
		"volumeName":         volumeName,
		"frontend":           frontend,
		"specSize":           specSize,
	})

	if types.IsUblkFrontend(frontend) {
		log = log.WithFields(logrus.Fields{
			"ublkQueueDepth":    getUblkQueueDepth(ublkQueueDepth),
			"ublkNumberOfQueue": getUblkNumberOfQueue(ublkNumberOfQueue),
		})
	}

	roundedSpecSize := util.RoundUp(specSize, helpertypes.MiB)
	if roundedSpecSize != specSize {
		log.Infof("Rounded up spec size from %v to %v since the spec size should be multiple of MiB", specSize, roundedSpecSize)
		log = log.WithField("roundedSpecSize", roundedSpecSize)
	}

	nvmeTcpFrontend := &NvmeTcpFrontend{}
	ublkFrontend := &UblkFrontend{
		UblkQueueDepth:    getUblkQueueDepth(ublkQueueDepth),
		UblkNumberOfQueue: getUblkNumberOfQueue(ublkNumberOfQueue),
	}

	return &EngineFrontend{
		Name:        engineFrontendName,
		EngineName:  engineName,
		VolumeName:  volumeName,
		VolumeNQN:   getStableVolumeNQN(volumeName),
		VolumeNGUID: getStableVolumeNGUID(volumeName),
		SpecSize:    specSize,

		Frontend: frontend,

		NvmeTcpFrontend: nvmeTcpFrontend,
		NvmeTCPPathMap:  map[string]*NvmeTCPPath{},
		UblkFrontend:    ublkFrontend,

		State:    types.InstanceStatePending,
		ErrorMsg: "",

		UpdateCh: engineFrontendUpdateCh,
		stopCh:   make(chan struct{}),
		log:      safelog.NewSafeLogger(log),
	}
}

func getStableVolumeNQN(volumeName string) string {
	return helpertypes.GetNQN("volume-" + volumeName)
}

func getStableVolumeNGUID(volumeName string) string {
	return generateNGUID("volume-" + volumeName)
}

func getStableVolumeNsUUID(volumeName string) string {
	return generateNsUUID("volume-" + volumeName)
}

func getEffectiveVolumeTargetIdentity(volumeName, volumeNQN, volumeNGUID string) (string, string) {
	if volumeNQN == "" {
		volumeNQN = getStableVolumeNQN(volumeName)
	}
	if volumeNGUID == "" {
		volumeNGUID = getStableVolumeNGUID(volumeName)
	}
	return volumeNQN, volumeNGUID
}

func getNvmeTCPPathAddress(targetIP string, targetPort int32) string {
	if targetIP == "" || targetPort == 0 {
		return ""
	}
	return net.JoinHostPort(targetIP, strconv.Itoa(int(targetPort)))
}

func (ef *EngineFrontend) ensureVolumeTargetIdentityLocked() {
	ef.VolumeNQN, ef.VolumeNGUID = getEffectiveVolumeTargetIdentity(ef.VolumeName, ef.VolumeNQN, ef.VolumeNGUID)
	if ef.NvmeTCPPathMap == nil {
		ef.NvmeTCPPathMap = map[string]*NvmeTCPPath{}
	}
}

func (ef *EngineFrontend) getVolumeTargetIdentity() (string, string) {
	return getEffectiveVolumeTargetIdentity(ef.VolumeName, ef.VolumeNQN, ef.VolumeNGUID)
}

func (ef *EngineFrontend) clearNVMeTCPPathsLocked() {
	ef.NvmeTCPPathMap = map[string]*NvmeTCPPath{}
	ef.ActivePath = ""
	ef.PreferredPath = ""
}

func (ef *EngineFrontend) upsertNVMeTCPPathLocked(targetIP string, targetPort int32, engineName, nqn, nguid string, anaState NvmeTCPANAState) string {
	ef.ensureVolumeTargetIdentityLocked()

	address := getNvmeTCPPathAddress(targetIP, targetPort)
	if address == "" {
		return ""
	}

	path := ef.NvmeTCPPathMap[address]
	if path == nil {
		path = &NvmeTCPPath{}
		ef.NvmeTCPPathMap[address] = path
	}
	path.TargetIP = targetIP
	path.TargetPort = targetPort
	path.EngineName = engineName
	path.Nqn = nqn
	path.Nguid = nguid
	path.ANAState = anaState

	return address
}

func (ef *EngineFrontend) promoteNVMeTCPPathLocked(address string) bool {
	if address == "" {
		return false
	}
	if _, exists := ef.NvmeTCPPathMap[address]; !exists {
		return false
	}

	for existingAddress, existingPath := range ef.NvmeTCPPathMap {
		if existingPath == nil {
			continue
		}
		if existingAddress == address {
			existingPath.ANAState = NvmeTCPANAStateOptimized
			continue
		}
		if existingPath.ANAState == NvmeTCPANAStateOptimized {
			existingPath.ANAState = NvmeTCPANAStateInaccessible
		}
	}

	ef.ActivePath = address
	if ef.PreferredPath == "" {
		ef.PreferredPath = address
	}
	return true
}

func (ef *EngineFrontend) removeNVMeTCPPathLocked(address string) {
	if address == "" || ef.NvmeTCPPathMap == nil {
		return
	}
	delete(ef.NvmeTCPPathMap, address)
	if ef.ActivePath == address {
		ef.ActivePath = ""
	}
	if ef.PreferredPath == address {
		ef.PreferredPath = ""
	}
	if ef.ActivePath == "" {
		for existingAddress, path := range ef.NvmeTCPPathMap {
			if path == nil {
				continue
			}
			if path.ANAState == NvmeTCPANAStateOptimized {
				ef.ActivePath = existingAddress
				break
			}
		}
	}
	if ef.PreferredPath == "" {
		if ef.ActivePath != "" {
			ef.PreferredPath = ef.ActivePath
		} else {
			for existingAddress := range ef.NvmeTCPPathMap {
				ef.PreferredPath = existingAddress
				break
			}
		}
	}
}

func (ef *EngineFrontend) syncCurrentNVMeTCPPathLocked() {
	if ef.NvmeTcpFrontend == nil {
		return
	}

	ef.ensureVolumeTargetIdentityLocked()

	address := getNvmeTCPPathAddress(ef.NvmeTcpFrontend.TargetIP, ef.NvmeTcpFrontend.TargetPort)
	if address == "" {
		return
	}

	ef.upsertNVMeTCPPathLocked(ef.NvmeTcpFrontend.TargetIP, ef.NvmeTcpFrontend.TargetPort,
		ef.EngineName, ef.NvmeTcpFrontend.Nqn, ef.NvmeTcpFrontend.Nguid, NvmeTCPANAStateOptimized)
	ef.promoteNVMeTCPPathLocked(address)
}

func (ef *EngineFrontend) setRemoteEngineTargetANAState(targetIP, engineName string, anaState NvmeTCPANAState) error {
	if targetIP == "" || engineName == "" {
		return nil
	}

	if ef.setRemoteEngineTargetANAStateFn != nil {
		return ef.setRemoteEngineTargetANAStateFn(targetIP, engineName, anaState)
	}

	engineAddress := net.JoinHostPort(targetIP, strconv.Itoa(types.SPDKServicePort))
	engineClient, err := GetServiceClient(engineAddress)
	if err != nil {
		return errors.Wrapf(err, "failed to get SPDK client for engine %s at %s", engineName, engineAddress)
	}
	defer func() {
		if errClose := engineClient.Close(); errClose != nil {
			ef.log.WithError(errClose).Warnf("Failed to close engine SPDK client for ANA sync on engine %s", engineName)
		}
	}()

	if err := engineClient.EngineSetTargetListenerANAState(engineName, string(anaState)); err != nil {
		return errors.Wrapf(err, "failed to set ANA state %s for engine %s", anaState, engineName)
	}

	return nil
}

func (ef *EngineFrontend) rollbackRemoteEngineTargetANAStates(oldTargetIP, oldEngineName, newTargetIP, newEngineName string) error {
	var rollbackErr error

	if rbErr := ef.setRemoteEngineTargetANAState(newTargetIP, newEngineName, NvmeTCPANAStateInaccessible); rbErr != nil {
		rollbackErr = multierr.Append(rollbackErr,
			errors.Wrapf(rbErr, "failed to revert new target %s ANA state to inaccessible", newEngineName))
	}
	if rbErr := ef.setRemoteEngineTargetANAState(oldTargetIP, oldEngineName, NvmeTCPANAStateOptimized); rbErr != nil {
		rollbackErr = multierr.Append(rollbackErr,
			errors.Wrapf(rbErr, "failed to restore old target %s ANA state to optimized", oldEngineName))
	}

	return rollbackErr
}

func (ef *EngineFrontend) resolveRemoteEngineName(targetIP string, targetPort int32, engineName string) (string, error) {
	address := getNvmeTCPPathAddress(targetIP, targetPort)

	ef.RLock()
	if address != "" {
		if path := ef.NvmeTCPPathMap[address]; path != nil {
			if engineName == "" {
				engineName = path.EngineName
			}
		}
	}
	ef.RUnlock()

	if engineName == "" && address != "" {
		resolvedEngineName, err := ef.resolveEngineNameByTargetAddress(address)
		if err != nil {
			return "", errors.Wrapf(err, "failed to resolve engine name for target %s", address)
		}
		engineName = resolvedEngineName
	}
	if engineName == "" {
		return "", fmt.Errorf("failed to resolve engine name for target %s", address)
	}

	return engineName, nil
}

func (ef *EngineFrontend) syncRemoteEngineTargetANAStates(oldTargetIP, oldEngineName, newTargetIP, newEngineName string) error {
	var syncErr error

	// Three-phase ANA transition to eliminate the "both optimized" window
	// that could cause concurrent writes to the same LBA to be routed to
	// different engines, risking replica-level inconsistency.
	//
	// Phase 1: new → non-optimized (old stays optimized)
	//   Kernel prefers the optimized old path; new path is usable as
	//   fallback but receives no I/O while old is optimized.
	//
	// Phase 2: old → inaccessible (new is non-optimized)
	//   Kernel falls back to the non-optimized new path — the only
	//   remaining usable path. No I/O blackout.
	//
	// Phase 3: new → optimized
	//   Kernel now routes all I/O through the fully-promoted new path.
	//
	// At every phase there is exactly ONE engine receiving I/O, and there
	// is always at least one usable path (no I/O errors).

	// Phase 1: Promote new path to non-optimized (usable fallback).
	if err := ef.setRemoteEngineTargetANAState(newTargetIP, newEngineName, NvmeTCPANAStateNonOptimized); err != nil {
		// Phase 1 failed. Do NOT proceed — demoting the old path without
		// a usable new path would leave no routable path at all.
		return multierr.Append(syncErr, err)
	}

	// Phase 2: Demote old path to inaccessible. Kernel falls back to new.
	if oldEngineName != newEngineName || oldTargetIP != newTargetIP {
		if err := ef.setRemoteEngineTargetANAState(oldTargetIP, oldEngineName, NvmeTCPANAStateInaccessible); err != nil {
			// If the old engine's SPDK subsystem no longer exists (e.g.
			// it was already cleaned up after a previous switchover), the
			// old target is effectively gone — treat this as success.
			if isSubsystemNotFoundError(err) {
				ef.log.WithError(err).WithFields(logrus.Fields{
					"oldEngineName": oldEngineName,
					"oldTargetIP":   oldTargetIP,
				}).Info("Old engine subsystem already removed, skipping ANA demotion")
			} else {
				// Phase 2 failed for a real reason. Do NOT proceed to
				// Phase 3 — promoting the new path to optimized while
				// the old path is still optimized would create a
				// dual-write window.
				//
				// Roll back Phase 1: restore the new target to
				// inaccessible so it doesn't linger in non-optimized
				// state after the failed sync attempt.
				if rbErr := ef.setRemoteEngineTargetANAState(newTargetIP, newEngineName, NvmeTCPANAStateInaccessible); rbErr != nil {
					ef.log.WithError(rbErr).Warn("Failed to roll back new target ANA state to inaccessible after Phase 2 failure")
					syncErr = multierr.Append(syncErr, rbErr)
				}
				syncErr = multierr.Append(syncErr, err)
				return syncErr
			}
		}
	}

	// Phase 3: Promote new path to optimized.
	if err := ef.setRemoteEngineTargetANAState(newTargetIP, newEngineName, NvmeTCPANAStateOptimized); err != nil {
		syncErr = multierr.Append(syncErr, err)
		if rbErr := ef.rollbackRemoteEngineTargetANAStates(oldTargetIP, oldEngineName, newTargetIP, newEngineName); rbErr != nil {
			syncErr = multierr.Append(syncErr, rbErr)
		}
		return syncErr
	}

	return syncErr
}

func isSubsystemNotFoundError(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "unable to find subsystem")
}

func (ef *EngineFrontend) syncRemoteEngineTargetANAStatesWithRetry(oldEngineName, newEngineName string, oldTargetIP string, oldTargetPort int32, targetIP string, targetPort int32) error {
	resolvedNewEngineName, err := ef.resolveRemoteEngineName(targetIP, targetPort, newEngineName)
	if err != nil {
		return err
	}

	resolvedOldEngineName := oldEngineName
	if oldTargetIP != "" && oldTargetPort != 0 {
		resolvedOldEngineName, err = ef.resolveRemoteEngineName(oldTargetIP, oldTargetPort, oldEngineName)
		if err != nil {
			return err
		}
	}

	syncFn := ef.syncRemoteEngineTargetANAStates
	if ef.syncRemoteEngineTargetANAStatesFn != nil {
		syncFn = ef.syncRemoteEngineTargetANAStatesFn
	}

	var syncErr error
	for attempt := uint(1); attempt <= anaSyncMaxAttempts; attempt++ {
		syncErr = syncFn(oldTargetIP, resolvedOldEngineName, targetIP, resolvedNewEngineName)
		if syncErr == nil {
			return nil
		}

		if attempt == anaSyncMaxAttempts {
			break
		}

		ef.log.WithError(syncErr).WithFields(logrus.Fields{
			"attempt":       attempt,
			"maxAttempts":   anaSyncMaxAttempts,
			"oldEngineName": resolvedOldEngineName,
			"engineName":    resolvedNewEngineName,
			"oldTargetIP":   oldTargetIP,
			"oldTargetPort": oldTargetPort,
			"targetIP":      targetIP,
			"targetPort":    targetPort,
		}).Warn("Failed to sync remote target ANA state during switchover, retrying")

		select {
		case <-time.After(anaSyncRetryInterval):
		case <-ef.stopCh:
			return syncErr
		}
	}

	return syncErr
}

func isNVMeTCPPathAlreadyConnectedError(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "already connected")
}

// Create creates the engine frontend. On failure, it sets the frontend state
// to InstanceStateError with the error message and returns the error so callers
// can surface the attach failure instead of treating it as a successful start.
func (ef *EngineFrontend) Create(spdkClient *spdkclient.Client, targetAddress string) (ret *spdkrpc.EngineFrontend, err error) {
	ef.log.WithFields(logrus.Fields{
		"targetAddress": targetAddress,
		"frontend":      ef.Name,
	}).Info("Creating engine frontend")

	targetIP, _, err := splitHostPort(targetAddress)
	if err != nil {
		return nil, errors.Mark(
			errors.Wrapf(err, "failed to split target address %v", targetAddress),
			ErrEngineFrontendCreateInvalidArgument)
	}

	// Phase 1: Acquire lock to check state and establish the isCreating guard
	ef.Lock()
	if ef.State != types.InstanceStatePending {
		ef.Unlock()
		return nil, errors.Wrapf(ErrEngineFrontendCreatePrecondition, "invalid state %s for engine frontend %s creation", ef.State, ef.Name)
	}
	if ef.isCreating {
		ef.Unlock()
		return nil, errors.Wrapf(ErrEngineFrontendCreatePrecondition, "engine frontend %s is already creating", ef.Name)
	}
	ef.isCreating = true
	ef.EngineIP = targetIP
	ef.Unlock()

	var requireUpdate bool
	var frontendErr error

	// Phase 3: Cleanup and final state resolution
	defer func() {
		if r := recover(); r != nil {
			ef.log.WithFields(logrus.Fields{
				"panic": string(debug.Stack()),
			}).Errorf("Recovered panic during engine frontend %s creation: %v", ef.Name, r)
			frontendErr = errors.Wrapf(fmt.Errorf("%v", r), "panic during engine frontend %s creation", ef.Name)
		}

		ef.Lock()

		ef.isCreating = false

		if frontendErr != nil {
			ef.log.WithError(frontendErr).Errorf("Failed to create engine frontend %s", ef.Name)
			if ef.State != types.InstanceStateError {
				ef.State = types.InstanceStateError
				requireUpdate = true
			}
			ef.ErrorMsg = frontendErr.Error()
			ret = nil
			err = frontendErr
		} else {
			if ef.State != types.InstanceStateError {
				ef.ErrorMsg = ""
			}
			if ef.State != types.InstanceStateRunning {
				ef.State = types.InstanceStateRunning
				requireUpdate = true
			}
			ef.log.Info("Created engine frontend")

			// Persist record AFTER successful creation.
			if err := saveEngineFrontendRecord(ef.metadataDir, ef); err != nil {
				ef.log.WithError(err).Warn("Failed to persist engine frontend record")
			}

			ret = ef.getWithoutLock()
		}
		ef.Unlock()

		if requireUpdate {
			ef.UpdateCh <- nil
		}
	}()

	// Phase 2: Operations without lock
	initiatorCreationRequired, err := ef.isInitiatorCreationRequired(targetIP)
	if err != nil {
		frontendErr = err
		return
	}
	if !initiatorCreationRequired {
		return
	}

	ef.log.UpdateLoggerWithWarnOnFailure(logrus.Fields{
		"initiatorCreationRequired": initiatorCreationRequired,
		"targetAddress":             targetAddress,
	}, "Failed to update logger with initiatorCreationRequired and targetAddress during engine creation")

	ef.log.Info("Handling frontend during engine frontend creation")

	if frontendErr = ef.handleFrontend(spdkClient, targetAddress); frontendErr != nil {
		return
	}

	return
}

func (ef *EngineFrontend) Delete(spdkClient *spdkclient.Client) (err error) {
	requireUpdate := false

	ef.Lock()
	if ef.isCreating {
		ef.Unlock()
		return errors.Wrapf(ErrEngineFrontendLifecyclePrecondition, "engine frontend %s is still creating", ef.Name)
	}
	if ef.isSwitchingOver {
		ef.Unlock()
		return errors.Wrapf(ErrSwitchOverTargetPrecondition, "engine frontend %s is switching over target", ef.Name)
	}
	if ef.isExpanding {
		ef.Unlock()
		return errors.Wrapf(ErrEngineFrontendLifecyclePrecondition, "engine frontend %s is still expanding", ef.Name)
	}

	defer func() {
		// Considering that there may be still pending validations, it's better to update the state after the deletion.
		if err != nil {
			ef.log.WithError(err).Errorf("Failed to delete engine frontend")
			if ef.State != types.InstanceStateError {
				ef.State = types.InstanceStateError
				ef.ErrorMsg = err.Error()
				requireUpdate = true
			}
		} else {
			if ef.State != types.InstanceStateError {
				ef.ErrorMsg = ""
			}
		}
		if ef.State != types.InstanceStateError && ef.State != types.InstanceStateTerminating && ef.State != types.InstanceStateStopped {
			ef.State = types.InstanceStateTerminating
			requireUpdate = true
		}

		ef.Unlock()

		if requireUpdate {
			ef.UpdateCh <- nil
		}
	}()

	// Signal background goroutines to stop.
	select {
	case <-ef.stopCh:
		// Already closed.
	default:
		close(ef.stopCh)
	}

	ef.log.WithField("hasInitiator", ef.initiator != nil).Info("Deleting engine frontend")

	if ef.initiator != nil {
		if _, err := ef.initiator.Stop(spdkClient, true, true, true); err != nil {
			return err
		}
		ef.initiator = nil
		ef.Endpoint = ""

		requireUpdate = true
	}

	if ef.NvmeTcpFrontend != nil {
		ef.NvmeTcpFrontend.TargetIP = ""
		ef.NvmeTcpFrontend.TargetPort = 0
		ef.NvmeTcpFrontend.Nqn = ""
		ef.NvmeTcpFrontend.Nguid = ""
		ef.clearNVMeTCPPathsLocked()
	}

	ef.log.Info("Deleted engine frontend")

	// Remove persisted record AFTER successful deletion.
	if err := removeEngineFrontendRecord(ef.metadataDir, ef.VolumeName); err != nil {
		ef.log.WithError(err).Warn("Failed to remove engine frontend record")
	}

	return nil
}

func (ef *EngineFrontend) handleFrontend(spdkClient *spdkclient.Client, targetAddress string) (err error) {
	switch ef.Frontend {
	case types.FrontendEmpty:
		ef.log.Info("No frontend specified, will not expose bdev for engine")
		return nil
	case types.FrontendUBLK:
		ef.log.Infof("Handling ublk frontend for engine frontend creation, targetAddress: %s", targetAddress)
		return ef.createUblkFrontend(spdkClient)
	case types.FrontendSPDKTCPBlockdev, types.FrontendSPDKTCPNvmf:
		ef.log.Infof("Handling NVMe/TCP frontend for engine frontend creation, targetAddress: %s", targetAddress)
		return ef.createNvmeTcpFrontend(spdkClient, targetAddress)
	default:
		return fmt.Errorf("unknown frontend type %s", ef.Frontend)
	}
}

func (ef *EngineFrontend) createNvmeTcpFrontend(spdkClient *spdkclient.Client, targetAddress string) (err error) {
	if ef.NvmeTcpFrontend == nil {
		return fmt.Errorf("failed to create NVMe/TCP frontend: invalid NvmeTcpFrontend: %v", ef.NvmeTcpFrontend)
	}

	targetIP, targetPort, err := splitHostPort(targetAddress)
	if err != nil {
		return errors.Wrapf(err, "failed to split host port for engine frontend %v", ef.Name)
	}

	frontendConfigured := ef.isNvmeTcpFrontendConfigured()

	var i *initiator.Initiator
	if frontendConfigured {
		// If the NVMe frontend is already configured, reuse the existing initiator and connection info.
		ef.log.Infof("Reusing existing initiator. TargetIP: %s, TargetPort: %d", ef.NvmeTcpFrontend.TargetIP, ef.NvmeTcpFrontend.TargetPort)

		i = ef.initiator
	} else {
		ef.log.Infof("Creating new initiator for NVMe/TCP frontend: targetAddress: %s", targetAddress)

		var nqn, nguid string
		i, nqn, nguid, err = ef.newNvmeTcpInitiator()
		if err != nil {
			return errors.Wrap(err, "failed to create NVMe/TCP initiator")
		}

		ef.Lock()
		ef.NvmeTcpFrontend.Nqn = nqn
		ef.NvmeTcpFrontend.Nguid = nguid
		ef.Unlock()
	}

	dmDeviceIsBusy := false

	defer func() {
		if err != nil {
			return
		}

		if !frontendConfigured {
			ef.Lock()
			switch ef.Frontend {
			case types.FrontendSPDKTCPBlockdev:
				ef.NvmeTcpFrontend.TargetIP = targetIP
				ef.NvmeTcpFrontend.TargetPort = targetPort
				ef.Endpoint = i.GetEndpoint()
				ef.initiator = i
				ef.dmDeviceIsBusy = dmDeviceIsBusy
			case types.FrontendSPDKTCPNvmf:
				ef.NvmeTcpFrontend.TargetIP = targetIP
				ef.NvmeTcpFrontend.TargetPort = targetPort
				ef.Endpoint = GetNvmfEndpoint(ef.NvmeTcpFrontend.Nqn, targetIP, targetPort)
			}
			ef.syncCurrentNVMeTCPPathLocked()
			endpoint := ef.Endpoint
			targetPortForLog := ef.NvmeTcpFrontend.TargetPort
			ef.Unlock()
			ef.log.UpdateLoggerWithWarnOnFailure(logrus.Fields{
				"endpoint":   endpoint,
				"targetPort": targetPortForLog,
			}, "Failed to update logger with endpoint and port during engine frontend handling")
		}

		ef.log.Infof("Created engine frontend")
	}()

	if ef.Frontend == types.FrontendSPDKTCPNvmf {
		return nil
	}

	// In the expansion flow we MUST NOT disconnect the NVMe target.
	//
	// Technical Reason:
	// 1. If we disconnect while the dm-linear device is still active (suspended but open),
	//    the Linux kernel cannot fully release the /dev/nvmeXnX resource, leading to
	//    a "zombie" device node.
	// 2. When we reconnect later, the kernel will detect a naming conflict and assign
	//    a new, non-healthy device name (e.g., nvme3n2) instead of reusing nvme1n1.
	// 3. By skipping disconnect, the kernel keeps the existing controller session in
	//    a "reconnecting" state. Once SPDK re-exposes the resized RAID, the kernel
	//    automatically recovers the original path (nvme1n1) and perceives the new size,
	//    allowing a successful 'dmsetup reload' without breaking the mount point.
	ef.RLock()
	disconnectTarget := !ef.isExpanding
	ef.RUnlock()

	dmDeviceIsBusy, err = i.StartNvmeTCPInitiator(targetIP, strconv.Itoa(int(targetPort)), true, disconnectTarget)
	if err != nil {
		return errors.Wrapf(err, "failed to start NVMe/TCP initiator for engine frontend %v", ef.Name)
	}

	return nil
}

func (ef *EngineFrontend) newNvmeTcpInitiator() (i *initiator.Initiator, nqn, nguid string, err error) {
	nqn, nguid = ef.getVolumeTargetIdentity()

	nvmeTCPInfo := &initiator.NVMeTCPInfo{
		SubsystemNQN: nqn,
	}
	i, err = initiator.NewInitiator(ef.VolumeName, initiator.HostProc, nvmeTCPInfo, nil)
	if err != nil {
		return i, "", "", errors.Wrapf(err, "failed to create NVMe/TCP initiator for engine %v", ef.Name)
	}

	return i, nqn, nguid, nil
}

// getEngineServiceAddress returns the gRPC service address of the engine node.
// It uses EngineIP which is set during Create for all frontend types.
func (ef *EngineFrontend) getEngineServiceAddress() string {
	if ef.EngineIP == "" {
		return ""
	}
	return net.JoinHostPort(ef.EngineIP, strconv.Itoa(types.SPDKServicePort))
}

func (ef *EngineFrontend) getWithoutLock() (res *spdkrpc.EngineFrontend) {
	res = &spdkrpc.EngineFrontend{
		Name:       ef.Name,
		VolumeName: ef.VolumeName,
		EngineName: ef.EngineName,
		SpecSize:   ef.SpecSize,
		ActualSize: ef.ActualSize,
		Frontend:   ef.Frontend,
		Endpoint:   ef.Endpoint,

		State:                 string(ef.State),
		ErrorMsg:              ef.ErrorMsg,
		IsExpanding:           ef.isExpanding,
		LastExpansionError:    ef.lastExpansionError,
		LastExpansionFailedAt: ef.lastExpansionFailedAt,
		ActivePath:            ef.ActivePath,
		PreferredPath:         ef.PreferredPath,
		Paths:                 ef.getProtoNvmeTCPPathsWithoutLock(),
	}

	if ef.NvmeTcpFrontend != nil {
		res.TargetIp = ef.NvmeTcpFrontend.TargetIP
		res.TargetPort = ef.NvmeTcpFrontend.TargetPort
	}

	if ef.UblkFrontend != nil {
		res.UblkId = ef.UblkFrontend.UblkID
	}

	return res
}

func (ef *EngineFrontend) getProtoNvmeTCPPathsWithoutLock() []*spdkrpc.EngineFrontendNvmeTcpPath {
	if len(ef.NvmeTCPPathMap) == 0 {
		return nil
	}

	addresses := make([]string, 0, len(ef.NvmeTCPPathMap))
	for address := range ef.NvmeTCPPathMap {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)

	paths := make([]*spdkrpc.EngineFrontendNvmeTcpPath, 0, len(addresses))
	for _, address := range addresses {
		path := ef.NvmeTCPPathMap[address]
		if path == nil {
			continue
		}
		paths = append(paths, &spdkrpc.EngineFrontendNvmeTcpPath{
			TargetIp:   path.TargetIP,
			TargetPort: path.TargetPort,
			EngineName: path.EngineName,
			Nqn:        path.Nqn,
			Nguid:      path.Nguid,
			AnaState:   string(path.ANAState),
		})
	}

	return paths
}

// SetErrorState sets the engine frontend to error state.
func (ef *EngineFrontend) SetErrorState() {
	needUpdate := false

	ef.Lock()
	defer func() {
		ef.Unlock()

		if needUpdate {
			ef.UpdateCh <- nil
		}
	}()

	if ef.State != types.InstanceStateStopped && ef.State != types.InstanceStateError {
		ef.log.Error("Setting engine frontend to error state")
		ef.State = types.InstanceStateError
		needUpdate = true
	}
}

// Get returns a copy of the current engine frontend state.
func (ef *EngineFrontend) Get() (res *spdkrpc.EngineFrontend) {
	ef.RLock()
	defer ef.RUnlock()

	return ef.getWithoutLock()
}

func (ef *EngineFrontend) isNvmeTcpFrontendConfigured() bool {
	ef.RLock()
	defer ef.RUnlock()

	if ef.NvmeTcpFrontend == nil || ef.initiator == nil {
		return false
	}

	if len(ef.NvmeTcpFrontend.TargetIP) == 0 || ef.NvmeTcpFrontend.TargetPort == 0 ||
		len(ef.NvmeTcpFrontend.Nqn) == 0 || len(ef.NvmeTcpFrontend.Nguid) == 0 {
		return false
	}

	return true
}

func (ef *EngineFrontend) createUblkFrontend(spdkClient *spdkclient.Client) (err error) {
	if ef.UblkFrontend == nil {
		return fmt.Errorf("failed to createUblkFrontend: invalid UblkFrontend: %v", ef.UblkFrontend)
	}
	dmDeviceIsBusy := false

	ublkInfo := &initiator.UblkInfo{
		BdevName:          ef.EngineName,
		UblkQueueDepth:    ef.UblkFrontend.UblkQueueDepth,
		UblkNumberOfQueue: ef.UblkFrontend.UblkNumberOfQueue,

		UblkID: initiator.UnInitializedUblkId,
	}
	i, err := initiator.NewInitiator(ef.VolumeName, initiator.HostProc, nil, ublkInfo)
	if err != nil {
		return errors.Wrapf(err, "failed to create initiator for engine %v", ef.Name)
	}

	defer func() {
		if err == nil {
			ef.Lock()
			ef.initiator = i
			ef.dmDeviceIsBusy = dmDeviceIsBusy
			ef.Endpoint = i.GetEndpoint()
			endpoint := ef.Endpoint
			ublkID := ef.UblkFrontend.UblkID
			ef.Unlock()

			ef.log.UpdateLoggerWithWarnOnFailure(logrus.Fields{
				"endpoint": endpoint,
				"ublkID":   ublkID,
			}, "Failed to update logger with endpoint and port during engine creation")
			ef.log.Infof("Created engine frontend")
		}
	}()

	dmDeviceIsBusy, err = i.StartUblkInitiator(spdkClient, true)
	if err != nil {
		return errors.Wrapf(err, "failed to start initiator for engine %v", ef.Name)
	}

	ef.Lock()
	ef.UblkFrontend.UblkID = i.UblkInfo.UblkID
	ef.Unlock()

	return nil
}

func (ef *EngineFrontend) isInitiatorCreationRequired(targetIP string) (bool, error) {
	if types.IsUblkFrontend(ef.Frontend) {
		return true, nil
	}

	if ef.NvmeTcpFrontend == nil {
		return false, fmt.Errorf("failed to isInitiatorCreationRequired: invalid NvmeTcpFrontend: %v", ef.NvmeTcpFrontend)
	}

	return ef.NvmeTcpFrontend.TargetPort == 0, nil
}

// Expand performs an online volume expansion for the Longhorn Engine using SPDK.
// It expands the underlying replica logical volumes (lvol), recreates the SPDK RAID bdev,
// suspends and resumes frontend I/O as needed, and ensures cleanup and status updates on failure.
func (ef *EngineFrontend) Expand(ctx context.Context, spdkClient *spdkclient.Client, size uint64) (retErr error) {
	ef.log.Info("Expanding engine frontend")

	// Phase 1: Acquire lock to read state and check expansion guards.
	ef.Lock()
	if ef.isCreating {
		ef.Unlock()
		return fmt.Errorf("engine frontend %s is still creating", ef.Name)
	}
	if ef.isSwitchingOver {
		ef.Unlock()
		return errors.Wrapf(ErrSwitchOverTargetPrecondition, "engine frontend %s is switching over target", ef.Name)
	}

	originalSize := ef.SpecSize
	engineName := ef.EngineName
	frontend := ef.Frontend

	var targetAddress string
	if ef.NvmeTcpFrontend != nil {
		targetAddress = net.JoinHostPort(ef.NvmeTcpFrontend.TargetIP, strconv.Itoa(int(ef.NvmeTcpFrontend.TargetPort)))
	}

	engineSpdkClient, err := GetServiceClient(ef.getEngineServiceAddress())
	if err != nil {
		ef.Unlock()
		return errors.Wrapf(err, "failed to get SPDK client to expand engine frontend %v", ef.Name)
	}
	defer func() {
		if errClose := engineSpdkClient.Close(); errClose != nil {
			ef.log.WithError(errClose).Error("Failed to close SPDK client")
		}
	}()

	requireExpansion, err := ef.requireExpansion(ctx, engineSpdkClient, size)
	if err != nil {
		ef.Unlock()
		return errors.Wrap(err, "failed to check whether expansion is required")
	}
	ef.Unlock()
	// Phase 1 ends: lock released.

	// engineErr will be set when the engine failed to do any non-recoverable operations.
	expanded := false
	backendExpansionError := ""
	backendExpansionFailedAt := ""
	var engineActualSize uint64

	defer func() {
		if r := recover(); r != nil {
			ef.log.WithFields(logrus.Fields{
				"panic": string(debug.Stack()),
			}).Errorf("Recovered panic during engine frontend %s expansion: %v", ef.Name, r)
			retErr = errors.Wrapf(fmt.Errorf("%v", r), "panic during engine frontend %s expansion", ef.Name)
		}

		// Phase 3: Re-acquire lock to update state.
		ef.Lock()
		ef.finishExpansion(originalSize, expanded, size, retErr, backendExpansionError, backendExpansionFailedAt, engineActualSize)
		ef.Unlock()

		ef.UpdateCh <- nil
	}()

	if !requireExpansion {
		ef.log.Info("No need to expand engine")
		expanded = true
		return nil
	}

	// Phase 2: Long-running operations without lock.
	suspended, err := ef.prepareExpansion()
	if err != nil {
		return errors.Wrap(err, "prepare raid for expansion failed")
	}
	if suspended {
		defer func() {
			if ef.initiator != nil {
				if frontendErr := ef.initiator.Resume(); frontendErr != nil {
					retErr = multierr.Append(retErr, errors.Wrapf(frontendErr, "original error; resume failed"))
				}
			}
		}()
	}

	if err := engineSpdkClient.EngineExpand(ctx, engineName, size); err != nil {
		return errors.Wrapf(err, "failed to expand engine %v", engineName)
	}

	engine, err := engineSpdkClient.EngineGet(engineName)
	if err != nil {
		return errors.Wrapf(err, "failed to get engine %v after expansion", engineName)
	}
	engineActualSize = engine.ActualSize
	if engine.LastExpansionError != "" {
		backendExpansionError = engine.LastExpansionError
		backendExpansionFailedAt = engine.LastExpansionFailedAt
		ef.log.Warnf("Engine %s partially failed to expand to %v; keeping engine frontend size at %v: %v",
			engineName, size, originalSize, backendExpansionError)
		return nil
	}

	if targetAddress != "" {
		if err := ef.handleFrontend(spdkClient, targetAddress); err != nil {
			return errors.Wrap(err, "failed to handle frontend")
		}
	}

	// It waits for the kernel to recognize the new physical NVMe capacity
	// and then reloads the dm table to propagate the size change up to the volume.
	if frontend != types.FrontendEmpty && ef.initiator != nil {
		if err := ef.initiator.SyncDmDeviceSize(size); err != nil {
			ef.log.WithError(err).Warnf("failed to sync linear dm device size during engine %s expansion", engineName)
		}
	}

	ef.log.Info("Expanding engine completed")
	expanded = true

	return nil
}

func (ef *EngineFrontend) requireExpansion(ctx context.Context, engineSpdkClient *client.SPDKClient, size uint64) (requireExpansion bool, err error) {
	if ef.isExpanding {
		return false, fmt.Errorf("%w", ErrExpansionInProgress)
	}

	if ef.IsRestoring {
		return false, fmt.Errorf("%w", ErrRestoringInProgress)
	}

	if ef.SpecSize > size {
		return false, fmt.Errorf("%w: cannot expand engine to a smaller size %v, current size %v", ErrExpansionInvalidSize, size, ef.SpecSize)
	}

	if ef.SpecSize == size {
		// EngineFrontend is already at the requested size. However, for offline
		// expansion the Engine's SpecSize may have been adjusted downward by
		// ValidateAndUpdate to match the actual RAID bdev size (built from
		// unexpanded replicas). Check whether the downstream Engine still needs
		// expansion before skipping.
		engine, err := engineSpdkClient.EngineGet(ef.EngineName)
		if err != nil {
			return false, errors.Wrapf(err, "failed to get engine %v during expansion check", ef.EngineName)
		}
		if engine.SpecSize >= size {
			ef.log.Infof("Engine already at requested size %v, skipping expansion", size)
			return false, nil
		}
		ef.log.Infof("Engine frontend at requested size %v but engine SpecSize is %v, proceeding with expansion", size, engine.SpecSize)
	}

	roundedNewSize := util.RoundUp(size, helpertypes.MiB)
	if roundedNewSize != size {
		return false, fmt.Errorf("%w: rounded up spec size from %v to %v since the spec size should be multiple of MiB",
			ErrExpansionInvalidSize, size, roundedNewSize)
	}

	// Check if the expansion is required for downstream engine and replicas
	if err := engineSpdkClient.EngineExpandPrecheck(ctx, ef.EngineName, size); err != nil {
		return false, errors.Wrapf(err, "failed to precheck engine %v expansion", ef.Name)
	}

	// Mark expansion as in progress
	ef.isExpanding = true
	ef.lastExpansionFailedAt = ""
	ef.lastExpansionError = ""

	return true, nil
}

func (ef *EngineFrontend) finishExpansion(fromSize uint64, expanded bool, size uint64, err error, backendExpansionError, backendExpansionFailedAt string, engineActualSize uint64) {
	// Sync ActualSize from the engine whenever we successfully queried it,
	// regardless of whether the expansion itself succeeded or failed.
	if engineActualSize > 0 {
		ef.ActualSize = engineActualSize
	}

	if err != nil {
		ef.log.WithError(err).Errorf("Engine %s failed to expand from size %v to %v", ef.Name, fromSize, size)
		ef.State = types.InstanceStateError
		ef.ErrorMsg = err.Error()
		ef.lastExpansionError = errors.Wrap(err, "engine failed to expand expansion").Error()
		ef.lastExpansionFailedAt = time.Now().UTC().Format(time.RFC3339Nano)

		ef.log.WithError(err).Errorf("Engine %s failed to expand", ef.Name)
		if expanded {
			// The backend expansion succeeded, but post-expansion frontend handling failed.
			ef.SpecSize = size
			ef.log.Warnf("Expanded from size %v to %v but encountered post-expansion error", fromSize, size)

			// Re-persist even on error so the updated SpecSize survives a restart.
			if err := saveEngineFrontendRecord(ef.metadataDir, ef); err != nil {
				ef.log.WithError(err).Warn("Failed to persist engine frontend record after partial expansion")
			}
		} else {
			ef.log.Infof("Failed to expand from size %v to %v", fromSize, size)
		}
		ef.isExpanding = false
		return
	}

	ef.State = types.InstanceStateRunning
	ef.ErrorMsg = ""
	if backendExpansionError != "" {
		ef.lastExpansionError = backendExpansionError
		if backendExpansionFailedAt != "" {
			ef.lastExpansionFailedAt = backendExpansionFailedAt
		} else {
			ef.lastExpansionFailedAt = time.Now().UTC().Format(time.RFC3339Nano)
		}
		ef.log.Warnf("Partially failed to expand from size %v to %v; keeping engine frontend size at %v: %v",
			fromSize, size, fromSize, backendExpansionError)
		ef.isExpanding = false
		return
	}
	if expanded {
		ef.log.Infof("Succeeded to expand from size %v to %v", fromSize, size)
		ef.SpecSize = size

		// Re-persist the record so the updated SpecSize survives a restart.
		if err := saveEngineFrontendRecord(ef.metadataDir, ef); err != nil {
			ef.log.WithError(err).Warn("Failed to persist engine frontend record after expansion")
		}
	} else {
		ef.log.Infof("Failed to expand from size %v to %v", fromSize, size)
	}

	// Clear stale expansion error on success (err == nil && backendExpansionError == "").
	// A previous partial failure may have left lastExpansionError set.
	ef.lastExpansionError = ""
	ef.lastExpansionFailedAt = ""

	ef.isExpanding = false
}

func (ef *EngineFrontend) prepareExpansion() (engineFrontendSuspended bool, err error) {
	switch ef.Frontend {
	case types.FrontendUBLK:
		return false, fmt.Errorf("not support ublk frontend for expansion for engine %s", ef.Name)
	case types.FrontendSPDKTCPBlockdev:
		if ef.Endpoint != "" {
			ef.log.Info("Suspending engine frontend")
			if err := ef.initiator.Suspend(false, false); err != nil {
				return false, errors.Wrapf(err, "failed to suspend engine frontend %s", ef.Name)
			}
			return true, nil
		}
	}
	return false, nil
}

// SuspendFrontend suspends the engine frontend. IO operations will be suspended.
func (ef *EngineFrontend) Suspend(_ *spdkclient.Client) (err error) {
	ef.Lock()
	defer func() {
		if err != nil {
			if ef.State != types.InstanceStateError {
				ef.log.WithError(err).Warn("Failed to suspend engine frontend")
				// Engine is still alive and running and is not really in error state.
				// longhorn-manager will retry to suspend the engine.
			}
			ef.ErrorMsg = err.Error()
		} else {
			ef.State = types.InstanceStateSuspended
			ef.ErrorMsg = ""

			ef.log.Info("Suspended engine frontend")
		}

		ef.Unlock()

		ef.UpdateCh <- nil
	}()

	if ef.State == types.InstanceStateSuspended {
		return nil
	}
	if ef.isSwitchingOver {
		return errors.Wrapf(ErrSwitchOverTargetPrecondition, "engine frontend %s is switching over target", ef.Name)
	}

	ef.log.Info("Suspending engine frontend")

	switch ef.Frontend {
	case types.FrontendSPDKTCPBlockdev:
		nvmeTCPInfo := &initiator.NVMeTCPInfo{
			SubsystemNQN: ef.NvmeTcpFrontend.Nqn,
		}
		i, err := initiator.NewInitiator(ef.VolumeName, initiator.HostProc, nvmeTCPInfo, nil)
		if err != nil {
			return errors.Wrapf(err, "failed to create initiator for suspending engine %s", ef.Name)
		}

		return i.Suspend(false, false)
	default:
		// TODO: support ublk frontend suspend
		return errors.Wrapf(ErrEngineFrontendLifecycleUnimplemented, "suspend frontend %s is unimplemented", ef.Frontend)
	}
}

// ResumeFrontend resumes the engine frontend. IO operations will be resumed.
func (ef *EngineFrontend) Resume(_ *spdkclient.Client) (err error) {
	ef.Lock()
	defer func() {
		if err != nil {
			if ef.State != types.InstanceStateError {
				ef.log.WithError(err).Warn("Failed to resume engine frontend")
				// Engine is still alive and running and is not really in error state.
				// longhorn-manager will retry to resume the engine.
			}
			ef.ErrorMsg = err.Error()
		} else {
			ef.State = types.InstanceStateRunning
			ef.ErrorMsg = ""

			ef.log.Infof("Resumed engine frontend")
		}

		ef.Unlock()

		ef.UpdateCh <- nil
	}()

	if ef.State == types.InstanceStateRunning {
		return nil
	}
	if ef.isSwitchingOver {
		return errors.Wrapf(ErrSwitchOverTargetPrecondition, "engine frontend %s is switching over target", ef.Name)
	}

	switch ef.Frontend {
	case types.FrontendSPDKTCPBlockdev:
		nvmeTCPInfo := &initiator.NVMeTCPInfo{
			SubsystemNQN: ef.NvmeTcpFrontend.Nqn,
		}
		i, err := initiator.NewInitiator(ef.VolumeName, initiator.HostProc, nvmeTCPInfo, nil)
		if err != nil {
			return errors.Wrapf(err, "failed to create initiator for resuming engine %s", ef.Name)
		}

		ef.log.Info("Resuming engine frontend")
		return i.Resume()
	default:
		// TODO: support ublk frontend resume
		return errors.Wrapf(ErrEngineFrontendLifecycleUnimplemented, "resume frontend %s is unimplemented", ef.Frontend)
	}
}

// SwitchOverTarget switches the backend target for an existing engine frontend.
// For blockdev frontend, switchover connects the new native multipath path without
// relying on dm-linear suspend/resume. Snapshot operations still use dm-linear
// suspend/resume separately.
// If newEngineName is empty, the function will try to resolve it via targetAddress.
func (ef *EngineFrontend) SwitchOverTarget(spdkClient *spdkclient.Client, newEngineName, targetAddress, switchoverPhase string) (err error) {
	if targetAddress == "" {
		return errors.Wrapf(ErrSwitchOverTargetInvalidInput, "target address is required for engine frontend %s switchover", ef.Name)
	}

	targetIP, targetPort, err := splitHostPort(targetAddress)
	if err != nil {
		return errors.Wrapf(ErrSwitchOverTargetInvalidInput, "failed to split target address %v for engine frontend %s switchover: %v", targetAddress, ef.Name, err)
	}
	if targetIP == "" || targetPort == 0 {
		return errors.Wrapf(ErrSwitchOverTargetInvalidInput, "invalid target address %q for engine frontend %s switchover", targetAddress, ef.Name)
	}

	updateRequired := false

	ef.Lock()
	if ef.isCreating {
		ef.Unlock()
		return fmt.Errorf("engine frontend %s is still creating", ef.Name)
	}
	if ef.isSwitchingOver {
		ef.Unlock()
		return errors.Wrapf(ErrSwitchOverTargetPrecondition, "engine frontend %s target switchover is already in progress", ef.Name)
	}
	if ef.isExpanding {
		ef.Unlock()
		return errors.Wrapf(ErrSwitchOverTargetPrecondition, "engine frontend %s expansion is in progress", ef.Name)
	}
	if ef.IsRestoring {
		ef.Unlock()
		return errors.Wrapf(ErrSwitchOverTargetPrecondition, "engine frontend %s restore is in progress", ef.Name)
	}
	if ef.NvmeTcpFrontend == nil {
		ef.Unlock()
		return errors.Wrapf(ErrSwitchOverTargetPrecondition, "invalid NvmeTcpFrontend for engine frontend %s switchover", ef.Name)
	}

	oldEngineName := ef.EngineName
	oldTargetIP := ef.NvmeTcpFrontend.TargetIP
	oldTargetPort := ef.NvmeTcpFrontend.TargetPort
	oldNQN := ef.NvmeTcpFrontend.Nqn
	oldNGUID := ef.NvmeTcpFrontend.Nguid
	oldEndpoint := ef.Endpoint
	oldDMDeviceIsBusy := ef.dmDeviceIsBusy
	frontend := ef.Frontend

	resolvedEngineName := newEngineName
	if resolvedEngineName == "" && oldTargetIP == targetIP && oldTargetPort == targetPort {
		// Treat duplicate request to current target as no-op without remote lookup.
		resolvedEngineName = oldEngineName
	}
	if oldTargetIP == targetIP && oldTargetPort == targetPort && oldEngineName == resolvedEngineName {
		if ef.State != types.InstanceStateError {
			ef.ErrorMsg = ""
		}
		ef.Unlock()
		return nil
	}

	initiatorCreationRequired := frontend == types.FrontendSPDKTCPBlockdev && ef.initiator == nil
	ef.isSwitchingOver = true
	ef.Unlock()

	defer func() {
		ef.Lock()
		ef.isSwitchingOver = false
		ef.Unlock()

		if updateRequired {
			ef.UpdateCh <- nil
		}
	}()

	if resolvedEngineName == "" {
		resolvedEngineName, err = ef.resolveEngineNameByTargetAddress(targetAddress)
		if err != nil {
			return errors.Wrapf(err, "failed to resolve engine name for target address %s", targetAddress)
		}
	}
	newNQN, newNGUID := ef.getVolumeTargetIdentity()
	phase := SwitchoverPhase(switchoverPhase)

	switch frontend {
	case types.FrontendSPDKTCPNvmf:
		if switchoverPhase != "" {
			return ef.switchOverTargetNvmfPhased(phase, resolvedEngineName, targetIP, targetPort, oldEngineName, oldTargetIP, oldTargetPort, newNQN, newNGUID, &updateRequired)
		}
		if err := ef.syncRemoteEngineTargetANAStatesWithRetry(oldEngineName, resolvedEngineName, oldTargetIP, oldTargetPort, targetIP, targetPort); err != nil {
			switchErr := errors.Wrapf(ErrSwitchOverTargetInternal,
				"failed to sync remote target ANA state for engine frontend %s switchover to %s: %v",
				ef.Name, targetAddress, err)
			ef.Lock()
			ef.ErrorMsg = switchErr.Error()
			ef.Unlock()
			updateRequired = true
			return switchErr
		}

		ef.Lock()
		ef.EngineName = resolvedEngineName
		ef.EngineIP = targetIP
		ef.NvmeTcpFrontend.TargetIP = targetIP
		ef.NvmeTcpFrontend.TargetPort = targetPort
		ef.NvmeTcpFrontend.Nqn = newNQN
		ef.NvmeTcpFrontend.Nguid = newNGUID
		ef.Endpoint = GetNvmfEndpoint(newNQN, targetIP, targetPort)
		ef.syncCurrentNVMeTCPPathLocked()
		if ef.State != types.InstanceStateError {
			ef.ErrorMsg = ""
		}
		ef.Unlock()
		updateRequired = true

		ef.log.WithFields(logrus.Fields{
			"oldEngineName": oldEngineName,
			"engineName":    resolvedEngineName,
			"oldTargetIP":   oldTargetIP,
			"oldTargetPort": oldTargetPort,
			"targetIP":      targetIP,
			"targetPort":    targetPort,
		}).Info("Switched over engine frontend target")

		// Persist updated record AFTER successful switchover.
		ef.RLock()
		if err := saveEngineFrontendRecord(ef.metadataDir, ef); err != nil {
			ef.log.WithError(err).Warn("Failed to persist engine frontend record after switchover")
		}
		ef.RUnlock()

		return nil

	case types.FrontendSPDKTCPBlockdev:
		if initiatorCreationRequired {
			// Recreate initiator if the cached one is missing but frontend metadata is still valid.
			i, nqn, nguid, initErr := ef.newNvmeTcpInitiator()
			if initErr != nil {
				return errors.Wrapf(ErrSwitchOverTargetInternal, "failed to create initiator for engine frontend %s switchover: %v", ef.Name, initErr)
			}
			ef.Lock()
			ef.initiator = i
			ef.NvmeTcpFrontend.Nqn = nqn
			ef.NvmeTcpFrontend.Nguid = nguid
			ef.Unlock()
		}

		if switchoverPhase != "" {
			return ef.switchOverTargetBlockdevPhased(phase, resolvedEngineName, targetAddress, targetIP, targetPort, oldEngineName, oldTargetIP, oldTargetPort, newNQN, newNGUID, oldNQN, oldNGUID, oldEndpoint, oldDMDeviceIsBusy, &updateRequired)
		}

		// Step 1: Before connecting the new multipath path, ensure the new
		// engine target's ANA state is "inaccessible". This prevents the
		// kernel NVMe multipath layer from routing I/O to the new path
		// immediately upon discovery. The target was likely created with
		// "optimized" during Engine.Create(); we must demote it first.
		ef.log.WithFields(logrus.Fields{
			"newEngineName": resolvedEngineName,
			"targetIP":      targetIP,
			"targetPort":    targetPort,
		}).Info("Setting new engine target ANA state to inaccessible before multipath connect")
		if err := ef.setRemoteEngineTargetANAState(targetIP, resolvedEngineName, NvmeTCPANAStateInaccessible); err != nil {
			switchErr := errors.Wrapf(ErrSwitchOverTargetInternal,
				"failed to set new engine target %s ANA state to inaccessible before multipath connect: %v",
				resolvedEngineName, err)
			ef.Lock()
			ef.ErrorMsg = switchErr.Error()
			ef.Unlock()
			updateRequired = true
			return switchErr
		}

		// Step 2: Connect the new multipath path. The kernel will discover
		// the new path in "inaccessible" state and will NOT route I/O to it.
		alreadyConnectedAndSynced := false
		switchErr := ef.connectNvmeTCPPath(targetIP, targetPort)
		if switchErr != nil && !isNVMeTCPPathAlreadyConnectedError(switchErr) {
			// Plain connect failure. Step 1 demoted the new target to
			// inaccessible; restore it to optimized since the switchover
			// is being abandoned and the new engine may still be used.
			if rErr := ef.setRemoteEngineTargetANAState(targetIP, resolvedEngineName, NvmeTCPANAStateOptimized); rErr != nil {
				ef.log.WithError(rErr).Warn("Failed to restore new target ANA state to optimized after connect failure")
			}
		} else if switchErr != nil {
			// The new path was already connected from a previous
			// SwitchOverTarget attempt. In this case the new target's ANA
			// state is "inaccessible" (set above at step 1), and the old
			// path may already be disconnected. With no optimized path the
			// kernel hides the namespace block device, so
			// loadInitiatorNVMeDeviceInfo would fail. We must run the ANA
			// sync FIRST (which sets new→optimized) to restore the block
			// device, then reload initiator state.
			ef.log.WithError(switchErr).WithFields(logrus.Fields{
				"engineName": resolvedEngineName,
				"targetIP":   targetIP,
				"targetPort": targetPort,
			}).Warn("NVMe/TCP multipath path already connected during switchover, syncing ANA states before reloading initiator state")

			if anaErr := ef.syncRemoteEngineTargetANAStatesWithRetry(oldEngineName, resolvedEngineName, oldTargetIP, oldTargetPort, targetIP, targetPort); anaErr != nil {
				switchErr = errors.Wrapf(ErrSwitchOverTargetInternal,
					"failed to sync ANA states for already connected multipath target %s during engine frontend %s switchover: %v",
					targetAddress, ef.Name, anaErr)
			} else {
				transportServiceID := strconv.Itoa(int(targetPort))
				if reloadErr := ef.loadInitiatorNVMeDeviceInfo(targetIP, transportServiceID, newNQN); reloadErr != nil {
					switchErr = errors.Wrapf(ErrSwitchOverTargetInternal,
						"failed to reload engine frontend %s NVMe device info for already connected multipath target %s: %v",
						ef.Name, targetAddress, reloadErr)
					// ANA sync succeeded (new→optimized, old→inaccessible).
					// Revert ANA so the old path becomes functional again.
					if rErr := ef.setRemoteEngineTargetANAState(targetIP, resolvedEngineName, NvmeTCPANAStateInaccessible); rErr != nil {
						ef.log.WithError(rErr).Warn("Failed to revert new target ANA state during already-connected rollback")
					}
					if rErr := ef.setRemoteEngineTargetANAState(oldTargetIP, oldEngineName, NvmeTCPANAStateOptimized); rErr != nil {
						ef.log.WithError(rErr).Warn("Failed to restore old target ANA state during already-connected rollback")
					}
				} else if endpointErr := ef.loadInitiatorEndpoint(true); endpointErr != nil {
					switchErr = errors.Wrapf(ErrSwitchOverTargetInternal,
						"failed to reload engine frontend %s endpoint for already connected multipath target %s: %v",
						ef.Name, targetAddress, endpointErr)
					// Same ANA revert as above.
					if rErr := ef.setRemoteEngineTargetANAState(targetIP, resolvedEngineName, NvmeTCPANAStateInaccessible); rErr != nil {
						ef.log.WithError(rErr).Warn("Failed to revert new target ANA state during already-connected rollback")
					}
					if rErr := ef.setRemoteEngineTargetANAState(oldTargetIP, oldEngineName, NvmeTCPANAStateOptimized); rErr != nil {
						ef.log.WithError(rErr).Warn("Failed to restore old target ANA state during already-connected rollback")
					}
				} else {
					switchErr = nil
					alreadyConnectedAndSynced = true
				}
			}
		}

		if switchErr != nil {
			switchErr = errors.Wrapf(ErrSwitchOverTargetInternal, "failed to connect engine frontend %s multipath target %s: %v", ef.Name, targetAddress, switchErr)
			ef.Lock()
			ef.EngineName = oldEngineName
			ef.NvmeTcpFrontend.TargetIP = oldTargetIP
			ef.NvmeTcpFrontend.TargetPort = oldTargetPort
			ef.NvmeTcpFrontend.Nqn = oldNQN
			ef.NvmeTcpFrontend.Nguid = oldNGUID
			ef.Endpoint = oldEndpoint
			ef.dmDeviceIsBusy = oldDMDeviceIsBusy
			ef.ErrorMsg = switchErr.Error()
			ef.Unlock()
			updateRequired = true
			return switchErr
		}

		// Step 2b: Wait for the new NVMe controller to reach "live" state.
		// The nvme connect above returns immediately while the TCP handshake
		// completes asynchronously. Without this wait, the ANA sync below
		// could mark the old path inaccessible while the new controller is
		// still "connecting", leaving no routable path.
		if !alreadyConnectedAndSynced {
			if waitErr := ef.waitForNvmeTCPControllerLive(targetIP, targetPort); waitErr != nil {
				switchErr = errors.Wrapf(ErrSwitchOverTargetInternal,
					"failed to wait for new NVMe controller live state during engine frontend %s switchover to %s: %v",
					ef.Name, targetAddress, waitErr)
				ef.Lock()
				ef.EngineName = oldEngineName
				ef.NvmeTcpFrontend.TargetIP = oldTargetIP
				ef.NvmeTcpFrontend.TargetPort = oldTargetPort
				ef.NvmeTcpFrontend.Nqn = oldNQN
				ef.NvmeTcpFrontend.Nguid = oldNGUID
				ef.Endpoint = oldEndpoint
				ef.dmDeviceIsBusy = oldDMDeviceIsBusy
				ef.ErrorMsg = switchErr.Error()
				ef.Unlock()
				updateRequired = true
				return switchErr
			}
		}

		if !alreadyConnectedAndSynced {
			if err := ef.syncRemoteEngineTargetANAStatesWithRetry(oldEngineName, resolvedEngineName, oldTargetIP, oldTargetPort, targetIP, targetPort); err != nil {
				switchErr = errors.Wrapf(ErrSwitchOverTargetInternal,
					"failed to sync remote target ANA state for engine frontend %s switchover to %s: %v",
					ef.Name, targetAddress, err)
				ef.Lock()
				ef.EngineName = oldEngineName
				ef.NvmeTcpFrontend.TargetIP = oldTargetIP
				ef.NvmeTcpFrontend.TargetPort = oldTargetPort
				ef.NvmeTcpFrontend.Nqn = oldNQN
				ef.NvmeTcpFrontend.Nguid = oldNGUID
				ef.Endpoint = oldEndpoint
				ef.dmDeviceIsBusy = oldDMDeviceIsBusy
				ef.ErrorMsg = switchErr.Error()
				ef.Unlock()
				updateRequired = true
				return switchErr
			}

			transportServiceID := strconv.Itoa(int(targetPort))
			if reloadErr := ef.loadInitiatorNVMeDeviceInfo(targetIP, transportServiceID, newNQN); reloadErr != nil {
				switchErr = errors.Wrapf(ErrSwitchOverTargetInternal,
					"failed to reload engine frontend %s NVMe device info after ANA sync for multipath target %s: %v",
					ef.Name, targetAddress, reloadErr)
				// Revert ANA states so the old path becomes functional again.
				// syncRemoteEngineTargetANAStatesWithRetry already set
				// new→optimized, old→inaccessible. Reverse both.
				if anaErr := ef.setRemoteEngineTargetANAState(targetIP, resolvedEngineName, NvmeTCPANAStateInaccessible); anaErr != nil {
					ef.log.WithError(anaErr).Warn("Failed to revert new target ANA state to inaccessible during monolithic switchover rollback")
				}
				if anaErr := ef.setRemoteEngineTargetANAState(oldTargetIP, oldEngineName, NvmeTCPANAStateOptimized); anaErr != nil {
					ef.log.WithError(anaErr).Warn("Failed to restore old target ANA state to optimized during monolithic switchover rollback")
				}
				ef.Lock()
				ef.EngineName = oldEngineName
				ef.NvmeTcpFrontend.TargetIP = oldTargetIP
				ef.NvmeTcpFrontend.TargetPort = oldTargetPort
				ef.NvmeTcpFrontend.Nqn = oldNQN
				ef.NvmeTcpFrontend.Nguid = oldNGUID
				ef.Endpoint = oldEndpoint
				ef.dmDeviceIsBusy = oldDMDeviceIsBusy
				ef.ErrorMsg = switchErr.Error()
				ef.Unlock()
				updateRequired = true
				return switchErr
			}
			// During a live switchover the dm device is actively used by the
			// workload. Pass true to skip the strict namespace name check
			// which can transiently fail during multipath ANA transitions.
			if endpointErr := ef.loadInitiatorEndpoint(true); endpointErr != nil {
				switchErr = errors.Wrapf(ErrSwitchOverTargetInternal,
					"failed to reload engine frontend %s endpoint after ANA sync for multipath target %s: %v",
					ef.Name, targetAddress, endpointErr)
				// Revert ANA states for the same reason as above.
				if anaErr := ef.setRemoteEngineTargetANAState(targetIP, resolvedEngineName, NvmeTCPANAStateInaccessible); anaErr != nil {
					ef.log.WithError(anaErr).Warn("Failed to revert new target ANA state to inaccessible during monolithic switchover rollback")
				}
				if anaErr := ef.setRemoteEngineTargetANAState(oldTargetIP, oldEngineName, NvmeTCPANAStateOptimized); anaErr != nil {
					ef.log.WithError(anaErr).Warn("Failed to restore old target ANA state to optimized during monolithic switchover rollback")
				}
				ef.Lock()
				ef.EngineName = oldEngineName
				ef.NvmeTcpFrontend.TargetIP = oldTargetIP
				ef.NvmeTcpFrontend.TargetPort = oldTargetPort
				ef.NvmeTcpFrontend.Nqn = oldNQN
				ef.NvmeTcpFrontend.Nguid = oldNGUID
				ef.Endpoint = oldEndpoint
				ef.dmDeviceIsBusy = oldDMDeviceIsBusy
				ef.ErrorMsg = switchErr.Error()
				ef.Unlock()
				updateRequired = true
				return switchErr
			}
		}

		ef.Lock()
		ef.EngineName = resolvedEngineName
		ef.EngineIP = targetIP
		ef.NvmeTcpFrontend.TargetIP = targetIP
		ef.NvmeTcpFrontend.TargetPort = targetPort
		ef.NvmeTcpFrontend.Nqn = newNQN
		ef.NvmeTcpFrontend.Nguid = newNGUID
		ef.Endpoint = ef.getInitiatorEndpoint()
		ef.dmDeviceIsBusy = oldDMDeviceIsBusy
		ef.syncCurrentNVMeTCPPathLocked()
		if ef.State != types.InstanceStateError {
			ef.ErrorMsg = ""
		}
		ef.Unlock()
		updateRequired = true

		ef.log.WithFields(logrus.Fields{
			"oldEngineName": oldEngineName,
			"engineName":    resolvedEngineName,
			"oldTargetIP":   oldTargetIP,
			"oldTargetPort": oldTargetPort,
			"targetIP":      targetIP,
			"targetPort":    targetPort,
		}).Info("Switched over engine frontend target")

		// Do NOT explicitly disconnect the old controller path. Removing it
		// immediately can race with the kernel processing the ANA state
		// change on the new path — if the kernel hasn't fully switched to
		// the new optimized path when the old controller is yanked, a brief
		// "no available path" window causes I/O errors that make ext4 go
		// read-only. Instead, let the kernel handle the stale controller
		// through ctrl-loss-tmo. The old path's ANA is already set to
		// "inaccessible" so no I/O will route through it.

		// Persist updated record AFTER successful switchover.
		ef.RLock()
		if err := saveEngineFrontendRecord(ef.metadataDir, ef); err != nil {
			ef.log.WithError(err).Warn("Failed to persist engine frontend record after switchover")
		}
		ef.RUnlock()

		return nil

	default:
		return errors.Wrapf(ErrSwitchOverTargetPrecondition, "frontend %s does not support target switchover for engine frontend %s", ef.Frontend, ef.Name)
	}
}

// switchOverTargetNvmfPhased executes a single phase of the nvmf frontend switchover.
// The control plane drives progression through preparing → switching → promoting.
func (ef *EngineFrontend) switchOverTargetNvmfPhased(phase SwitchoverPhase, newEngineName string, newTargetIP string, newTargetPort int32, oldEngineName, oldTargetIP string, oldTargetPort int32, newNQN, newNGUID string, updateRequired *bool) error {
	resolvedNewEngineName, err := ef.resolveRemoteEngineName(newTargetIP, newTargetPort, newEngineName)
	if err != nil {
		return errors.Wrapf(ErrSwitchOverTargetInternal, "failed to resolve new engine name for %s:%d: %v", newTargetIP, newTargetPort, err)
	}

	switch phase {
	case SwitchoverPhasePreparing:
		// Phase 1: Set new target → non-optimized. Old stays optimized.
		// Kernel prefers old; new is usable fallback but receives no I/O.
		ef.log.WithFields(logrus.Fields{
			"phase":         phase,
			"newEngineName": resolvedNewEngineName,
			"newTargetIP":   newTargetIP,
		}).Info("Switchover nvmf preparing: setting new target ANA to non-optimized")
		if err := ef.setRemoteEngineTargetANAState(newTargetIP, resolvedNewEngineName, NvmeTCPANAStateNonOptimized); err != nil {
			return errors.Wrapf(ErrSwitchOverTargetInternal,
				"failed to set new engine target %s ANA to non-optimized during preparing phase: %v",
				resolvedNewEngineName, err)
		}
		return nil

	case SwitchoverPhaseSwitching:
		// Phase 2: Set old target → inaccessible. Kernel falls back to new non-optimized path.
		ef.log.WithFields(logrus.Fields{
			"phase":         phase,
			"oldEngineName": oldEngineName,
			"oldTargetIP":   oldTargetIP,
		}).Info("Switchover nvmf switching: setting old target ANA to inaccessible")
		if oldEngineName != resolvedNewEngineName || oldTargetIP != newTargetIP {
			resolvedOldEngineName, resolveErr := ef.resolveRemoteEngineName(oldTargetIP, oldTargetPort, oldEngineName)
			if resolveErr != nil {
				return errors.Wrapf(ErrSwitchOverTargetInternal, "failed to resolve old engine name for %s:%d: %v", oldTargetIP, oldTargetPort, resolveErr)
			}
			if err := ef.setRemoteEngineTargetANAState(oldTargetIP, resolvedOldEngineName, NvmeTCPANAStateInaccessible); err != nil {
				if isSubsystemNotFoundError(err) {
					ef.log.WithError(err).Info("Old engine subsystem already removed, skipping ANA demotion in switching phase")
				} else {
					return errors.Wrapf(ErrSwitchOverTargetInternal,
						"failed to set old engine target %s ANA to inaccessible during switching phase: %v",
						resolvedOldEngineName, err)
				}
			}
		}
		return nil

	case SwitchoverPhasePromoting:
		// Phase 3: Set new target → optimized, update internal state, persist.

		// Resolve the old engine name for rollback. The switching phase
		// resolved it independently, but each phased call is stateless.
		resolvedOldEngineName := oldEngineName
		if oldTargetIP != "" && oldTargetPort != 0 {
			if resolved, resolveErr := ef.resolveRemoteEngineName(oldTargetIP, oldTargetPort, oldEngineName); resolveErr == nil {
				resolvedOldEngineName = resolved
			} else {
				ef.log.WithError(resolveErr).Warn("Failed to resolve old engine name in promoting phase, using unresolved name for rollback")
			}
		}

		ef.log.WithFields(logrus.Fields{
			"phase":         phase,
			"newEngineName": resolvedNewEngineName,
			"newTargetIP":   newTargetIP,
		}).Info("Switchover nvmf promoting: setting new target ANA to optimized")
		if err := ef.setRemoteEngineTargetANAState(newTargetIP, resolvedNewEngineName, NvmeTCPANAStateOptimized); err != nil {
			promoteErr := errors.Wrapf(ErrSwitchOverTargetInternal,
				"failed to set new engine target %s ANA to optimized during promoting phase: %v",
				resolvedNewEngineName, err)
			if rbErr := ef.rollbackRemoteEngineTargetANAStates(oldTargetIP, resolvedOldEngineName, newTargetIP, resolvedNewEngineName); rbErr != nil {
				ef.log.WithError(rbErr).Warn("Failed to roll back ANA states during nvmf promoting phase")
				promoteErr = multierr.Append(promoteErr, rbErr)
			}
			return promoteErr
		}

		ef.Lock()
		ef.EngineName = resolvedNewEngineName
		ef.EngineIP = newTargetIP
		ef.NvmeTcpFrontend.TargetIP = newTargetIP
		ef.NvmeTcpFrontend.TargetPort = newTargetPort
		ef.NvmeTcpFrontend.Nqn = newNQN
		ef.NvmeTcpFrontend.Nguid = newNGUID
		ef.Endpoint = GetNvmfEndpoint(newNQN, newTargetIP, newTargetPort)
		ef.syncCurrentNVMeTCPPathLocked()
		if ef.State != types.InstanceStateError {
			ef.ErrorMsg = ""
		}
		ef.Unlock()
		*updateRequired = true

		ef.RLock()
		if err := saveEngineFrontendRecord(ef.metadataDir, ef); err != nil {
			ef.log.WithError(err).Warn("Failed to persist engine frontend record after promoting phase")
		}
		ef.RUnlock()

		ef.log.WithFields(logrus.Fields{
			"oldEngineName": oldEngineName,
			"engineName":    resolvedNewEngineName,
			"oldTargetIP":   oldTargetIP,
			"newTargetIP":   newTargetIP,
		}).Info("Switchover nvmf promoting complete")
		return nil

	default:
		return errors.Wrapf(ErrSwitchOverTargetInvalidInput, "unknown switchover phase %q for engine frontend %s", phase, ef.Name)
	}
}

// switchOverTargetBlockdevPhased executes a single phase of the blockdev frontend switchover.
// The control plane drives progression through preparing → switching → promoting.
func (ef *EngineFrontend) switchOverTargetBlockdevPhased(phase SwitchoverPhase, newEngineName, targetAddress string, newTargetIP string, newTargetPort int32, oldEngineName, oldTargetIP string, oldTargetPort int32, newNQN, newNGUID, oldNQN, oldNGUID, oldEndpoint string, oldDMDeviceIsBusy bool, updateRequired *bool) error {
	switch phase {
	case SwitchoverPhasePreparing:
		// Step 1: Set new target → inaccessible to prevent routing on connect.
		ef.log.WithFields(logrus.Fields{
			"phase":         phase,
			"newEngineName": newEngineName,
			"newTargetIP":   newTargetIP,
		}).Info("Switchover blockdev preparing: setting new target ANA to inaccessible before connect")
		if err := ef.setRemoteEngineTargetANAState(newTargetIP, newEngineName, NvmeTCPANAStateInaccessible); err != nil {
			setErr := errors.Wrapf(ErrSwitchOverTargetInternal,
				"failed to set new engine target %s ANA to inaccessible before connect: %v",
				newEngineName, err)
			ef.Lock()
			ef.ErrorMsg = setErr.Error()
			ef.Unlock()
			*updateRequired = true
			return setErr
		}

		// Step 2: Connect new multipath path.
		connectErr := ef.connectNvmeTCPPath(newTargetIP, newTargetPort)
		if connectErr != nil && isNVMeTCPPathAlreadyConnectedError(connectErr) {
			ef.log.WithError(connectErr).Info("NVMe/TCP path already connected during preparing phase, continuing")
			connectErr = nil
		}
		if connectErr != nil {
			connectErr = errors.Wrapf(ErrSwitchOverTargetInternal,
				"failed to connect engine frontend %s multipath target %s during preparing phase: %v",
				ef.Name, targetAddress, connectErr)
			ef.Lock()
			ef.ErrorMsg = connectErr.Error()
			ef.Unlock()
			*updateRequired = true
			return connectErr
		}

		// Step 3: Wait for controller live.
		if waitErr := ef.waitForNvmeTCPControllerLive(newTargetIP, newTargetPort); waitErr != nil {
			waitErr = errors.Wrapf(ErrSwitchOverTargetInternal,
				"failed to wait for new NVMe controller live during preparing phase for %s: %v",
				ef.Name, waitErr)
			ef.Lock()
			ef.ErrorMsg = waitErr.Error()
			ef.Unlock()
			*updateRequired = true
			return waitErr
		}

		// Step 4: Set new target → non-optimized (ANA phase 1).
		ef.log.Info("Switchover blockdev preparing: setting new target ANA to non-optimized")
		if err := ef.setRemoteEngineTargetANAState(newTargetIP, newEngineName, NvmeTCPANAStateNonOptimized); err != nil {
			setErr := errors.Wrapf(ErrSwitchOverTargetInternal,
				"failed to set new engine target %s ANA to non-optimized during preparing phase: %v",
				newEngineName, err)
			ef.Lock()
			ef.ErrorMsg = setErr.Error()
			ef.Unlock()
			*updateRequired = true
			return setErr
		}

		ef.log.Info("Switchover blockdev preparing phase complete")
		return nil

	case SwitchoverPhaseSwitching:
		// Set old target → inaccessible (ANA phase 2).
		ef.log.WithFields(logrus.Fields{
			"phase":         phase,
			"oldEngineName": oldEngineName,
			"oldTargetIP":   oldTargetIP,
		}).Info("Switchover blockdev switching: setting old target ANA to inaccessible")
		if oldEngineName != newEngineName || oldTargetIP != newTargetIP {
			if err := ef.setRemoteEngineTargetANAState(oldTargetIP, oldEngineName, NvmeTCPANAStateInaccessible); err != nil {
				if isSubsystemNotFoundError(err) {
					ef.log.WithError(err).Info("Old engine subsystem already removed, skipping ANA demotion in switching phase")
				} else {
					switchErr := errors.Wrapf(ErrSwitchOverTargetInternal,
						"failed to set old engine target %s ANA to inaccessible during switching phase: %v",
						oldEngineName, err)
					ef.Lock()
					ef.ErrorMsg = switchErr.Error()
					ef.Unlock()
					*updateRequired = true
					return switchErr
				}
			}
		}

		ef.log.Info("Switchover blockdev switching phase complete")
		return nil

	case SwitchoverPhasePromoting:
		// Resolve the old engine name for rollback. Each phased call is
		// stateless, so we resolve independently of the switching phase.
		resolvedOldEngineName := oldEngineName
		if oldTargetIP != "" && oldTargetPort != 0 {
			if resolved, resolveErr := ef.resolveRemoteEngineName(oldTargetIP, oldTargetPort, oldEngineName); resolveErr == nil {
				resolvedOldEngineName = resolved
			} else {
				ef.log.WithError(resolveErr).Warn("Failed to resolve old engine name in promoting phase, using unresolved name for rollback")
			}
		}

		// Step 1: Set new target → optimized (ANA phase 3).
		ef.log.WithFields(logrus.Fields{
			"phase":         phase,
			"newEngineName": newEngineName,
			"newTargetIP":   newTargetIP,
		}).Info("Switchover blockdev promoting: setting new target ANA to optimized")
		if err := ef.setRemoteEngineTargetANAState(newTargetIP, newEngineName, NvmeTCPANAStateOptimized); err != nil {
			promoteErr := errors.Wrapf(ErrSwitchOverTargetInternal,
				"failed to set new engine target %s ANA to optimized during promoting phase: %v",
				newEngineName, err)
			if rbErr := ef.rollbackRemoteEngineTargetANAStates(oldTargetIP, resolvedOldEngineName, newTargetIP, newEngineName); rbErr != nil {
				ef.log.WithError(rbErr).Warn("Failed to roll back ANA states during blockdev promoting phase")
				promoteErr = multierr.Append(promoteErr, rbErr)
			}
			ef.Lock()
			ef.ErrorMsg = promoteErr.Error()
			ef.Unlock()
			*updateRequired = true
			return promoteErr
		}

		// Step 2: Reload NVMe device info.
		transportServiceID := strconv.Itoa(int(newTargetPort))
		if reloadErr := ef.loadInitiatorNVMeDeviceInfo(newTargetIP, transportServiceID, newNQN); reloadErr != nil {
			promoteErr := errors.Wrapf(ErrSwitchOverTargetInternal,
				"failed to reload NVMe device info during promoting phase for %s: %v",
				ef.Name, reloadErr)
			// Revert ANA states so the old path becomes functional again.
			// New target was set to OPTIMIZED in step 1; old target was set
			// to INACCESSIBLE in the switching phase. Reverse both so the
			// kernel routes I/O back through the old (still-running) engine.
			if rbErr := ef.rollbackRemoteEngineTargetANAStates(oldTargetIP, resolvedOldEngineName, newTargetIP, newEngineName); rbErr != nil {
				ef.log.WithError(rbErr).Warn("Failed to roll back ANA states during blockdev promoting step 2 rollback")
				promoteErr = multierr.Append(promoteErr, rbErr)
			}
			ef.Lock()
			ef.EngineName = oldEngineName
			ef.NvmeTcpFrontend.TargetIP = oldTargetIP
			ef.NvmeTcpFrontend.TargetPort = oldTargetPort
			ef.NvmeTcpFrontend.Nqn = oldNQN
			ef.NvmeTcpFrontend.Nguid = oldNGUID
			ef.Endpoint = oldEndpoint
			ef.dmDeviceIsBusy = oldDMDeviceIsBusy
			ef.ErrorMsg = promoteErr.Error()
			ef.Unlock()
			*updateRequired = true
			return promoteErr
		}

		// Step 3: Load endpoint.
		if endpointErr := ef.loadInitiatorEndpoint(true); endpointErr != nil {
			promoteErr := errors.Wrapf(ErrSwitchOverTargetInternal,
				"failed to reload endpoint during promoting phase for %s: %v",
				ef.Name, endpointErr)
			if rbErr := ef.rollbackRemoteEngineTargetANAStates(oldTargetIP, resolvedOldEngineName, newTargetIP, newEngineName); rbErr != nil {
				ef.log.WithError(rbErr).Warn("Failed to roll back ANA states during blockdev promoting step 3 rollback")
				promoteErr = multierr.Append(promoteErr, rbErr)
			}
			ef.Lock()
			ef.EngineName = oldEngineName
			ef.NvmeTcpFrontend.TargetIP = oldTargetIP
			ef.NvmeTcpFrontend.TargetPort = oldTargetPort
			ef.NvmeTcpFrontend.Nqn = oldNQN
			ef.NvmeTcpFrontend.Nguid = oldNGUID
			ef.Endpoint = oldEndpoint
			ef.dmDeviceIsBusy = oldDMDeviceIsBusy
			ef.ErrorMsg = promoteErr.Error()
			ef.Unlock()
			*updateRequired = true
			return promoteErr
		}

		// Step 4: Commit new state.
		ef.Lock()
		ef.EngineName = newEngineName
		ef.EngineIP = newTargetIP
		ef.NvmeTcpFrontend.TargetIP = newTargetIP
		ef.NvmeTcpFrontend.TargetPort = newTargetPort
		ef.NvmeTcpFrontend.Nqn = newNQN
		ef.NvmeTcpFrontend.Nguid = newNGUID
		ef.Endpoint = ef.getInitiatorEndpoint()
		ef.dmDeviceIsBusy = oldDMDeviceIsBusy
		ef.syncCurrentNVMeTCPPathLocked()
		if ef.State != types.InstanceStateError {
			ef.ErrorMsg = ""
		}
		ef.Unlock()
		*updateRequired = true

		ef.RLock()
		if err := saveEngineFrontendRecord(ef.metadataDir, ef); err != nil {
			ef.log.WithError(err).Warn("Failed to persist engine frontend record after promoting phase")
		}
		ef.RUnlock()

		ef.log.WithFields(logrus.Fields{
			"oldEngineName": oldEngineName,
			"engineName":    newEngineName,
			"oldTargetIP":   oldTargetIP,
			"newTargetIP":   newTargetIP,
		}).Info("Switchover blockdev promoting complete")
		return nil

	default:
		return errors.Wrapf(ErrSwitchOverTargetInvalidInput, "unknown switchover phase %q for engine frontend %s", phase, ef.Name)
	}
}

func (ef *EngineFrontend) connectNvmeTCPPath(transportAddress string, transportPort int32) error {
	transportServiceID := strconv.Itoa(int(transportPort))
	if ef.connectNvmeTCPPathFn != nil {
		return ef.connectNvmeTCPPathFn(transportAddress, transportServiceID)
	}
	if ef.initiator == nil {
		return errors.Wrapf(ErrSwitchOverTargetInternal, "initiator is nil for engine frontend %s", ef.Name)
	}
	return ef.initiator.ConnectNVMeTCPPath(transportAddress, transportServiceID)
}

func (ef *EngineFrontend) reconnectNvmeTCPPath(transportAddress string, transportPort int32) error {
	transportServiceID := strconv.Itoa(int(transportPort))
	if ef.reconnectNvmeTCPPathFn != nil {
		return ef.reconnectNvmeTCPPathFn(transportAddress, transportServiceID)
	}
	if ef.initiator == nil {
		return errors.Wrapf(ErrSwitchOverTargetInternal, "initiator is nil for engine frontend %s", ef.Name)
	}
	return ef.initiator.ReconnectNVMeTCPPath(transportAddress, transportServiceID)
}

// waitForNvmeTCPControllerLive waits for the NVMe-TCP controller at the given
// address to reach "live" state. The nvme connect command returns immediately
// while the TCP handshake completes asynchronously. Without this wait, the ANA
// sync could mark the old path inaccessible while the new controller is still
// "connecting", leaving no routable path and causing I/O errors.
func (ef *EngineFrontend) waitForNvmeTCPControllerLive(transportAddress string, transportPort int32) error {
	if ef.waitForNvmeTCPControllerLiveFn != nil {
		return ef.waitForNvmeTCPControllerLiveFn(transportAddress, transportPort)
	}
	if ef.initiator == nil {
		return nil
	}

	transportServiceID := strconv.Itoa(int(transportPort))

	const maxAttempts = 30
	const retryInterval = 500 * time.Millisecond

	return ef.initiator.WaitForControllerLive(transportAddress, transportServiceID, maxAttempts, retryInterval)
}

func (ef *EngineFrontend) loadInitiatorNVMeDeviceInfo(transportAddress, transportServiceID, subsystemNQN string) error {
	if ef.loadInitiatorNVMeDeviceInfoFn != nil {
		return ef.loadInitiatorNVMeDeviceInfoFn(transportAddress, transportServiceID, subsystemNQN)
	}
	if ef.initiator == nil {
		return errors.Wrapf(ErrSwitchOverTargetInternal, "initiator is nil for engine frontend %s", ef.Name)
	}
	return ef.initiator.LoadNVMeDeviceInfo(transportAddress, transportServiceID, subsystemNQN)
}

func (ef *EngineFrontend) loadInitiatorEndpoint(dmDeviceIsBusy bool) error {
	if ef.loadInitiatorEndpointFn != nil {
		return ef.loadInitiatorEndpointFn(dmDeviceIsBusy)
	}
	if ef.initiator == nil {
		return errors.Wrapf(ErrSwitchOverTargetInternal, "initiator is nil for engine frontend %s", ef.Name)
	}
	return ef.initiator.LoadEndpointForNvmeTcpFrontend(dmDeviceIsBusy)
}

func (ef *EngineFrontend) getInitiatorEndpoint() string {
	if ef.getInitiatorEndpointFn != nil {
		return ef.getInitiatorEndpointFn()
	}
	if ef.initiator == nil {
		return ""
	}
	return ef.initiator.GetEndpoint()
}

func (ef *EngineFrontend) resolveEngineNameByTargetAddress(targetAddress string) (string, error) {
	if ef.resolveEngineNameByTargetAddressFn != nil {
		return ef.resolveEngineNameByTargetAddressFn(targetAddress)
	}

	targetIP, targetPort, err := splitHostPort(targetAddress)
	if err != nil {
		return "", errors.Wrapf(ErrSwitchOverTargetInvalidInput, "failed to split target address %s: %v", targetAddress, err)
	}
	if targetIP == "" || targetPort == 0 {
		return "", errors.Wrapf(ErrSwitchOverTargetInvalidInput, "invalid target address %s", targetAddress)
	}

	targetSpdkClient, err := GetServiceClient(targetAddress)
	if err != nil {
		return "", errors.Wrapf(ErrSwitchOverTargetInternal, "failed to get SPDK client for target address %s: %v", targetAddress, err)
	}
	defer func() {
		if errClose := targetSpdkClient.Close(); errClose != nil {
			ef.log.WithError(errClose).Error("Failed to close target SPDK client")
		}
	}()

	engineMap, err := targetSpdkClient.EngineList()
	if err != nil {
		return "", errors.Wrapf(ErrSwitchOverTargetInternal, "failed to list engines from target address %s: %v", targetAddress, err)
	}

	matchedEngineName := ""
	for engineName, e := range engineMap {
		if e == nil {
			continue
		}
		if e.IP == targetIP && e.Port == targetPort {
			if matchedEngineName != "" {
				return "", errors.Wrapf(ErrSwitchOverTargetPrecondition, "multiple engines match target address %s", targetAddress)
			}
			matchedEngineName = engineName
		}
	}

	if matchedEngineName == "" {
		return "", errors.Wrapf(ErrSwitchOverTargetEngineNotFound, "cannot find engine by target address %s", targetAddress)
	}

	return matchedEngineName, nil
}

func (ef *EngineFrontend) SnapshotCreate(inputSnapshotName string) (snapshotName string, err error) {
	ef.log.Infof("Creating snapshot %s", inputSnapshotName)

	return ef.snapshotOperation(inputSnapshotName, SnapshotOperationCreate, nil)
}

func (ef *EngineFrontend) SnapshotDelete(snapshotName string) (err error) {
	ef.log.Infof("Deleting snapshot %s", snapshotName)

	_, err = ef.snapshotOperation(snapshotName, SnapshotOperationDelete, nil)
	return err
}

func (ef *EngineFrontend) SnapshotRevert(snapshotName string) (err error) {
	ef.log.Infof("Reverting snapshot %s", snapshotName)

	_, err = ef.snapshotOperation(snapshotName, SnapshotOperationRevert, nil)
	return err
}

func (ef *EngineFrontend) SnapshotPurge() (err error) {
	ef.log.Infof("Purging snapshots")

	_, err = ef.snapshotOperation("", SnapshotOperationPurge, nil)
	return err
}

func (ef *EngineFrontend) snapshotOperation(inputSnapshotName string, snapshotOp SnapshotOperationType, opts any) (snapshotName string, err error) {
	updateRequired := false

	ef.Lock()
	if ef.isCreating {
		ef.Unlock()
		return "", fmt.Errorf("engine frontend %s is still creating", ef.Name)
	}
	if ef.isSwitchingOver {
		ef.Unlock()
		return "", errors.Wrapf(ErrSwitchOverTargetPrecondition, "engine frontend %s is switching over target", ef.Name)
	}

	defer func() {
		ef.Unlock()

		if updateRequired {
			ef.UpdateCh <- nil
		}
	}()

	var engineFrontendErr error

	defer func() {
		if engineFrontendErr != nil {
			if ef.State != types.InstanceStateError {
				ef.log.Error("Setting engine frontend to error state due to snapshot operation failure")
				ef.State = types.InstanceStateError
				updateRequired = true
			}
			ef.ErrorMsg = engineFrontendErr.Error()
		} else {
			if ef.State != types.InstanceStateError {
				ef.ErrorMsg = ""
			}
		}
	}()

	// Pause the IO, flush outstanding IO and attempt to synchronize filesystem by suspending the NVMe initiator
	if snapshotOp == SnapshotOperationCreate {
		if ef.isSuspendSupported() {
			ef.log.Infof("Suspending before the snapshot operation %s for snapshot %s", snapshotOp, inputSnapshotName)
			if err := ef.suspend(false, false); err != nil {
				return "", errors.Wrapf(err, "failed to suspend before the snapshot operation %s for snapshot %q", snapshotOp, inputSnapshotName)
			}
			defer func() {
				ef.log.Infof("Resuming after the snapshot operation %s for snapshot %s", snapshotOp, inputSnapshotName)
				if resumeErr := ef.resume(); resumeErr != nil {
					engineFrontendErr = errors.Wrapf(resumeErr, "failed to resume after the snapshot operation %s for snapshot %q", snapshotOp, inputSnapshotName)
					return
				}
				ef.log.Infof("Resumed after the snapshot operation %s for snapshot %q", snapshotOp, inputSnapshotName)
			}()
		}
	}

	engineSpdkClient, err := GetServiceClient(ef.getEngineServiceAddress())
	if err != nil {
		return "", errors.Wrapf(err, "failed to get SPDK client to perform snapshot operation %s for snapshot %q", snapshotOp, inputSnapshotName)
	}
	defer func() {
		if errClose := engineSpdkClient.Close(); errClose != nil {
			ef.log.WithError(errClose).Error("Failed to close SPDK client")
		}
	}()

	switch snapshotOp {
	case SnapshotOperationCreate:
		return engineSpdkClient.EngineSnapshotCreate(ef.EngineName, inputSnapshotName)
	case SnapshotOperationDelete:
		return "", engineSpdkClient.EngineSnapshotDelete(ef.EngineName, inputSnapshotName)
	case SnapshotOperationRevert:
		return "", engineSpdkClient.EngineSnapshotRevert(ef.EngineName, inputSnapshotName)
	case SnapshotOperationPurge:
		return "", engineSpdkClient.EngineSnapshotPurge(ef.EngineName)
	default:
		return "", fmt.Errorf("unknown snapshot operation %v for snapshot %q", snapshotOp, inputSnapshotName)
	}
}

func (ef *EngineFrontend) isSuspendSupported() bool {
	return ef.Frontend == types.FrontendSPDKTCPBlockdev && ef.Endpoint != ""
}

func (ef *EngineFrontend) suspend(noflush, nolockfs bool) error {
	if ef.initiator == nil {
		return fmt.Errorf("initiator is not initialized for engine frontend %s", ef.Name)
	}
	return ef.initiator.Suspend(noflush, nolockfs)
}

func (ef *EngineFrontend) resume() error {
	if ef.initiator == nil {
		return fmt.Errorf("initiator is not initialized for engine frontend %s", ef.Name)
	}
	return ef.initiator.Resume()
}

// ValidateAndUpdate validates the engine frontend (initiator-side) state and updates
// fields (e.g., Endpoint) as needed. Called periodically by the server verify loop.
// This only validates the local initiator/device state — target-side subsystem
// validation is the responsibility of the Engine.
func (ef *EngineFrontend) ValidateAndUpdate(spdkClient *spdkclient.Client) (err error) {
	updateRequired := false

	ef.Lock()
	defer func() {
		if err != nil {
			if ef.State != types.InstanceStateError {
				ef.log.WithError(err).Error("Setting engine frontend to error state due to validation failure")
				ef.State = types.InstanceStateError
				updateRequired = true
			}
			ef.ErrorMsg = err.Error()
		}
		ef.Unlock()

		if updateRequired {
			ef.UpdateCh <- nil
		}
	}()

	if ef.State != types.InstanceStateRunning {
		return nil
	}

	if ef.isCreating {
		ef.log.Debug("Engine frontend is creating, will skip the validation and update")
		return nil
	}

	if ef.isSwitchingOver {
		ef.log.Debug("Engine frontend is switching over target, will skip the validation and update")
		return nil
	}

	if ef.isExpanding {
		ef.log.Debug("Engine frontend is expanding, will skip the validation and update")
		return nil
	}

	if ef.IsRestoring {
		ef.log.Debug("Engine frontend is restoring, will skip the validation and update")
		return nil
	}

	return ef.validateAndUpdateFrontend(spdkClient)
}

func (ef *EngineFrontend) validateAndUpdateFrontend(client *spdkclient.Client) (err error) {
	if !types.IsFrontendSupported(ef.Frontend) {
		return fmt.Errorf("unknown frontend type %s", ef.Frontend)
	}
	if ef.Frontend == types.FrontendEmpty {
		if ef.Endpoint != "" {
			return fmt.Errorf("found non-empty endpoint %s for engine frontend %s with empty frontend", ef.Endpoint, ef.Name)
		}
		return nil
	}
	switch ef.Frontend {
	case types.FrontendUBLK:
		return ef.validateAndUpdateUblkFrontend(client)
	case types.FrontendSPDKTCPBlockdev, types.FrontendSPDKTCPNvmf:
		return ef.validateAndUpdateNvmeTcpFrontend()
	default:
		return fmt.Errorf("unsupported frontend type %s for engine frontend %s validation", ef.Frontend, ef.Name)
	}
}

func (ef *EngineFrontend) validateAndUpdateUblkFrontend(client *spdkclient.Client) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "failed to validateAndUpdateUblkFrontend for engine frontend %v", ef.Name)
		}
	}()
	if ef.UblkFrontend == nil {
		return fmt.Errorf("UblkFrontend is nil")
	}

	ublkDeviceList, err := client.UblkGetDisks(ef.UblkFrontend.UblkID)
	if err != nil {
		return err
	}
	for _, ublkDevice := range ublkDeviceList {
		if ublkDevice.BdevName == ef.EngineName && ublkDevice.ID != ef.UblkFrontend.UblkID {
			return fmt.Errorf("found mismatching between UblkFrontend.UblkID %v and actual ublk device id %v", ef.UblkFrontend.UblkID, ublkDevice.ID)
		}
	}
	return nil
}

// validateAndUpdateNvmeTcpFrontend validates the initiator-side NVMe/TCP state:
// ensures the initiator exists, loads device info, and checks endpoint consistency.
func (ef *EngineFrontend) validateAndUpdateNvmeTcpFrontend() (err error) {
	if ef.NvmeTcpFrontend == nil {
		return fmt.Errorf("failed to validateAndUpdateNvmeTcpFrontend for engine frontend %v: NvmeTcpFrontend is nil", ef.Name)
	}

	switch ef.Frontend {
	case types.FrontendSPDKTCPBlockdev:
		if ef.initiator == nil {
			nvmeTCPInfo := &initiator.NVMeTCPInfo{
				SubsystemNQN: ef.NvmeTcpFrontend.Nqn,
			}
			i, err := initiator.NewInitiator(ef.VolumeName, initiator.HostProc, nvmeTCPInfo, nil)
			if err != nil {
				return errors.Wrapf(err, "failed to create initiator for engine frontend %v during frontend validation and update", ef.Name)
			}
			ef.initiator = i
		}
		if ef.initiator.NVMeTCPInfo == nil {
			return fmt.Errorf("invalid initiator with nil NvmeTcpInfo")
		}
		if err := ef.loadInitiatorNVMeDeviceInfo(ef.initiator.NVMeTCPInfo.TransportAddress, ef.initiator.NVMeTCPInfo.TransportServiceID, ef.initiator.NVMeTCPInfo.SubsystemNQN); err != nil {
			if strings.Contains(err.Error(), "connecting state") ||
				strings.Contains(err.Error(), "resetting state") ||
				strings.Contains(err.Error(), "live state") {
				ef.log.WithError(err).Warn("Ignored to validate and update engine frontend, because the device is still in a transient state")
				return nil
			}
			return err
		}
		if err := ef.loadInitiatorEndpoint(ef.dmDeviceIsBusy); err != nil {
			return err
		}
		blockDevEndpoint := ef.getInitiatorEndpoint()
		if ef.Endpoint == "" {
			ef.Endpoint = blockDevEndpoint
		}
		if ef.Endpoint != blockDevEndpoint {
			return fmt.Errorf("found mismatching between engine frontend endpoint %s and actual block device endpoint %s for engine frontend %s", ef.Endpoint, blockDevEndpoint, ef.Name)
		}
	case types.FrontendSPDKTCPNvmf:
		nvmfEndpoint := GetNvmfEndpoint(ef.NvmeTcpFrontend.Nqn, ef.NvmeTcpFrontend.TargetIP, ef.NvmeTcpFrontend.TargetPort)
		if ef.Endpoint == "" {
			ef.Endpoint = nvmfEndpoint
		}
		if ef.Endpoint != nvmfEndpoint {
			return fmt.Errorf("found mismatching between engine frontend endpoint %s and actual nvmf endpoint %s for engine frontend %s", ef.Endpoint, nvmfEndpoint, ef.Name)
		}
	default:
		return fmt.Errorf("unknown frontend type %s", ef.Frontend)
	}

	return nil
}

// RecoverFromHost attempts to recover the engine frontend's initiator state by
// detecting existing NVMe controllers and dm-devices on the host. This is called
// during server startup for engine frontends that were persisted before restart.
//
// On success, the frontend transitions to Running state.
// On failure, it transitions to Error state so that the upper-layer controller
// can reconcile (e.g. by calling Delete + Create).
func (ef *EngineFrontend) RecoverFromHost(spdkClient *spdkclient.Client) error {
	ef.Lock()
	if ef.State != types.InstanceStatePending {
		ef.Unlock()
		return fmt.Errorf("invalid state %s for engine frontend %s recovery", ef.State, ef.Name)
	}
	ef.Unlock()

	var recoverErr error
	var deviceNotFound bool

	defer func() {
		ef.Lock()
		defer ef.Unlock()

		if deviceNotFound {
			// Device not found on host — record already removed, nothing to reconcile.
			return
		}
		if recoverErr != nil {
			ef.log.WithError(recoverErr).Errorf("Failed to recover engine frontend %s from host", ef.Name)
			ef.State = types.InstanceStateError
			ef.ErrorMsg = recoverErr.Error()
		} else {
			ef.State = types.InstanceStateRunning
			ef.ErrorMsg = ""
			ef.log.Info("Successfully recovered engine frontend from host")
		}
		ef.UpdateCh <- nil
	}()

	switch ef.Frontend {
	case types.FrontendEmpty:
		// No initiator to recover for empty frontend.
		return nil

	case types.FrontendSPDKTCPNvmf:
		// For NVMe-oF (non-blockdev) frontend, there is no local initiator.
		// Just reconstruct the endpoint from persisted TargetPort.
		nqn, nguid := ef.getVolumeTargetIdentity()

		ef.Lock()
		ef.NvmeTcpFrontend.Nqn = nqn
		ef.NvmeTcpFrontend.Nguid = nguid
		if ef.NvmeTcpFrontend.TargetPort != 0 {
			ef.Endpoint = GetNvmfEndpoint(nqn, ef.NvmeTcpFrontend.TargetIP, ef.NvmeTcpFrontend.TargetPort)
		}
		ef.syncCurrentNVMeTCPPathLocked()
		ef.Unlock()

		return nil

	case types.FrontendSPDKTCPBlockdev:
		// Recover the NVMe-oF initiator (blockdev frontend with dm-device).
		i, nqn, nguid, err := ef.newNvmeTcpInitiator()
		if err != nil {
			recoverErr = errors.Wrapf(err, "failed to create NVMe/TCP initiator for recovery of engine frontend %s", ef.Name)
			return recoverErr
		}

		ef.Lock()
		ef.initiator = i
		ef.NvmeTcpFrontend.Nqn = nqn
		ef.NvmeTcpFrontend.Nguid = nguid
		ef.Unlock()

		// Try to load the existing NVMe device info from sysfs.
		// Prefer persisted target address/port for controller selection to
		// avoid binding to an arbitrary controller on a multipath device.
		transportAddress, transportServiceID := "", ""
		if ef.NvmeTcpFrontend.TargetIP != "" && ef.NvmeTcpFrontend.TargetPort != 0 {
			transportAddress = ef.NvmeTcpFrontend.TargetIP
			transportServiceID = strconv.Itoa(int(ef.NvmeTcpFrontend.TargetPort))
		}
		loadErr := ef.loadInitiatorNVMeDeviceInfo(transportAddress, transportServiceID, nqn)
		// If loading with the persisted target address failed, fall back
		// to any available controller. This handles the case where the
		// persisted target is stale but the multipath device still has a
		// live controller at a different address.
		if loadErr != nil && transportAddress != "" {
			ef.log.WithError(loadErr).Warnf("Failed to load NVMe device info with persisted target %s:%s for engine frontend %s, falling back to any available controller",
				transportAddress, transportServiceID, ef.Name)
			if fallbackErr := ef.loadInitiatorNVMeDeviceInfo("", "", nqn); fallbackErr == nil {
				loadErr = nil
			}
		}
		if loadErr != nil {
			reconnected := false
			if strings.Contains(loadErr.Error(), helpertypes.ErrorMessageCannotFindValidNvmeDevice) {
				if ef.NvmeTcpFrontend.TargetIP != "" && ef.NvmeTcpFrontend.TargetPort != 0 {
					ef.log.WithError(loadErr).Warnf("NVMe device not found on host during recovery of engine frontend %s, reconnecting persisted multipath target", ef.Name)
					if reconnectErr := ef.reconnectNvmeTCPPath(ef.NvmeTcpFrontend.TargetIP, ef.NvmeTcpFrontend.TargetPort); reconnectErr != nil {
						recoverErr = errors.Wrapf(reconnectErr, "failed to reconnect NVMe/TCP path during recovery of engine frontend %s", ef.Name)
						return recoverErr
					}
					reconnected = true
				} else {
					ef.log.WithError(loadErr).Warnf("NVMe device not found on host during recovery of engine frontend %s, removing persisted record", ef.Name)
					if removeErr := removeEngineFrontendRecord(ef.metadataDir, ef.VolumeName); removeErr != nil {
						ef.log.WithError(removeErr).Warn("Failed to remove engine frontend record")
					}
					deviceNotFound = true
					return ErrRecoverDeviceNotFound
				}
			}
			if !reconnected {
				recoverErr = errors.Wrapf(loadErr, "failed to load NVMe device info during recovery of engine frontend %s", ef.Name)
				return recoverErr
			}
		}

		// Try to load the existing dm-device endpoint.
		if err := ef.loadInitiatorEndpoint(false); err != nil {
			recoverErr = errors.Wrapf(err, "failed to load endpoint during recovery of engine frontend %s", ef.Name)
			return recoverErr
		}

		ef.Lock()
		ef.Endpoint = ef.getInitiatorEndpoint()
		// Recover target port from the detected transport service ID.
		if transportServiceID := i.GetTransportServiceID(); transportServiceID != "" {
			if port, parseErr := strconv.Atoi(transportServiceID); parseErr == nil {
				ef.NvmeTcpFrontend.TargetPort = int32(port)
			}
		}
		// Recover target IP from the detected transport address.
		if transportAddress := i.GetTransportAddress(); transportAddress != "" {
			ef.NvmeTcpFrontend.TargetIP = transportAddress
			ef.EngineIP = transportAddress
		}
		ef.syncCurrentNVMeTCPPathLocked()
		ef.Unlock()

		return nil

	default:
		recoverErr = fmt.Errorf("unsupported frontend type %s for recovery of engine frontend %s", ef.Frontend, ef.Name)
		return recoverErr
	}
}

// BackupRestore initiates a backup restore via this frontend.
// The EngineFrontend must not have an active endpoint — it is expected to be a
// dedicated restore frontend with no pre-existing initiator connection.
// A temporary NVMe-TCP target is created on the engine for data transfer and torn
// down (along with the initiator) once the restore goroutine completes.
// Flow: EngineFrontend.BackupRestore -> Engine.BackupRestore
func (ef *EngineFrontend) BackupRestore(engine *Engine, spdkClient *spdkclient.Client, backupUrl string, credential map[string]string, concurrentLimit int32, superiorPortAllocator *commonbitmap.Bitmap) (resp *spdkrpc.EngineBackupRestoreResponse, err error) {
	ef.log.Infof("Starting backup restore for backup %s", backupUrl)
	targetPrepared := false
	frontendPrepared := false

	ef.Lock()
	if ef.isCreating {
		ef.Unlock()
		return nil, fmt.Errorf("engine frontend %s is still creating", ef.Name)
	}
	if ef.isSwitchingOver {
		ef.Unlock()
		return nil, fmt.Errorf("engine frontend %s is switching over target", ef.Name)
	}
	if ef.Endpoint != "" {
		ef.Unlock()
		return nil, fmt.Errorf("engine frontend %s already has an active endpoint %s; backup restore requires a dedicated frontend with no existing connection", ef.Name, ef.Endpoint)
	}
	ef.IsRestoring = true
	ef.Frontend = types.FrontendSPDKTCPBlockdev
	ef.Unlock()

	defer func() {
		if err != nil {
			if targetPrepared {
				engine.cleanupTemporaryNvmeTcpTargetForRestore(spdkClient, superiorPortAllocator, "restore flow failure")
			}
			if frontendPrepared {
				ef.teardownRestoreInitiator(spdkClient)
			}
			ef.Lock()
			ef.IsRestoring = false
			ef.Frontend = ""
			ef.Unlock()
		} else {
			ef.UpdateCh <- nil
		}
	}()

	// Ensure the engine has a NVMe-TCP target to connect to (creates one if absent).
	if err := engine.ensureNvmeTcpTargetForRestore(spdkClient, superiorPortAllocator); err != nil {
		return nil, errors.Wrapf(err, "engine %s: failed to ensure NVMe-TCP target for backup restore", engine.Name)
	}
	targetPrepared = true

	engine.RLock()
	targetAddress := net.JoinHostPort(engine.NvmeTcpTarget.IP, strconv.Itoa(int(engine.NvmeTcpTarget.Port)))
	engine.RUnlock()

	ef.log.Infof("Setting up NVMe-TCP initiator for backup restore, target: %s", targetAddress)
	if err := ef.createNvmeTcpFrontend(spdkClient, targetAddress); err != nil {
		return nil, errors.Wrapf(err, "failed to setup NVMe-TCP frontend for backup restore")
	}
	frontendPrepared = true

	ef.RLock()
	endpoint := ef.Endpoint
	ef.RUnlock()

	if endpoint == "" {
		return nil, fmt.Errorf("engine frontend %s has no endpoint after frontend setup for backup restore", ef.Name)
	}

	ef.log.Infof("Using endpoint %s for backup restore", endpoint)
	resp, doneCh, err := engine.BackupRestore(spdkClient, backupUrl, endpoint, credential, concurrentLimit, superiorPortAllocator)
	if err != nil {
		return nil, err
	}

	// Tear down the initiator connection once the restore goroutine signals completion.
	go func() {
		<-doneCh
		ef.log.Info("Backup restore complete, tearing down NVMe-TCP frontend")
		ef.teardownRestoreInitiator(spdkClient)

		ef.Lock()
		ef.IsRestoring = false
		ef.Unlock()
		ef.UpdateCh <- nil
	}()

	return resp, nil
}

func (ef *EngineFrontend) teardownRestoreInitiator(spdkClient *spdkclient.Client) {
	ef.Lock()
	defer ef.Unlock()

	if ef.initiator != nil {
		if _, stopErr := ef.initiator.Stop(spdkClient, true, true, true); stopErr != nil {
			ef.log.WithError(stopErr).Warn("Failed to stop NVMe-TCP initiator after backup restore")
		}
		ef.initiator = nil
	}
	ef.Endpoint = ""
	ef.Frontend = ""

	if ef.NvmeTcpFrontend != nil {
		ef.NvmeTcpFrontend.TargetIP = ""
		ef.NvmeTcpFrontend.TargetPort = 0
		ef.NvmeTcpFrontend.Nqn = ""
		ef.NvmeTcpFrontend.Nguid = ""
	}

	// The restore frontend is only a temporary data path. Once it is torn down,
	// report the frontend as stopped so the control plane can recreate a normal
	// attach frontend instead of treating this now-empty process as still ready.
	if ef.State != types.InstanceStateError {
		ef.State = types.InstanceStateStopped
		ef.ErrorMsg = ""
	}
}
