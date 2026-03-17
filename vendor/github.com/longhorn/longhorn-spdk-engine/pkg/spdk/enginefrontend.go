package spdk

import (
	"context"
	"fmt"
	"net"
	"runtime/debug"
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

	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"

	"github.com/longhorn/go-spdk-helper/pkg/initiator"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
)

type EngineFrontend struct {
	sync.RWMutex

	Name       string
	EngineName string
	VolumeName string
	SpecSize   uint64
	ActualSize uint64

	Frontend string
	Endpoint string

	EngineIP string

	NvmeTcpFrontend *NvmeTcpFrontend
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
	// Test hook for switchover target connect/rollback.
	startNvmeTCPInitiatorFn func(transportAddress, transportServiceID string, dmDeviceAndEndpointCleanupRequired bool, stop bool) (dmDeviceIsBusy bool, err error)
	// Test hook for endpoint retrieval after switchover.
	getInitiatorEndpointFn func() string

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
		Name:       engineFrontendName,
		EngineName: engineName,
		VolumeName: volumeName,
		SpecSize:   specSize,

		Frontend: frontend,

		NvmeTcpFrontend: nvmeTcpFrontend,
		UblkFrontend:    ublkFrontend,

		State:    types.InstanceStatePending,
		ErrorMsg: "",

		UpdateCh: engineFrontendUpdateCh,
		stopCh:   make(chan struct{}),
		log:      safelog.NewSafeLogger(log),
	}
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
	nqn = helpertypes.GetNQN(ef.EngineName)
	nguid = generateNGUID(ef.EngineName)

	nvmeTCPInfo := &initiator.NVMeTCPInfo{
		SubsystemNQN: nqn,
	}
	i, err = initiator.NewInitiator(ef.VolumeName, initiator.HostProc, nvmeTCPInfo, nil)
	if err != nil {
		return i, "", "", errors.Wrapf(err, "failed to create NVMe/TCP initiator for engine %v", ef.Name)
	}

	return i, nqn, nguid, nil
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
	engineIP := ef.EngineIP
	engineName := ef.EngineName
	frontend := ef.Frontend

	var targetAddress string
	if ef.NvmeTcpFrontend != nil {
		targetAddress = net.JoinHostPort(ef.NvmeTcpFrontend.TargetIP, strconv.Itoa(int(ef.NvmeTcpFrontend.TargetPort)))
	}

	engineSpdkClient, err := GetServiceClient(net.JoinHostPort(engineIP, strconv.Itoa(types.SPDKServicePort)))
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
// For blockdev frontend, the caller must suspend the frontend before switch-over.
// If newEngineName is empty, the function will try to resolve it via targetAddress.
func (ef *EngineFrontend) SwitchOverTarget(spdkClient *spdkclient.Client, newEngineName, targetAddress string) (err error) {
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

	oldEngineIP := ef.EngineIP
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
	if oldTargetIP == targetIP && oldTargetPort == targetPort && oldEngineIP == targetIP && oldEngineName == resolvedEngineName {
		if ef.State != types.InstanceStateError {
			ef.ErrorMsg = ""
		}
		ef.Unlock()
		return nil
	}

	if frontend == types.FrontendSPDKTCPBlockdev && ef.State != types.InstanceStateSuspended {
		state := ef.State
		ef.Unlock()
		return errors.Wrapf(ErrSwitchOverTargetPrecondition, "invalid state %v for engine frontend %s target switchover, must be suspended", state, ef.Name)
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
	newNQN := helpertypes.GetNQN(resolvedEngineName)
	newNGUID := generateNGUID(resolvedEngineName)

	switch frontend {
	case types.FrontendSPDKTCPNvmf:
		ef.Lock()
		ef.EngineIP = targetIP
		ef.EngineName = resolvedEngineName
		ef.NvmeTcpFrontend.TargetIP = targetIP
		ef.NvmeTcpFrontend.TargetPort = targetPort
		ef.NvmeTcpFrontend.Nqn = newNQN
		ef.NvmeTcpFrontend.Nguid = newNGUID
		ef.Endpoint = GetNvmfEndpoint(newNQN, targetIP, targetPort)
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
		// Do NOT overwrite SubsystemNQN before startNvmeTCPInitiator.
		// The stop path inside startNvmeTCPInitiator uses SubsystemNQN to
		// disconnect the old NVMe controller. If we set newNQN here, the old
		// controller (with oldNQN) would never be disconnected, causing ~30s
		// of kernel NVMe reconnect retries until timeout.
		// startNvmeTCPInitiator will set the correct NQN after connecting
		// the new target via discoverAndConnectNVMeTCPTarget.

		dmDeviceIsBusy, switchErr := ef.startNvmeTCPInitiator(targetIP, targetPort, true, true)
		if switchErr != nil {
			switchErr = errors.Wrapf(ErrSwitchOverTargetInternal, "failed to switch engine frontend %s target to %s: %v", ef.Name, targetAddress, switchErr)

			var rollbackErr error
			if oldTargetIP != "" && oldTargetPort != 0 {
				ef.log.WithError(switchErr).Warnf("Failed to switch target, initiating rollback to previous target %s:%d", oldTargetIP, oldTargetPort)
				if ef.initiator.NVMeTCPInfo != nil {
					ef.initiator.NVMeTCPInfo.SubsystemNQN = oldNQN
				}
				var rollbackDMDeviceIsBusy bool
				if rollbackDMDeviceIsBusy, rollbackErr = ef.startNvmeTCPInitiator(oldTargetIP, oldTargetPort, true, true); rollbackErr != nil {
					rollbackErr = errors.Wrapf(ErrSwitchOverTargetInternal, "failed to rollback engine frontend %s target to %s:%d: %v", ef.Name, oldTargetIP, oldTargetPort, rollbackErr)
					ef.log.WithError(rollbackErr).Errorf("Failed to rollback engine frontend %s target to previous target", ef.Name)
				} else {
					ef.log.Info("Successfully rolled back engine frontend target")
					oldDMDeviceIsBusy = rollbackDMDeviceIsBusy
				}
			}

			// Restore all metadata to original state regardless of rollback success to avoid go struct inconsistency
			ef.Lock()
			ef.EngineIP = oldEngineIP
			ef.EngineName = oldEngineName
			ef.NvmeTcpFrontend.TargetIP = oldTargetIP
			ef.NvmeTcpFrontend.TargetPort = oldTargetPort
			ef.NvmeTcpFrontend.Nqn = oldNQN
			ef.NvmeTcpFrontend.Nguid = oldNGUID
			ef.Endpoint = oldEndpoint
			ef.dmDeviceIsBusy = oldDMDeviceIsBusy

			if rollbackErr != nil {
				combinedErr := multierr.Append(switchErr, rollbackErr)
				ef.State = types.InstanceStateError
				ef.ErrorMsg = combinedErr.Error()
				ef.Unlock()
				updateRequired = true
				return combinedErr
			}
			ef.ErrorMsg = switchErr.Error()
			ef.Unlock()
			updateRequired = true
			return switchErr
		}

		ef.Lock()
		ef.EngineIP = targetIP
		ef.EngineName = resolvedEngineName
		ef.NvmeTcpFrontend.TargetIP = targetIP
		ef.NvmeTcpFrontend.TargetPort = targetPort
		ef.NvmeTcpFrontend.Nqn = newNQN
		ef.NvmeTcpFrontend.Nguid = newNGUID
		ef.Endpoint = ef.getInitiatorEndpoint()
		ef.dmDeviceIsBusy = dmDeviceIsBusy
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

	default:
		return errors.Wrapf(ErrSwitchOverTargetPrecondition, "frontend %s does not support target switchover for engine frontend %s", ef.Frontend, ef.Name)
	}
}

func (ef *EngineFrontend) startNvmeTCPInitiator(transportAddress string, transportPort int32, dmDeviceAndEndpointCleanupRequired bool, stop bool) (bool, error) {
	transportServiceID := strconv.Itoa(int(transportPort))
	if ef.startNvmeTCPInitiatorFn != nil {
		return ef.startNvmeTCPInitiatorFn(transportAddress, transportServiceID, dmDeviceAndEndpointCleanupRequired, stop)
	}
	if ef.initiator == nil {
		return false, errors.Wrapf(ErrSwitchOverTargetInternal, "initiator is nil for engine frontend %s", ef.Name)
	}
	return ef.initiator.StartNvmeTCPInitiator(transportAddress, transportServiceID, dmDeviceAndEndpointCleanupRequired, stop)
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

	engineSpdkClient, err := GetServiceClient(net.JoinHostPort(ef.EngineIP, strconv.Itoa(types.SPDKServicePort)))
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
		if err := ef.initiator.LoadNVMeDeviceInfo(ef.initiator.NVMeTCPInfo.TransportAddress, ef.initiator.NVMeTCPInfo.TransportServiceID, ef.initiator.NVMeTCPInfo.SubsystemNQN); err != nil {
			if strings.Contains(err.Error(), "connecting state") ||
				strings.Contains(err.Error(), "resetting state") {
				ef.log.WithError(err).Warn("Ignored to validate and update engine frontend, because the device is still in a transient state")
				return nil
			}
			return err
		}
		if err := ef.initiator.LoadEndpointForNvmeTcpFrontend(ef.dmDeviceIsBusy); err != nil {
			return err
		}
		blockDevEndpoint := ef.initiator.GetEndpoint()
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
		// Just reconstruct the endpoint from persisted TargetPort and EngineIP.
		nqn := helpertypes.GetNQN(ef.EngineName)
		nguid := generateNGUID(ef.EngineName)

		ef.Lock()
		ef.NvmeTcpFrontend.Nqn = nqn
		ef.NvmeTcpFrontend.Nguid = nguid
		if ef.NvmeTcpFrontend.TargetIP == "" {
			ef.NvmeTcpFrontend.TargetIP = ef.EngineIP
		}
		if ef.NvmeTcpFrontend.TargetPort != 0 {
			ef.Endpoint = GetNvmfEndpoint(nqn, ef.NvmeTcpFrontend.TargetIP, ef.NvmeTcpFrontend.TargetPort)
		}
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
		ef.NvmeTcpFrontend.Nqn = nqn
		ef.NvmeTcpFrontend.Nguid = nguid
		ef.NvmeTcpFrontend.TargetIP = ef.EngineIP
		ef.Unlock()

		// Try to load the existing NVMe device info from sysfs.
		// Use empty transport address/port since we want to discover by NQN.
		if err := i.LoadNVMeDeviceInfo("", "", nqn); err != nil {
			if strings.Contains(err.Error(), helpertypes.ErrorMessageCannotFindValidNvmeDevice) {
				ef.log.WithError(err).Warnf("NVMe device not found on host during recovery of engine frontend %s, removing persisted record", ef.Name)
				if removeErr := removeEngineFrontendRecord(ef.metadataDir, ef.VolumeName); removeErr != nil {
					ef.log.WithError(removeErr).Warn("Failed to remove engine frontend record")
				}
				deviceNotFound = true
				return ErrRecoverDeviceNotFound
			}
			recoverErr = errors.Wrapf(err, "failed to load NVMe device info during recovery of engine frontend %s", ef.Name)
			return recoverErr
		}

		// Try to load the existing dm-device endpoint.
		if err := i.LoadEndpointForNvmeTcpFrontend(false); err != nil {
			recoverErr = errors.Wrapf(err, "failed to load endpoint during recovery of engine frontend %s", ef.Name)
			return recoverErr
		}

		ef.Lock()
		ef.initiator = i
		ef.Endpoint = i.GetEndpoint()
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
		ef.Unlock()

		return nil

	default:
		recoverErr = fmt.Errorf("unsupported frontend type %s for recovery of engine frontend %s", ef.Frontend, ef.Name)
		return recoverErr
	}
}
