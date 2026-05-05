package spdk

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"

	retrygo "github.com/avast/retry-go/v4"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	btypes "github.com/longhorn/backupstore/types"
	butil "github.com/longhorn/backupstore/util"
	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	commonnet "github.com/longhorn/go-common-libs/net"
	commonutils "github.com/longhorn/go-common-libs/utils"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"

	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"
)

type NvmeTcpTarget struct {
	IP   string
	Port int32

	Nqn      string
	Nguid    string
	ANAState NvmeTCPANAState
}

func toSPDKListenerANAState(anaState NvmeTCPANAState) (spdktypes.NvmfSubsystemListenerAnaState, error) {
	switch anaState {
	case NvmeTCPANAStateOptimized:
		return spdktypes.NvmfSubsystemListenerAnaStateOptimized, nil
	case NvmeTCPANAStateNonOptimized:
		return spdktypes.NvmfSubsystemListenerAnaStateNonOptimized, nil
	case NvmeTCPANAStateInaccessible:
		return spdktypes.NvmfSubsystemListenerAnaStateInaccessible, nil
	default:
		return "", fmt.Errorf("unsupported NVMe/TCP ANA state %q", anaState)
	}
}

// ReplicaAdder abstracts the two pluggable steps of the replica-add flow:
// shallow copy and finish. Production code uses realReplicaAdder; tests
// can supply a MockReplicaAdder via Engine.SetReplicaAdder().
type ReplicaAdder interface {
	ReplicaShallowCopy(dstReplicaServiceCli *client.SPDKClient, srcReplicaName, dstReplicaName string, rebuildingSnapshots []*api.Lvol, fastSync bool) error
	ReplicaAddFinish(srcReplicaServiceCli, dstReplicaServiceCli *client.SPDKClient, srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress string) error
}

// realReplicaAdder is the production ReplicaAdder that delegates to Engine methods.
type realReplicaAdder struct {
	e *Engine
}

func (ra *realReplicaAdder) ReplicaShallowCopy(dstReplicaServiceCli *client.SPDKClient, srcReplicaName, dstReplicaName string, rebuildingSnapshots []*api.Lvol, fastSync bool) error {
	return ra.e.replicaShallowCopy(dstReplicaServiceCli, srcReplicaName, dstReplicaName, rebuildingSnapshots, fastSync)
}

func (ra *realReplicaAdder) ReplicaAddFinish(srcReplicaServiceCli, dstReplicaServiceCli *client.SPDKClient, srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress string) error {
	return ra.e.replicaAddFinish(srcReplicaServiceCli, dstReplicaServiceCli, srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress)
}

// MockReplicaAdder allows tests to override specific replica-add operations.
// Set individual function fields to non-nil to mock that operation; nil fields
// fall through to the real Engine implementation via the embedded Real adder.
//
// When a mock FinishFunc injects an error, it is the mock's responsibility
// to call Real.ReplicaAddFinish() for SPDK resource cleanup before returning
// the error. The engine goroutine will NOT perform fallback cleanup.
type MockReplicaAdder struct {
	Real            ReplicaAdder
	ShallowCopyFunc func(dstReplicaServiceCli *client.SPDKClient, srcReplicaName, dstReplicaName string, rebuildingSnapshots []*api.Lvol, fastSync bool) error
	FinishFunc      func(srcReplicaServiceCli, dstReplicaServiceCli *client.SPDKClient, srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress string) error
}

func (m *MockReplicaAdder) ReplicaShallowCopy(dstReplicaServiceCli *client.SPDKClient, srcReplicaName, dstReplicaName string, rebuildingSnapshots []*api.Lvol, fastSync bool) error {
	if m.ShallowCopyFunc != nil {
		return m.ShallowCopyFunc(dstReplicaServiceCli, srcReplicaName, dstReplicaName, rebuildingSnapshots, fastSync)
	}
	return m.Real.ReplicaShallowCopy(dstReplicaServiceCli, srcReplicaName, dstReplicaName, rebuildingSnapshots, fastSync)
}

func (m *MockReplicaAdder) ReplicaAddFinish(srcReplicaServiceCli, dstReplicaServiceCli *client.SPDKClient, srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress string) error {
	if m.FinishFunc != nil {
		return m.FinishFunc(srcReplicaServiceCli, dstReplicaServiceCli, srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress)
	}
	return m.Real.ReplicaAddFinish(srcReplicaServiceCli, dstReplicaServiceCli, srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress)
}

type Engine struct {
	sync.RWMutex

	ctx       context.Context
	cancelCtx context.CancelFunc

	restore *EngineRestore

	Name       string
	VolumeName string
	SpecSize   uint64
	ActualSize uint64
	Frontend   string

	ctrlrLossTimeout     int
	fastIOFailTimeoutSec int
	ReplicaStatusMap     map[string]*EngineReplicaStatus

	RaidBdevUUID string

	NvmeTcpTarget *NvmeTcpTarget

	State    types.InstanceState
	ErrorMsg string

	Head        *api.Lvol
	SnapshotMap map[string]*api.Lvol

	IsRestoring           bool
	RestoringSnapshotName string

	isExpanding           bool
	lastExpansionFailedAt string
	lastExpansionError    string

	// UpdateCh should not be protected by the engine lock
	UpdateCh chan interface{}

	log *safelog.SafeLogger

	// replicaAdder provides the pluggable replica-add operations (shallow copy + finish).
	// Production uses realReplicaAdder; tests can supply MockReplicaAdder.
	replicaAdder ReplicaAdder

	// replicaAddFinishUnlockedHook is an optional hook that fires inside
	// replicaAddFinish after the Engine lock is released and before the slow
	// RPC calls (ReplicaRebuildingSrcFinish / ReplicaRebuildingDstFinish).
	//
	// Purpose: regression guard for the 3-phase lock pattern. The hook lets
	// tests call TryLock() to prove the lock is truly released during phase 2.
	// Without this, a future change that accidentally holds the lock through
	// the RPCs (reverting to single-phase) would be undetectable from
	// external behavior alone — replica-add would still succeed or fail
	// identically, but all other Engine operations would stall for 10+
	// seconds on same-node NVMe-oF ETIMEDOUT.
	replicaAddFinishUnlockedHook func()
}

type EngineReplicaStatus struct {
	Address  string
	BdevName string
	Mode     types.Mode
}

func NewEngine(engineName, volumeName, frontend string, specSize uint64, engineUpdateCh chan interface{}) *Engine {
	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"engineName": engineName,
		"volumeName": volumeName,
	})

	roundedSpecSize := util.RoundUp(specSize, helpertypes.MiB)
	if roundedSpecSize != specSize {
		log.Infof("Rounded up spec size from %v to %v since the spec size should be multiple of MiB", specSize, roundedSpecSize)
	}
	log.WithField("specSize", roundedSpecSize)

	ctx, cancelCtx := context.WithCancel(context.Background())

	e := &Engine{
		ctx:       ctx,
		cancelCtx: cancelCtx,

		Name:       engineName,
		VolumeName: volumeName,
		Frontend:   frontend,
		SpecSize:   specSize,

		// TODO: support user-defined values
		ctrlrLossTimeout:     replicaCtrlrLossTimeoutSec,
		fastIOFailTimeoutSec: replicaFastIOFailTimeoutSec,

		ReplicaStatusMap: map[string]*EngineReplicaStatus{},

		NvmeTcpTarget: &NvmeTcpTarget{},

		State: types.InstanceStatePending,

		SnapshotMap: map[string]*api.Lvol{},

		UpdateCh: engineUpdateCh,

		log: safelog.NewSafeLogger(log),
	}
	e.replicaAdder = &realReplicaAdder{e: e}
	return e
}

func (e *Engine) Create(spdkClient *spdkclient.Client, replicaAddressMap map[string]string, portCount int32, superiorPortAllocator *commonbitmap.Bitmap,
	salvageRequested bool) (ret *spdkrpc.Engine, err error) {
	e.log.WithFields(logrus.Fields{
		"portCount":         portCount,
		"replicaAddressMap": replicaAddressMap,
		"salvageRequested":  salvageRequested,
		"frontend":          e.Frontend,
	}).Info("Creating engine")

	requireUpdate := true

	e.Lock()
	defer func() {
		e.Unlock()
		if requireUpdate {
			e.UpdateCh <- nil
		}
	}()

	if e.State != types.InstanceStatePending {
		requireUpdate = false
		return nil, fmt.Errorf("invalid state %s for engine %s creation", e.State, e.Name)
	}

	if err := e.validateReplicaSize(replicaAddressMap); err != nil {
		return nil, errors.Wrapf(err, "failed to validate replica size during engine target creation")
	}

	defer func() {
		if err != nil {
			e.log.WithError(err).Errorf("Failed to create engine %s", e.Name)
			e.State = types.InstanceStateError
			e.ErrorMsg = err.Error()

			// Return the error-state engine object instead of a Go error
			// so the gRPC layer can persist and report the engine status
			// without triggering a gRPC error code.
			ret = e.getWithoutLock()
			err = nil
		} else if e.State != types.InstanceStateError {
			e.ErrorMsg = ""
		}
	}()

	_, err = spdkClient.BdevRaidGet(e.Name, 0)
	if err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return nil, errors.Wrapf(err, "failed to get raid bdev %v during engine creation", e.Name)
	}

	if salvageRequested {
		e.log.Info("Requesting salvage for engine replicas")
		replicaAddressMap, err = e.filterSalvageCandidates(replicaAddressMap)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to update replica mode to filter salvage candidates")
		}
	}

	replicaBdevList := e.connectReplicas(spdkClient, replicaAddressMap)

	e.log.UpdateLoggerWithWarnOnFailure(logrus.Fields{
		"replicaStatusMap": e.ReplicaStatusMap,
	}, "Failed to update logger with replica status map during engine creation")

	e.checkAndUpdateInfoFromReplicasNoLock()

	e.log.Infof("Connected all available replicas %+v, then launching raid during engine creation", e.ReplicaStatusMap)
	if _, err := spdkClient.BdevRaidCreate(e.Name, spdktypes.BdevRaidLevel1, 0, replicaBdevList, ""); err != nil {
		return nil, err
	}

	switch e.Frontend {
	case types.FrontendSPDKTCPBlockdev, types.FrontendSPDKTCPNvmf:
		e.log.Infof("Creating NVMe TCP target for engine %v", e.Name)
		if err := e.createNVMeTCPTarget(spdkClient, superiorPortAllocator, portCount, NvmeTCPANAStateOptimized); err != nil {
			return nil, errors.Wrapf(err, "failed to create NVMe TCP target for engine %v", e.Name)
		}
	case types.FrontendUBLK:
		e.log.Infof("Creating UBLK target for engine %v", e.Name)
		if err := spdkClient.UblkCreateTarget("", true); err != nil {
			return nil, err
		}
	}

	e.State = types.InstanceStateRunning

	e.log.Info("Created engine target")

	return e.getWithoutLock(), nil
}

// createNVMeTCPTarget creates the NVMe/TCP target for this engine.
// initialANAState controls the ANA state the listener is created with.
// Normal (first-time) engine creation should use NvmeTCPANAStateOptimized.
// Migration/switchover targets should use NvmeTCPANAStateInaccessible so the
// kernel multipath layer does not route I/O to the new path until explicitly
// promoted.
func (e *Engine) createNVMeTCPTarget(spdkClient *spdkclient.Client, superiorPortAllocator *commonbitmap.Bitmap, portCount int32, initialANAState NvmeTCPANAState) error {
	podIP, err := commonnet.GetIPForPod()
	if err != nil {
		return err
	}

	port, _, err := superiorPortAllocator.AllocateRange(portCount)
	if err != nil {
		return errors.Wrapf(err, "failed to allocate port for engine target %v", e.Name)
	}
	e.log.Infof("Allocated port %v for engine target %v", port, e.Name)

	e.NvmeTcpTarget.IP = podIP
	e.NvmeTcpTarget.Port = port
	e.NvmeTcpTarget.Nqn = getStableVolumeNQN(e.VolumeName)
	e.NvmeTcpTarget.Nguid = getStableVolumeNGUID(e.VolumeName)

	spdkANAState, err := toSPDKListenerANAState(initialANAState)
	if err != nil {
		return errors.Wrapf(err, "invalid initial ANA state %q for engine target %v", initialANAState, e.Name)
	}

	e.log.Info("Blindly stopping expose RAID bdev for engine")
	if err := spdkClient.StopExposeBdev(e.NvmeTcpTarget.Nqn); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "failed to blindly stop exposing RAID bdev for engine target %v", e.Name)
	}

	cntlid := getEngineCntlid(e.Name)
	nsUUID := getStableVolumeNsUUID(e.VolumeName)

	e.log.Infof("Starting to expose RAID bdev for engine target %v on %v:%v with initial ANA state %v, cntlid %v, nsUUID %v",
		e.Name, e.NvmeTcpTarget.IP, e.NvmeTcpTarget.Port, initialANAState, cntlid, nsUUID)
	if err := spdkClient.StartExposeBdevWithANAState(e.NvmeTcpTarget.Nqn, e.Name, e.NvmeTcpTarget.Nguid, nsUUID,
		e.NvmeTcpTarget.IP, strconv.Itoa(int(e.NvmeTcpTarget.Port)), spdkANAState, cntlid, cntlid); err != nil {
		// No need to release ports here. The engine will be marked as ERR by
		// Create's deferred error handler, and Delete will release the ports
		// when the user cleans up this engine.
		return errors.Wrapf(err, "failed to start exposing RAID bdev for engine target %v", e.Name)
	}

	e.NvmeTcpTarget.ANAState = initialANAState

	return nil
}

// connectReplicas connects to each replica's NVMf bdev and populates
// ReplicaStatusMap. It returns the list of successfully connected bdev names.
func (e *Engine) connectReplicas(spdkClient *spdkclient.Client, replicaAddressMap map[string]string) []string {
	replicaBdevList := []string{}
	for replicaName, replicaAddr := range replicaAddressMap {
		e.ReplicaStatusMap[replicaName] = &EngineReplicaStatus{
			Address: replicaAddr,
		}

		bdevName, err := connectNVMfBdev(spdkClient, replicaName, replicaAddr, e.ctrlrLossTimeout, e.fastIOFailTimeoutSec, maxRetries, retryInterval)
		if err != nil {
			e.log.WithError(err).Warnf("Failed to get bdev from replica %s with address %s during engine creation, will mark the mode to ERR and continue", replicaName, replicaAddr)
			e.ReplicaStatusMap[replicaName].Mode = types.ModeERR
		} else {
			// TODO: Check if a replica is really a RW replica rather than a rebuilding failed replica
			e.ReplicaStatusMap[replicaName].Mode = types.ModeRW
			e.ReplicaStatusMap[replicaName].BdevName = bdevName
			replicaBdevList = append(replicaBdevList, bdevName)
		}
	}
	return replicaBdevList
}

func (e *Engine) SetTargetListenerANAState(spdkClient *spdkclient.Client, anaState NvmeTCPANAState) error {
	if e == nil {
		return fmt.Errorf("engine is nil")
	}
	if spdkClient == nil {
		return fmt.Errorf("SPDK client is nil for engine %s", e.Name)
	}

	spdkANAState, err := toSPDKListenerANAState(anaState)
	if err != nil {
		return err
	}

	// Hold the write lock for the entire RPC + state update.
	//
	// Expand also holds the write lock while it tears down and re-creates the
	// NVMe-TCP subsystem. Because the subsystem keeps the same stable NQN,
	// IP, and port across an Expand, a check-after-RPC pattern cannot detect
	// whether the subsystem was recycled. Without the write lock the SPDK RPC
	// could land on the newly recreated subsystem and silently overwrite the
	// ANA state that Expand carefully preserved.
	//
	// This is safe because:
	//  - NvmfSubsystemListenerSetANAState is a fast SPDK RPC (flips an
	//    internal flag, no I/O involved).
	//  - SetTargetListenerANAState is only called during switchover, so
	//    contention with Expand is rare.
	//  - The SPDK RPC does not call back into Engine, so there is no
	//    deadlock risk.
	e.Lock()
	defer e.Unlock()

	if e.NvmeTcpTarget == nil {
		return fmt.Errorf("engine %s does not have an NVMe/TCP target", e.Name)
	}

	nqn := e.NvmeTcpTarget.Nqn
	ip := e.NvmeTcpTarget.IP
	port := e.NvmeTcpTarget.Port

	if nqn == "" || ip == "" || port == 0 {
		return fmt.Errorf("engine %s has incomplete NVMe/TCP target information", e.Name)
	}

	targetAdrfam := spdkclient.DetectAddressFamily(ip)

	_, err = spdkClient.NvmfSubsystemListenerSetANAState(
		nqn,
		ip,
		strconv.Itoa(int(port)),
		spdktypes.NvmeTransportTypeTCP,
		targetAdrfam,
		spdkANAState,
		spdktypes.DefaultNvmfANAGroupID,
	)
	if err != nil {
		return errors.Wrapf(err, "failed to set target listener ANA state %s for engine %s", anaState, e.Name)
	}

	// NOTE: We intentionally do NOT read back and verify the ANA state via
	// NvmfSubsystemGetListeners. The SPDK nvmf_subsystem_get_listeners RPC
	// does not always include the ana_state field in its response (it depends
	// on the SPDK version and whether ANA reporting is enabled at the
	// subsystem level). The nvmf_subsystem_listener_set_ana_state RPC returns
	// true on success, which is sufficient confirmation.

	e.NvmeTcpTarget.ANAState = anaState

	e.log.WithFields(logrus.Fields{
		"targetIP":   ip,
		"targetPort": port,
		"anaState":   anaState,
	}).Info("Updated engine target listener ANA state")

	return nil
}

func (e *Engine) validateReplicaSize(replicaAddressMap map[string]string) error {
	if len(replicaAddressMap) == 0 {
		return fmt.Errorf("no replicas provided for engine %s", e.Name)
	}

	// Validate the engine & replica sizes before creating the engine
	replicaSizeMap := make(map[string]uint64, len(replicaAddressMap))
	for replicaName, replicaAddr := range replicaAddressMap {
		replicaClient, err := GetServiceClient(replicaAddr)
		if err != nil {
			return err
		}
		replica, err := replicaClient.ReplicaGet(replicaName)
		if err != nil {
			return errors.Wrapf(err, "failed to get replica %v from %v", replicaName, replicaAddr)
		}

		replicaSizeMap[replicaName] = replica.SpecSize
	}

	// check if all replica sizes are the same
	expectedSize := uint64(0)
	for _, replicaSize := range replicaSizeMap {
		if expectedSize == 0 {
			expectedSize = replicaSize
			continue
		}

		if expectedSize != replicaSize {
			return fmt.Errorf("found different replica sizes: %+v", replicaSizeMap)
		}
	}

	if e.SpecSize < expectedSize {
		return fmt.Errorf("engine spec size %d is smaller than replica size %d", e.SpecSize, expectedSize)
	}

	return nil
}

// filterSalvageCandidates updates the replicaAddressMap by retaining only replicas
// eligible for salvage based on the largest volume head size.
//
// It iterates through all replicas and:
//   - Retrieves the volume head size for each replica.
//   - Identifies replicas with the largest volume head size as salvage candidates.
//   - Remove the replicas that are not eligible as salvage candidates.
func (e *Engine) filterSalvageCandidates(replicaAddressMap map[string]string) (map[string]string, error) {
	// Initialize filteredCandidates to hold a copy of replicaAddressMap.
	filteredCandidates := map[string]string{}
	for key, value := range replicaAddressMap {
		filteredCandidates[key] = value
	}

	volumeHeadSizeToReplicaNames := map[uint64][]string{}

	// Collect volume head size for each replica.
	for replicaName, replicaAddress := range replicaAddressMap {
		func() {
			// Get service client for the current replica.
			replicaServiceCli, err := GetServiceClient(replicaAddress)
			if err != nil {
				e.log.WithError(err).Warnf("Skipping salvage for replica %s with address %s due to failed to get replica service client", replicaName, replicaAddress)
				return
			}

			defer func() {
				if errClose := replicaServiceCli.Close(); errClose != nil {
					e.log.WithError(errClose).Errorf("Failed to close replica %s client with address %s during salvage candidate filtering", replicaName, replicaAddress)
				}
			}()

			// Retrieve replica information.
			replica, err := replicaServiceCli.ReplicaGet(replicaName)
			if err != nil {
				e.log.WithError(err).Warnf("Skipping salvage for replica %s with address %s due to failed to get replica info", replicaName, replicaAddress)
				delete(filteredCandidates, replicaName)
				return
			}

			// Map volume head size to replica names.
			volumeHeadSizeToReplicaNames[replica.Head.ActualSize] = append(volumeHeadSizeToReplicaNames[replica.Head.ActualSize], replicaName)
		}()
	}

	// Sort the volume head sizes to find the largest.
	volumeHeadSizeSorted, err := commonutils.SortKeys(volumeHeadSizeToReplicaNames)
	if err != nil {
		return nil, errors.Wrap(err, "failed to sort keys of salvage candidate by volume head size")
	}

	if len(volumeHeadSizeSorted) == 0 {
		return nil, errors.New("failed to find any salvage candidate with volume head size")
	}

	// Determine salvage candidates with the largest volume head size.
	largestVolumeHeadSize := volumeHeadSizeSorted[len(volumeHeadSizeSorted)-1]
	e.log.Infof("Selecting salvage candidates with the largest volume head size %v from %+v", largestVolumeHeadSize, volumeHeadSizeToReplicaNames)

	// Filter out replicas that do not match the largest volume head size.
	salvageCandidates := volumeHeadSizeToReplicaNames[largestVolumeHeadSize]
	for replicaName := range replicaAddressMap {
		if !commonutils.Contains(salvageCandidates, replicaName) {
			e.log.Infof("Skipping salvage for replica %s with address %s due to not having the largest volume head size (%v)", replicaName, replicaAddressMap[replicaName])
			delete(filteredCandidates, replicaName)
			continue
		}

		e.log.Infof("Including replica %s as a salvage candidate", replicaName)
	}

	return filteredCandidates, nil
}

func (e *Engine) Delete(spdkClient *spdkclient.Client, superiorPortAllocator *commonbitmap.Bitmap) (err error) {
	requireUpdate := false

	e.Lock()
	defer func() {
		// Considering that there may be still pending validations, it's better to update the state after the deletion.
		if err != nil {
			e.log.WithError(err).Errorf("Failed to delete engine %s", e.Name)
			if e.State != types.InstanceStateError {
				e.State = types.InstanceStateError
				e.ErrorMsg = err.Error()
				e.log.WithError(err).Error("Failed to delete engine")
				requireUpdate = true
			}
		} else {
			if e.State != types.InstanceStateError {
				e.ErrorMsg = ""
			}
		}
		if e.State == types.InstanceStateRunning {
			e.State = types.InstanceStateTerminating
			requireUpdate = true
		}

		e.Unlock()

		if requireUpdate {
			e.UpdateCh <- nil
		}
	}()

	e.log.Info("Deleting engine")
	if e.IsRestoring && e.restore != nil {
		e.log.Info("Canceling volume restoration before engine deletion")
		e.cancelCtx()
		e.restore.Stop()
	}

	e.log.Infof("Stopping to expose RAID bdev for engine %s", e.Name)
	switch e.Frontend {
	case types.FrontendUBLK:
		if err := spdkClient.UblkDestroyTarget(); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return errors.Wrapf(err, "failed to destroy UBLK target for engine %s", e.Name)
		}
	case types.FrontendSPDKTCPBlockdev, types.FrontendSPDKTCPNvmf:
		if err := spdkClient.StopExposeBdev(e.NvmeTcpTarget.Nqn); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return errors.Wrapf(err, "failed to stop exposing bdev for engine %s", e.Name)
		}
	}

	if e.NvmeTcpTarget != nil {
		e.NvmeTcpTarget.Nqn = ""
		e.NvmeTcpTarget.Nguid = ""
		e.NvmeTcpTarget.IP = ""
		// Port is released by releasePorts below.
	}

	// Release the ports if they are allocated
	if err := e.releasePorts(superiorPortAllocator); err != nil {
		return err
	}

	requireUpdate = true

	if _, err := spdkClient.BdevRaidDelete(e.Name); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return err
	}

	requireUpdate, err = e.disconnectReplicas(spdkClient)
	if err != nil {
		return err
	}

	e.log.Info("Deleted engine")

	return nil
}

func (e *Engine) disconnectReplicas(spdkClient *spdkclient.Client) (requireUpdate bool, err error) {
	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		if err := disconnectNVMfBdev(spdkClient, replicaStatus.BdevName, disconnectMaxRetries, disconnectRetryInterval); err != nil {
			if replicaStatus.Mode != types.ModeERR {
				e.log.WithError(err).Errorf("Engine failed to disconnect replica %s with bdev %s during deletion, will update the mode from %v to ERR", replicaName, replicaStatus.BdevName, replicaStatus.Mode)
				replicaStatus.Mode = types.ModeERR
				requireUpdate = true
			}
			return requireUpdate, err
		}
		delete(e.ReplicaStatusMap, replicaName)
		requireUpdate = true
	}

	return requireUpdate, nil
}

func (e *Engine) releasePorts(superiorPortAllocator *commonbitmap.Bitmap) error {
	if e.NvmeTcpTarget == nil {
		return nil
	}

	err := releasePortIfExists(superiorPortAllocator,
		map[int32]struct{}{
			e.NvmeTcpTarget.Port: {},
		},
		e.NvmeTcpTarget.Port)

	e.NvmeTcpTarget.Port = 0

	return err
}

func releasePortIfExists(superiorPortAllocator *commonbitmap.Bitmap, ports map[int32]struct{}, port int32) error {
	if port == 0 {
		return nil
	}

	_, exists := ports[port]
	if exists {
		if err := superiorPortAllocator.ReleaseRange(port, port); err != nil {
			return err
		}
		delete(ports, port)
	}

	return nil
}

func (e *Engine) Get() (res *spdkrpc.Engine) {
	e.RLock()
	defer e.RUnlock()

	return e.getWithoutLock()
}

func (e *Engine) getWithoutLock() (res *spdkrpc.Engine) {
	res = &spdkrpc.Engine{
		Name:                  e.Name,
		VolumeName:            e.VolumeName,
		SpecSize:              e.SpecSize,
		ActualSize:            e.ActualSize,
		ReplicaAddressMap:     map[string]string{},
		ReplicaModeMap:        map[string]spdkrpc.ReplicaMode{},
		Snapshots:             map[string]*spdkrpc.Lvol{},
		Frontend:              e.Frontend,
		State:                 string(e.State),
		ErrorMsg:              e.ErrorMsg,
		IsExpanding:           e.isExpanding,
		LastExpansionError:    e.lastExpansionError,
		LastExpansionFailedAt: e.lastExpansionFailedAt,
	}

	if e.NvmeTcpTarget != nil {
		res.Ip = e.NvmeTcpTarget.IP
		res.Port = e.NvmeTcpTarget.Port
	}

	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		res.ReplicaAddressMap[replicaName] = replicaStatus.Address
		res.ReplicaModeMap[replicaName] = types.ReplicaModeToGRPCReplicaMode(replicaStatus.Mode)
	}

	res.Head = api.LvolToProtoLvol(e.Head)

	for snapshotName, snapApiLvol := range e.SnapshotMap {
		res.Snapshots[snapshotName] = api.LvolToProtoLvol(snapApiLvol)
	}

	return res
}

type replicaAddFrontendSuspendResumeWrapper func(work func() error) error

// ReplicaAdd performs the full replica-add flow consisting of three phases:
//
// Phase 0 — Synchronous Setup (under Engine lock, returns on completion):
//  1. Validate engine state is Running, dst replica doesn't exist, no other WO replica.
//  2. Obtain replica gRPC clients for all existing replicas, plus src/dst rebuild clients.
//  3. Pick an RW replica as the rebuild source.
//  4. Call replicaAddStart (optionally wrapped by frontendSuspendResumeWrapper for EF suspend/resume):
//     a. Create rebuild snapshot across all replicas.
//     b. Get rebuilding snapshot list from src replica.
//     c. ReplicaRebuildingSrcStart: src replica exposes snapshot as NVMe-oF target.
//     d. ReplicaRebuildingDstStart: dst replica attaches external snapshot, creates head.
//     e. BdevRaidGrowBaseBdev: add dst head bdev to RAID as base bdev.
//     f. Mark dst replica as WO in ReplicaStatusMap.
//  5. Launch replicaAddAsync goroutine (Phase 1–2 below).
//  6. Return nil (or setupErr on failure) — background goroutine takes over.
//     On sync error: outer defer marks dst replica ERR and (if applicable) sets engine to Error state.
//
// Phase 1 — Shallow Copy (replicaAddAsync goroutine):
//  7. Check for setup errors: if Phase 0 failed (setupErr != nil), set asyncErr and skip to cleanup.
//  8. adder.ReplicaShallowCopy(): copy all snapshots from src to dst; on failure set asyncErr.
//
// Phase 2 — Finish or Cleanup (replicaAddCleanupOrFinish, two mutually exclusive paths):
//
//	Path A — Failure (asyncErr != nil):
//	  9. Call e.replicaAddFinish() directly (no frontendSuspendResumeWrapper, no suspend/resume) for SPDK resource cleanup.
//	     Replica is already ERR. replicaAddFinish uses the same DstFinish→SrcFinish order as the success path.
//
//	Path B — Success (asyncErr == nil):
//	 10. Call adder.ReplicaAddFinish() via frontendSuspendResumeWrapper (if present) or directly.
//	     frontendSuspendResumeWrapper (buildGRPCReplicaAddFrontendSuspendResumeWrapper) does:
//	       a. EF Suspend (gRPC to EngineFrontend).
//	       b. Execute replicaAddFinish (3-phase lock pattern):
//	          Phase 1 (lock): read dst mode. Phase 2 (unlock): RPC calls. Phase 3 (lock): update mode.
//	       c. EF Resume (gRPC to EngineFrontend).
//	 11. If finish returns error: mark dst replica ERR. SPDK resource cleanup is NOT retried —
//	     it is the responsibility of the ReplicaAdder (mock should call Real.ReplicaAddFinish()
//	     before returning error) or r.Delete() when the replica is subsequently removed.
func (e *Engine) ReplicaAdd(spdkClient *spdkclient.Client, dstReplicaName, dstReplicaAddress string, fastSync bool, frontendSuspendResumeWrapper replicaAddFrontendSuspendResumeWrapper) (err error) {
	updateRequired := false

	e.Lock()
	defer func() {
		e.Unlock()

		if updateRequired {
			e.UpdateCh <- nil
		}
	}()

	e.log.Infof("Engine is starting replica %s add", dstReplicaName)

	// Syncing with the SPDK TGT server only when the engine is running.
	if e.State != types.InstanceStateRunning {
		return fmt.Errorf("invalid state %v for engine %s replica %s add start", e.State, e.Name, dstReplicaName)
	}
	if e.IsRestoring {
		return fmt.Errorf("cannot add replica %s while engine %s restore is in progress", dstReplicaName, e.Name)
	}

	if _, exists := e.ReplicaStatusMap[dstReplicaName]; exists {
		return fmt.Errorf("replica %s already exists", dstReplicaName)
	}

	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		if replicaStatus.Mode == types.ModeWO {
			return fmt.Errorf("cannot add a new replica %s since there is already a rebuilding replica %s", dstReplicaName, replicaName)
		}
	}

	// engineErr will be set when the engine failed to do any non-recoverable operations, then there is no way to make the engine continue working. Typically, it's related to the frontend suspend or resume failures.
	// While err means replica-related operation errors. It will fail the current replica add flow.
	var engineErr error
	var srcReplicaName, srcReplicaAddress string
	var srcReplicaServiceCli, dstReplicaServiceCli *client.SPDKClient

	defer func() {
		if engineErr != nil {
			if e.State != types.InstanceStateError {
				e.State = types.InstanceStateError
				updateRequired = true
			}
			e.ErrorMsg = engineErr.Error()
		} else {
			if e.State != types.InstanceStateError {
				e.ErrorMsg = ""
			}
		}
		if engineErr != nil || err != nil {
			prevMode := types.Mode("")
			if e.ReplicaStatusMap[dstReplicaName] != nil {
				prevMode = e.ReplicaStatusMap[dstReplicaName].Mode
				e.ReplicaStatusMap[dstReplicaName].Mode = types.ModeERR
				e.ReplicaStatusMap[dstReplicaName].Address = dstReplicaAddress
			} else {
				e.ReplicaStatusMap[dstReplicaName] = &EngineReplicaStatus{
					Mode:    types.ModeERR,
					Address: dstReplicaAddress,
				}
			}

			e.log.WithError(err).Errorf("Engine failed to start replica %s rebuilding, will mark the rebuilding replica mode from %v to ERR", dstReplicaName, prevMode)
			updateRequired = true
		}
	}()

	replicaClients, err := e.getReplicaClients()
	if err != nil {
		return err
	}
	defer e.closeReplicaClients(replicaClients)

	srcReplicaName, srcReplicaAddress, err = e.getReplicaAddSrcReplica()
	if err != nil {
		return err
	}

	// On error, getSrcAndDstReplicaClients() closes any partially created
	// replica service clients before returning. On success, these clients are
	// intentionally kept open here and are closed later by replicaAddFinish(),
	// which is reached from either the async cleanup path or the async finish path.
	srcReplicaServiceCli, dstReplicaServiceCli, err = e.getSrcAndDstReplicaClients(srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress)
	if err != nil {
		return err
	}

	// Perform the synchronous setup phase (optionally wrapped for EF suspend/resume).
	var rebuildingSnapshotList []*api.Lvol
	var setupErr error
	startFn := func() error {
		var startEngineErr error
		var startUpdateRequired bool
		rebuildingSnapshotList, startUpdateRequired, startEngineErr, setupErr = e.replicaAddStart(spdkClient, replicaClients,
			srcReplicaServiceCli, dstReplicaServiceCli, srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress)
		updateRequired = updateRequired || startUpdateRequired
		if startEngineErr != nil {
			engineErr = startEngineErr
		}
		if setupErr != nil {
			return setupErr
		}
		return startEngineErr
	}

	if frontendSuspendResumeWrapper != nil {
		if wrapErr := frontendSuspendResumeWrapper(startFn); wrapErr != nil {
			// The wrapper itself may fail (e.g. suspend or resume failure).
			// If the inner startFn already set engineErr, those are captured
			// via closure.
			if err == nil && engineErr == nil {
				engineErr = wrapErr
			}
			setupErr = wrapErr
		}
	} else {
		if fnErr := startFn(); fnErr != nil {
			setupErr = fnErr
		}
	}

	// Launch the async phase: shallow copy followed by cleanup or finish.
	// Even on setup failure, the goroutine handles SPDK resource cleanup
	// (exposed snapshot, NVMe connections) via replicaAddCleanupOrFinish.
	go e.replicaAddAsync(srcReplicaServiceCli, dstReplicaServiceCli, srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress, rebuildingSnapshotList, fastSync, frontendSuspendResumeWrapper, setupErr)

	if setupErr != nil {
		return setupErr
	}

	// TODO: Mark the destination replica as WO mode here does not prevent the RAID bdev from using this. May need to have a SPDK API to control the corresponding base bdev mode.
	// Reading data from this dst replica is not a good choice as the flow will be more zigzag than reading directly from the src replica:
	// application -> RAID1 -> this base bdev (dest replica) -> the exposed snapshot (src replica).
	e.log.UpdateLoggerWithWarnOnFailure(logrus.Fields{
		"replicaStatusMap": e.ReplicaStatusMap,
	}, "Failed to update logger with replica status map during engine creation")

	e.log.Infof("Engine started to rebuild replica %s from healthy replica %s with fastSync %v", dstReplicaName, srcReplicaName, fastSync)

	return nil
}

// replicaAddStart performs the synchronous setup phase of replica add:
// creates a rebuild snapshot, starts src/dst rebuilding, connects the
// dst head bdev, and grows the RAID base bdev.
//
// Returns:
//   - rebuildingSnapshotList: snapshots to be shallow-copied in the async phase
//   - startUpdateRequired: true if engine state changed and UpdateCh should be notified
//   - engineErr: non-nil if the engine should transition to Error state
//   - err: non-nil for replica-related operation errors
func (e *Engine) replicaAddStart(spdkClient *spdkclient.Client, replicaClients map[string]*client.SPDKClient,
	srcReplicaServiceCli, dstReplicaServiceCli *client.SPDKClient,
	srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress string,
) (rebuildingSnapshotList []*api.Lvol, startUpdateRequired bool, engineErr, err error) {
	snapshotName := GenerateRebuildingSnapshotName()
	opts := &api.SnapshotOptions{
		Timestamp: util.Now(),
	}

	var replicasErr error
	startUpdateRequired, replicasErr, engineErr = e.snapshotOperationWithoutLock(spdkClient, replicaClients, snapshotName, SnapshotOperationCreate, opts)
	if replicasErr != nil {
		return nil, startUpdateRequired, engineErr, replicasErr
	}
	if engineErr != nil {
		return nil, startUpdateRequired, engineErr, nil
	}
	e.checkAndUpdateInfoFromReplicasNoLock()

	rebuildingSnapshotList, err = getRebuildingSnapshotList(srcReplicaServiceCli, srcReplicaName)
	if err != nil {
		return nil, startUpdateRequired, nil, err
	}

	// Ask the source replica to expose the newly created snapshot if the source replica and destination replica are not on the same node.
	externalSnapshotAddress, err := srcReplicaServiceCli.ReplicaRebuildingSrcStart(srcReplicaName, dstReplicaName, dstReplicaAddress, snapshotName)
	if err != nil {
		return nil, startUpdateRequired, nil, err
	}

	// The destination replica attaches the source replica exposed snapshot as the external snapshot then create a head based on it.
	dstHeadLvolAddress, err := dstReplicaServiceCli.ReplicaRebuildingDstStart(dstReplicaName, srcReplicaName, srcReplicaAddress, snapshotName, externalSnapshotAddress, rebuildingSnapshotList)
	if err != nil {
		return nil, startUpdateRequired, nil, err
	}

	// Ensure the dst head lvol size matches the engine spec size before connecting and growing the RAID.
	if err := e.ensureRebuildingReplicaSize(dstReplicaServiceCli, dstReplicaName); err != nil {
		return nil, startUpdateRequired, nil, err
	}

	// Add rebuilding replica head bdev to the base bdev list of the RAID bdev
	dstHeadLvolBdevName, err := connectNVMfBdev(spdkClient, dstReplicaName, dstHeadLvolAddress, e.ctrlrLossTimeout, e.fastIOFailTimeoutSec, maxRetries, retryInterval)
	if err != nil {
		return nil, startUpdateRequired, nil, err
	}

	// Double-confirm the actual bdev size reported by SPDK after connecting.
	bdevList, err := spdkClient.BdevGetBdevs(dstHeadLvolBdevName, 0)
	if err != nil {
		return nil, startUpdateRequired, nil, errors.Wrapf(err, "failed to get bdev info for rebuilding replica %s head bdev %s", dstReplicaName, dstHeadLvolBdevName)
	}
	if len(bdevList) != 1 {
		return nil, startUpdateRequired, nil, fmt.Errorf("expected 1 bdev for rebuilding replica %s head bdev %s, got %d", dstReplicaName, dstHeadLvolBdevName, len(bdevList))
	}
	if err := validateReplicaBdevSize(e, dstReplicaName, &bdevList[0]); err != nil {
		return nil, startUpdateRequired, nil, errors.Wrapf(err, "rebuilding replica %s head bdev %s has wrong size, cannot add to engine %s", dstReplicaName, dstHeadLvolBdevName, e.Name)
	}

	e.log.Infof("Adding rebuilding replica %s head bdev %s to the base bdev list for engine %s", dstReplicaName, dstHeadLvolBdevName, e.Name)
	if _, err := spdkClient.BdevRaidGrowBaseBdev(e.Name, dstHeadLvolBdevName); err != nil {
		return nil, startUpdateRequired, nil, errors.Wrapf(err, "failed to adding the rebuilding replica %s head bdev %s to the base bdev list for engine %s", dstReplicaName, dstHeadLvolBdevName, e.Name)
	}

	e.ReplicaStatusMap[dstReplicaName] = &EngineReplicaStatus{
		Address:  dstReplicaAddress,
		Mode:     types.ModeWO,
		BdevName: dstHeadLvolBdevName,
	}
	startUpdateRequired = true
	return rebuildingSnapshotList, startUpdateRequired, nil, nil
}

// replicaAddAsync runs the asynchronous phase of replica add: shallow copy
// followed by cleanup or finish. It is launched as a goroutine from ReplicaAdd.
// setupErr is non-nil if the synchronous setup phase (replicaAddStart) failed.
func (e *Engine) replicaAddAsync(
	srcReplicaServiceCli, dstReplicaServiceCli *client.SPDKClient,
	srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress string,
	rebuildingSnapshotList []*api.Lvol,
	fastSync bool,
	frontendSuspendResumeWrapper replicaAddFrontendSuspendResumeWrapper,
	setupErr error,
) {
	defer func() {
		if r := recover(); r != nil {
			e.log.Errorf("Recovered panic during engine %s replica %s add: %v", e.Name, dstReplicaName, r)
		}
	}()

	// Resolve the replica adder under lock
	e.RLock()
	adder := e.replicaAdder
	e.RUnlock()

	var asyncErr error
	defer func() {
		e.replicaAddCleanupOrFinish(adder, srcReplicaServiceCli, dstReplicaServiceCli, srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress, frontendSuspendResumeWrapper, asyncErr)
	}()

	// Check for errors from the synchronous setup phase
	if setupErr != nil {
		asyncErr = fmt.Errorf("replica add setup failed: %v", setupErr)
		e.log.Errorf("Engine %s won't do shallow copy for replica %s add due to setup error: %v", e.Name, dstReplicaName, setupErr)
		return
	}

	// Shallow copy phase
	if scErr := adder.ReplicaShallowCopy(dstReplicaServiceCli, srcReplicaName, dstReplicaName, rebuildingSnapshotList, fastSync); scErr != nil {
		asyncErr = scErr
		e.log.WithError(scErr).Errorf("Engine %s failed to do the shallow copy for replica %s add", e.Name, dstReplicaName)
		return
	}
}

// replicaAddCleanupOrFinish handles the completion of the async replica add phase.
// If asyncErr is non-nil, it calls replicaAddFinish directly (no frontendSuspendResumeWrapper) for
// SPDK resource cleanup. If asyncErr is nil, it runs the finish flow via the adder
// (optionally wrapped by frontendSuspendResumeWrapper for suspend/resume).
func (e *Engine) replicaAddCleanupOrFinish(
	adder ReplicaAdder,
	srcReplicaServiceCli, dstReplicaServiceCli *client.SPDKClient,
	srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress string,
	frontendSuspendResumeWrapper replicaAddFrontendSuspendResumeWrapper,
	asyncErr error,
) {
	if asyncErr != nil {
		// Setup or shallow copy failed. The replica is already
		// marked ERR by replicaShallowCopy's defer or ReplicaAdd's
		// outer defer. Call replicaAddFinish directly (no wrapper)
		// for SPDK resource cleanup (exposed snapshot, NVMe
		// connections). The mode is already ERR, so replicaAddFinish
		// uses the same DstFinish→SrcFinish order as the success path.
		if cleanupErr := e.replicaAddFinish(srcReplicaServiceCli, dstReplicaServiceCli, srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress); cleanupErr != nil {
			e.log.WithError(cleanupErr).Errorf("Engine %s failed to clean up after replica %s add failure", e.Name, dstReplicaName)
		}
		return
	}

	// Success path: shallow copy completed, run finish.
	e.log.Infof("Starting to finish replica %s add for engine %s", dstReplicaName, e.Name)

	finishFn := func() error {
		return adder.ReplicaAddFinish(srcReplicaServiceCli, dstReplicaServiceCli, srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress)
	}

	var finishErr error
	if frontendSuspendResumeWrapper != nil {
		finishErr = frontendSuspendResumeWrapper(finishFn)
	} else {
		finishErr = finishFn()
	}
	if finishErr != nil {
		e.log.WithError(finishErr).Errorf("Engine %s failed to finish replica %s add", e.Name, dstReplicaName)

		// Mark the replica as ERR. SPDK resource cleanup is the
		// responsibility of the ReplicaAdder (mock should call
		// Real.ReplicaAddFinish before returning error) or will
		// be handled by r.Delete() when the replica is removed.
		e.Lock()
		if dstStatus := e.ReplicaStatusMap[dstReplicaName]; dstStatus != nil && dstStatus.Mode != types.ModeERR {
			dstStatus.Mode = types.ModeERR
		}
		e.Unlock()
	}
}

func (e *Engine) closeReplicaAddClients(srcReplicaServiceCli, dstReplicaServiceCli *client.SPDKClient, srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress, phase string) {
	if srcReplicaServiceCli != nil {
		if errClose := srcReplicaServiceCli.Close(); errClose != nil {
			e.log.WithError(errClose).Errorf("Engine %s failed to close source replica %s client with address %s during %s", e.Name, srcReplicaName, srcReplicaAddress, phase)
		}
	}
	if dstReplicaServiceCli != nil {
		if errClose := dstReplicaServiceCli.Close(); errClose != nil {
			e.log.WithError(errClose).Errorf("Engine %s failed to close dest replica %s client with address %s during %s", e.Name, dstReplicaName, dstReplicaAddress, phase)
		}
	}
}

func (e *Engine) getSrcAndDstReplicaClients(srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress string) (srcReplicaServiceCli, dstReplicaServiceCli *client.SPDKClient, err error) {
	defer func() {
		if err != nil {
			if srcReplicaServiceCli != nil {
				if errClose := srcReplicaServiceCli.Close(); errClose != nil {
					e.log.WithError(errClose).Errorf("Failed to close source replica %s client with address %s during get get src and dst replica clients", srcReplicaName, srcReplicaAddress)
				}
			}
			if dstReplicaServiceCli != nil {
				if errClose := dstReplicaServiceCli.Close(); errClose != nil {
					e.log.WithError(errClose).Errorf("Failed to close dest replica %s client with address %s during get get src and dst replica clients", dstReplicaName, dstReplicaAddress)
				}
			}
			srcReplicaServiceCli = nil
			dstReplicaServiceCli = nil
		}
	}()

	srcReplicaServiceCli, err = GetServiceClient(srcReplicaAddress)
	if err != nil {
		return
	}
	dstReplicaServiceCli, err = GetServiceClient(dstReplicaAddress)
	return
}

func (e *Engine) replicaShallowCopy(dstReplicaServiceCli *client.SPDKClient, srcReplicaName, dstReplicaName string, rebuildingSnapshotList []*api.Lvol, fastSync bool) (err error) {
	updateRequired := false
	defer func() {
		if updateRequired {
			e.UpdateCh <- nil
		}
	}()

	defer func() {
		// Blindly mark the rebuilding replica as mode ERR now.
		if err != nil {
			e.Lock()
			if e.ReplicaStatusMap[dstReplicaName] != nil && e.ReplicaStatusMap[dstReplicaName].Mode != types.ModeERR {
				e.log.WithError(err).Errorf("Engine failed to do shallow copy from src replica %s to dst replica %s, will mark the rebuilding replica mode from %v to ERR", srcReplicaName, dstReplicaName, e.ReplicaStatusMap[dstReplicaName].Mode)
				e.ReplicaStatusMap[dstReplicaName].Mode = types.ModeERR
				updateRequired = true
			}
			e.Unlock()
		}
	}()

	e.log.Infof("Engine is starting snapshots shallow copy from rebuilding src replica %s to rebuilding dst replica %s", srcReplicaName, dstReplicaName)

	rebuildingSnapshotMap := map[string]*api.Lvol{}
	for _, snapshotApiLvol := range rebuildingSnapshotList {
		rebuildingSnapshotMap[snapshotApiLvol.Name] = snapshotApiLvol
	}

	// Traverse the src replica snapshot tree with a DFS way and do shallow copy one by one
	timer := time.NewTimer(MaxShallowCopyWaitTime)
	defer timer.Stop()
	ticker := time.NewTicker(ShallowCopyCheckInterval)
	defer ticker.Stop()
	currentSnapshotName := ""
	for idx := 0; idx < len(rebuildingSnapshotList); idx++ {
		currentSnapshotName = rebuildingSnapshotList[idx].Name
		e.log.Infof("Engine is syncing snapshot %s from rebuilding src replica %s to rebuilding dst replica %s", currentSnapshotName, srcReplicaName, dstReplicaName)

		if err := dstReplicaServiceCli.ReplicaRebuildingDstShallowCopyStart(dstReplicaName, currentSnapshotName, fastSync); err != nil {
			return errors.Wrapf(err, "failed to start shallow copy snapshot %s", currentSnapshotName)
		}

		timer.Reset(MaxShallowCopyWaitTime)
		continuousRetryCount := 0
		for finished := false; !finished; {
			select {
			case <-timer.C:
				return errors.Errorf("Timeout engine failed to check the dst replica %s snapshot %s shallow copy status over %d times", dstReplicaName, currentSnapshotName, maxRetries)
			case <-ticker.C:
				shallowCopyStatus, err := dstReplicaServiceCli.ReplicaRebuildingDstShallowCopyCheck(dstReplicaName)
				if err != nil {
					continuousRetryCount++
					if continuousRetryCount > maxRetries {
						return errors.Wrapf(err, "Engine failed to check the dst replica %s snapshot %s shallow copy status over %d times", dstReplicaName, currentSnapshotName, maxRetries)
					}
					e.log.WithError(err).Errorf("Engine failed to check the dst replica %s snapshot %s shallow copy status, retry count %d", dstReplicaName, currentSnapshotName, continuousRetryCount)
					continue
				}
				if shallowCopyStatus.State == helpertypes.ShallowCopyStateError || shallowCopyStatus.Error != "" {
					return fmt.Errorf("rebuilding error during shallow copy for snapshot %s: %s", shallowCopyStatus.SnapshotName, shallowCopyStatus.Error)
				}

				continuousRetryCount = 0
				if shallowCopyStatus.State == helpertypes.ShallowCopyStateComplete {
					if shallowCopyStatus.Progress != 100 {
						e.log.Warnf("Shallow copy snapshot %s is %s but somehow the progress is not 100%%", shallowCopyStatus.SnapshotName, helpertypes.ShallowCopyStateComplete)
					}
					e.log.Infof("Shallow copied snapshot %s", shallowCopyStatus.SnapshotName)
					finished = true
					break // nolint: staticcheck
				}
			}
		}

		snapshotOptions := &api.SnapshotOptions{
			UserCreated: rebuildingSnapshotMap[currentSnapshotName].UserCreated,
			Timestamp:   rebuildingSnapshotMap[currentSnapshotName].SnapshotTimestamp,
		}

		if err = dstReplicaServiceCli.ReplicaRebuildingDstSnapshotCreate(dstReplicaName, currentSnapshotName, snapshotOptions); err != nil {
			return err
		}
	}

	e.log.Infof("Engine shallow copied all snapshots from rebuilding src replica %s to rebuilding dst replica %s", srcReplicaName, dstReplicaName)

	return nil
}

// replicaAddFinish tries its best to finish the replica add no matter if the dst replica is rebuilt successfully or not.
// It returns fatal errors that lead to engine unavailable only. As for the errors during replica rebuilding wrap-up, it will be logged and ignored.
//
// The function uses a 3-phase lock pattern to avoid holding the Engine lock during
// potentially slow RPC calls (ReplicaRebuildingSrcFinish, ReplicaRebuildingDstFinish):
//
//	Phase 1 (lock):   Read dst replica mode from ReplicaStatusMap
//	Phase 2 (unlock): Execute RPC calls (DstFinish → SrcFinish) to src/dst replicas
//	Phase 3 (lock):   Update replica mode and engine state
func (e *Engine) replicaAddFinish(srcReplicaServiceCli, dstReplicaServiceCli *client.SPDKClient, srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress string) error {
	defer e.closeReplicaAddClients(srcReplicaServiceCli, dstReplicaServiceCli,
		srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress, "add replica finish")

	// Phase 1: Read replica state under lock
	e.Lock()
	dstReplicaStatus := e.ReplicaStatusMap[dstReplicaName]
	var dstMode types.Mode
	if dstReplicaStatus != nil {
		dstMode = dstReplicaStatus.Mode
	}
	e.Unlock()

	// Phase 2: Execute RPC calls without holding the Engine lock.
	// These calls may be slow (e.g. bdev_nvme_detach_controller returning ETIMEDOUT).
	// By releasing the lock, other Engine operations (status queries, other replica
	// operations) are not blocked during these potentially slow RPCs.
	e.RLock()
	phase2Hook := e.replicaAddFinishUnlockedHook
	e.RUnlock()
	if phase2Hook != nil {
		phase2Hook()
	}

	// The RPC order is always DstFinish first, then SrcFinish.
	// DstFinish calls BdevLvolSetParent (for ModeWO) to switch the dst
	// replica's snapshot chain from the external (src-exposed) snapshot
	// to the locally rebuilt chain. This parent-switch requires the
	// external snapshot bdev to still be accessible, so the src must
	// keep exposing it until DstFinish completes. For ModeERR the parent
	// switch is a no-op, but we still call DstFinish to actively clean
	// up dst-side resources (external snapshot attachment, NVMe controller).
	var dstReplicaErr error
	if dstReplicaStatus == nil {
		// Dst replica was already removed from the engine map during Phase 1→2.
		// Skip dst-side finish, but still clean up src-side resources (exposed
		// snapshot, NVMe-oF target, port) so they don't leak.
		e.log.Infof("Engine skipped finishing rebuilding dst replica %s as it was already removed, will still clean up src replica %s", dstReplicaName, srcReplicaName)
		if srcReplicaServiceCli != nil {
			if srcErr := srcReplicaServiceCli.ReplicaRebuildingSrcFinish(srcReplicaName, dstReplicaName); srcErr != nil {
				// WARNING: src replica may retain residual rebuilding state
				// (exposed snapshot, NVMe-oF target, port, dstRebuildingBdevName)
				// that will block subsequent rebuilds using this src replica.
				// Because dst replica is already removed from the engine map,
				// there is no engine-side state to mark ERR.
				// The residual state will be cleaned up when the src replica is
				// itself deleted (Replica.Delete calls doCleanupForRebuildingSrc).
				e.log.WithError(srcErr).Errorf("Engine failed to finish rebuilding src replica %s after dst replica %s was removed: src may retain residual rebuilding state (exposed snapshot, port) until src replica is deleted", srcReplicaName, dstReplicaName)
			}
		}
	} else if srcReplicaServiceCli == nil || dstReplicaServiceCli == nil {
		// The clients can be nil when replicaAddFinish is called for cleanup
		// after an early failure in ReplicaAdd (e.g. getReplicaClients or
		// getReplicaAddSrcReplica failed before clients were created).
		// Skip RPC calls; Phase 3 will still update the replica mode.
		e.log.Warnf("Engine skipping rebuilding RPC cleanup for replica %s because replica service clients are unavailable (src=%v, dst=%v)", dstReplicaName, srcReplicaServiceCli != nil, dstReplicaServiceCli != nil)
	} else {
		// Unified path: DstFinish first, then SrcFinish
		if dstErr := dstReplicaServiceCli.ReplicaRebuildingDstFinish(dstReplicaName); dstErr != nil {
			e.log.WithError(dstErr).Errorf("Engine failed to finish rebuilding dst replica %s, will update the mode from %v to ERR then continue rebuilding src replica %s finish", dstReplicaName, dstMode, srcReplicaName)
			dstReplicaErr = dstErr
		}

		// The source replica blindly stops exposing the snapshot and wipes
		// the rebuilding info. If this fails, the src replica retains residual
		// rebuilding state (exposed snapshot, NVMe-oF target, port,
		// dstRebuildingBdevName) that will be cleaned up when the src replica
		// is itself deleted (doCleanupForRebuildingSrc). This does NOT block
		// dst promotion to RW because the dst data is already correct after
		// a successful DstFinish (parent switch completed).
		if srcErr := srcReplicaServiceCli.ReplicaRebuildingSrcFinish(srcReplicaName, dstReplicaName); srcErr != nil {
			// TODO: Should we mark this healthy replica as error?
			e.log.WithError(srcErr).Errorf("Engine failed to finish rebuilding src replica %s, will ignore this error", srcReplicaName)
		}

		if dstReplicaErr == nil && dstMode == types.ModeWO {
			e.log.Infof("Engine succeeded to finish rebuilding dst replica %s, will update the mode from %v to RW", dstReplicaName, dstMode)
		}
	}

	// Phase 3: Update engine state under lock
	updateRequired := false

	e.Lock()
	defer func() {
		e.Unlock()

		if updateRequired {
			e.UpdateCh <- nil
		}
	}()

	if e.State != types.InstanceStateError {
		e.ErrorMsg = ""
	}

	// Re-read replica status — it may have been removed while we were unlocked.
	// Use the current mode (not the phase-1 snapshot dstMode) to decide the
	// state transition, so that concurrent downgrades (e.g. validateReplicaStatusMapNoLock
	// setting WO → ERR during unlocked phase 2) are not overwritten with RW.
	dstReplicaStatus = e.ReplicaStatusMap[dstReplicaName]
	if dstReplicaStatus != nil {
		switch dstReplicaStatus.Mode {
		case types.ModeERR:
			updateRequired = true
		case types.ModeWO:
			if dstReplicaErr != nil {
				dstReplicaStatus.Mode = types.ModeERR
			} else {
				dstReplicaStatus.Mode = types.ModeRW
			}
			updateRequired = true
		}
	}

	e.checkAndUpdateInfoFromReplicasNoLock()

	if dstReplicaErr != nil {
		e.log.Errorf("Engine failed to finish rebuilding replica %s from healthy replica %s (dstErr=%v)", dstReplicaName, srcReplicaName, dstReplicaErr)
	} else if dstReplicaStatus != nil && dstReplicaStatus.Mode == types.ModeERR {
		// All RPCs succeeded, but the replica mode is ERR because another
		// goroutine (e.g. validateReplicaStatusMapNoLock) downgraded WO → ERR
		// during the unlocked phase 2. Phase 3 correctly preserved that
		// concurrent downgrade instead of overwriting it with RW.
		e.log.Warnf("Engine finished rebuilding RPC cleanup for replica %s from healthy replica %s, but replica mode is ERR due to concurrent downgrade during unlocked phase", dstReplicaName, srcReplicaName)
	} else {
		e.log.Infof("Engine finished rebuilding replica %s from healthy replica %s", dstReplicaName, srcReplicaName)
	}

	return nil
}

// getReplicaAddSrcReplica picks the first RW replica from ReplicaStatusMap as the rebuild source.
func (e *Engine) getReplicaAddSrcReplica() (srcReplicaName, srcReplicaAddress string, err error) {
	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		if replicaStatus.Mode != types.ModeRW {
			continue
		}
		srcReplicaName = replicaName
		srcReplicaAddress = replicaStatus.Address
		break
	}
	if srcReplicaName == "" || srcReplicaAddress == "" {
		return "", "", fmt.Errorf("cannot find an RW replica in engine %s during replica add", e.Name)
	}
	return srcReplicaName, srcReplicaAddress, nil
}

// getRebuildingSnapshotList fetches the snapshot tree from the src replica and
// returns the ordered list of snapshots that need to be shallow-copied to the
// dst replica. It finds the ancestor snapshot (empty parent or backing image
// parent) and traverses the tree via DFS to produce the copy order.
func getRebuildingSnapshotList(srcReplicaServiceCli *client.SPDKClient, srcReplicaName string) ([]*api.Lvol, error) {
	rpcSrcReplica, err := srcReplicaServiceCli.ReplicaGet(srcReplicaName)
	if err != nil {
		return []*api.Lvol{}, err
	}
	ancestorSnapshotName, latestSnapshotName := "", ""
	for snapshotName, snapApiLvol := range rpcSrcReplica.Snapshots {
		// If the parent is empty, it's the ancestor snapshot
		// Notice that the ancestor snapshot parent is still empty even if there is a backing image
		if snapApiLvol.Parent == "" || types.IsBackingImageSnapLvolName(snapApiLvol.Parent) {
			ancestorSnapshotName = snapshotName
		}
		if snapApiLvol.Children[types.VolumeHead] {
			latestSnapshotName = snapshotName
		}
	}
	if ancestorSnapshotName == "" || latestSnapshotName == "" {
		return []*api.Lvol{}, fmt.Errorf("cannot find the ancestor snapshot %s or latest snapshot %s from RW replica %s snapshot map during engine replica add", ancestorSnapshotName, latestSnapshotName, srcReplicaName)
	}

	return retrieveRebuildingSnapshotList(rpcSrcReplica, ancestorSnapshotName, []*api.Lvol{}), nil
}

// retrieveRebuildingSnapshotList recursively traverses the replica snapshot tree with a DFS way
func retrieveRebuildingSnapshotList(rpcSrcReplica *api.Replica, currentSnapshotName string, rebuildingSnapshotList []*api.Lvol) []*api.Lvol {
	if currentSnapshotName == "" || currentSnapshotName == types.VolumeHead {
		return rebuildingSnapshotList
	}
	rebuildingSnapshotList = append(rebuildingSnapshotList, rpcSrcReplica.Snapshots[currentSnapshotName])
	for childSnapshotName := range rpcSrcReplica.Snapshots[currentSnapshotName].Children {
		rebuildingSnapshotList = retrieveRebuildingSnapshotList(rpcSrcReplica, childSnapshotName, rebuildingSnapshotList)
	}
	return rebuildingSnapshotList
}

func (e *Engine) ReplicaDelete(spdkClient *spdkclient.Client, replicaName, replicaAddress string) (err error) {
	e.log.Infof("Deleting replica %s with address %s from engine", replicaName, replicaAddress)

	e.Lock()
	defer e.Unlock()

	if replicaName == "" {
		for rName, rStatus := range e.ReplicaStatusMap {
			if rStatus.Address == replicaAddress {
				replicaName = rName
				break
			}
		}
	}
	if replicaName == "" {
		return fmt.Errorf("cannot find replica name with address %s for engine %s replica delete", replicaAddress, e.Name)
	}
	replicaStatus := e.ReplicaStatusMap[replicaName]
	if replicaStatus == nil {
		return fmt.Errorf("cannot find replica %s from the replica status map for engine %s replica delete", replicaName, e.Name)
	}
	if replicaAddress != "" && replicaStatus.Address != replicaAddress {
		return fmt.Errorf("replica %s recorded address %s does not match the input address %s for engine %s replica delete", replicaName, replicaStatus.Address, replicaAddress, e.Name)
	}

	e.log.Infof("Removing base bdev %v from engine", replicaStatus.BdevName)
	if _, err := spdkClient.BdevRaidRemoveBaseBdev(replicaStatus.BdevName); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "failed to remove base bdev %s for deleting replica %s", replicaStatus.BdevName, replicaName)
	}

	controllerName := helperutil.GetNvmeControllerNameFromNamespaceName(replicaStatus.BdevName)
	// Fallback to use replica name. Make sure there won't be a leftover controller even if somehow `replicaStatus.BdevName` has no record
	if controllerName == "" {
		e.log.Infof("No NVMf controller found for replica %s, so fallback to use replica name %s", replicaName, replicaName)
		controllerName = replicaName
	}
	// Detaching the corresponding NVMf controller to remote replica
	e.log.Infof("Detaching the corresponding NVMf controller %v during remote replica %s delete", controllerName, replicaName)
	if _, err := spdkClient.BdevNvmeDetachController(controllerName); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "failed to detach controller %s for deleting replica %s", controllerName, replicaName)
	}

	delete(e.ReplicaStatusMap, replicaName)

	e.log.UpdateLoggerWithWarnOnFailure(logrus.Fields{
		"replicaStatusMap": e.ReplicaStatusMap,
	}, "Failed to update logger with replica status map during engine creation")

	return nil
}

type SnapshotOperationType string

const (
	SnapshotOperationCreate = SnapshotOperationType("snapshot-create")
	SnapshotOperationDelete = SnapshotOperationType("snapshot-delete")
	SnapshotOperationRevert = SnapshotOperationType("snapshot-revert")
	SnapshotOperationPurge  = SnapshotOperationType("snapshot-purge")
	SnapshotOperationHash   = SnapshotOperationType("snapshot-hash")
)

func (e *Engine) SnapshotCreate(spdkClient *spdkclient.Client, inputSnapshotName string) (snapshotName string, err error) {
	e.log.Infof("Creating snapshot %s", inputSnapshotName)

	opts := &api.SnapshotOptions{
		UserCreated: true,
		Timestamp:   util.Now(),
	}

	return e.snapshotOperation(spdkClient, inputSnapshotName, SnapshotOperationCreate, opts)
}

func (e *Engine) SnapshotDelete(spdkClient *spdkclient.Client, snapshotName string) (err error) {
	e.log.Infof("Deleting snapshot %s", snapshotName)

	_, err = e.snapshotOperation(spdkClient, snapshotName, SnapshotOperationDelete, nil)
	return err
}

func (e *Engine) SnapshotRevert(spdkClient *spdkclient.Client, snapshotName string) (err error) {
	e.log.Infof("Reverting snapshot %s", snapshotName)

	_, err = e.snapshotOperation(spdkClient, snapshotName, SnapshotOperationRevert, nil)
	return err
}

func (e *Engine) SnapshotPurge(spdkClient *spdkclient.Client) (err error) {
	e.log.Infof("Purging snapshots")

	_, err = e.snapshotOperation(spdkClient, "", SnapshotOperationPurge, nil)
	return err
}

func (e *Engine) SnapshotHash(spdkClient *spdkclient.Client, snapshotName string, rehash bool) (err error) {
	e.log.Infof("Hashing snapshot %s, rehash %v", snapshotName, rehash)

	_, err = e.snapshotOperation(spdkClient, snapshotName, SnapshotOperationHash, rehash)
	return err
}

func (e *Engine) snapshotOperation(spdkClient *spdkclient.Client, inputSnapshotName string, snapshotOp SnapshotOperationType, opts any) (snapshotName string, err error) {
	updateRequired := false

	e.Lock()
	defer func() {
		e.Unlock()

		if updateRequired {
			e.UpdateCh <- nil
		}
	}()

	// Syncing with the SPDK TGT server only when the engine is running.
	if e.State != types.InstanceStateRunning {
		return "", fmt.Errorf("invalid state %v for engine %s snapshot %s operation", e.State, e.Name, inputSnapshotName)
	}

	replicaClients, err := e.getReplicaClients()
	if err != nil {
		return "", err
	}
	defer e.closeReplicaClients(replicaClients)

	if snapshotName, err = e.snapshotOperationPreCheckWithoutLock(replicaClients, inputSnapshotName, snapshotOp); err != nil {
		return "", err
	}

	var engineErr, replicasErr error
	defer func() {
		if engineErr != nil {
			if e.State != types.InstanceStateError {
				e.State = types.InstanceStateError
				updateRequired = true
			}
			e.ErrorMsg = engineErr.Error()
		} else {
			if e.State != types.InstanceStateError {
				e.ErrorMsg = ""
			}
		}
	}()

	updateRequired, replicasErr, engineErr = e.snapshotOperationWithoutLock(spdkClient, replicaClients, snapshotName, snapshotOp, opts)
	if replicasErr != nil {
		return "", replicasErr
	}
	if engineErr != nil {
		return "", engineErr
	}

	e.checkAndUpdateInfoFromReplicasNoLock()

	e.log.Infof("Engine finished snapshot operation %s name %s", snapshotOp, snapshotName)

	return snapshotName, nil
}

func (e *Engine) getReplicaClients() (replicaClients map[string]*client.SPDKClient, err error) {
	replicaClients = map[string]*client.SPDKClient{}
	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		if replicaStatus.Mode != types.ModeRW && replicaStatus.Mode != types.ModeWO {
			continue
		}
		if replicaStatus.Address == "" {
			continue
		}
		c, err := GetServiceClient(replicaStatus.Address)
		if err != nil {
			return nil, err
		}
		replicaClients[replicaName] = c
	}

	return replicaClients, nil
}

func (e *Engine) closeReplicaClients(replicaClients map[string]*client.SPDKClient) {
	for replicaName := range replicaClients {
		if replicaClients[replicaName] != nil {
			if errClose := replicaClients[replicaName].Close(); errClose != nil {
				e.log.WithError(errClose).Errorf("Failed to close replica %s client", replicaName)
			}
		}
	}
}

func (e *Engine) snapshotOperationPreCheckWithoutLock(replicaClients map[string]*client.SPDKClient, snapshotName string, snapshotOp SnapshotOperationType) (string, error) {
	if snapshotOp == SnapshotOperationCreate && snapshotName == "" {
		snapshotName = util.UUID()[:8]
	}

	if snapshotOp == SnapshotOperationDelete {
		if snapshotName == "" {
			return "", fmt.Errorf("empty snapshot name for engine %s snapshot deletion", e.Name)
		}
		// Refresh snapshot topology before validation to avoid stale SnapshotMap checks.
		e.checkAndUpdateInfoFromReplicasNoLock()
		if e.SnapshotMap[snapshotName] == nil {
			return "", fmt.Errorf("engine %s does not contain snapshot %s during snapshot deletion", e.Name, snapshotName)
		}
		if len(e.SnapshotMap[snapshotName].Children) > 1 {
			return "", fmt.Errorf("engine %s cannot delete snapshot %s since it contains multiple children %+v", e.Name, snapshotName, e.SnapshotMap[snapshotName].Children)
		}
	}

	for replicaName := range replicaClients {
		replicaStatus := e.ReplicaStatusMap[replicaName]
		if replicaStatus == nil {
			return "", fmt.Errorf("cannot find replica %s in the engine %s replica status map before snapshot %s operation", replicaName, e.Name, snapshotName)
		}
		switch snapshotOp {
		case SnapshotOperationCreate:
		case SnapshotOperationDelete:
			if replicaStatus.Mode == types.ModeWO {
				return "", fmt.Errorf("engine %s contains WO replica %s during snapshot %s delete", e.Name, replicaName, snapshotName)
			}
		case SnapshotOperationRevert:
			if snapshotName == "" {
				return "", fmt.Errorf("empty snapshot name for engine %s snapshot deletion", e.Name)
			}
			if e.Frontend != types.FrontendEmpty {
				return "", fmt.Errorf("invalid frontend %v for engine %s snapshot %s revert", e.Frontend, e.Name, snapshotName)
			}
			if replicaStatus.Mode == types.ModeWO {
				return "", fmt.Errorf("engine %s contains WO replica %s during snapshot %s revert", e.Name, replicaName, snapshotName)
			}
			r, err := replicaClients[replicaName].ReplicaGet(replicaName)
			if err != nil {
				return "", err
			}
			if r.Snapshots[snapshotName] == nil {
				return "", fmt.Errorf("replica %s does not contain the reverting snapshot %s", replicaName, snapshotName)
			}
		case SnapshotOperationPurge:
			if replicaStatus.Mode == types.ModeWO {
				return "", fmt.Errorf("engine %s contains WO replica %s during snapshot purge", e.Name, replicaName)
			}
			// TODO: Do we need to verify that all replicas hold the same system snapshot list?
		case SnapshotOperationHash:
			if replicaStatus.Mode == types.ModeWO {
				return "", fmt.Errorf("engine %s contains WO replica %s during snapshot hash", e.Name, replicaName)
			}
			// TODO: Do we need to verify that all replicas hold the same system snapshot list?
		default:
			return "", fmt.Errorf("unknown replica snapshot operation %s", snapshotOp)
		}
	}

	return snapshotName, nil
}

func (e *Engine) snapshotOperationWithoutLock(spdkClient *spdkclient.Client, replicaClients map[string]*client.SPDKClient, snapshotName string, snapshotOp SnapshotOperationType, opts any) (updated bool, replicasErr error, engineErr error) {
	if snapshotOp == SnapshotOperationRevert {
		if _, err := spdkClient.BdevRaidDelete(e.Name); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			e.log.WithError(err).Errorf("Failed to delete RAID after snapshot %s revert", snapshotName)
			return false, err, err
		}
	}

	replicaErrorList := []error{}
	for replicaName := range replicaClients {
		replicaStatus := e.ReplicaStatusMap[replicaName]
		if replicaStatus == nil {
			return false, fmt.Errorf("cannot find replica %s in the engine %s replica status map during snapshot %s operation", replicaName, e.Name, snapshotName), nil
		}
		if err := e.replicaSnapshotOperation(spdkClient, replicaClients[replicaName], replicaName, snapshotName, snapshotOp, opts); err != nil && replicaStatus.Mode != types.ModeERR {
			replicaErrorList = append(replicaErrorList, err)
			if snapshotOp != SnapshotOperationHash {
				e.log.WithError(err).Errorf("Engine failed to issue operation %s for replica %s snapshot %s, will mark the replica mode from %v to ERR", snapshotOp, replicaName, snapshotName, replicaStatus.Mode)
				replicaStatus.Mode = types.ModeERR
				updated = true
			}
		}
	}
	replicasErr = util.CombineErrors(replicaErrorList...)

	if snapshotOp == SnapshotOperationRevert {
		replicaBdevList := []string{}
		for _, replicaStatus := range e.ReplicaStatusMap {
			if replicaStatus.Mode != types.ModeRW {
				continue
			}
			if replicaStatus.BdevName == "" {
				continue
			}
			replicaBdevList = append(replicaBdevList, replicaStatus.BdevName)
		}

		engineErr = retrygo.Do(
			func() error {
				_, err := spdkClient.BdevRaidCreate(e.Name, spdktypes.BdevRaidLevel1, 0, replicaBdevList, "")
				return err
			},
			retrygo.Attempts(uint(maxRetries)),
			retrygo.Delay(retryInterval),
			retrygo.LastErrorOnly(true),
		)
	}

	return updated, replicasErr, engineErr
}

func (e *Engine) replicaSnapshotOperation(spdkClient *spdkclient.Client, replicaClient *client.SPDKClient, replicaName, snapshotName string, snapshotOp SnapshotOperationType, opts any) error {
	switch snapshotOp {
	case SnapshotOperationCreate:
		// TODO: execute `sync` for the NVMe initiator before snapshot start
		optsPtr, ok := opts.(*api.SnapshotOptions)
		if !ok {
			return fmt.Errorf("invalid opts types %+v for snapshot create operation", opts)
		}
		return replicaClient.ReplicaSnapshotCreate(replicaName, snapshotName, optsPtr)
	case SnapshotOperationDelete:
		return replicaClient.ReplicaSnapshotDelete(replicaName, snapshotName)
	case SnapshotOperationRevert:
		replicaStatus := e.ReplicaStatusMap[replicaName]
		if replicaStatus == nil {
			return fmt.Errorf("cannot find replica %s in the engine %s replica status map during snapshot %s operation", replicaName, e.Name, snapshotName)
		}
		if err := disconnectNVMfBdev(spdkClient, replicaStatus.BdevName, disconnectMaxRetries, disconnectRetryInterval); err != nil {
			return err
		}
		replicaStatus.BdevName = ""
		// If the below step failed, the replica will be marked as ERR during ValidateAndUpdate.
		if err := replicaClient.ReplicaSnapshotRevert(replicaName, snapshotName); err != nil {
			return err
		}
		bdevName, err := connectNVMfBdev(spdkClient, replicaName, replicaStatus.Address, e.ctrlrLossTimeout, e.fastIOFailTimeoutSec, maxRetries, retryInterval)
		if err != nil {
			return err
		}
		if bdevName != "" {
			replicaStatus.BdevName = bdevName
		}
	case SnapshotOperationPurge:
		return replicaClient.ReplicaSnapshotPurge(replicaName)
	case SnapshotOperationHash:
		rehash, ok := opts.(bool)
		if !ok {
			return fmt.Errorf("rehash should be a boolean value for snapshot hash operation")
		}
		if err := replicaClient.ReplicaSnapshotHash(replicaName, snapshotName, rehash); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown replica snapshot operation %s", snapshotOp)
	}

	return nil
}

func (e *Engine) SnapshotHashStatus(snapshotName string) (*spdkrpc.EngineSnapshotHashStatusResponse, error) {
	resp := &spdkrpc.EngineSnapshotHashStatusResponse{
		Status: map[string]*spdkrpc.ReplicaSnapshotHashStatusResponse{},
	}

	e.Lock()
	defer e.Unlock()

	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		if replicaStatus.Mode != types.ModeRW {
			continue
		}

		replicaSnapshotHashStatusResponse, err := e.getReplicaSnapshotHashStatus(replicaName, replicaStatus.Address, snapshotName)
		if err != nil {
			return nil, err
		}
		resp.Status[replicaStatus.Address] = replicaSnapshotHashStatusResponse
	}

	return resp, nil
}

type replicaCandidate struct {
	ip      string
	lvsUUID string
	address string
}

func (e *Engine) SnapshotClone(snapshotName, srcEngineName, srcEngineAddress string, cloneMode spdkrpc.CloneMode) (err error) {
	e.Lock()
	defer e.Unlock()

	defer func() {
		err = errors.Wrap(err, "failed to do SnapshotClone")
	}()

	e.log.Infof("Engine is starting cloning snapshot %s", snapshotName)

	if len(e.ReplicaStatusMap) != 1 {
		return fmt.Errorf("destination engine must only have 1 replica when doing snapshot clone. Current "+
			"replica count is %v", len(e.ReplicaStatusMap))
	}

	dstReplicaName, dstReplicaAddr := "", ""
	for rName, rStatus := range e.ReplicaStatusMap {
		if rStatus.Mode != types.ModeRW {
			continue
		}
		dstReplicaName = rName
		dstReplicaAddr = rStatus.Address
		break
	}

	if dstReplicaName == "" || dstReplicaAddr == "" {
		return fmt.Errorf("cannot find a RW destination replica")
	}

	e.log.Infof("Selecting replica %v with address %v as dst replica for cloning", dstReplicaName, dstReplicaAddr)

	dstReplicaServiceCli, err := GetServiceClient(dstReplicaAddr)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := dstReplicaServiceCli.Close(); errClose != nil {
			e.log.WithError(errClose).Errorf("Engine %v failed to close dst replica %v client with address %v",
				e.Name, dstReplicaName, dstReplicaAddr)
		}
	}()

	dstReplica, err := dstReplicaServiceCli.ReplicaGet(dstReplicaName)
	if err != nil {
		return err
	}

	srcEngineServiceCli, err := GetServiceClient(srcEngineAddress)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := srcEngineServiceCli.Close(); errClose != nil {
			e.log.WithError(errClose).Errorf("Engine %v failed to close src engine %v client with address %v"+
				" during snapshot clone", e.Name, srcEngineName, srcEngineAddress)
		}
	}()

	srcEngine, err := srcEngineServiceCli.EngineGet(srcEngineName)
	if err != nil {
		return err
	}
	srcReplicas, err := srcEngineServiceCli.EngineReplicaList(srcEngineName)
	if err != nil {
		return err
	}

	srcReplicaCandidates := map[string]replicaCandidate{}
	for rName, mode := range srcEngine.ReplicaModeMap {
		if mode != types.ModeRW {
			continue
		}
		rAddr, ok := srcEngine.ReplicaAddressMap[rName]
		if !ok {
			continue
		}
		r, ok := srcReplicas[rName]
		if !ok {
			continue
		}
		srcReplicaCandidates[rName] = replicaCandidate{ip: r.IP, lvsUUID: r.LvsUUID, address: rAddr}
	}

	srcReplicaName := ""
	srcReplicaAddress := ""
	for rName, cand := range srcReplicaCandidates {
		if cand.ip == dstReplica.IP && cand.lvsUUID == dstReplica.LvsUUID {
			srcReplicaName = rName
			srcReplicaAddress = cand.address
			break
		}
	}

	if srcReplicaName == "" || srcReplicaAddress == "" {
		if cloneMode == spdkrpc.CloneMode_CLONE_MODE_LINKED_CLONE {
			return fmt.Errorf("cannot find the src replica at the same address %v and on same LvsUUID %v as the "+
				"dst replica", dstReplica.IP, dstReplica.LvsUUID)
		}
		for rName, cand := range srcReplicaCandidates {
			srcReplicaName = rName
			srcReplicaAddress = cand.address
			break
		}
	}

	if srcReplicaName == "" || srcReplicaAddress == "" {
		return fmt.Errorf("cannot find the src replica for cloning")
	}

	return dstReplicaServiceCli.ReplicaSnapshotCloneDstStart(dstReplicaName, snapshotName, srcReplicaName, srcReplicaAddress, cloneMode)
}

func (e *Engine) getReplicaSnapshotHashStatus(replicaName, replicaAddress, snapshotName string) (*spdkrpc.ReplicaSnapshotHashStatusResponse, error) {
	replicaServiceCli, err := GetServiceClient(replicaAddress)
	if err != nil {
		return nil, err
	}
	defer func() {
		if errClose := replicaServiceCli.Close(); errClose != nil {
			e.log.WithError(errClose).Errorf("Failed to close replica client with address %s during get hash status", replicaAddress)
		}
	}()

	return replicaServiceCli.ReplicaSnapshotHashStatus(replicaName, snapshotName)
}

func (e *Engine) ReplicaList(spdkClient *spdkclient.Client) (ret map[string]*api.Replica, err error) {
	e.Lock()
	defer e.Unlock()

	replicas := map[string]*api.Replica{}

	for name, replicaStatus := range e.ReplicaStatusMap {
		replicaServiceCli, err := GetServiceClient(replicaStatus.Address)
		if err != nil {
			e.log.WithError(err).Errorf("Failed to get service client for replica %s with address %s during list replicas", name, replicaStatus.Address)
			continue
		}

		func() {
			defer func() {
				if errClose := replicaServiceCli.Close(); errClose != nil {
					e.log.WithError(errClose).Errorf("Failed to close replica %s client with address %s during list replicas", name, replicaStatus.Address)
				}
			}()

			replica, err := replicaServiceCli.ReplicaGet(name)
			if err != nil {
				e.log.WithError(err).Errorf("Failed to get replica %s with address %s", name, replicaStatus.Address)
				return
			}

			replicas[name] = replica
		}()
	}

	return replicas, nil
}

func (e *Engine) SetErrorState() {
	needUpdate := false

	e.Lock()
	defer func() {
		e.Unlock()

		if needUpdate {
			e.UpdateCh <- nil
		}
	}()

	if e.State != types.InstanceStateStopped && e.State != types.InstanceStateError {
		e.State = types.InstanceStateError
		needUpdate = true
	}
}

func (e *Engine) BackupCreate(backupName, volumeName, engineName, snapshotName, backingImageName, backingImageChecksum string,
	labels []string, backupTarget string, credential map[string]string, concurrentLimit int32, compressionMethod, storageClassName string, size uint64) (*BackupCreateInfo, error) {
	e.log.Infof("Creating backup %s", backupName)

	e.Lock()
	defer func() {
		e.Unlock()
		e.UpdateCh <- nil
	}()

	replicaName, replicaAddress := "", ""
	for name, replicaStatus := range e.ReplicaStatusMap {
		if replicaStatus.Mode != types.ModeRW {
			continue
		}
		replicaName = name
		replicaAddress = replicaStatus.Address
		break
	}

	e.log.Infof("Creating backup %s for volume %s on replica %s address %s", backupName, volumeName, replicaName, replicaAddress)

	replicaServiceCli, err := GetServiceClient(replicaAddress)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "%v", err)
	}
	defer func() {
		if errClose := replicaServiceCli.Close(); errClose != nil {
			e.log.WithError(errClose).Errorf("Failed to close replica %s client with address %s during create backup", replicaName, replicaAddress)
		}
	}()

	recv, err := replicaServiceCli.ReplicaBackupCreate(&client.BackupCreateRequest{
		BackupName:           backupName,
		SnapshotName:         snapshotName,
		VolumeName:           volumeName,
		ReplicaName:          replicaName,
		Size:                 size,
		BackupTarget:         backupTarget,
		StorageClassName:     storageClassName,
		BackingImageName:     backingImageName,
		BackingImageChecksum: backingImageChecksum,
		CompressionMethod:    compressionMethod,
		ConcurrentLimit:      concurrentLimit,
		Labels:               labels,
		Credential:           credential,
	})
	if err != nil {
		return nil, err
	}
	return &BackupCreateInfo{
		BackupName:     recv.Backup,
		IsIncremental:  recv.IsIncremental,
		ReplicaAddress: replicaAddress,
	}, nil
}

func (e *Engine) BackupStatus(backupName, replicaAddress string) (*spdkrpc.BackupStatusResponse, error) {
	e.Lock()
	defer e.Unlock()

	found := false
	for name, replicaStatus := range e.ReplicaStatusMap {
		if replicaStatus.Address == replicaAddress {
			if replicaStatus.Mode != types.ModeRW {
				return nil, grpcstatus.Errorf(grpccodes.Internal, "replica %s is not in RW mode", name)
			}
			found = true
			break
		}
	}

	if !found {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "replica address %s is not found in engine %s for getting backup %v status", replicaAddress, e.Name, backupName)
	}

	replicaServiceCli, err := GetServiceClient(replicaAddress)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "%v", err)
	}
	defer func() {
		if errClose := replicaServiceCli.Close(); errClose != nil {
			e.log.WithError(errClose).Errorf("Failed to close replica client with address %s during get backup %s status", replicaAddress, backupName)
		}
	}()

	return replicaServiceCli.ReplicaBackupStatus(backupName)
}

// BackupRestore initiates a backup restore for the engine.
// It returns a done channel that is closed when the restore goroutine completes
// (whether successfully, with an error, or cancelled). Callers that set up a
// temporary frontend connection should wait on this channel before tearing it down.
func (e *Engine) BackupRestore(spdkClient *spdkclient.Client, backupUrl, endpoint string, credential map[string]string, concurrentLimit int32, superiorPortAllocator *commonbitmap.Bitmap) (resp *spdkrpc.EngineBackupRestoreResponse, doneCh <-chan struct{}, err error) {
	e.log.Infof("Restoring backup %s", backupUrl)

	resp = &spdkrpc.EngineBackupRestoreResponse{
		Errors: map[string]string{},
	}

	e.Lock()
	if err := e.precheckBackupRestore(backupUrl); err != nil {
		e.Unlock()
		return resp, nil, err
	}
	e.Unlock()

	backupInfo, err := backupstore.InspectBackup(backupUrl)
	if err != nil {
		return resp, nil, err
	}

	e.Lock()
	defer e.Unlock()

	// need to recheck the backup restore precheck after inspecting the backup,
	// because we release the lock during backup inspection which can take a long time, and the engine state may change when we reacquire the lock.
	if err := e.precheckBackupRestore(backupUrl); err != nil {
		return resp, nil, err
	}

	if backupInfo.VolumeSize != int64(e.SpecSize) {
		return resp, nil, fmt.Errorf("the backup volume %v size %v must be the same as the Longhorn volume size %v", backupInfo.VolumeName, backupInfo.VolumeSize, e.SpecSize)
	}

	isFullRestore, err := e.backupRestorePrepare(spdkClient, backupUrl, credential, superiorPortAllocator)
	if err != nil {
		return resp, nil, err
	}

	lastRestored := e.restore.LastRestored

	defer func() {
		if err != nil {
			e.IsRestoring = false
		}
	}()

	// The frontend (NVMe-TCP initiator) is managed by EngineFrontend.
	// Store the provided endpoint so EngineRestore.OpenVolumeDev can access the block device.
	// Also copy it into e.restore.endpoint so OpenVolumeDev does not need to re-acquire the
	// engine lock (which would deadlock — BackupRestore already holds e.Lock()).
	e.log.Infof("Using endpoint %v for backup restore", endpoint)
	e.restore.endpoint = endpoint

	// Start the backup restore. The goroutine is launched only after these calls
	// succeed so that a failure here does not leave completeBackupRestore blocked
	// forever in waitForRestoreComplete (goroutine leak).
	if isFullRestore {
		e.log.Infof("Starting a new full restore for backup %v", backupUrl)
		if err := e.backupRestore(backupUrl, concurrentLimit); err != nil {
			return resp, nil, errors.Wrapf(err, "failed to start full backup restore")
		}
		e.log.Infof("Successfully initiated full restore for %v to %v", backupUrl, e.Name)
	} else {
		e.log.Infof("Starting an incremental restore for backup %v", backupUrl)
		if err := e.backupRestoreIncrementally(backupUrl, lastRestored, concurrentLimit); err != nil {
			return resp, nil, errors.Wrapf(err, "failed to start incremental backup restore")
		}
		e.log.Infof("Successfully initiated incremental restore for %v to %v", backupUrl, e.Name)
	}

	// ch is closed when completeBackupRestore finishes (success, error, or cancel).
	// The caller (EngineFrontend) waits on this channel before tearing down the initiator.
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		e.log.WithFields(logrus.Fields{
			"snapshotName": backupInfo.SnapshotName,
			"endpoint":     endpoint,
		}).Info("Waiting for replica restore completion in background")
		if err := e.completeBackupRestore(spdkClient, backupInfo.SnapshotName); err != nil {
			e.log.WithError(err).Warn("Failed to complete backup restore")
			return
		}
		e.log.WithField("snapshotName", backupInfo.SnapshotName).Info("Background backup restore completion finished")
	}()

	return resp, ch, nil
}

func (e *Engine) precheckBackupRestore(backupURL string) error {
	if len(e.ReplicaStatusMap) == 0 {
		return fmt.Errorf("cannot restore backup %s: no replicas available", backupURL)
	}

	for _, replicaStatus := range e.ReplicaStatusMap {
		if replicaStatus.Mode != types.ModeRW {
			return fmt.Errorf("cannot restore backup %s: replica %s is in mode %v",
				backupURL, replicaStatus.Address, replicaStatus.Mode)
		}
	}

	if e.IsRestoring {
		return fmt.Errorf("%w", ErrRestoringInProgress)
	}

	return nil
}

func (e *Engine) backupRestorePrepare(spdkClient *spdkclient.Client, backupUrl string, credential map[string]string, superiorPortAllocator *commonbitmap.Bitmap) (isFullRestore bool, err error) {
	backupType, err := butil.CheckBackupType(backupUrl)
	if err != nil {
		return false, errors.Wrapf(err, "failed to check backup type for %v", backupUrl)
	}
	if err = butil.SetupCredential(backupType, credential); err != nil {
		return false, errors.Wrapf(err, "failed to setup credential for %v", backupUrl)
	}

	backupName, _, _, err := backupstore.DecodeBackupURL(util.UnescapeURL(backupUrl))
	if err != nil {
		return false, errors.Wrapf(err, "failed to decode backup URL %v", backupUrl)
	}

	if e.restore == nil || e.restore.State == btypes.ProgressStateError || e.restore.State == btypes.ProgressStateCanceled {
		e.restore = NewEngineRestore(spdkClient, backupUrl, backupName, e, superiorPortAllocator)
	} else {
		if e.restore.LastRestored == backupName {
			return false, fmt.Errorf("already restored backup %v", backupName)
		}
		validLastRestoredBackup := e.canDoIncrementalRestore(e.restore, backupUrl, backupName)
		e.restore.StartNewRestore(backupUrl, backupName, validLastRestoredBackup)
	}

	e.IsRestoring = true
	e.log.WithFields(logrus.Fields{
		"replicas":        len(e.ReplicaStatusMap),
		"requestedBackup": backupUrl,
		"lastRestored":    e.restore.LastRestored,
	}).Info("Engine marked as restoring")

	isFullRestore = e.restore.LastRestored == ""
	return isFullRestore, nil
}

func (e *Engine) canDoIncrementalRestore(restore *EngineRestore, backupURL, requestedBackupName string) bool {
	if restore.LastRestored == "" {
		e.log.Warnf("There is a restore record but last restored backup is empty with restore state %v, will do full restore instead", restore.State)
		return false
	}
	if _, err := backupstore.InspectBackup(strings.Replace(backupURL, requestedBackupName, restore.LastRestored, 1)); err != nil {
		e.log.WithError(err).Warnf("The last restored backup %v becomes invalid for incremental restore, will do full restore instead", restore.LastRestored)
		return false
	}
	return true
}

func (e *Engine) backupRestore(backupURL string, concurrentLimit int32) error {
	backupURL = butil.UnescapeURL(backupURL)

	e.log.WithFields(logrus.Fields{
		"backupURL":       backupURL,
		"concurrentLimit": concurrentLimit,
	}).Info("Starting full backup restore")

	return backupstore.RestoreDeltaBlockBackup(e.ctx, &backupstore.DeltaRestoreConfig{
		BackupURL:       backupURL,
		DeltaOps:        e.restore,
		Filename:        "",
		ConcurrentLimit: concurrentLimit,
	})
}

func (e *Engine) backupRestoreIncrementally(backupURL, lastRestored string, concurrentLimit int32) error {
	backupURL = butil.UnescapeURL(backupURL)

	e.log.WithFields(logrus.Fields{
		"backupURL":       backupURL,
		"lastRestored":    lastRestored,
		"concurrentLimit": concurrentLimit,
	}).Info("Starting incremental backup restore")

	return backupstore.RestoreDeltaBlockBackupIncrementally(e.ctx, &backupstore.DeltaRestoreConfig{
		BackupURL:       backupURL,
		DeltaOps:        e.restore,
		LastBackupName:  lastRestored,
		Filename:        "",
		ConcurrentLimit: concurrentLimit,
	})
}

func (e *Engine) completeBackupRestore(spdkClient *spdkclient.Client, backupSnapshotName string) (err error) {
	// waitForRestoreComplete only reads e.restore fields under e.restore.RLock;
	// no engine lock is needed (and must not be held — SnapshotCreate/Delete acquire it).
	waitErr := e.waitForRestoreComplete()

	// Acquire the engine lock to mutate shared fields and tear down the temporary
	// NVMe-TCP target. This runs regardless of success or failure so that the target
	// is always stopped and the port always released.
	// The EngineFrontend initiator is still connected at this point;
	// it will be torn down after doneCh is closed (i.e. after this function returns).
	e.Lock()
	e.log.Infof("Finalizing backup restore state")

	if !e.IsRestoring {
		e.Unlock()
		return fmt.Errorf("BUG: engine is not being restored")
	}

	isCanceled := e.restore != nil && e.restore.State == btypes.ProgressStateCanceled

	// Snapshot name recorded by a previous restore cycle (used below once the lock is dropped).
	oldSnapshotName := ""
	if e.restore != nil {
		oldSnapshotName = e.restore.SnapshotName
	}

	// Stop the temporary NVMe-TCP target and release its port.
	if e.Frontend == types.FrontendEmpty {
		e.cleanupTemporaryNvmeTcpTargetForRestoreLocked(spdkClient, e.restore.superiorPortAllocator, "backup restore completion")
	}

	e.Unlock()

	if waitErr != nil {
		e.Lock()
		e.IsRestoring = false
		if e.restore != nil {
			e.restore.UpdateRestoreStatus(e.restore.SnapshotName, 0, waitErr)
		}
		e.Unlock()
		return errors.Wrapf(waitErr, "failed to wait for engine restore complete")
	}

	// Finalize restore state under the engine lock after snapshot operations complete.
	defer func() {
		e.Lock()
		e.IsRestoring = false
		if e.restore != nil {
			if err != nil {
				e.restore.UpdateRestoreStatus(e.restore.SnapshotName, 0, err)
			} else {
				e.restore.FinishRestore()
			}
		}
		e.Unlock()
	}()

	if isCanceled {
		e.log.Info("Doing nothing for canceled backup restoration")
		return nil
	}

	// Delete previous snapshot after restore if it exists.
	// SnapshotDelete acquires e.Lock() internally.
	if oldSnapshotName != "" {
		e.RLock()
		_, snapshotExists := e.SnapshotMap[oldSnapshotName]
		e.RUnlock()
		if snapshotExists {
			e.log.Infof("Deleting existing snapshot %v of the restored volume", oldSnapshotName)
			if delErr := e.SnapshotDelete(spdkClient, oldSnapshotName); delErr != nil {
				e.log.WithError(delErr).Warnf("Failed to delete existing snapshot %v of the restored volume", oldSnapshotName)
			}
		}
	}

	// Prefer using the backup source snapshot name for better traceability.
	// Fall back to a UUID-based name if it is not available.
	// SnapshotCreate acquires e.Lock() internally.
	var newSnapshotName string
	if backupSnapshotName == "" {
		newSnapshotName = fmt.Sprintf("restore-%s", util.UUID())
	} else {
		newSnapshotName = fmt.Sprintf("restore-%s", backupSnapshotName)

		// Avoid conflict if the snapshot already exists.
		e.RLock()
		_, exists := e.SnapshotMap[newSnapshotName]
		e.RUnlock()
		if exists {
			suffix := util.UUID()[:5]
			e.log.Warnf("Snapshot %v already exists, generating a unique restored snapshot name", newSnapshotName)
			newSnapshotName = fmt.Sprintf("restore-%s-%s", backupSnapshotName, suffix)
		}
	}

	e.log.Infof("Creating snapshot %v for the restored volume", newSnapshotName)
	if _, createErr := e.SnapshotCreate(spdkClient, newSnapshotName); createErr != nil {
		e.log.WithError(createErr).Warnf("Failed to create snapshot %v for the restored volume", newSnapshotName)
	}

	e.restore.Lock()
	e.restore.SnapshotName = newSnapshotName
	e.restore.Unlock()
	e.log.Infof("Successfully created restored snapshot %v", newSnapshotName)

	return nil
}

func (e *Engine) cleanupTemporaryNvmeTcpTargetForRestore(spdkClient *spdkclient.Client, superiorPortAllocator *commonbitmap.Bitmap, reason string) {
	e.Lock()
	defer e.Unlock()
	e.cleanupTemporaryNvmeTcpTargetForRestoreLocked(spdkClient, superiorPortAllocator, reason)
}

func (e *Engine) cleanupTemporaryNvmeTcpTargetForRestoreLocked(spdkClient *spdkclient.Client, superiorPortAllocator *commonbitmap.Bitmap, reason string) {
	if e.Frontend != types.FrontendEmpty || e.NvmeTcpTarget == nil {
		return
	}

	if e.NvmeTcpTarget.Nqn != "" {
		e.log.Infof("Cleaning up temporary NVMe-TCP target %s after %s", e.NvmeTcpTarget.Nqn, reason)
		if stopErr := spdkClient.StopExposeBdev(e.NvmeTcpTarget.Nqn); stopErr != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(stopErr) {
			e.log.WithError(stopErr).Warnf("Failed to stop exposing bdev during %s", reason)
		}
	}

	e.NvmeTcpTarget.Nqn = ""
	e.NvmeTcpTarget.Nguid = ""
	e.NvmeTcpTarget.IP = ""
	if relErr := e.releasePorts(superiorPortAllocator); relErr != nil {
		e.log.WithError(relErr).Warnf("Failed to release ports during %s", reason)
	}
}

func (e *Engine) waitForRestoreComplete() error {
	e.log.WithFields(logrus.Fields{
		"interval":     restorePeriodicRefreshInterval.String(),
		"snapshotName": e.RestoringSnapshotName,
	}).Info("Waiting for restore to complete")

	err := retrygo.Do(
		func() error {
			e.restore.RLock()
			restoreProgress := e.restore.Progress
			restoreError := e.restore.Error
			restoreState := e.restore.State
			e.restore.RUnlock()

			if restoreState == btypes.ProgressStateCanceled {
				return retrygo.Unrecoverable(fmt.Errorf("%v", btypes.ErrorMsgRestoreCancelled))
			}
			if restoreProgress == 100 {
				e.log.Infof("Backup restore is done: %v%%", restoreProgress)
				return nil
			}

			e.log.WithFields(logrus.Fields{
				"progress":     restoreProgress,
				"state":        restoreState,
				"snapshotName": e.RestoringSnapshotName,
			}).Debug("Restore is still in progress")

			if restoreError != "" {
				err := fmt.Errorf("%v", restoreError)
				e.log.WithError(err).Error("Found backup restoration error")
				return retrygo.Unrecoverable(err)
			}

			return fmt.Errorf("restore is still in progress")
		},
		retrygo.Delay(restorePeriodicRefreshInterval),
		retrygo.MaxDelay(restorePeriodicRefreshInterval),
		retrygo.DelayType(retrygo.FixedDelay),
		retrygo.Attempts(0), // retry forever until success or unrecoverable error
	)

	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) RestoreStatus() (*spdkrpc.RestoreStatusResponse, error) {
	resp := &spdkrpc.RestoreStatusResponse{
		Status: map[string]*spdkrpc.ReplicaRestoreStatusResponse{},
	}

	e.RLock()
	defer e.RUnlock()

	if e.restore == nil {
		for replicaName, replicaStatus := range e.ReplicaStatusMap {
			resp.Status[replicaStatus.Address] = &spdkrpc.ReplicaRestoreStatusResponse{
				ReplicaName:    replicaName,
				ReplicaAddress: GetBackendReplicaURL(replicaStatus.Address),
				IsRestoring:    false,
			}
		}
		return resp, nil
	}

	e.restore.RLock()
	restoreProgress := e.restore.Progress
	restoreError := e.restore.Error
	restoreState := e.restore.State
	lastRestored := e.restore.LastRestored
	currentRestoringBackup := e.restore.CurrentRestoringBackup
	backupURL := e.restore.BackupURL
	e.restore.RUnlock()

	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		resp.Status[replicaStatus.Address] = &spdkrpc.ReplicaRestoreStatusResponse{
			ReplicaName:            replicaName,
			ReplicaAddress:         GetBackendReplicaURL(replicaStatus.Address),
			IsRestoring:            e.IsRestoring,
			LastRestored:           lastRestored,
			Progress:               int32(restoreProgress),
			Error:                  restoreError,
			State:                  string(restoreState),
			BackupUrl:              backupURL,
			CurrentRestoringBackup: currentRestoringBackup,
		}
	}

	return resp, nil
}

// Expand performs an online volume expansion for the Longhorn Engine using SPDK.
// It expands the underlying replica logical volumes (lvol), recreates the SPDK RAID bdev,
// suspends and resumes frontend I/O as needed, and ensures cleanup and status updates on failure.
func (e *Engine) Expand(spdkClient *spdkclient.Client, size uint64) (err error) {
	// Add precheck
	requireExpansion, err := e.ExpandPrecheck(spdkClient, size)
	if err != nil {
		return err
	}

	e.Lock()
	originalSize := e.SpecSize
	if !requireExpansion {
		if e.SpecSize < size {
			e.SpecSize = size
		}
		// Clear stale expansion error from a previous partial failure,
		// since there is nothing left to expand.
		e.lastExpansionError = ""
		e.lastExpansionFailedAt = ""
		e.Unlock()
		return nil
	}
	defer e.Unlock()
	if e.isExpanding {
		return fmt.Errorf("%w", ErrExpansionInProgress)
	}
	e.isExpanding = true
	e.lastExpansionFailedAt = ""
	e.lastExpansionError = ""

	e.log.Info("Expanding engine frontend")

	defer func() {
		e.isExpanding = false
		e.finishExpansion(originalSize, size, err)
	}()

	var expandErr error

	replicaClients, err := e.getReplicaClients()
	if err != nil {
		return err
	}
	defer e.closeReplicaClients(replicaClients)

	e.log.Infof("Stopping to expose RAID bdev for engine %s", e.Name)
	switch e.Frontend {
	case types.FrontendUBLK:
		return fmt.Errorf("not support ublk frontend for expansion for engine %s", e.Name)
	case types.FrontendSPDKTCPBlockdev, types.FrontendSPDKTCPNvmf:
		if err := spdkClient.StopExposeBdev(e.NvmeTcpTarget.Nqn); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return errors.Wrapf(err, "failed to stop exposing bdev for engine %s", e.Name)
		}
	}

	e.log.Infof("Tearing down RAID bdev for engine %s", e.Name)
	raidBdevUUID, err := e.tearDownRaidBdev(spdkClient)
	if err != nil {
		return errors.Wrap(err, "failed to tear down expansion")
	}
	if e.RaidBdevUUID == "" {
		e.RaidBdevUUID = raidBdevUUID
	}

	// Perform expansion
	// We should always try to reconstruct the RAID bdev even if the expansion fails.
	if err := e.expandReplicas(spdkClient, replicaClients, size); err != nil {
		e.log.WithError(err).Errorf("Failed to expand replicas for engine %s", e.Name)
		// If expansion failed, we should return the error to the caller,
		// but we still need to Reconstruct the RAID bdev and Expose it
		// to make sure volume is still usable (with old size).
		// We will capture this error and return it at the end of the function.
		// We don't return here because we need to reconstruct the RAID bdev.
		// If we return here, the volume will be lost as we already tear down the RAID bdev.
		// We will return this error after the deferred functions are executed.
		// However, the err variable is named return variable, so we can just assign it.
		// But we need to be careful not to overwrite it with nil if subsequent steps succeed.
		// So we use a separate variable.
		expandErr = err
	}

	e.log.Infof("Reconstructing RAID bdev for engine %s", e.Name)
	if err := e.reconstructRaidBdev(spdkClient, e.RaidBdevUUID); err != nil {
		return errors.Wrap(err, "failed to reconstruct RAID bdev")
	}

	switch e.Frontend {
	case types.FrontendSPDKTCPBlockdev, types.FrontendSPDKTCPNvmf:
		cntlid := getEngineCntlid(e.Name)
		nsUUID := getStableVolumeNsUUID(e.VolumeName)
		// Preserve the current ANA state across the expand. If this engine
		// was demoted to inaccessible during a switchover, re-exposing with
		// optimized would create a dual-write window.
		currentANAState := e.NvmeTcpTarget.ANAState
		if currentANAState == "" {
			currentANAState = NvmeTCPANAStateOptimized
		}
		spdkANAState, err := toSPDKListenerANAState(currentANAState)
		if err != nil {
			return errors.Wrapf(err, "invalid ANA state %q for engine target %v during expand", currentANAState, e.Name)
		}
		e.log.Infof("Starting to expose RAID bdev for engine target %v on %v:%v with ANA state %v, cntlid %v, nsUUID %v",
			e.Name, e.NvmeTcpTarget.IP, e.NvmeTcpTarget.Port, currentANAState, cntlid, nsUUID)
		if err := spdkClient.StartExposeBdevWithANAState(e.NvmeTcpTarget.Nqn, e.Name, e.NvmeTcpTarget.Nguid, nsUUID,
			e.NvmeTcpTarget.IP, strconv.Itoa(int(e.NvmeTcpTarget.Port)),
			spdkANAState, cntlid, cntlid); err != nil {
			return errors.Wrapf(err, "failed to start exposing RAID bdev for engine target %v", e.Name)
		}
	case types.FrontendEmpty:
		e.log.Infof("Skipping RAID bdev exposure for engine %s after expansion because frontend is empty", e.Name)
	}

	return expandErr
}

func (e *Engine) finishExpansion(fromSize, toSize uint64, err error) {
	if err != nil {
		e.SpecSize = fromSize
		e.State = types.InstanceStateError
		e.ErrorMsg = err.Error()
		e.lastExpansionError = errors.Wrap(err, "engine failed to expand expansion").Error()
		e.lastExpansionFailedAt = time.Now().UTC().Format(time.RFC3339Nano)

		e.log.WithError(err).Errorf("Engine %s failed to expand", e.Name)
		e.log.Infof("Failed to expand from size %v to %v", fromSize, toSize)
		return
	}

	e.State = types.InstanceStateRunning
	e.ErrorMsg = ""
	if e.lastExpansionError != "" {
		e.SpecSize = fromSize
		if e.lastExpansionFailedAt == "" {
			e.lastExpansionFailedAt = time.Now().UTC().Format(time.RFC3339Nano)
		}
		e.log.Warnf("Partially failed to expand from size %v to %v; keeping engine size at %v: %v",
			fromSize, toSize, fromSize, e.lastExpansionError)
		return
	}

	e.SpecSize = toSize
	e.log.Infof("Succeeded to expand from size %v to %v", fromSize, toSize)
}

func (e *Engine) tearDownRaidBdev(spdkClient *spdkclient.Client) (bdevUUID string, err error) {
	bdevRaid, err := spdkClient.BdevRaidGet(e.Name, 0)
	if err != nil {
		if jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			// RAID bdev does not exist, do nothing
			return "", nil
		}
		return "", errors.Wrapf(err, "failed to get RAID bdev %s", e.Name)
	}
	if len(bdevRaid) == 0 {
		// RAID already deleted, do nothing
		return "", nil
	}

	bdevUUID = bdevRaid[0].UUID

	deleted, err := spdkClient.BdevRaidDelete(e.Name)
	if err != nil {
		if jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			e.log.WithField("engineName", e.Name).Info("RAID bdev already deleted")
			return bdevUUID, nil
		}
		return bdevUUID, err
	}

	if deleted {
		return bdevUUID, nil
	}

	return bdevUUID, fmt.Errorf("failed to delete RAID bdev %s", e.Name)
}

func (e *Engine) expandReplicas(spdkClient *spdkclient.Client, replicaClients map[string]*client.SPDKClient, size uint64) error {
	e.log.Info("Expanding replicas")

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		failed = make(map[string]error)
	)

	recordFailure := func(replicaName string, err error) {
		if err == nil {
			return
		}
		mu.Lock()
		failed[replicaName] = err
		mu.Unlock()
	}

	for replicaName, replicaClient := range replicaClients {
		replicaName, replicaClient := replicaName, replicaClient

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					recordFailure(replicaName, fmt.Errorf("panic during replica expansion: %v", r))
					e.log.WithField("replica", replicaName).Errorf("Panic during replica expansion: %v", r)
				}
			}()

			if err := e.expandSingleReplica(spdkClient, replicaName, replicaClient, size); err != nil {
				recordFailure(replicaName, err)
			}
		}()
	}

	wg.Wait()

	return e.handleReplicaExpandResult(replicaClients, failed)
}

func (e *Engine) expandSingleReplica(spdkClient *spdkclient.Client, replicaName string, replicaClient *client.SPDKClient, size uint64) error {
	replicaStatus, ok := e.ReplicaStatusMap[replicaName]
	if !ok {
		e.log.WithField("replica", replicaName).Warn("Replica not found in status map")
		return nil
	}

	replica, err := replicaClient.ReplicaGet(replicaName)
	if err != nil {
		return errors.Wrap(err, "get replica failure")
	}

	if replica.SpecSize == size {
		return nil
	}

	if err := disconnectNVMfBdev(spdkClient, replicaStatus.BdevName, disconnectMaxRetries, disconnectRetryInterval); err != nil {
		return err
	}

	if err := replicaClient.ReplicaExpand(replicaName, size); err != nil {
		return err
	}

	_, err = connectNVMfBdev(spdkClient, replicaName, replicaStatus.Address, e.ctrlrLossTimeout, e.fastIOFailTimeoutSec, maxRetries, retryInterval)
	return err
}

func (e *Engine) handleReplicaExpandResult(replicaClients map[string]*client.SPDKClient, failed map[string]error) error {
	if len(failed) == 0 {
		e.log.Info("All replicas expand success")
		return nil
	}

	aggregatedErr := aggregateReplicaErrors(failed)

	if len(failed) == len(replicaClients) {
		e.log.WithFields(logrus.Fields{"failedReplicas": aggregatedErr}).
			Error("All replicas failed to expand")
		return fmt.Errorf("all replicas failed to expand; aborting RAID recreation: %+v", aggregatedErr)
	}

	e.markReplicasERR(failed)
	e.lastExpansionError = fmt.Sprintf("%+v", aggregatedErr)
	e.log.WithFields(logrus.Fields{"failedReplicas": aggregatedErr}).
		Warn("Some replicas failed to expand and have been marked as ERR")
	return nil
}

func aggregateReplicaErrors(failed map[string]error) map[string]string {
	out := make(map[string]string, len(failed))
	for replicaName, err := range failed {
		out[replicaName] = err.Error()
	}
	return out
}

func (e *Engine) markReplicasERR(failed map[string]error) {
	for replicaName := range failed {
		if status, ok := e.ReplicaStatusMap[replicaName]; ok {
			status.Mode = types.ModeERR
		}
	}
}

func (e *Engine) reconstructRaidBdev(spdkClient *spdkclient.Client, bdevRaidUUID string) (err error) {
	e.log.WithFields(logrus.Fields{
		"engineName": e.Name,
		"volumeName": e.VolumeName,
		"frontend":   e.Frontend,
	}).Info("Reconstructing RAID bdev")

	// create the same name of raid bdev
	replicaBdevList := []string{}
	for _, replicaStatus := range e.ReplicaStatusMap {
		if replicaStatus.Mode != types.ModeRW {
			continue
		}
		if replicaStatus.BdevName == "" {
			continue
		}
		replicaBdevList = append(replicaBdevList, replicaStatus.BdevName)
	}
	if len(replicaBdevList) == 0 {
		return fmt.Errorf("no healthy replica bdevs available for RAID creation")
	}

	if _, err := spdkClient.BdevRaidCreate(e.Name, spdktypes.BdevRaidLevel1, 0, replicaBdevList, bdevRaidUUID); err != nil {
		return err
	}

	// wait the raid bdev is created
	backoff := wait.Backoff{
		Steps:    10,
		Duration: time.Second,
		Factor:   1.5,
		Jitter:   0.1,
		Cap:      time.Second * 10,
	}

	if err := retry.RetryOnConflict(backoff, func() error {
		_, err := spdkClient.BdevRaidGet(e.Name, 0)
		return err
	}); err != nil {
		return err
	}

	return nil
}

func (e *Engine) ExpandPrecheck(spdkClient *spdkclient.Client, size uint64) (requireExpansion bool, err error) {
	e.Lock()
	defer e.Unlock()

	e.log.Info("Prechecking engine expansion")

	if e.isExpanding {
		return false, fmt.Errorf("%w", ErrExpansionInProgress)
	}

	if e.IsRestoring {
		return false, fmt.Errorf("%w", ErrRestoringInProgress)
	}

	defer func() {
		if err != nil {
			e.log.WithError(err).Error("Engine precheck expansion failed")
		} else {
			e.log.Infof("Engine precheck expansion result: requireExpansion=%v", requireExpansion)
		}
	}()

	replicaClients, err := e.getReplicaClients()
	if err != nil {
		return false, errors.Wrapf(err, "failed to get replica clients")
	}
	defer e.closeReplicaClients(replicaClients)

	// Ensure all replicas are in RW mode and have the same size
	if len(e.ReplicaStatusMap) == 0 {
		return false, fmt.Errorf("cannot expand engine with no replica")
	}

	currentReplicaSize := uint64(0)
	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		e.log.Infof("Checking replica %s status", replicaName)
		if replicaStatus.Mode != types.ModeRW {
			return false, fmt.Errorf("cannot expand engine with replica %s in mode %v", replicaName, replicaStatus.Mode)
		}

		replicaClient, ok := replicaClients[replicaName]
		if !ok {
			return false, fmt.Errorf("cannot find client for replica %s", replicaName)
		}
		replica, err := replicaClient.ReplicaGet(replicaName)
		if err != nil {
			return false, errors.Wrapf(err, "cannot get replica %s before expansion", replicaName)
		}

		if currentReplicaSize == 0 {
			currentReplicaSize = replica.SpecSize
			continue
		}

		if currentReplicaSize != replica.SpecSize {
			return false, fmt.Errorf("cannot expand engine with replicas in different sizes: replica %s has size %v while other replicas have size %v",
				replicaName, replica.SpecSize, currentReplicaSize)
		}
	}

	if currentReplicaSize > size {
		return false, fmt.Errorf("%w: cannot expand engine to a smaller size %v, current replica size %v",
			ErrExpansionInvalidSize, size, currentReplicaSize)
	}
	if currentReplicaSize == size {
		e.log.Infof("Replicas already at requested size %v, skipping expansion", size)
		return false, nil // no need to expand
	}

	return true, nil
}

func (e *Engine) ValidateAndUpdate(spdkClient *spdkclient.Client) (err error) {
	updateRequired := false

	e.Lock()
	defer func() {
		e.Unlock()

		if updateRequired {
			e.UpdateCh <- nil
		}
	}()

	if e.shouldSkipValidateAndUpdateNoLock() {
		return nil
	}

	bdevMap, err := GetBdevMap(spdkClient)
	if err != nil {
		return err
	}

	defer func() {
		e.applyValidateAndUpdateErrorNoLock(err, &updateRequired)
	}()

	bdevRaid, err := e.getRaidBdevNoLock(bdevMap)
	if err != nil {
		return err
	}

	if err := e.validateAndMaybeAdjustSpecSizeNoLock(bdevRaid); err != nil {
		return err
	}

	containValidReplica := e.validateReplicaStatusMapNoLock(bdevMap, &updateRequired)

	e.log.UpdateLoggerWithWarnOnFailure(logrus.Fields{
		"replicaStatusMap": e.ReplicaStatusMap,
	}, "Failed to update logger with replica status map during engine creation")

	if !containValidReplica {
		e.State = types.InstanceStateError
		e.log.Error("Engine had no RW replica found at the end of ValidateAndUpdate, will be marked as error")
		updateRequired = true
		// TODO: should we delete the engine automatically here?
	}

	e.checkAndUpdateInfoFromReplicasNoLock()

	return nil
}

func (e *Engine) shouldSkipValidateAndUpdateNoLock() bool {
	if e.IsRestoring {
		e.log.Debug("Engine is restoring, will skip the validation and update")
		return true
	}

	if e.isExpanding {
		e.log.Debug("Engine is expanding, will skip the validation and update")
		return true
	}

	// Syncing with the SPDK TGT server only when the engine is running.
	if e.State != types.InstanceStateRunning {
		return true
	}

	return false
}

func (e *Engine) applyValidateAndUpdateErrorNoLock(err error, updateRequired *bool) {
	// TODO: we may not need to mark the engine as ERR for each error
	if err != nil {
		if e.State != types.InstanceStateError {
			e.State = types.InstanceStateError
			e.log.WithError(err).Error("Found error during engine validation and update")
			*updateRequired = true
		}
		e.ErrorMsg = err.Error()
		return
	}

	if e.State != types.InstanceStateError {
		e.ErrorMsg = ""
	}
}

func (e *Engine) getRaidBdevNoLock(bdevMap map[string]*spdktypes.BdevInfo) (*spdktypes.BdevInfo, error) {
	bdevRaid := bdevMap[e.Name]
	if spdktypes.GetBdevType(bdevRaid) != spdktypes.BdevTypeRaid {
		return nil, fmt.Errorf("cannot find a raid bdev for engine %v", e.Name)
	}
	return bdevRaid, nil
}

func (e *Engine) validateAndMaybeAdjustSpecSizeNoLock(bdevRaid *spdktypes.BdevInfo) error {
	bdevRaidSize := bdevRaid.NumBlocks * uint64(bdevRaid.BlockSize)

	if e.SpecSize > bdevRaidSize {
		// not directly return error
		//
		// If the volume is not attached and do the expand
		// At first, we create and attach the engine with new size, but not yet to expand
		// it will cause infinite loop for size mismatching
		// loop to destroy and create engine
		// and there is no chance to execute EngineExpand()
		//
		// wait the lh-manager to reconcile engine CR and call EngineExpand()

		e.SpecSize = bdevRaidSize
		e.log.Warnf("found mismatching between engine spec size %d and actual raid bdev size %d for engine %s", e.SpecSize, bdevRaidSize, e.Name)
		return nil
	}

	if e.SpecSize < bdevRaidSize {
		// should not happen
		return fmt.Errorf("engine spec size %d is smaller than actual raid bdev size %d for engine %s", e.SpecSize, bdevRaidSize, e.Name)
	}

	return nil
}

func (e *Engine) validateReplicaStatusMapNoLock(bdevMap map[string]*spdktypes.BdevInfo, updateRequired *bool) bool {
	containValidReplica := false

	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		if replicaStatus.Address == "" || replicaStatus.BdevName == "" {
			if replicaStatus.Mode != types.ModeERR {
				e.log.Errorf("Engine marked replica %s mode from %v to ERR since its address %s or bdev name %s is empty during ValidateAndUpdate", replicaName, replicaStatus.Mode, replicaStatus.Address, replicaStatus.BdevName)
				replicaStatus.Mode = types.ModeERR
				*updateRequired = true
			}
		}

		if replicaStatus.Mode != types.ModeRW && replicaStatus.Mode != types.ModeWO && replicaStatus.Mode != types.ModeERR {
			e.log.Errorf("Engine found replica %s invalid mode %v during ValidateAndUpdate", replicaName, replicaStatus.Mode)
			replicaStatus.Mode = types.ModeERR
			*updateRequired = true
		}

		if replicaStatus.Mode != types.ModeERR {
			e.log.Debugf("Engine validating replica %s with bdev name %s and address %s during ValidateAndUpdate", replicaName, replicaStatus.BdevName, replicaStatus.Address)
			mode, err := e.validateAndUpdateReplicaNvme(replicaName, bdevMap[replicaStatus.BdevName])
			if err != nil {
				e.log.WithError(err).Errorf("Engine found valid NVMe for replica %v, will update the mode from %s to ERR during ValidateAndUpdate", replicaName, replicaStatus.Mode)
				replicaStatus.Mode = types.ModeERR
				*updateRequired = true
			} else if replicaStatus.Mode != mode {
				replicaStatus.Mode = mode
				*updateRequired = true
			}
		}

		if replicaStatus.Mode == types.ModeRW {
			containValidReplica = true
		}
	}

	return containValidReplica
}

type replicaInspection struct {
	replica           *api.Replica
	ancestor          *api.Lvol
	foundBackingImage bool
	foundSnapshot     bool
}

// checkAndUpdateInfoFromReplicasNoLock refreshes engine-level info (SnapshotMap, Head,
// ActualSize) from the replicas. It iterates ReplicaStatusMap, inspects each RW/WO
// replica, resolves its ancestor lineage, and selects the replica with the earliest
// ancestor CreationTime as the info source. Must be called with the Engine lock held.
func (e *Engine) checkAndUpdateInfoFromReplicasNoLock() {
	replicaMap := map[string]*api.Replica{}
	replicaAncestorMap := map[string]*api.Lvol{}
	hasBackingImage := false
	hasSnapshot := false

	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		if !e.ensureReplicaModeForInfoUpdate(replicaName, replicaStatus) {
			continue
		}

		inspection, ok := e.inspectReplicaForInfoUpdate(replicaName, replicaStatus, hasBackingImage, hasSnapshot)
		if !ok {
			continue
		}

		if inspection.foundBackingImage {
			hasBackingImage = true
		}
		if inspection.foundSnapshot {
			hasSnapshot = true
		}

		replicaMap[replicaName] = inspection.replica
		replicaAncestorMap[replicaName] = inspection.ancestor
	}

	e.selectAndApplyEarliestReplicaInfo(replicaMap, replicaAncestorMap, hasBackingImage, hasSnapshot)
}

// ensureReplicaModeForInfoUpdate checks whether a replica's mode qualifies it
// for info update inspection. Returns true for RW and WO replicas. For any
// unexpected mode (not RW, WO, or ERR), it downgrades the mode to ERR and
// returns false.
func (e *Engine) ensureReplicaModeForInfoUpdate(replicaName string, replicaStatus *EngineReplicaStatus) bool {
	if replicaStatus.Mode == types.ModeRW || replicaStatus.Mode == types.ModeWO {
		return true
	}
	if replicaStatus.Mode != types.ModeERR {
		e.log.Warnf("Engine found unexpected mode for replica %s with address %s during info update from replica, mark the mode from %v to ERR and continue info update for other replicas",
			replicaName, replicaStatus.Address, replicaStatus.Mode)
		replicaStatus.Mode = types.ModeERR
	}
	return false
}

// inspectReplicaForInfoUpdate validates and inspects a replica as an info source candidate.
//
// Here, "info source" means the replica selected as the source of truth for this update round,
// i.e. the replica whose data may be used to update engine state such as SnapshotMap, Head,
// and ActualSize.
//
// Flow:
//  1. Build replica service client and fetch replica object.
//  2. If the replica is WO (rebuilding), only check shallow-copy state; do not use it as an info source.
//  3. If the replica is RW, resolve its ancestor (backing image snapshot / oldest snapshot / head)
//     based on current global context (hasBackingImage, hasSnapshot).
func (e *Engine) inspectReplicaForInfoUpdate(replicaName string, replicaStatus *EngineReplicaStatus, hasBackingImage bool, hasSnapshot bool) (*replicaInspection, bool) {
	replicaServiceCli, err := GetServiceClient(replicaStatus.Address)
	if err != nil {
		e.log.WithError(err).Errorf("Engine failed to get service client for replica %s with address %s, will skip this replica and continue info update for other replicas", replicaName, replicaStatus.Address)
		return nil, false
	}
	defer func() {
		if errClose := replicaServiceCli.Close(); errClose != nil {
			e.log.WithError(errClose).Errorf("Engine failed to close replica %s client with address %s during check and update info from replica", replicaName, replicaStatus.Address)
		}
	}()

	replica, err := replicaServiceCli.ReplicaGet(replicaName)
	if err != nil {
		e.log.WithError(err).Warnf("Engine failed to get replica %s with address %s, mark the mode from %v to ERR", replicaName, replicaStatus.Address, replicaStatus.Mode)
		replicaStatus.Mode = types.ModeERR
		return nil, false
	}

	if replicaStatus.Mode == types.ModeWO {
		if err := e.handleWOReplicaDuringInfoUpdate(replicaServiceCli, replicaName, replicaStatus); err != nil {
			e.log.WithError(err).Warn("Skip WO replica during info update")
		}
		return nil, false
	}

	inspection := &replicaInspection{replica: replica}
	ancestor, foundBackingImage, foundSnapshot, ok := e.resolveReplicaAncestor(replicaServiceCli, replicaName, replica, replicaStatus, hasBackingImage, hasSnapshot)
	if !ok {
		return nil, false
	}
	inspection.ancestor = ancestor
	inspection.foundBackingImage = foundBackingImage
	inspection.foundSnapshot = foundSnapshot

	return inspection, true
}

func (e *Engine) handleWOReplicaDuringInfoUpdate(replicaServiceCli *client.SPDKClient, replicaName string, replicaStatus *EngineReplicaStatus) error {
	shallowCopyStatus, err := replicaServiceCli.ReplicaRebuildingDstShallowCopyCheck(replicaName)
	if err != nil {
		return errors.Wrapf(err, "Engine failed to get rebuilding replica %s shallow copy info, will skip this replica and continue info update for other replicas", replicaName)
	}
	if shallowCopyStatus.TotalState == helpertypes.ShallowCopyStateError || shallowCopyStatus.Error != "" {
		replicaStatus.Mode = types.ModeERR
		return fmt.Errorf("Engine found rebuilding replica %s error %v during info update from replica, will mark the mode from WO to ERR and continue info update for other replicas", replicaName, shallowCopyStatus.Error)
	}
	// rebuilding replica is not used as info source
	return nil
}

// resolveReplicaAncestor determines the ancestor lvol used to compare replica lineage
// during engine info refresh.
//
// Selection order per replica:
// 1. Backing image snapshot (if the replica has a backing image)
// 2. Oldest snapshot (snapshot with empty Parent)
// 3. Head (when no snapshots exist)
//
// It also enforces cross-replica consistency for this round:
// - If any replica has backing image lineage, replicas without backing image lineage are skipped.
// - If any replica has snapshot lineage (and no backing image lineage), replicas without snapshots are skipped.
//
// Returns:
// - ancestor: selected lvol for lineage/creation-time comparison.
// - foundBackingImage: true if this replica contributes backing-image lineage.
// - foundSnapshot: true if this replica contributes snapshot lineage.
// - ok: false when the replica should be skipped (inconsistent lineage, missing ancestor, or lookup failure).
func (e *Engine) resolveReplicaAncestor(replicaServiceCli *client.SPDKClient, replicaName string,
	replica *api.Replica, replicaStatus *EngineReplicaStatus, hasBackingImage bool, hasSnapshot bool) (ancestor *api.Lvol, foundBackingImage bool, foundSnapshot bool, ok bool) {
	if replica.BackingImageName != "" {
		backingImage, err := replicaServiceCli.BackingImageGet(replica.BackingImageName, replica.LvsUUID)
		if err != nil {
			e.log.WithError(err).Warnf("Failed to get backing image %s with disk UUID %s from replica %s head parent %s, will mark the mode from %v to ERR and continue info update for other replicas", replica.BackingImageName, replica.LvsUUID, replicaName, replica.Head.Parent, replicaStatus.Mode)
			replicaStatus.Mode = types.ModeERR
			return nil, false, false, false
		}
		return backingImage.Snapshot, true, len(replica.Snapshots) > 0, true
	}

	if len(replica.Snapshots) > 0 {
		if hasBackingImage {
			e.log.Warnf("Engine found replica %s does not have a backing image while other replicas have during info update for other replicas", replicaName)
			return nil, false, false, false
		}
		for _, snapApiLvol := range replica.Snapshots {
			if snapApiLvol.Parent == "" {
				return snapApiLvol, false, true, true
			}
		}
		e.log.Warnf("Engine cannot find replica %s ancestor, will skip this replica and continue info update for other replicas", replicaName)
		return nil, false, false, false
	}

	if hasSnapshot {
		e.log.Warnf("Engine found replica %s does not have a snapshot while other replicas have during info update for other replicas", replicaName)
		return nil, false, false, false
	}
	return replica.Head, false, false, true
}

// selectAndApplyEarliestReplicaInfo chooses one replica as the engine info source
// and applies its state to the engine.
//
// From replicas that already passed inspection, it filters candidates by lineage type:
// - backing-image lineage if hasBackingImage is true
// - snapshot lineage if hasBackingImage is false and hasSnapshot is true
// - head lineage otherwise
//
// It then selects the candidate whose chosen ancestor has the earliest CreationTime.
// Once selected, it updates engine state from that replica:
// - e.SnapshotMap
// - e.Head
// - e.ActualSize
//
// If candidate switching happens and ancestor names differ, it emits a warning log.
//
// Notes:
// - Replicas with invalid/unparsable ancestor CreationTime are skipped.
// - If no valid candidate remains, engine state is left unchanged.
func (e *Engine) selectAndApplyEarliestReplicaInfo(replicaMap map[string]*api.Replica, replicaAncestorMap map[string]*api.Lvol, hasBackingImage bool, hasSnapshot bool) {
	candidateReplicaName := ""
	earliestCreationTime := time.Now()

	for replicaName, ancestorApiLvol := range replicaAncestorMap {
		if !shouldConsiderAncestor(ancestorApiLvol, replicaName, hasBackingImage, hasSnapshot) {
			continue
		}

		creationTime, err := time.Parse(time.RFC3339, ancestorApiLvol.CreationTime)
		if err != nil {
			e.log.WithError(err).Warnf("Failed to parse replica %s ancestor creation time, will skip this replica and continue info update for other replicas: %+v", replicaName, ancestorApiLvol)
			continue
		}
		if !earliestCreationTime.After(creationTime) {
			continue
		}

		earliestCreationTime = creationTime
		e.SnapshotMap = replicaMap[replicaName].Snapshots
		e.Head = replicaMap[replicaName].Head
		e.ActualSize = replicaMap[replicaName].ActualSize

		if candidateReplicaName != "" && candidateReplicaName != replicaName {
			e.logReplicaAncestorSwitch(candidateReplicaName, replicaName, replicaAncestorMap)
		}
		candidateReplicaName = replicaName
	}
}

func shouldConsiderAncestor(ancestor *api.Lvol, replicaName string, hasBackingImage bool, hasSnapshot bool) bool {
	if hasBackingImage {
		return ancestor.Name != types.VolumeHead && !IsReplicaSnapshotLvol(replicaName, ancestor.Name)
	}
	if hasSnapshot {
		return ancestor.Name != types.VolumeHead
	}
	return ancestor.Name == types.VolumeHead
}

func (e *Engine) logReplicaAncestorSwitch(prevReplica string, currReplica string, replicaAncestorMap map[string]*api.Lvol) {
	prevName := replicaAncestorMap[prevReplica].Name
	currName := replicaAncestorMap[currReplica].Name

	prevDisplay := normalizeAncestorNameForLog(e, prevName)
	currDisplay := normalizeAncestorNameForLog(e, currName)

	if prevDisplay != currDisplay {
		e.log.Warnf("Comparing with replica %s ancestor %s, replica %s has a different and earlier ancestor %s, will update info from this replica",
			prevReplica, prevName, currReplica, currName)
	}
}

func normalizeAncestorNameForLog(e *Engine, name string) string {
	if !types.IsBackingImageSnapLvolName(name) {
		return name
	}
	backingImageName, _, err := ExtractBackingImageAndDiskUUID(name)
	if err != nil {
		e.log.WithError(err).Warnf("BUG: ancestor name %v is from backingImage.Snapshot lvol name, it should be a valid backing image lvol name", name)
		return name
	}
	return backingImageName
}

func (e *Engine) validateAndUpdateReplicaNvme(replicaName string, bdev *spdktypes.BdevInfo) (types.Mode, error) {
	if bdev == nil {
		return types.ModeERR, fmt.Errorf("cannot find a bdev for replica %s", replicaName)
	}

	if err := validateReplicaBdevSize(e, replicaName, bdev); err != nil {
		return types.ModeERR, err
	}

	nvmeInfo, err := validateAndGetSingleNvmeInfo(replicaName, bdev)
	if err != nil {
		return types.ModeERR, err
	}
	if err := validateNvmeTransport(replicaName, bdev.Name, nvmeInfo); err != nil {
		return types.ModeERR, err
	}

	replicaStatus := e.ReplicaStatusMap[replicaName]
	if err := validateReplicaAddress(replicaName, bdev.Name, replicaStatus.Address, nvmeInfo); err != nil {
		return types.ModeERR, err
	}
	if err := validateControllerName(replicaName, bdev.Name, replicaStatus.BdevName); err != nil {
		return types.ModeERR, err
	}

	return replicaStatus.Mode, nil
}

// ensureRebuildingReplicaSize checks the dst replica's spec size against the engine spec size
// before the NVMe bdev is connected. If the replica is smaller (e.g. created before a volume
// expansion), it is expanded in place so that only a single connectNVMfBdev call is needed.
// Returns an error if the replica is larger than the engine spec size.
func (e *Engine) ensureRebuildingReplicaSize(dstReplicaServiceCli *client.SPDKClient, replicaName string) error {
	replica, err := dstReplicaServiceCli.ReplicaGet(replicaName)
	if err != nil {
		return errors.Wrapf(err, "failed to get rebuilding replica %s info before size check", replicaName)
	}

	if replica.SpecSize == e.SpecSize {
		return nil
	}
	if replica.SpecSize > e.SpecSize {
		return fmt.Errorf("rebuilding replica %s size %d exceeds engine %s spec size %d",
			replicaName, replica.SpecSize, e.Name, e.SpecSize)
	}

	e.log.Infof("Rebuilding replica %s size %d is smaller than engine %s spec size %d, expanding before connecting",
		replicaName, replica.SpecSize, e.Name, e.SpecSize)
	if err := dstReplicaServiceCli.ReplicaExpand(replicaName, e.SpecSize); err != nil {
		return errors.Wrapf(err, "failed to expand rebuilding replica %s to engine %s spec size %d", replicaName, e.Name, e.SpecSize)
	}
	return nil
}

func validateReplicaBdevSize(e *Engine, replicaName string, bdev *spdktypes.BdevInfo) error {
	bdevSpecSize := bdev.NumBlocks * uint64(bdev.BlockSize)
	if e.SpecSize != bdevSpecSize {
		return fmt.Errorf(
			"found mismatching between replica bdev %s spec size %d and the engine %s spec size %d during replica %s mode validation",
			bdev.Name, bdevSpecSize, e.Name, e.SpecSize, replicaName,
		)
	}
	return nil
}

func validateAndGetSingleNvmeInfo(replicaName string, bdev *spdktypes.BdevInfo) (spdktypes.NvmeNamespaceInfo, error) {
	if spdktypes.GetBdevType(bdev) != spdktypes.BdevTypeNvme {
		return spdktypes.NvmeNamespaceInfo{}, fmt.Errorf(
			"found bdev type %v rather than %v during replica %s mode validation",
			spdktypes.GetBdevType(bdev), spdktypes.BdevTypeNvme, replicaName,
		)
	}
	if bdev.DriverSpecific.Nvme == nil || len(*bdev.DriverSpecific.Nvme) != 1 {
		return spdktypes.NvmeNamespaceInfo{}, fmt.Errorf(
			"found zero or multiple NVMe info in a NVMe base bdev %v during replica %s mode validation",
			bdev.Name, replicaName,
		)
	}
	return (*bdev.DriverSpecific.Nvme)[0], nil
}

func validateNvmeTransport(replicaName, bdevName string, nvmeInfo spdktypes.NvmeNamespaceInfo) error {
	if !strings.EqualFold(string(nvmeInfo.Trid.Trtype), string(spdktypes.NvmeTransportTypeTCP)) {
		return fmt.Errorf(
			"found invalid transport type %s in a remote NVMe base bdev %s during replica %s mode validation",
			nvmeInfo.Trid.Trtype, bdevName, replicaName,
		)
	}
	if !strings.EqualFold(string(nvmeInfo.Trid.Adrfam), string(spdktypes.NvmeAddressFamilyIPv4)) &&
		!strings.EqualFold(string(nvmeInfo.Trid.Adrfam), string(spdktypes.NvmeAddressFamilyIPv6)) {
		return fmt.Errorf(
			"found invalid address family %s in a remote NVMe base bdev %s during replica %s mode validation",
			nvmeInfo.Trid.Adrfam, bdevName, replicaName,
		)
	}
	return nil
}

func validateReplicaAddress(replicaName, bdevName, expectedAddr string, nvmeInfo spdktypes.NvmeNamespaceInfo) error {
	actualAddr := net.JoinHostPort(nvmeInfo.Trid.Traddr, nvmeInfo.Trid.Trsvcid)
	if expectedAddr != actualAddr {
		return fmt.Errorf(
			"found mismatching between replica bdev %s address %s and the NVMe bdev actual address %s during replica %s mode validation",
			bdevName, expectedAddr, actualAddr, replicaName,
		)
	}
	return nil
}

func validateControllerName(replicaName, bdevName, namespaceBdevName string) error {
	controllerName := helperutil.GetNvmeControllerNameFromNamespaceName(namespaceBdevName)
	if controllerName != replicaName {
		return fmt.Errorf(
			"found unexpected the NVMe bdev controller name %s (bdev name %s) during replica %s mode validation",
			controllerName, bdevName, replicaName,
		)
	}
	return nil
}

// SetReplicaAdder replaces the ReplicaAdder used by ReplicaAdd.
// For testing, pass a *MockReplicaAdder. Pass nil to restore production behavior.
func (e *Engine) SetReplicaAdder(adder ReplicaAdder) {
	e.Lock()
	defer e.Unlock()
	if adder == nil {
		e.replicaAdder = &realReplicaAdder{e: e}
	} else {
		// If substituting a MockReplicaAdder, inject the real adder for fallback.
		if m, ok := adder.(*MockReplicaAdder); ok && m.Real == nil {
			m.Real = &realReplicaAdder{e: e}
		}
		e.replicaAdder = adder
	}
}

// SetReplicaAddFinishUnlockedHook injects (or clears) the regression-guard
// hook for the 3-phase lock pattern in replicaAddFinish. See the field comment
// on replicaAddFinishUnlockedHook for details. Pass nil to clear.
func (e *Engine) SetReplicaAddFinishUnlockedHook(hook func()) {
	e.Lock()
	defer e.Unlock()
	e.replicaAddFinishUnlockedHook = hook
}

func GetBackendReplicaURL(address string) string {
	return "tcp://" + address
}

// ensureNvmeTcpTargetForRestore creates a temporary NVMe-TCP target if the engine does not
// already have one (e.g. engines with Frontend = ""). Called by EngineFrontend before
// connecting its restore initiator so that the target address is available.
func (e *Engine) ensureNvmeTcpTargetForRestore(spdkClient *spdkclient.Client, superiorPortAllocator *commonbitmap.Bitmap) error {
	e.Lock()
	defer e.Unlock()

	if e.NvmeTcpTarget.IP != "" && e.NvmeTcpTarget.Port != 0 {
		return nil // target already present (e.g. from a prior partial restore attempt)
	}

	e.log.Info("Creating temporary NVMe-TCP target for backup restore")
	if err := e.createNVMeTCPTarget(spdkClient, superiorPortAllocator, 1, NvmeTCPANAStateOptimized); err != nil {
		if relErr := e.releasePorts(superiorPortAllocator); relErr != nil {
			e.log.WithError(relErr).Warn("Failed to release ports after temporary NVMe-TCP target creation failure")
		}
		e.NvmeTcpTarget.IP = ""
		e.NvmeTcpTarget.Nguid = ""
		e.NvmeTcpTarget.Nqn = ""
		return errors.Wrap(err, "failed to create temporary NVMe-TCP target for backup restore")
	}
	return nil
}
