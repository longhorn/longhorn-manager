package spdk

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/go-spdk-helper/pkg/initiator"
	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

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

type Engine struct {
	sync.RWMutex

	Name       string
	VolumeName string
	SpecSize   uint64
	ActualSize uint64
	Frontend   string
	Endpoint   string

	ctrlrLossTimeout     int
	fastIOFailTimeoutSec int
	ReplicaStatusMap     map[string]*EngineReplicaStatus

	NvmeTcpFrontend *NvmeTcpFrontend
	UblkFrontend    *UblkFrontend
	initiator       *initiator.Initiator
	dmDeviceIsBusy  bool

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
}

type EngineReplicaStatus struct {
	Address  string
	BdevName string
	Mode     types.Mode
}

type NvmeTcpFrontend struct {
	IP                string
	Port              int32 // Port that initiator is connecting to
	TargetIP          string
	TargetPort        int32 // Port of the target that is used for letting initiator connect to
	StandbyTargetPort int32

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

func NewEngine(engineName, volumeName, frontend string, specSize uint64, engineUpdateCh chan interface{}, ublkQueueDepth, ublkNumberOfQueue int32) *Engine {
	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"engineName": engineName,
		"volumeName": volumeName,
		"frontend":   frontend,
	})

	roundedSpecSize := util.RoundUp(specSize, helpertypes.MiB)
	if roundedSpecSize != specSize {
		log.Infof("Rounded up spec size from %v to %v since the spec size should be multiple of MiB", specSize, roundedSpecSize)
	}
	log.WithField("specSize", roundedSpecSize)

	var nvmeTcpFrontend *NvmeTcpFrontend
	var ublkFrontend *UblkFrontend
	if types.IsUblkFrontend(frontend) {
		ublkFrontend = &UblkFrontend{
			UblkQueueDepth:    ublkQueueDepth,
			UblkNumberOfQueue: ublkNumberOfQueue,
		}
	} else {
		nvmeTcpFrontend = &NvmeTcpFrontend{}
	}

	return &Engine{
		Name:       engineName,
		VolumeName: volumeName,
		Frontend:   frontend,
		SpecSize:   specSize,

		// TODO: support user-defined values
		ctrlrLossTimeout:     replicaCtrlrLossTimeoutSec,
		fastIOFailTimeoutSec: replicaFastIOFailTimeoutSec,

		ReplicaStatusMap: map[string]*EngineReplicaStatus{},

		NvmeTcpFrontend: nvmeTcpFrontend,
		UblkFrontend:    ublkFrontend,

		State: types.InstanceStatePending,

		SnapshotMap: map[string]*api.Lvol{},

		UpdateCh: engineUpdateCh,

		log: safelog.NewSafeLogger(log),
	}
}

func (e *Engine) isNewNvmeTcpFrontendEngine() bool {
	return e.NvmeTcpFrontend != nil && e.NvmeTcpFrontend.IP == "" && e.NvmeTcpFrontend.TargetIP == "" && e.NvmeTcpFrontend.StandbyTargetPort == 0
}

func (e *Engine) checkInitiatorAndTargetCreationRequirements(podIP, initiatorIP, targetIP string) (bool, bool, error) {
	initiatorCreationRequired, targetCreationRequired := false, false
	var err error

	if types.IsUblkFrontend(e.Frontend) {
		return true, true, nil
	}
	if e.NvmeTcpFrontend == nil {
		return false, false, fmt.Errorf("failed to checkInitiatorAndTargetCreationRequirements: invalid NvmeTcpFrontend: %v", e.NvmeTcpFrontend)
	}

	if podIP == initiatorIP && podIP == targetIP {
		// If the engine is running on the same pod, it should have both initiator and target instances
		if e.NvmeTcpFrontend.Port == 0 && e.NvmeTcpFrontend.TargetPort == 0 {
			// Both initiator and target instances are not created yet
			e.log.Info("Creating both initiator and target instances")
			initiatorCreationRequired = true
			targetCreationRequired = true
		} else if e.NvmeTcpFrontend.Port != 0 && e.NvmeTcpFrontend.TargetPort == 0 {
			// Only target instance creation is required, because the initiator instance is already running
			e.log.Info("Creating a target instance")
			if e.NvmeTcpFrontend.StandbyTargetPort != 0 {
				e.log.Warnf("Standby target instance with port %v is already created, will skip the target creation", e.NvmeTcpFrontend.StandbyTargetPort)
			} else {
				targetCreationRequired = true
			}
		} else {
			e.log.Infof("Initiator instance with port %v and target instance with port %v are already created, will skip the creation", e.NvmeTcpFrontend.Port, e.NvmeTcpFrontend.TargetPort)
		}
	} else if podIP == initiatorIP && podIP != targetIP {
		// Only initiator instance creation is required, because the target instance is running on a different pod
		e.log.Info("Creating an initiator instance")
		initiatorCreationRequired = true
	} else if podIP == targetIP && podIP != initiatorIP {
		// Only target instance creation is required, because the initiator instance is running on a different pod
		e.log.Info("Creating a target instance")
		targetCreationRequired = true
	} else {
		err = fmt.Errorf("invalid initiator and target addresses for engine %s creation with initiator address %v and target address %v", e.Name, initiatorIP, targetIP)
	}

	return initiatorCreationRequired, targetCreationRequired, err
}

func (e *Engine) Create(spdkClient *spdkclient.Client, replicaAddressMap map[string]string, portCount int32, superiorPortAllocator *commonbitmap.Bitmap, initiatorAddress, targetAddress string, salvageRequested bool) (ret *spdkrpc.Engine, err error) {
	e.log.WithFields(logrus.Fields{
		"portCount":         portCount,
		"replicaAddressMap": replicaAddressMap,
		"initiatorAddress":  initiatorAddress,
		"targetAddress":     targetAddress,
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

	podIP, err := commonnet.GetIPForPod()
	if err != nil {
		return nil, err
	}

	initiatorIP, _, err := splitHostPort(initiatorAddress)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to split initiator address %v", initiatorAddress)
	}

	targetIP, _, err := splitHostPort(targetAddress)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to split target address %v", targetAddress)
	}

	if e.State != types.InstanceStatePending {
		switchingOverBack := e.State == types.InstanceStateRunning && initiatorIP == targetIP
		if !switchingOverBack {
			requireUpdate = false
			return nil, fmt.Errorf("invalid state %s for engine %s creation", e.State, e.Name)
		}
	}

	if err := e.ValidateReplicaSize(replicaAddressMap); err != nil {
		return nil, errors.Wrapf(err, "failed to validate replica size during engine creation")
	}

	defer func() {
		if err != nil {
			e.log.WithError(err).Errorf("Failed to create engine %s", e.Name)
			if e.State != types.InstanceStateError {
				e.State = types.InstanceStateError
			}
			e.ErrorMsg = err.Error()

			ret = e.getWithoutLock()
			err = nil
		} else {
			if e.State != types.InstanceStateError {
				e.ErrorMsg = ""
			}
		}
	}()

	initiatorCreationRequired, targetCreationRequired, err := e.checkInitiatorAndTargetCreationRequirements(podIP, initiatorIP, targetIP)
	if err != nil {
		return nil, err
	}
	if !initiatorCreationRequired && !targetCreationRequired {
		return e.getWithoutLock(), nil
	}

	if e.isNewNvmeTcpFrontendEngine() {
		if initiatorCreationRequired {
			e.NvmeTcpFrontend.IP = initiatorIP
		}
		e.NvmeTcpFrontend.TargetIP = targetIP
	}

	if e.NvmeTcpFrontend != nil {
		e.NvmeTcpFrontend.Nqn = helpertypes.GetNQN(e.Name)
		if errUpdateLogger := e.log.UpdateLogger(logrus.Fields{
			"initiatorIP": e.NvmeTcpFrontend.IP,
			"targetIP":    e.NvmeTcpFrontend.TargetIP,
		}); errUpdateLogger != nil {
			e.log.WithError(errUpdateLogger).Warn("Failed to update logger with initiator and target IP during engine creation")
		}
	}

	if targetCreationRequired || types.IsUblkFrontend(e.Frontend) {
		_, err := spdkClient.BdevRaidGet(e.Name, 0)
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

		replicaBdevList := []string{}
		for replicaName, replicaAddr := range replicaAddressMap {
			e.ReplicaStatusMap[replicaName] = &EngineReplicaStatus{
				Address: replicaAddr,
			}

			bdevName, err := connectNVMfBdev(spdkClient, replicaName, replicaAddr, e.ctrlrLossTimeout, e.fastIOFailTimeoutSec)
			if err != nil {
				e.log.WithError(err).Warnf("Failed to get bdev from replica %s with address %s during creation, will mark the mode to ERR and continue", replicaName, replicaAddr)
				e.ReplicaStatusMap[replicaName].Mode = types.ModeERR
			} else {
				// TODO: Check if a replica is really a RW replica rather than a rebuilding failed replica
				e.ReplicaStatusMap[replicaName].Mode = types.ModeRW
				e.ReplicaStatusMap[replicaName].BdevName = bdevName
				replicaBdevList = append(replicaBdevList, bdevName)
			}
		}

		if errUpdateLogger := e.log.UpdateLogger(logrus.Fields{
			"replicaStatusMap": e.ReplicaStatusMap,
		}); errUpdateLogger != nil {
			e.log.WithError(errUpdateLogger).Warn("Failed to update logger with replica status map during engine creation")
		}

		e.checkAndUpdateInfoFromReplicaNoLock()

		e.log.Infof("Connecting all available replicas %+v, then launching raid during engine creation", e.ReplicaStatusMap)
		if _, err := spdkClient.BdevRaidCreate(e.Name, spdktypes.BdevRaidLevel1, 0, replicaBdevList, ""); err != nil {
			return nil, err
		}
	} else {
		e.log.Info("Skipping target creation during engine creation")

		targetSPDKServiceAddress := net.JoinHostPort(e.NvmeTcpFrontend.TargetIP, strconv.Itoa(types.SPDKServicePort))
		targetSPDKClient, err := GetServiceClient(targetSPDKServiceAddress)
		if err != nil {
			return nil, err
		}
		defer func() {
			if errClose := targetSPDKClient.Close(); errClose != nil {
				e.log.WithError(errClose).Errorf("Failed to close target spdk client with address %s during create engine", targetSPDKServiceAddress)
			}
		}()

		e.log.Info("Fetching replica list from target engine")
		targetEngine, err := targetSPDKClient.EngineGet(e.Name)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get engine %v from %v", e.Name, targetAddress)
		}

		for replicaName, replicaAddr := range replicaAddressMap {
			e.ReplicaStatusMap[replicaName] = &EngineReplicaStatus{
				Address: replicaAddr,
			}
			if _, ok := targetEngine.ReplicaAddressMap[replicaName]; !ok {
				e.log.WithError(err).Warnf("Failed to get bdev from replica %s with address %s during creation, will mark the mode to ERR and continue", replicaName, replicaAddr)
				e.ReplicaStatusMap[replicaName].Mode = types.ModeERR
			} else {
				// TODO: Check if a replica is really a RW replica rather than a rebuilding failed replica
				e.ReplicaStatusMap[replicaName].Mode = types.ModeRW
				e.ReplicaStatusMap[replicaName].BdevName = replicaName
			}
		}

		if errUpdateLogger := e.log.UpdateLogger(logrus.Fields{
			"replicaStatusMap": e.ReplicaStatusMap,
		}); errUpdateLogger != nil {
			e.log.WithError(errUpdateLogger).Warn("Failed to update logger with replica status map during engine creation")
		}

		e.log.Infof("Connected all available replicas %+v for engine reconstruction during upgrade", e.ReplicaStatusMap)
	}

	if errUpdateLogger := e.log.UpdateLogger(logrus.Fields{
		"initiatorCreationRequired": initiatorCreationRequired,
		"targetCreationRequired":    targetCreationRequired,
		"initiatorAddress":          initiatorAddress,
		"targetAddress":             targetAddress,
	}); errUpdateLogger != nil {
		e.log.WithError(errUpdateLogger).Warn("Failed to update logger with initiator and target creation requirements during engine creation")
	}

	e.log.Info("Handling frontend during engine creation")

	if err := e.handleFrontend(spdkClient, superiorPortAllocator, portCount, targetAddress, initiatorCreationRequired, targetCreationRequired); err != nil {
		return nil, err
	}

	e.State = types.InstanceStateRunning

	e.log.Info("Created engine")

	return e.getWithoutLock(), nil
}

func (e *Engine) ValidateReplicaSize(replicaAddressMap map[string]string) error {
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

func (e *Engine) isStandbyTargetCreationRequired() bool {
	// e.Port is non-zero which means the initiator instance is already created and connected to a target instance.
	// e.TargetPort is zero which means the target instance is not created on the same pod.
	// Thus, a standby target instance should be created for the target instance switch-over.
	return e.NvmeTcpFrontend != nil && e.NvmeTcpFrontend.Port != 0 && e.NvmeTcpFrontend.TargetPort == 0
}

func (e *Engine) handleFrontend(spdkClient *spdkclient.Client, superiorPortAllocator *commonbitmap.Bitmap, portCount int32, targetAddress string,
	initiatorCreationRequired, targetCreationRequired bool) (err error) {
	if !types.IsFrontendSupported(e.Frontend) {
		return fmt.Errorf("unknown frontend type %s", e.Frontend)
	}

	if e.Frontend == types.FrontendEmpty {
		e.log.Info("No frontend specified, will not expose bdev for engine")
		return nil
	}

	if types.IsUblkFrontend(e.Frontend) {
		return e.handleUblkFrontend(spdkClient)
	}
	return e.handleNvmeTcpFrontend(spdkClient, superiorPortAllocator, portCount, targetAddress, initiatorCreationRequired, targetCreationRequired)
}

func (e *Engine) handleNvmeTcpFrontend(spdkClient *spdkclient.Client, superiorPortAllocator *commonbitmap.Bitmap, portCount int32, targetAddress string,
	initiatorCreationRequired, targetCreationRequired bool) (err error) {
	if e.NvmeTcpFrontend == nil {
		return fmt.Errorf("failed to handleNvmeTcpFrontend: invalid NvmeTcpFrontend: %v", e.NvmeTcpFrontend)
	}
	standbyTargetCreationRequired := e.isStandbyTargetCreationRequired()
	frontendConfigured := e.isNvmeFrontendConfigured(standbyTargetCreationRequired)

	var (
		targetIP string
		port     int32
		i        *initiator.Initiator
	)

	// If the NVMe frontend is already configured, reuse the existing initiator and connection info.
	if frontendConfigured {
		e.log.Infof("Reusing existing initiator. TargetIP: %s, TargetPort: %d, FrontendPort: %d",
			e.NvmeTcpFrontend.TargetIP, e.NvmeTcpFrontend.TargetPort, e.NvmeTcpFrontend.Port)

		targetIP = e.NvmeTcpFrontend.TargetIP
		i = e.initiator
		port = e.NvmeTcpFrontend.Port
	} else {
		targetIP, i, port, err = e.configureNvmeTcpFrontend(initiatorCreationRequired, targetCreationRequired, standbyTargetCreationRequired, superiorPortAllocator, portCount, targetAddress)
		if err != nil {
			return errors.Wrap(err, "failed to configureNvmeTcpFrontend")
		}
	}

	dmDeviceIsBusy := false
	defer func() {
		if err == nil {
			if !standbyTargetCreationRequired && !frontendConfigured {
				e.initiator = i
				e.dmDeviceIsBusy = dmDeviceIsBusy
				e.Endpoint = i.GetEndpoint()

				if errUpdateLogger := e.log.UpdateLogger(logrus.Fields{
					"endpoint":   e.Endpoint,
					"port":       e.NvmeTcpFrontend.Port,
					"targetPort": e.NvmeTcpFrontend.TargetPort,
				}); errUpdateLogger != nil {
					e.log.WithError(errUpdateLogger).Warn("Failed to update logger with endpoint and port during engine creation")
				}
			}

			e.log.Infof("Finished handling frontend for engine: %+v", e)
		}
	}()

	e.log.Info("Blindly stopping expose bdev for engine")
	if err := spdkClient.StopExposeBdev(e.NvmeTcpFrontend.Nqn); err != nil {
		return errors.Wrapf(err, "failed to blindly stop expose bdev for engine %v", e.Name)
	}

	portStr := strconv.Itoa(int(e.NvmeTcpFrontend.Port))
	if err := spdkClient.StartExposeBdev(e.NvmeTcpFrontend.Nqn, e.Name, e.NvmeTcpFrontend.Nguid, targetIP, portStr); err != nil {
		return err
	}

	if e.Frontend == types.FrontendSPDKTCPNvmf {
		e.Endpoint = GetNvmfEndpoint(e.NvmeTcpFrontend.Nqn, targetIP, port)
		return nil
	}

	if !initiatorCreationRequired && targetCreationRequired {
		e.log.Infof("Only creating target instance for engine, no need to start initiator")
		return nil
	}

	dmDeviceIsBusy, err = i.StartNvmeTCPInitiator(targetIP, portStr, true)
	if err != nil {
		return errors.Wrapf(err, "failed to start initiator for engine %v", e.Name)
	}

	return nil
}

func (e *Engine) configureNvmeTcpFrontend(initiatorCreationRequired, targetCreationRequired, standbyTargetCreationRequired bool, superiorPortAllocator *commonbitmap.Bitmap, portCount int32, targetAddress string) (
	targetIP string, i *initiator.Initiator, port int32, err error) {

	var targetPort int32

	targetIP, targetPort, err = splitHostPort(targetAddress)
	if err != nil {
		return targetIP, i, port, err
	}

	e.NvmeTcpFrontend.Nqn = helpertypes.GetNQN(e.Name)
	e.NvmeTcpFrontend.Nguid = commonutils.RandomID(nvmeNguidLength)

	nvmeTCPInfo := &initiator.NVMeTCPInfo{
		SubsystemNQN: e.NvmeTcpFrontend.Nqn,
	}
	i, err = initiator.NewInitiator(e.VolumeName, initiator.HostProc, nvmeTCPInfo, nil)
	if err != nil {
		return targetIP, i, port, errors.Wrapf(err, "failed to create initiator for engine %v", e.Name)
	}

	if initiatorCreationRequired && !targetCreationRequired {
		i.NVMeTCPInfo.TransportAddress = targetIP
		i.NVMeTCPInfo.TransportServiceID = strconv.Itoa(int(targetPort))
		e.log.Infof("Target instance already exists on %v, no need to create target instance", targetAddress)
		e.NvmeTcpFrontend.Port = targetPort

		// TODO:
		// "nvme list -o json" might be empty devices for a while instance manager pod is just started.
		// The root cause is not clear, so we need to retry to load NVMe device info.
		for r := 0; r < maxNumRetries; r++ {
			err = i.LoadNVMeDeviceInfo(i.NVMeTCPInfo.TransportAddress, i.NVMeTCPInfo.TransportServiceID, i.NVMeTCPInfo.SubsystemNQN)
			if err != nil && strings.Contains(err.Error(), "failed to get devices") {
				time.Sleep(retryInterval)
				continue
			}
			if err == nil {
				e.log.Info("Loaded NVMe device info for engine")
				break
			}
			return targetIP, i, port, errors.Wrapf(err, "failed to load NVMe device info for engine %v", e.Name)
		}

		err = i.LoadEndpointForNvmeTcpFrontend(false)
		if err != nil {
			return targetIP, i, port, errors.Wrapf(err, "failed to load endpoint for engine %v", e.Name)
		}

		return targetIP, i, port, nil
	}

	port, _, err = superiorPortAllocator.AllocateRange(portCount)
	if err != nil {
		return targetIP, i, port, errors.Wrapf(err, "failed to allocate port for engine %v", e.Name)
	}
	e.log.Infof("Allocated port %v for engine", port)

	if initiatorCreationRequired {
		e.NvmeTcpFrontend.Port = port
	}
	if targetCreationRequired {
		if standbyTargetCreationRequired {
			e.NvmeTcpFrontend.StandbyTargetPort = port
		} else {
			e.NvmeTcpFrontend.TargetPort = port
		}
	}

	return targetIP, i, port, nil
}

func (e *Engine) isNvmeFrontendConfigured(standbyTargetCreationRequired bool) bool {
	if e.NvmeTcpFrontend == nil || e.initiator == nil {
		return false
	}

	if standbyTargetCreationRequired && e.NvmeTcpFrontend.StandbyTargetPort == 0 {
		return false
	}

	if len(e.NvmeTcpFrontend.IP) == 0 || e.NvmeTcpFrontend.Port == 0 ||
		len(e.NvmeTcpFrontend.TargetIP) == 0 || e.NvmeTcpFrontend.TargetPort == 0 ||
		len(e.NvmeTcpFrontend.Nqn) == 0 || len(e.NvmeTcpFrontend.Nguid) == 0 {
		return false
	}

	return true
}
func (e *Engine) handleUblkFrontend(spdkClient *spdkclient.Client) (err error) {
	if e.UblkFrontend == nil {
		return fmt.Errorf("failed to handleUblkFrontend: invalid NvmeTcpFrontend: %v", e.NvmeTcpFrontend)
	}
	dmDeviceIsBusy := false
	ublkInfo := &initiator.UblkInfo{
		BdevName:          e.Name,
		UblkQueueDepth:    e.UblkFrontend.UblkQueueDepth,
		UblkNumberOfQueue: e.UblkFrontend.UblkNumberOfQueue,

		UblkID: initiator.UnInitializedUblkId,
	}
	i, err := initiator.NewInitiator(e.VolumeName, initiator.HostProc, nil, ublkInfo)
	if err != nil {
		return errors.Wrapf(err, "failed to create initiator for engine %v", e.Name)
	}

	defer func() {
		if err == nil {
			e.initiator = i
			e.dmDeviceIsBusy = dmDeviceIsBusy
			e.Endpoint = i.GetEndpoint()

			if errUpdateLogger := e.log.UpdateLogger(logrus.Fields{
				"endpoint": e.Endpoint,
				"ublkID":   e.UblkFrontend.UblkID,
			}); errUpdateLogger != nil {
				e.log.WithError(errUpdateLogger).Warn("Failed to update logger with endpoint and port during engine creation")
			}
			e.log.Infof("Finished handling frontend for engine: %+v", e)
		}
	}()

	dmDeviceIsBusy, err = i.StartUblkInitiator(spdkClient, true)
	if err != nil {
		return errors.Wrapf(err, "failed to start initiator for engine %v", e.Name)
	}

	e.UblkFrontend.UblkID = i.UblkInfo.UblkID

	return nil
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

	if e.NvmeTcpFrontend != nil && e.NvmeTcpFrontend.Nqn == "" {
		e.NvmeTcpFrontend.Nqn = helpertypes.GetNQN(e.Name)
	}

	// Stop the frontend
	if e.initiator != nil {
		if _, err := e.initiator.Stop(spdkClient, true, true, true); err != nil {
			return err
		}
		e.initiator = nil
		e.Endpoint = ""

		requireUpdate = true
	}

	if e.NvmeTcpFrontend != nil {
		if err := spdkClient.StopExposeBdev(e.NvmeTcpFrontend.Nqn); err != nil {
			e.log.Error(err)
			return err
		}
	}

	// Release the ports if they are allocated
	err = e.releasePorts(superiorPortAllocator)
	if err != nil {
		return err
	}
	requireUpdate = true

	// Delete the Raid bdev and disconnect the replicas
	if _, err := spdkClient.BdevRaidDelete(e.Name); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return err
	}

	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		if err := disconnectNVMfBdev(spdkClient, replicaStatus.BdevName); err != nil {
			if replicaStatus.Mode != types.ModeERR {
				e.log.WithError(err).Errorf("Engine failed to disconnect replica %s with bdev %s during deletion, will update the mode from %v to ERR", replicaName, replicaStatus.BdevName, replicaStatus.Mode)
				replicaStatus.Mode = types.ModeERR
				requireUpdate = true
			}
			return err
		}
		delete(e.ReplicaStatusMap, replicaName)
		requireUpdate = true
	}

	e.log.Info("Deleted engine")

	return nil
}

func (e *Engine) releasePorts(superiorPortAllocator *commonbitmap.Bitmap) (err error) {
	if e.NvmeTcpFrontend == nil {
		return nil
	}
	ports := map[int32]struct{}{
		e.NvmeTcpFrontend.Port:       {},
		e.NvmeTcpFrontend.TargetPort: {},
	}

	if errRelease := releasePortIfExists(superiorPortAllocator, ports, e.NvmeTcpFrontend.Port); errRelease != nil {
		err = multierr.Append(err, errRelease)
	}
	e.NvmeTcpFrontend.Port = 0

	if errRelease := releasePortIfExists(superiorPortAllocator, ports, e.NvmeTcpFrontend.TargetPort); errRelease != nil {
		err = multierr.Append(err, errRelease)
	}
	e.NvmeTcpFrontend.TargetPort = 0

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
		Name:              e.Name,
		SpecSize:          e.SpecSize,
		ActualSize:        e.ActualSize,
		ReplicaAddressMap: map[string]string{},
		ReplicaModeMap:    map[string]spdkrpc.ReplicaMode{},
		Snapshots:         map[string]*spdkrpc.Lvol{},
		Frontend:          e.Frontend,
		Endpoint:          e.Endpoint,
		State:             string(e.State),
		ErrorMsg:          e.ErrorMsg,
	}

	if e.NvmeTcpFrontend != nil {
		res.Ip = e.NvmeTcpFrontend.IP
		res.Port = e.NvmeTcpFrontend.Port
		res.TargetIp = e.NvmeTcpFrontend.TargetIP
		res.TargetPort = e.NvmeTcpFrontend.TargetPort
		res.StandbyTargetPort = e.NvmeTcpFrontend.StandbyTargetPort
	}

	if e.UblkFrontend != nil {
		res.UblkId = e.UblkFrontend.UblkID
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

func (e *Engine) ValidateAndUpdate(spdkClient *spdkclient.Client) (err error) {
	updateRequired := false

	e.Lock()
	defer func() {
		e.Unlock()

		if updateRequired {
			e.UpdateCh <- nil
		}
	}()

	if e.IsRestoring {
		e.log.Debug("Engine is restoring, will skip the validation and update")
		return nil
	}

	if e.isExpanding {
		e.log.Debug("Engine is expanding, will skip the validation and update")
		return nil
	}

	// Syncing with the SPDK TGT server only when the engine is running.
	if e.State != types.InstanceStateRunning {
		return nil
	}

	subsystemMap, err := GetNvmfSubsystemMap(spdkClient)
	if err != nil {
		return err
	}
	bdevMap, err := GetBdevMap(spdkClient)
	if err != nil {
		return err
	}

	defer func() {
		// TODO: we may not need to mark the engine as ERR for each error
		if err != nil {
			if e.State != types.InstanceStateError {
				e.State = types.InstanceStateError
				e.log.WithError(err).Error("Found error during engine validation and update")
				updateRequired = true
			}
			e.ErrorMsg = err.Error()
		} else {
			if e.State != types.InstanceStateError {
				e.ErrorMsg = ""
			}
		}
	}()

	if e.NvmeTcpFrontend != nil && e.NvmeTcpFrontend.IP != e.NvmeTcpFrontend.TargetIP {
		return nil
	}

	if err := e.validateAndUpdateFrontend(spdkClient, subsystemMap); err != nil {
		return err
	}

	bdevRaid := bdevMap[e.Name]
	if spdktypes.GetBdevType(bdevRaid) != spdktypes.BdevTypeRaid {
		return fmt.Errorf("cannot find a raid bdev for engine %v", e.Name)
	}

	bdevRaidSize := bdevRaid.NumBlocks * uint64(bdevRaid.BlockSize)
	if e.SpecSize > bdevRaidSize {
		// not directly return error

		// If the volume is not attached and do the expand
		// At first, we create and attach the engine with new size, but not yet to expand
		// it will cause infinite loop for size mismatching
		// loop to destroy and create engine
		// and there is no chance to execute EngineExpand()

		// wait the lh-manager to reconcile engine CR and call EngineExpand()

		e.SpecSize = bdevRaidSize
		e.log.Warnf("found mismatching between engine spec size %d and actual raid bdev size %d for engine %s", e.SpecSize, bdevRaidSize, e.Name)
		return nil
	}

	if e.SpecSize < bdevRaidSize {
		// should not happen
		return fmt.Errorf("engine spec size %d is smaller than actual raid bdev size %d for engine %s", e.SpecSize, bdevRaidSize, e.Name)
	}

	// Verify replica status map
	containValidReplica := false
	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		if replicaStatus.Address == "" || replicaStatus.BdevName == "" {
			if replicaStatus.Mode != types.ModeERR {
				e.log.Errorf("Engine marked replica %s mode from %v to ERR since its address %s or bdev name %s is empty during ValidateAndUpdate", replicaName, replicaStatus.Mode, replicaStatus.Address, replicaStatus.BdevName)
				replicaStatus.Mode = types.ModeERR
				updateRequired = true
			}
		}
		if replicaStatus.Mode != types.ModeRW && replicaStatus.Mode != types.ModeWO && replicaStatus.Mode != types.ModeERR {
			e.log.Errorf("Engine found replica %s invalid mode %v during ValidateAndUpdate", replicaName, replicaStatus.Mode)
			replicaStatus.Mode = types.ModeERR
			updateRequired = true
		}
		if replicaStatus.Mode != types.ModeERR {
			mode, err := e.validateAndUpdateReplicaNvme(replicaName, bdevMap[replicaStatus.BdevName])
			if err != nil {
				e.log.WithError(err).Errorf("Engine found valid NVMe for replica %v, will update the mode from %s to ERR during ValidateAndUpdate", replicaName, replicaStatus.Mode)
				replicaStatus.Mode = types.ModeERR
				updateRequired = true
			} else if replicaStatus.Mode != mode {
				replicaStatus.Mode = mode
				updateRequired = true
			}
		}
		if replicaStatus.Mode == types.ModeRW {
			containValidReplica = true
		}
	}

	if errUpdateLogger := e.log.UpdateLogger(logrus.Fields{
		"replicaStatusMap": e.ReplicaStatusMap,
	}); errUpdateLogger != nil {
		e.log.WithError(errUpdateLogger).Warn("Failed to update logger with replica status map during engine creation")
	}

	if !containValidReplica {
		e.State = types.InstanceStateError
		e.log.Error("Engine had no RW replica found at the end of ValidateAndUpdate, will be marked as error")
		updateRequired = true
		// TODO: should we delete the engine automatically here?
	}

	e.checkAndUpdateInfoFromReplicaNoLock()

	return nil
}

func (e *Engine) checkAndUpdateInfoFromReplicaNoLock() {
	replicaMap := map[string]*api.Replica{}
	replicaAncestorMap := map[string]*api.Lvol{}
	hasBackingImage := false
	hasSnapshot := false

	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		if replicaStatus.Mode != types.ModeRW && replicaStatus.Mode != types.ModeWO {
			if replicaStatus.Mode != types.ModeERR {
				e.log.Warnf("Engine found unexpected mode for replica %s with address %s during info update from replica, mark the mode from %v to ERR and continue info update for other replicas", replicaName, replicaStatus.Address, replicaStatus.Mode)
				replicaStatus.Mode = types.ModeERR
			}
			continue
		}

		// Ensure the replica is not rebuilding
		func() {
			replicaServiceCli, err := GetServiceClient(replicaStatus.Address)
			if err != nil {
				e.log.WithError(err).Errorf("Engine failed to get service client for replica %s with address %s, will skip this replica and continue info update for other replicas", replicaName, replicaStatus.Address)
				return
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
				return
			}

			if replicaStatus.Mode == types.ModeWO {
				shallowCopyStatus, err := replicaServiceCli.ReplicaRebuildingDstShallowCopyCheck(replicaName)
				if err != nil {
					e.log.WithError(err).Warnf("Engine failed to get rebuilding replica %s shallow copy info, will skip this replica and continue info update for other replicas", replicaName)
					return
				}
				if shallowCopyStatus.TotalState == helpertypes.ShallowCopyStateError || shallowCopyStatus.Error != "" {
					e.log.Errorf("Engine found rebuilding replica %s error %v during info update from replica, will mark the mode from WO to ERR and continue info update for other replicas", replicaName, shallowCopyStatus.Error)
					replicaStatus.Mode = types.ModeERR
				}
				// No need to do anything if `shallowCopyStatus.TotalState == helpertypes.ShallowCopyStateComplete`, engine should leave the rebuilding logic to update its mode
				return
			}

			// The ancestor check sequence: the backing image, then the oldest snapshot, finally head
			if replica.BackingImageName != "" {
				hasBackingImage = true
				backingImage, err := replicaServiceCli.BackingImageGet(replica.BackingImageName, replica.LvsUUID)
				if err != nil {
					e.log.WithError(err).Warnf("Failed to get backing image %s with disk UUID %s from replica %s head parent %s, will mark the mode from %v to ERR and continue info update for other replicas", replica.BackingImageName, replica.LvsUUID, replicaName, replica.Head.Parent, replicaStatus.Mode)
					replicaStatus.Mode = types.ModeERR
					return
				}
				replicaAncestorMap[replicaName] = backingImage.Snapshot
				if len(replica.Snapshots) > 0 {
					hasSnapshot = true
				}
			} else {
				if len(replica.Snapshots) > 0 {
					if hasBackingImage {
						e.log.Warnf("Engine found replica %s does not have a backing image while other replicas have during info update for other replicas", replicaName)
					} else {
						hasSnapshot = true
						for snapshotName, snapApiLvol := range replica.Snapshots {
							if snapApiLvol.Parent == "" {
								replicaAncestorMap[replicaName] = replica.Snapshots[snapshotName]
								break
							}
						}
					}
				} else {
					if hasSnapshot {
						e.log.Warnf("Engine found replica %s does not have a snapshot while other replicas have during info update for other replicas", replicaName)
					} else {
						replicaAncestorMap[replicaName] = replica.Head
					}
				}
			}
			if replicaAncestorMap[replicaName] == nil {
				e.log.Warnf("Engine cannot find replica %s ancestor, will skip this replica and continue info update for other replicas", replicaName)
				return
			}
			replicaMap[replicaName] = replica
		}()
	}

	// If there are multiple candidates, the priority is:
	//  1. the earliest backing image if one replica contains a backing image
	//  2. the earliest snapshot if one replica contains a snapshot
	//  3. the earliest volume head
	candidateReplicaName := ""
	earliestCreationTime := time.Now()
	for replicaName, ancestorApiLvol := range replicaAncestorMap {
		if hasBackingImage {
			if ancestorApiLvol.Name == types.VolumeHead || IsReplicaSnapshotLvol(replicaName, ancestorApiLvol.Name) {
				continue
			}
		} else {
			if hasSnapshot {
				if ancestorApiLvol.Name == types.VolumeHead {
					continue
				}
			} else {
				if ancestorApiLvol.Name != types.VolumeHead {
					continue
				}
			}
		}

		creationTime, err := time.Parse(time.RFC3339, ancestorApiLvol.CreationTime)
		if err != nil {
			e.log.WithError(err).Warnf("Failed to parse replica %s ancestor creation time, will skip this replica and continue info update for other replicas: %+v", replicaName, ancestorApiLvol)
			continue
		}
		if earliestCreationTime.After(creationTime) {
			earliestCreationTime = creationTime
			e.SnapshotMap = replicaMap[replicaName].Snapshots
			e.Head = replicaMap[replicaName].Head
			e.ActualSize = replicaMap[replicaName].ActualSize
			if candidateReplicaName != replicaName {
				if candidateReplicaName != "" {
					candidateReplicaAncestorName := replicaAncestorMap[candidateReplicaName].Name
					currentReplicaAncestorName := ancestorApiLvol.Name
					// The ancestor can be backing image, so we need to extract the backing image name from the lvol name
					// Notice that, the disks are not the same for all the replicas, so their backing image lvol names are not the same.
					if types.IsBackingImageSnapLvolName(candidateReplicaAncestorName) {
						candidateReplicaAncestorName, _, err = ExtractBackingImageAndDiskUUID(candidateReplicaAncestorName)
						if err != nil {
							e.log.WithError(err).Warnf("BUG: ancestor name %v is from backingImage.Snapshot lvol name, it should be a valid backing image lvol name", candidateReplicaAncestorName)
						}
					}
					if types.IsBackingImageSnapLvolName(currentReplicaAncestorName) {
						currentReplicaAncestorName, _, err = ExtractBackingImageAndDiskUUID(currentReplicaAncestorName)
						if err != nil {
							e.log.WithError(err).Warnf("BUG: ancestor name %v is from backingImage.Snapshot lvol name, it should be a valid backing image lvol name", currentReplicaAncestorName)
						}
					}

					if candidateReplicaName != "" && candidateReplicaAncestorName != currentReplicaAncestorName {
						e.log.Warnf("Comparing with replica %s ancestor %s, replica %s has a different and earlier ancestor %s, will update info from this replica", candidateReplicaName, replicaAncestorMap[candidateReplicaName].Name, replicaName, ancestorApiLvol.Name)
					}
				}
				candidateReplicaName = replicaName
			}
		}
	}
}

func (e *Engine) validateAndUpdateFrontend(client *spdkclient.Client, subsystemMap map[string]*spdktypes.NvmfSubsystem) (err error) {
	if !types.IsFrontendSupported(e.Frontend) {
		return fmt.Errorf("unknown frontend type %s", e.Frontend)
	}
	if e.Frontend == types.FrontendEmpty && e.Endpoint != "" {
		return fmt.Errorf("found non-empty endpoint %s for engine %s with empty frontend", e.Endpoint, e.Name)
	}
	if e.NvmeTcpFrontend != nil {
		return e.validateAndUpdateNvmeTcpFrontend(subsystemMap)
	} else if e.UblkFrontend != nil {
		return e.validateAndUpdateUblkFrontend(client)
	}
	return fmt.Errorf("both e.NvmeTcpFrontend and e.UblkFrontend are nil")
}

func (e *Engine) validateAndUpdateUblkFrontend(client *spdkclient.Client) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to validateAndUpdateUblkFrontend for engine %v", e.Name)
	}()
	if e.UblkFrontend == nil {
		return fmt.Errorf("UblkFrontend is nil")
	}

	ublkDeviceList, err := client.UblkGetDisks(e.UblkFrontend.UblkID)
	if err != nil {
		return err
	}
	for _, ublkDevice := range ublkDeviceList {
		if ublkDevice.BdevName == e.Name && ublkDevice.ID != e.UblkFrontend.UblkID {
			return fmt.Errorf("found mismatching between e.UblkFrontend.UblkID %v and actual ublk device id %v ", e.UblkFrontend.UblkID, ublkDevice.ID)
		}
	}
	return nil
}

func (e *Engine) validateAndUpdateNvmeTcpFrontend(subsystemMap map[string]*spdktypes.NvmfSubsystem) (err error) {
	if e.NvmeTcpFrontend == nil {
		return fmt.Errorf("failed to validateAndUpdateNvmeTcpFrontend for engine %v NvmeTcpFrontend is nil", e.Name)
	}
	if e.NvmeTcpFrontend.Nqn == "" {
		return fmt.Errorf("NQN is empty for engine %s", e.Name)
	}

	subsystem := subsystemMap[e.NvmeTcpFrontend.Nqn]

	if e.Frontend == types.FrontendEmpty {
		if subsystem != nil {
			return fmt.Errorf("found NVMf subsystem %s for engine %s with empty frontend", e.NvmeTcpFrontend.Nqn, e.Name)
		}
		if e.NvmeTcpFrontend.Port != 0 {
			return fmt.Errorf("found non-zero port %v for engine %s with empty frontend", e.NvmeTcpFrontend.Port, e.Name)
		}
		return nil
	}

	if subsystem == nil {
		return fmt.Errorf("cannot find the NVMf subsystem for engine %s", e.Name)
	}

	if len(subsystem.ListenAddresses) == 0 {
		return fmt.Errorf("cannot find any listener for NVMf subsystem %s for engine %s", e.NvmeTcpFrontend.Nqn, e.Name)
	}

	port := 0
	for _, listenAddr := range subsystem.ListenAddresses {
		if !strings.EqualFold(string(listenAddr.Adrfam), string(spdktypes.NvmeAddressFamilyIPv4)) ||
			!strings.EqualFold(string(listenAddr.Trtype), string(spdktypes.NvmeTransportTypeTCP)) {
			continue
		}
		if port, err = strconv.Atoi(listenAddr.Trsvcid); err != nil {
			return err
		}
		if e.NvmeTcpFrontend.Port == int32(port) {
			break
		}
	}
	if port == 0 || e.NvmeTcpFrontend.Port != int32(port) {
		return fmt.Errorf("cannot find a matching listener with port %d from NVMf subsystem for engine %s", e.NvmeTcpFrontend.Port, e.Name)
	}

	switch e.Frontend {
	case types.FrontendSPDKTCPBlockdev:
		if e.initiator == nil {
			nvmeTCPInfo := &initiator.NVMeTCPInfo{
				SubsystemNQN: e.NvmeTcpFrontend.Nqn,
			}
			i, err := initiator.NewInitiator(e.VolumeName, initiator.HostProc, nvmeTCPInfo, nil)
			if err != nil {
				return errors.Wrapf(err, "failed to create initiator for engine %v during frontend validation and update", e.Name)
			}
			e.initiator = i
		}
		if e.initiator.NVMeTCPInfo == nil {
			return fmt.Errorf("invalid initiator with nil NvmeTcpInfo")
		}
		if err := e.initiator.LoadNVMeDeviceInfo(e.initiator.NVMeTCPInfo.TransportAddress, e.initiator.NVMeTCPInfo.TransportServiceID, e.initiator.NVMeTCPInfo.SubsystemNQN); err != nil {
			if strings.Contains(err.Error(), "connecting state") ||
				strings.Contains(err.Error(), "resetting state") {
				e.log.WithError(err).Warn("Ignored to validate and update engine, because the device is still in a transient state")
				return nil
			}
			return err
		}
		if err := e.initiator.LoadEndpointForNvmeTcpFrontend(e.dmDeviceIsBusy); err != nil {
			return err
		}
		blockDevEndpoint := e.initiator.GetEndpoint()
		if e.Endpoint == "" {
			e.Endpoint = blockDevEndpoint
		}
		if e.Endpoint != blockDevEndpoint {
			return fmt.Errorf("found mismatching between engine endpoint %s and actual block device endpoint %s for engine %s", e.Endpoint, blockDevEndpoint, e.Name)
		}
	case types.FrontendSPDKTCPNvmf:
		nvmfEndpoint := GetNvmfEndpoint(e.NvmeTcpFrontend.Nqn, e.NvmeTcpFrontend.IP, e.NvmeTcpFrontend.Port)
		if e.Endpoint == "" {
			e.Endpoint = nvmfEndpoint
		}
		if e.Endpoint != "" && e.Endpoint != nvmfEndpoint {
			return fmt.Errorf("found mismatching between engine endpoint %s and actual nvmf endpoint %s for engine %s", e.Endpoint, nvmfEndpoint, e.Name)
		}
	default:
		return fmt.Errorf("unknown frontend type %s", e.Frontend)
	}

	return nil
}

func (e *Engine) validateAndUpdateReplicaNvme(replicaName string, bdev *spdktypes.BdevInfo) (mode types.Mode, err error) {
	if bdev == nil {
		return types.ModeERR, fmt.Errorf("cannot find a bdev for replica %s", replicaName)
	}

	bdevSpecSize := bdev.NumBlocks * uint64(bdev.BlockSize)
	if e.SpecSize != bdevSpecSize {
		return types.ModeERR, fmt.Errorf("found mismatching between replica bdev %s spec size %d and the engine %s spec size %d during replica %s mode validation", bdev.Name, bdevSpecSize, e.Name, e.SpecSize, replicaName)
	}

	if spdktypes.GetBdevType(bdev) != spdktypes.BdevTypeNvme {
		return types.ModeERR, fmt.Errorf("found bdev type %v rather than %v during replica %s mode validation", spdktypes.GetBdevType(bdev), spdktypes.BdevTypeNvme, replicaName)
	}
	if len(*bdev.DriverSpecific.Nvme) != 1 {
		return types.ModeERR, fmt.Errorf("found zero or multiple NVMe info in a NVMe base bdev %v during replica %s mode validation", bdev.Name, replicaName)
	}
	nvmeInfo := (*bdev.DriverSpecific.Nvme)[0]
	if !strings.EqualFold(string(nvmeInfo.Trid.Adrfam), string(spdktypes.NvmeAddressFamilyIPv4)) ||
		!strings.EqualFold(string(nvmeInfo.Trid.Trtype), string(spdktypes.NvmeTransportTypeTCP)) {
		return types.ModeERR, fmt.Errorf("found invalid address family %s and transport type %s in a remote NVMe base bdev %s during replica %s mode validation", nvmeInfo.Trid.Adrfam, nvmeInfo.Trid.Trtype, bdev.Name, replicaName)
	}
	bdevAddr := net.JoinHostPort(nvmeInfo.Trid.Traddr, nvmeInfo.Trid.Trsvcid)
	if e.ReplicaStatusMap[replicaName].Address != bdevAddr {
		return types.ModeERR, fmt.Errorf("found mismatching between replica bdev %s address %s and the NVMe bdev actual address %s during replica %s mode validation", bdev.Name, e.ReplicaStatusMap[replicaName].Address, bdevAddr, replicaName)
	}
	controllerName := helperutil.GetNvmeControllerNameFromNamespaceName(e.ReplicaStatusMap[replicaName].BdevName)
	if controllerName != replicaName {
		return types.ModeERR, fmt.Errorf("found unexpected the NVMe bdev controller name %s (bdev name %s) during replica %s mode validation", controllerName, bdev.Name, replicaName)
	}

	return e.ReplicaStatusMap[replicaName].Mode, nil
}

// Expand performs an online volume expansion for the Longhorn Engine using SPDK.
// It expands the underlying replica logical volumes (lvol), recreates the SPDK RAID bdev,
// suspends and resumes frontend I/O as needed, and ensures cleanup and status updates on failure.
func (e *Engine) Expand(spdkClient *spdkclient.Client, size uint64, superiorPortAllocator *commonbitmap.Bitmap) (retErr error) {
	e.Lock()
	defer e.Unlock()

	e.log.Info("Expanding engine")

	// fetch all replica clients
	replicaClients, err := e.getReplicaClients()
	if err != nil {
		return err
	}
	defer e.closeReplicaClients(replicaClients)

	// requireExpansion checks if the expansion is needed and validates the request.
	requireExpansion, err := e.requireExpansion(size, replicaClients)
	if err != nil {
		return errors.Wrap(err, "requireExpansion failed")
	}

	// engineErr will be set when the engine failed to do any non-recoverable operations.
	var (
		expanded = false
	)
	defer func() {
		e.finishExpansion(expanded, size, retErr)
	}()

	if !requireExpansion {
		e.log.Info("No need to expand engine")
		expanded = true
		return nil
	}

	// prepareRaidForExpansion checks if RAID exists, suspends frontend if needed, and deletes the RAID bdev.
	isSuspended, bdevRaidUUID, err := e.prepareRaidForExpansion(spdkClient)
	if isSuspended {
		defer func() {
			if frontendErr := e.initiator.Resume(); frontendErr != nil {
				retErr = multierr.Append(retErr, errors.Wrapf(frontendErr, "original error; resume failed"))
			}
		}()
	}
	if err != nil {
		return errors.Wrap(err, "prepare raid for expansion failed")
	}

	// expand replicas
	if err := e.expandReplicas(replicaClients, spdkClient, size); err != nil {
		return errors.Wrap(err, "failed to expand replicas")
	}

	// recreate RAID and reconnect frontend
	if err := e.reconnectFrontend(spdkClient, bdevRaidUUID, superiorPortAllocator); err != nil {
		return errors.Wrap(err, "failed to reconnect frontend")
	}
	e.log.Info("Expanding engine completed")

	expanded = true // which could be true even in partial success
	return nil
}

func (e *Engine) requireExpansion(size uint64, replicaClients map[string]*client.SPDKClient) (requireExpansion bool, err error) {
	if e.isExpanding {
		return false, fmt.Errorf("expansion is in progress")
	}

	if e.IsRestoring {
		return false, fmt.Errorf("restoring is in progress")
	}

	if e.SpecSize > size {
		return false, fmt.Errorf("cannot expand engine to a smaller size %v, current size %v", size, e.SpecSize)
	}
	if e.SpecSize == size {
		e.log.Infof("Engine already at requested size %v, skipping expansion", size)
		return false, nil // no need to expand
	}

	roundedNewSize := util.RoundUp(size, helpertypes.MiB)
	if roundedNewSize != size {
		return false, fmt.Errorf("rounded up spec size from %v to %v since the spec size should be multiple of MiB", size, roundedNewSize)
	}

	// Ensure all replicas are in RW mode and have the same size
	if len(e.ReplicaStatusMap) == 0 {
		e.log.Warn("Cannot expand engine: no replica found")
		return false, fmt.Errorf("cannot expand engine with no replica")
	}

	currentReplicaSize := uint64(0)
	for replicaName, replicaStatus := range e.ReplicaStatusMap {
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
			return false, fmt.Errorf("cannot expand engine with replicas in different sizes: replica %s has size %v while other replicas have size %v", replicaName, replica.SpecSize, currentReplicaSize)
		}
	}

	if currentReplicaSize > size {
		return false, fmt.Errorf("cannot expand engine to a smaller size %v, current replica size %v", size, currentReplicaSize)
	}
	if currentReplicaSize == size {
		e.log.Infof("Replicas already at requested size %v, skipping expansion", size)
		return false, nil // no need to expand
	}

	// Mark expansion as in progress
	e.isExpanding = true
	e.lastExpansionFailedAt = ""
	e.lastExpansionError = ""

	return true, nil
}

func (e *Engine) finishExpansion(expanded bool, size uint64, err error) {
	if err != nil {
		e.State = types.InstanceStateError
		e.ErrorMsg = err.Error()
		e.lastExpansionError = errors.Wrap(err, "engine failed to expand expansion").Error()
		e.lastExpansionFailedAt = time.Now().UTC().Format(time.RFC3339Nano)

		if errUpdateLogger := e.log.UpdateLogger(logrus.Fields{
			"replicaStatusMap": e.ReplicaStatusMap,
		}); errUpdateLogger != nil {
			e.log.WithError(errUpdateLogger).Warn("Failed to update logger with replica status map during engine creation")
		}

		e.log.WithError(err).Errorf("Engine %s failed to expand", e.Name)
	}

	if expanded {
		if e.lastExpansionError != "" {
			e.log.Infof("Succeeded to expand from size %v to %v but there are some replica expansion failures: %v", e.SpecSize, size, e.lastExpansionError)
		} else {
			e.log.Infof("Succeeded to expand from size %v to %v", e.SpecSize, size)
		}
		e.SpecSize = size
	} else {
		e.log.Infof("Failed to expand from size %v to %v", e.SpecSize, size)
	}

	e.isExpanding = false
}

func (e *Engine) prepareRaidForExpansion(spdkClient *spdkclient.Client) (suspendFrontend bool, bdevUUID string, err error) {
	// check if bdev raid exist
	bdevRaid, err := spdkClient.BdevRaidGet(e.Name, 0)
	if err != nil {
		if jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return suspendFrontend, "", nil // RAID does not exist, nothing to do
		}
		return suspendFrontend, "", errors.Wrap(err, "failed to get bdev raid")
	}
	if len(bdevRaid) == 0 {
		return suspendFrontend, "", nil // RAID already deleted
	}
	bdevUUID = bdevRaid[0].UUID

	// Suspend IO if frontend is active
	if e.Frontend == types.FrontendSPDKTCPBlockdev && e.Endpoint != "" {
		if err := e.initiator.Suspend(false, false); err != nil {
			return suspendFrontend, "", errors.Wrapf(err, "failed to suspend initiator for engine %s", e.Name)
		}
		suspendFrontend = true
	} else if types.IsUblkFrontend(e.Frontend) {
		return suspendFrontend, "", fmt.Errorf("not support ublk frontend for expansion for engine %s", e.Name)
	}

	if e.Frontend != types.FrontendEmpty {
		currentTargetAddress := net.JoinHostPort(e.initiator.NVMeTCPInfo.TransportAddress, e.initiator.NVMeTCPInfo.TransportServiceID)
		if err := e.disconnectTarget(currentTargetAddress); err != nil {
			return suspendFrontend, "", errors.Wrapf(err, "failed to disconnect target %s for engine %s", currentTargetAddress, e.Name)
		}

		if err := spdkClient.StopExposeBdev(e.NvmeTcpFrontend.Nqn); err != nil {
			return suspendFrontend, bdevUUID, errors.Wrapf(err, "failed to stop exposing bdev for engine %s", e.Name)
		}
	}

	// delete raid bdev if still exist
	deleted, err := spdkClient.BdevRaidDelete(e.Name)
	if err != nil {
		if jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			e.log.WithField("engineName", e.Name).Info("RAID bdev already deleted")
		} else {
			return suspendFrontend, bdevUUID, err
		}
	} else if !deleted {
		return suspendFrontend, bdevUUID, fmt.Errorf("engine %s raid delete failed", e.Name)
	}

	return suspendFrontend, bdevUUID, nil
}

func (e *Engine) expandReplicas(replicaClients map[string]*client.SPDKClient, spdkClient *spdkclient.Client, size uint64) (err error) {
	e.log.Info("Expand replicas")

	var (
		errorLock sync.Mutex
		wg        sync.WaitGroup
	)

	failedReplica := map[string]error{}

	for replicaName, replicaClient := range replicaClients {
		wg.Add(1)

		go func(replicaName string, replicaClient *client.SPDKClient) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					e.log.WithField("replica", replicaName).Errorf("Panic during replica expansion: %v", r)
				}
			}()

			replicaStatus, ok := e.ReplicaStatusMap[replicaName]
			if !ok {
				e.log.WithField("replica", replicaName).Warn("Replica not found in status map")
				return
			}

			replica, err := replicaClient.ReplicaGet(replicaName)
			if err != nil {
				errorLock.Lock()
				failedReplica[replicaName] = errors.Wrap(err, "Get replica failure")
				errorLock.Unlock()
				return
			}

			if replica.SpecSize == size {
				return
			}

			if err := disconnectNVMfBdev(spdkClient, replicaStatus.BdevName); err != nil {
				errorLock.Lock()
				failedReplica[replicaName] = err
				errorLock.Unlock()
				return
			}

			if err := replicaClient.ReplicaExpand(replicaName, size); err != nil {
				errorLock.Lock()
				failedReplica[replicaName] = err
				errorLock.Unlock()
				return
			}

			_, err = connectNVMfBdev(spdkClient, replicaName, replicaStatus.Address, e.ctrlrLossTimeout, e.fastIOFailTimeoutSec)
			if err != nil {
				errorLock.Lock()
				failedReplica[replicaName] = err
				errorLock.Unlock()
			}

		}(replicaName, replicaClient)
	}

	wg.Wait()

	var aggregatedErr map[string]string
	if len(failedReplica) > 0 {
		aggregatedErr = map[string]string{}
		for replicaName, err := range failedReplica {
			aggregatedErr[replicaName] = err.Error()
		}
	}

	switch {
	case len(failedReplica) == 0:
		// all success
		e.log.Info("All replicas expand success")
		return nil
	case len(failedReplica) == len(replicaClients):
		// all fail, no based lvols are resized
		e.log.WithFields(logrus.Fields{"failedReplicas": aggregatedErr}).
			Error("All replicas failed to expand")

		return fmt.Errorf("all replicas failed to expand; aborting RAID recreation")
	default:
		// partial success
		// the failedReplica is seems as outOfSync
		// set it to ERR
		for replicaName := range failedReplica {
			e.ReplicaStatusMap[replicaName].Mode = types.ModeERR
		}

		e.log.WithFields(logrus.Fields{"failedReplicas": aggregatedErr}).
			Warn("Some replicas failed to expand and have been marked as ERR")

		return nil
	}
}

func (e *Engine) reconnectFrontend(spdkClient *spdkclient.Client, bdevRaidUUID string, superiorPortAllocator *commonbitmap.Bitmap) (err error) {
	e.log.WithFields(logrus.Fields{
		"engineName": e.Name,
		"volumeName": e.VolumeName,
		"frontend":   e.Frontend,
	}).Info("reconnectFrontend")

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

	if e.Frontend == types.FrontendEmpty {
		e.log.Info("No need to reconnect frontend for empty frontend")
		return nil
	}

	if types.IsUblkFrontend(e.Frontend) {
		// TODO: support ublk frontend for expansion
		return errors.New("ublk frontend is not supported for expansion")
	}

	// reconnect
	if (e.Frontend == types.FrontendSPDKTCPBlockdev || e.Frontend == types.FrontendSPDKTCPNvmf) && e.Endpoint != "" {
		targetAddress := fmt.Sprintf("%s:%d", e.NvmeTcpFrontend.TargetIP, e.NvmeTcpFrontend.TargetPort)
		podIP, err := commonnet.GetIPForPod()
		if err != nil {
			return err
		}

		initiatorCreationRequired, targetCreationRequired, err := e.checkInitiatorAndTargetCreationRequirements(podIP, e.NvmeTcpFrontend.IP, e.NvmeTcpFrontend.TargetIP)
		if err != nil {
			return err
		}

		err = e.handleNvmeTcpFrontend(spdkClient, superiorPortAllocator, 0, targetAddress, initiatorCreationRequired, targetCreationRequired)
		if err != nil {
			e.log.WithError(err).Errorf("failed to reconnect nvme tcp frontend during engine %s expansion", e.Name)
			return err
		}
	}

	return nil
}

func (e *Engine) ReplicaAdd(spdkClient *spdkclient.Client, dstReplicaName, dstReplicaAddress string) (err error) {
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

	srcReplicaName, srcReplicaAddress, err := e.getReplicaAddSrcReplica()
	if err != nil {
		return err
	}

	srcReplicaServiceCli, dstReplicaServiceCli, err := e.getSrcAndDstReplicaClients(srcReplicaName, srcReplicaAddress, dstReplicaName, dstReplicaAddress)
	if err != nil {
		return err
	}

	var rebuildingSnapshotList []*api.Lvol
	// Need to make sure the replica clients available before set this deferred goroutine
	defer func() {
		go func() {
			defer func() {
				// Do not use e.log here. Since this deferred goroutine is not protected by a lock while e.log may change from time to time
				if errClose := srcReplicaServiceCli.Close(); errClose != nil {
					e.log.WithError(errClose).Errorf("Engine %s failed to close source replica %s client with address %s during add replica", e.Name, srcReplicaName, srcReplicaAddress)
				}
				if errClose := dstReplicaServiceCli.Close(); errClose != nil {
					e.log.WithError(errClose).Errorf("Engine %s failed to close dest replica %s client with address %s during add replica", e.Name, dstReplicaName, dstReplicaAddress)
				}
				// TODO: handle the floating connections after erroring out
			}()

			if err == nil && engineErr == nil {
				if err = e.replicaShallowCopy(dstReplicaServiceCli, srcReplicaName, dstReplicaName, rebuildingSnapshotList); err != nil {
					e.log.WithError(err).Errorf("Engine %s failed to do the shallow copy for replica %s add", e.Name, dstReplicaName)
				}
			} else {
				e.log.Errorf("Engine %s won't do shallow copy for replica %s add due to replica error: %v, or engine error %v", e.Name, dstReplicaName, err, engineErr)
			}
			// Should be executed no matter if there is an error. It's used to clean up the replica add related resources.
			if finishErr := e.replicaAddFinish(srcReplicaServiceCli, dstReplicaServiceCli, srcReplicaName, dstReplicaName); finishErr != nil {
				e.log.WithError(finishErr).Errorf("Engine %s failed to finish replica %s add", e.Name, dstReplicaName)
			}
		}()
	}()

	// Pause the IO and flush cache by suspending the NVMe initiator
	if e.Frontend == types.FrontendSPDKTCPBlockdev && e.Endpoint != "" {
		// The system-created snapshot during a rebuilding does not need to guarantee the integrity of the filesystem.
		if err = e.initiator.Suspend(true, true); err != nil {
			err = errors.Wrapf(err, "failed to suspend NVMe initiator during engine %s replica %s add start", e.Name, dstReplicaName)
			engineErr = err
			return err
		}
		defer func() {
			if frontendErr := e.initiator.Resume(); frontendErr != nil {
				frontendErr = errors.Wrapf(frontendErr, "failed to resume NVMe initiator during engine %s replica %s add start", e.Name, dstReplicaName)
				engineErr = frontendErr
			}
		}()
	}

	snapshotName := GenerateRebuildingSnapshotName()
	opts := &api.SnapshotOptions{
		Timestamp: util.Now(),
	}
	updateRequired, replicasErr, engineErr := e.snapshotOperationWithoutLock(spdkClient, replicaClients, snapshotName, SnapshotOperationCreate, opts)
	if replicasErr != nil {
		return replicasErr
	}
	if engineErr != nil {
		return engineErr
	}
	e.checkAndUpdateInfoFromReplicaNoLock()

	rebuildingSnapshotList, err = getRebuildingSnapshotList(srcReplicaServiceCli, srcReplicaName)
	if err != nil {
		return err
	}

	// Ask the source replica to expose the newly created snapshot if the source replica and destination replica are not on the same node.
	externalSnapshotAddress, err := srcReplicaServiceCli.ReplicaRebuildingSrcStart(srcReplicaName, dstReplicaName, dstReplicaAddress, snapshotName)
	if err != nil {
		return err
	}

	// The destination replica attaches the source replica exposed snapshot as the external snapshot then create a head based on it.
	dstHeadLvolAddress, err := dstReplicaServiceCli.ReplicaRebuildingDstStart(dstReplicaName, srcReplicaName, srcReplicaAddress, snapshotName, externalSnapshotAddress, rebuildingSnapshotList)
	if err != nil {
		return err
	}

	// Add rebuilding replica head bdev to the base bdev list of the RAID bdev
	dstHeadLvolBdevName, err := connectNVMfBdev(spdkClient, dstReplicaName, dstHeadLvolAddress, e.ctrlrLossTimeout, e.fastIOFailTimeoutSec)
	if err != nil {
		return err
	}
	if _, err := spdkClient.BdevRaidGrowBaseBdev(e.Name, dstHeadLvolBdevName); err != nil {
		return errors.Wrapf(err, "failed to adding the rebuilding replica %s head bdev %s to the base bdev list for engine %s", dstReplicaName, dstHeadLvolBdevName, e.Name)
	}

	e.ReplicaStatusMap[dstReplicaName] = &EngineReplicaStatus{
		Address:  dstReplicaAddress,
		Mode:     types.ModeWO,
		BdevName: dstHeadLvolBdevName,
	}
	updateRequired = true

	// TODO: Mark the destination replica as WO mode here does not prevent the RAID bdev from using this. May need to have a SPDK API to control the corresponding base bdev mode.
	// Reading data from this dst replica is not a good choice as the flow will be more zigzag than reading directly from the src replica:
	// application -> RAID1 -> this base bdev (dest replica) -> the exposed snapshot (src replica).
	if errUpdateLogger := e.log.UpdateLogger(logrus.Fields{
		"replicaStatusMap": e.ReplicaStatusMap,
	}); errUpdateLogger != nil {
		e.log.WithError(errUpdateLogger).Warn("Failed to update logger with replica status map during engine creation")
	}

	e.log.Infof("Engine started to rebuild replica %s from healthy replica %s", dstReplicaName, srcReplicaName)

	return nil
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

func (e *Engine) replicaShallowCopy(dstReplicaServiceCli *client.SPDKClient, srcReplicaName, dstReplicaName string, rebuildingSnapshotList []*api.Lvol) (err error) {
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
		e.log.Debugf("Engine is syncing snapshot %s from rebuilding src replica %s to rebuilding dst replica %s", currentSnapshotName, srcReplicaName, dstReplicaName)

		if err := dstReplicaServiceCli.ReplicaRebuildingDstShallowCopyStart(dstReplicaName, currentSnapshotName); err != nil {
			return errors.Wrapf(err, "failed to start shallow copy snapshot %s", currentSnapshotName)
		}

		timer.Reset(MaxShallowCopyWaitTime)
		continuousRetryCount := 0
		for finished := false; !finished; {
			select {
			case <-timer.C:
				return errors.Errorf("Timeout engine failed to check the dst replica %s snapshot %s shallow copy status over %d times", dstReplicaName, currentSnapshotName, maxNumRetries)
			case <-ticker.C:
				shallowCopyStatus, err := dstReplicaServiceCli.ReplicaRebuildingDstShallowCopyCheck(dstReplicaName)
				if err != nil {
					continuousRetryCount++
					if continuousRetryCount > maxNumRetries {
						return errors.Wrapf(err, "Engine failed to check the dst replica %s snapshot %s shallow copy status over %d times", dstReplicaName, currentSnapshotName, maxNumRetries)
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
func (e *Engine) replicaAddFinish(srcReplicaServiceCli, dstReplicaServiceCli *client.SPDKClient, srcReplicaName, dstReplicaName string) (err error) {

	updateRequired := false

	e.Lock()
	defer func() {
		e.Unlock()

		if updateRequired {
			e.UpdateCh <- nil
		}
	}()

	defer func() {
		if err != nil {
			if e.State != types.InstanceStateError {
				e.State = types.InstanceStateError
				updateRequired = true
			}
			e.ErrorMsg = err.Error()
		} else {
			if e.State != types.InstanceStateError {
				e.ErrorMsg = ""
			}
		}
	}()

	// Pause the IO again by suspending the NVMe initiator
	// If something goes wrong, the engine will be marked as error, then we don't need to do anything for replicas. The deletion logic will take over the responsibility of cleanup.
	if e.Frontend == types.FrontendSPDKTCPBlockdev && e.Endpoint != "" {
		if err = e.initiator.Suspend(true, true); err != nil {
			return errors.Wrapf(err, "failed to suspend NVMe initiator")
		}
		defer func() {
			if err = e.initiator.Resume(); err != nil {
				return
			}
		}()
	}

	// The destination replica will change the parent of the head to the newly rebuilt snapshot chain and detach the external snapshot.
	// Besides, it should clean up the attached rebuilding lvol if exists.
	if e.ReplicaStatusMap[dstReplicaName] == nil {
		e.log.Infof("Engine skipped finishing rebuilding dst replica %s as it was already removed", dstReplicaName)
	} else if e.ReplicaStatusMap[dstReplicaName].Mode == types.ModeWO {
		if dstReplicaErr := dstReplicaServiceCli.ReplicaRebuildingDstFinish(dstReplicaName); dstReplicaErr != nil {
			e.log.WithError(dstReplicaErr).Errorf("Engine failed to finish rebuilding dst replica %s, will update the mode from %v to ERR then continue rebuilding src replica %s finish", dstReplicaName, e.ReplicaStatusMap[dstReplicaName].Mode, srcReplicaName)
			e.ReplicaStatusMap[dstReplicaName].Mode = types.ModeERR
		} else {
			e.log.Infof("Engine succeeded to finish rebuilding dst replica %s, will update the mode from %v to RW", dstReplicaName, e.ReplicaStatusMap[dstReplicaName].Mode)
			e.ReplicaStatusMap[dstReplicaName].Mode = types.ModeRW
		}
		updateRequired = true
	}
	e.checkAndUpdateInfoFromReplicaNoLock()

	// The source replica blindly stops exposing the snapshot and wipe the rebuilding info.
	if srcReplicaErr := srcReplicaServiceCli.ReplicaRebuildingSrcFinish(srcReplicaName, dstReplicaName); srcReplicaErr != nil {
		// TODO: Should we mark this healthy replica as error?
		e.log.WithError(srcReplicaErr).Errorf("Engine failed to finish rebuilding src replica %s, will ignore this error", srcReplicaName)
	}

	e.log.Infof("Engine finished rebuilding replica %s from healthy replica %s", dstReplicaName, srcReplicaName)

	return nil
}

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

	if errUpdateLogger := e.log.UpdateLogger(logrus.Fields{
		"replicaStatusMap": e.ReplicaStatusMap,
	}); errUpdateLogger != nil {
		e.log.WithError(errUpdateLogger).Warn("Failed to update logger with replica status map during engine creation")
	}

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

	if snapshotOp == SnapshotOperationCreate {
		// Pause the IO, flush outstanding IO and attempt to synchronize filesystem by suspending the NVMe initiator
		if e.Frontend == types.FrontendSPDKTCPBlockdev && e.Endpoint != "" {
			e.log.Infof("Requesting initiator suspend before to create snapshot %s", snapshotName)
			if err = e.initiator.Suspend(false, false); err != nil {
				return "", errors.Wrapf(err, "failed to suspend NVMe initiator before the creation of snapshot %s", snapshotName)
			}
			e.log.Infof("Engine suspended initiator before the creation of snapshot %s", snapshotName)

			defer func() {
				if resumeErr := e.initiator.Resume(); resumeErr != nil {
					e.log.Errorf("Failed to resume initiator after the creation of snapshot %s", snapshotName)
					engineErr = errors.Wrapf(resumeErr, "failed to resume NVMe initiator after the creation of snapshot %s", snapshotName)
					return
				}
				e.log.Infof("Engine resumed initiator after the creation of snapshot %s", snapshotName)
			}()
		}
	}

	updateRequired, replicasErr, engineErr = e.snapshotOperationWithoutLock(spdkClient, replicaClients, snapshotName, snapshotOp, opts)
	if replicasErr != nil {
		return "", replicasErr
	}
	if engineErr != nil {
		return "", engineErr
	}

	e.checkAndUpdateInfoFromReplicaNoLock()

	e.log.Infof("Engine finished snapshot operation %s %s", snapshotOp, snapshotName)

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
	for replicaName := range replicaClients {
		replicaStatus := e.ReplicaStatusMap[replicaName]
		if replicaStatus == nil {
			return "", fmt.Errorf("cannot find replica %s in the engine %s replica status map before snapshot %s operation", replicaName, e.Name, snapshotName)
		}
		switch snapshotOp {
		case SnapshotOperationCreate:
			if snapshotName == "" {
				snapshotName = util.UUID()[:8]
			}
		case SnapshotOperationDelete:
			if snapshotName == "" {
				return "", fmt.Errorf("empty snapshot name for engine %s snapshot deletion", e.Name)
			}
			if e.SnapshotMap[snapshotName] == nil {
				return "", fmt.Errorf("engine %s does not contain snapshot %s during snapshot deletion", e.Name, snapshotName)
			}
			if replicaStatus.Mode == types.ModeWO {
				return "", fmt.Errorf("engine %s contains WO replica %s during snapshot %s delete", e.Name, replicaName, snapshotName)
			}
			e.checkAndUpdateInfoFromReplicaNoLock()
			if len(e.SnapshotMap[snapshotName].Children) > 1 {
				return "", fmt.Errorf("engine %s cannot delete snapshot %s since it contains multiple children %+v", e.Name, snapshotName, e.SnapshotMap[snapshotName].Children)
			}
			// TODO: SPDK allows deleting the parent of the volume head. To make the behavior consistent between v1 and v2 engines, we manually disable if for now.
			for childName := range e.SnapshotMap[snapshotName].Children {
				if childName == types.VolumeHead {
					return "", fmt.Errorf("engine %s cannot delete snapshot %s since it is the parent of volume head", e.Name, snapshotName)
				}
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
		if _, raidErr := spdkClient.BdevRaidCreate(e.Name, spdktypes.BdevRaidLevel1, 0, replicaBdevList, ""); raidErr != nil {
			engineErr = errors.Wrapf(raidErr, "engine %s failed to re-create RAID after snapshot %s revert", e.Name, snapshotName)
		}
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
		if err := disconnectNVMfBdev(spdkClient, replicaStatus.BdevName); err != nil {
			return err
		}
		replicaStatus.BdevName = ""
		// If the below step failed, the replica will be marked as ERR during ValidateAndUpdate.
		if err := replicaClient.ReplicaSnapshotRevert(replicaName, snapshotName); err != nil {
			return err
		}
		bdevName, err := connectNVMfBdev(spdkClient, replicaName, replicaStatus.Address, e.ctrlrLossTimeout, e.fastIOFailTimeoutSec)
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

func (e *Engine) BackupRestore(spdkClient *spdkclient.Client, backupUrl, engineName, snapshotName string, credential map[string]string, concurrentLimit int32) (*spdkrpc.EngineBackupRestoreResponse, error) {
	e.log.Infof("Restoring backup %s", backupUrl)

	e.Lock()
	defer e.Unlock()

	resp := &spdkrpc.EngineBackupRestoreResponse{
		Errors: map[string]string{},
	}

	backupInfo, err := backupstore.InspectBackup(backupUrl)
	if err != nil {
		for _, replicaStatus := range e.ReplicaStatusMap {
			resp.Errors[replicaStatus.Address] = err.Error()
		}
		return resp, nil
	}

	if backupInfo.VolumeSize != int64(e.SpecSize) {
		return nil, fmt.Errorf("the backup volume %v size %v must be the same as the Longhorn volume size %v", backupInfo.VolumeName, backupInfo.VolumeSize, e.SpecSize)
	}

	e.log.Infof("Deleting raid bdev %s before restoration", e.Name)
	if _, err := spdkClient.BdevRaidDelete(e.Name); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return nil, errors.Wrapf(err, "failed to delete raid bdev %s before restoration", e.Name)
	}

	e.log.Info("Disconnecting all replicas before restoration")
	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		if err := disconnectNVMfBdev(spdkClient, replicaStatus.BdevName); err != nil {
			e.log.Infof("Failed to remove replica %s before restoration", replicaName)
			return nil, errors.Wrapf(err, "failed to remove replica %s before restoration", replicaName)
		}
		replicaStatus.BdevName = ""
	}

	e.IsRestoring = true

	switch {
	case snapshotName != "":
		e.RestoringSnapshotName = snapshotName
		e.log.Infof("Using input snapshot name %s for the restore", e.RestoringSnapshotName)
	case len(e.SnapshotMap) == 0:
		e.RestoringSnapshotName = util.UUID()
		e.log.Infof("Using new generated snapshot name %s for the full restore", e.RestoringSnapshotName)
	case e.RestoringSnapshotName != "":
		e.log.Infof("Using existing snapshot name %s for the incremental restore", e.RestoringSnapshotName)
	default:
		e.RestoringSnapshotName = util.UUID()
		e.log.Infof("Using new generated snapshot name %s for the incremental restore because e.FinalSnapshotName is empty", e.RestoringSnapshotName)
	}

	defer func() {
		go func() {
			if err := e.completeBackupRestore(spdkClient); err != nil {
				e.log.WithError(err).Warn("Failed to complete backup restore")
			}
		}()
	}()

	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		e.log.Infof("Restoring backup on replica %s address %s", replicaName, replicaStatus.Address)

		replicaServiceCli, err := GetServiceClient(replicaStatus.Address)
		if err != nil {
			e.log.WithError(err).Errorf("Failed to restore backup on replica %s with address %s", replicaName, replicaStatus.Address)
			resp.Errors[replicaStatus.Address] = err.Error()
			continue
		}

		func() {
			defer func() {
				if errClose := replicaServiceCli.Close(); errClose != nil {
					e.log.WithError(errClose).Errorf("Failed to close replica %s client with address %s during restore backup", replicaName, replicaStatus.Address)
				}
			}()

			err = replicaServiceCli.ReplicaBackupRestore(&client.BackupRestoreRequest{
				BackupUrl:       backupUrl,
				ReplicaName:     replicaName,
				SnapshotName:    e.RestoringSnapshotName,
				Credential:      credential,
				ConcurrentLimit: concurrentLimit,
			})
			if err != nil {
				e.log.WithError(err).Errorf("Failed to restore backup on replica %s address %s", replicaName, replicaStatus.Address)
				resp.Errors[replicaStatus.Address] = err.Error()
			}
		}()
	}

	return resp, nil
}

func (e *Engine) completeBackupRestore(spdkClient *spdkclient.Client) error {
	if err := e.waitForRestoreComplete(); err != nil {
		return errors.Wrapf(err, "failed to wait for restore complete")
	}

	return e.BackupRestoreFinish(spdkClient)
}

func (e *Engine) waitForRestoreComplete() error {
	periodicChecker := time.NewTicker(time.Duration(restorePeriodicRefreshInterval.Seconds()) * time.Second)
	defer periodicChecker.Stop()

	var err error
	for range periodicChecker.C {
		isReplicaRestoreCompleted := true
		for replicaName, replicaStatus := range e.ReplicaStatusMap {
			if replicaStatus.Mode != types.ModeRW {
				continue
			}

			isReplicaRestoreCompleted, err = e.isReplicaRestoreCompleted(replicaName, replicaStatus.Address)
			if err != nil {
				return errors.Wrapf(err, "failed to check replica %s restore status", replicaName)
			}

			if !isReplicaRestoreCompleted {
				break
			}
		}

		if isReplicaRestoreCompleted {
			e.log.Info("Backup restoration completed successfully")
			return nil
		}
	}

	return errors.Errorf("failed to wait for engine %s restore complete", e.Name)
}

func (e *Engine) isReplicaRestoreCompleted(replicaName, replicaAddress string) (bool, error) {
	log := e.log.WithFields(logrus.Fields{
		"replica": replicaName,
		"address": replicaAddress,
	})
	log.Trace("Checking replica restore status")

	replicaServiceCli, err := GetServiceClient(replicaAddress)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get replica %v service client %s", replicaName, replicaAddress)
	}
	defer func() {
		if errClose := replicaServiceCli.Close(); errClose != nil {
			log.WithError(errClose).Errorf("Failed to close replica %s client with address %s during check restore status", replicaName, replicaAddress)
		}
	}()

	status, err := replicaServiceCli.ReplicaRestoreStatus(replicaName)
	if err != nil {
		return false, errors.Wrapf(err, "failed to check replica %s restore status", replicaName)
	}

	return !status.IsRestoring, nil
}

func (e *Engine) BackupRestoreFinish(spdkClient *spdkclient.Client) error {
	e.Lock()
	defer e.Unlock()

	replicaBdevList := []string{}
	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		replicaAddress := replicaStatus.Address
		replicaIP, replicaPort, err := net.SplitHostPort(replicaAddress)
		if err != nil {
			return err
		}
		e.log.Infof("Attaching replica %s with address %s before finishing restoration", replicaName, replicaAddress)
		nvmeBdevNameList, err := spdkClient.BdevNvmeAttachController(replicaName, helpertypes.GetNQN(replicaName), replicaIP, replicaPort,
			spdktypes.NvmeTransportTypeTCP, spdktypes.NvmeAddressFamilyIPv4,
			int32(e.ctrlrLossTimeout), replicaReconnectDelaySec, int32(e.fastIOFailTimeoutSec), replicaMultipath)
		if err != nil {
			return err
		}

		if len(nvmeBdevNameList) != 1 {
			return fmt.Errorf("got unexpected nvme bdev list %v", nvmeBdevNameList)
		}

		replicaStatus.BdevName = nvmeBdevNameList[0]

		replicaBdevList = append(replicaBdevList, replicaStatus.BdevName)
	}

	e.log.Infof("Creating raid bdev %s with replicas %+v before finishing restoration", e.Name, replicaBdevList)
	if _, err := spdkClient.BdevRaidCreate(e.Name, spdktypes.BdevRaidLevel1, 0, replicaBdevList, ""); err != nil {
		if !jsonrpc.IsJSONRPCRespErrorFileExists(err) {
			e.log.WithError(err).Errorf("Failed to create raid bdev before finishing restoration")
			return err
		}
	}

	e.IsRestoring = false

	return nil
}

func (e *Engine) RestoreStatus() (*spdkrpc.RestoreStatusResponse, error) {
	resp := &spdkrpc.RestoreStatusResponse{
		Status: map[string]*spdkrpc.ReplicaRestoreStatusResponse{},
	}

	e.Lock()
	defer e.Unlock()

	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		if replicaStatus.Mode != types.ModeRW {
			continue
		}

		restoreStatus, err := e.getReplicaRestoreStatus(replicaName, replicaStatus.Address)
		if err != nil {
			return nil, err
		}
		resp.Status[replicaStatus.Address] = restoreStatus
	}

	return resp, nil
}

func (e *Engine) getReplicaRestoreStatus(replicaName, replicaAddress string) (*spdkrpc.ReplicaRestoreStatusResponse, error) {
	replicaServiceCli, err := GetServiceClient(replicaAddress)
	if err != nil {
		return nil, err
	}
	defer func() {
		if errClose := replicaServiceCli.Close(); errClose != nil {
			e.log.WithError(errClose).Errorf("Failed to close replica client with address %s during get restore status", replicaAddress)
		}
	}()

	status, err := replicaServiceCli.ReplicaRestoreStatus(replicaName)
	if err != nil {
		return nil, err
	}

	return status, nil
}

// Suspend suspends the engine. IO operations will be suspended.
func (e *Engine) Suspend(spdkClient *spdkclient.Client) (err error) {
	e.Lock()
	defer func() {
		e.Unlock()

		if err != nil {
			if e.State != types.InstanceStateError {
				e.log.WithError(err).Warn("Failed to suspend engine")
				// Engine is still alive and running and is not really in error state.
				// longhorn-manager will retry to suspend the engine.
			}
			e.ErrorMsg = err.Error()
		} else {
			e.State = types.InstanceStateSuspended
			e.ErrorMsg = ""

			e.log.Infof("Suspended engine")
		}

		e.UpdateCh <- nil
	}()

	if e.NvmeTcpFrontend == nil {
		return fmt.Errorf("ublk frontend suspend is unimplemented")
	}
	e.log.Info("Creating initiator for suspending engine")
	nvmeTCPInfo := &initiator.NVMeTCPInfo{
		SubsystemNQN: e.NvmeTcpFrontend.Nqn,
	}
	i, err := initiator.NewInitiator(e.VolumeName, initiator.HostProc, nvmeTCPInfo, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to create initiator for suspending engine %s", e.Name)
	}

	e.log.Info("Suspending engine")
	return i.Suspend(false, false)
}

// Resume resumes the engine. IO operations will be resumed.
func (e *Engine) Resume(spdkClient *spdkclient.Client) (err error) {
	e.Lock()
	defer func() {
		e.Unlock()

		if err != nil {
			if e.State != types.InstanceStateError {
				e.log.WithError(err).Warn("Failed to resume engine")
				// Engine is still alive and running and is not really in error state.
				// longhorn-manager will retry to resume the engine.
			}
			e.ErrorMsg = err.Error()
		} else {
			e.State = types.InstanceStateRunning
			e.ErrorMsg = ""

			e.log.Infof("Resumed engine")
		}

		e.UpdateCh <- nil
	}()

	if e.State == types.InstanceStateRunning {
		return nil
	}

	if e.NvmeTcpFrontend == nil {
		return fmt.Errorf("ublk frontend resume is unimplemented")
	}
	e.log.Info("Creating initiator for resuming engine")
	nvmeTCPInfo := &initiator.NVMeTCPInfo{
		SubsystemNQN: e.NvmeTcpFrontend.Nqn,
	}
	i, err := initiator.NewInitiator(e.VolumeName, initiator.HostProc, nvmeTCPInfo, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to create initiator for resuming engine %s", e.Name)
	}

	e.log.Info("Resuming engine")
	return i.Resume()
}

// SwitchOverTarget function in the Engine struct is responsible for switching the engine's target to a new address.
func (e *Engine) SwitchOverTarget(spdkClient *spdkclient.Client, newTargetAddress string) (err error) {
	e.log.Infof("Switching over engine to target address %s", newTargetAddress)

	if e.NvmeTcpFrontend == nil {
		return fmt.Errorf("invalid value for NvmeTcpFrontend: %v", e.NvmeTcpFrontend)
	}
	if newTargetAddress == "" {
		return fmt.Errorf("invalid empty target address for engine %s target switchover", e.Name)
	}

	currentTargetAddress := ""

	podIP, err := commonnet.GetIPForPod()
	if err != nil {
		return errors.Wrapf(err, "failed to get IP for pod for engine %s target switchover", e.Name)
	}

	newTargetIP, newTargetPort, err := splitHostPort(newTargetAddress)
	if err != nil {
		return errors.Wrapf(err, "failed to split target address %s for engine %s target switchover", newTargetAddress, e.Name)
	}

	e.Lock()
	defer func() {
		e.Unlock()

		if err != nil {
			e.log.WithError(err).Warnf("Failed to switch over engine to target address %s", newTargetAddress)

			if disconnected, errCheck := e.isTargetDisconnected(); errCheck != nil {
				e.log.WithError(errCheck).Warnf("Failed to check if target %s is disconnected", newTargetAddress)
			} else if disconnected {
				if currentTargetAddress != "" {
					if errConnect := e.connectTarget(currentTargetAddress); errConnect != nil {
						e.log.WithError(errConnect).Warnf("Failed to connect target back to %s", currentTargetAddress)
					} else {
						e.log.Infof("Connected target back to %s", currentTargetAddress)

						if errReload := e.reloadDevice(); errReload != nil {
							e.log.WithError(errReload).Warnf("Failed to reload device mapper")
						} else {
							e.log.Infof("Reloaded device mapper for connecting target back to %s", currentTargetAddress)
						}
					}
				}
			}
		} else {
			e.ErrorMsg = ""

			e.log.Infof("Switched over target to %s", newTargetAddress)
		}

		e.UpdateCh <- nil
	}()
	nvmeTCPInfo := &initiator.NVMeTCPInfo{
		SubsystemNQN: e.NvmeTcpFrontend.Nqn,
	}
	i, err := initiator.NewInitiator(e.VolumeName, initiator.HostProc, nvmeTCPInfo, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to create initiator for engine %s target switchover", e.Name)
	}

	// Check if the engine is suspended before target switchover.
	suspended, err := i.IsSuspended()
	if err != nil {
		return errors.Wrapf(err, "failed to check if engine %s is suspended", e.Name)
	}
	if !suspended {
		return fmt.Errorf("engine %s must be suspended before target switchover", e.Name)
	}

	// Load NVMe device info before target switchover.
	if err := i.LoadNVMeDeviceInfo(i.NVMeTCPInfo.TransportAddress, i.NVMeTCPInfo.TransportServiceID, i.NVMeTCPInfo.SubsystemNQN); err != nil {
		if !helpertypes.ErrorIsValidNvmeDeviceNotFound(err) {
			return errors.Wrapf(err, "failed to load NVMe device info for engine %s target switchover", e.Name)
		}
	}

	currentTargetAddress = net.JoinHostPort(i.NVMeTCPInfo.TransportAddress, i.NVMeTCPInfo.TransportServiceID)
	if isSwitchOverTargetRequired(currentTargetAddress, newTargetAddress) {
		if currentTargetAddress != "" {
			if err := e.disconnectTarget(currentTargetAddress); err != nil {
				return errors.Wrapf(err, "failed to disconnect target %s for engine %s", currentTargetAddress, e.Name)
			}
		}

		if err := e.connectTarget(newTargetAddress); err != nil {
			return errors.Wrapf(err, "failed to connect target %s for engine %s", newTargetAddress, e.Name)
		}
	}

	// Replace IP and Port with the new target address.
	// No need to update TargetIP, because old target is not delete yet.
	e.NvmeTcpFrontend.IP = newTargetIP
	e.NvmeTcpFrontend.Port = newTargetPort

	if newTargetIP == podIP {
		e.NvmeTcpFrontend.TargetPort = newTargetPort
		e.NvmeTcpFrontend.StandbyTargetPort = 0
	} else {
		e.NvmeTcpFrontend.StandbyTargetPort = e.NvmeTcpFrontend.TargetPort
		e.NvmeTcpFrontend.TargetPort = 0
	}

	e.log.Info("Reloading device mapper after target switchover")
	if err := e.reloadDevice(); err != nil {
		return errors.Wrapf(err, "failed to reload device mapper after engine %s target switchover", e.Name)
	}

	return nil
}

func (e *Engine) isTargetDisconnected() (bool, error) {
	if e.NvmeTcpFrontend == nil {
		return false, fmt.Errorf("invalid value for NvmeTcpFrontend: %v", e.NvmeTcpFrontend)
	}
	nvmeTCPInfo := &initiator.NVMeTCPInfo{
		SubsystemNQN: e.NvmeTcpFrontend.Nqn,
	}
	i, err := initiator.NewInitiator(e.VolumeName, initiator.HostProc, nvmeTCPInfo, nil)
	if err != nil {
		return false, errors.Wrapf(err, "failed to create initiator for checking engine %s target disconnected", e.Name)
	}

	suspended, err := i.IsSuspended()
	if err != nil {
		return false, errors.Wrapf(err, "failed to check if engine %s is suspended", e.Name)
	}
	if !suspended {
		return false, fmt.Errorf("engine %s must be suspended before checking target disconnected", e.Name)
	}

	if err := i.LoadNVMeDeviceInfo(i.NVMeTCPInfo.TransportAddress, i.NVMeTCPInfo.TransportServiceID, i.NVMeTCPInfo.SubsystemNQN); err != nil {
		if !helpertypes.ErrorIsValidNvmeDeviceNotFound(err) {
			return false, errors.Wrapf(err, "failed to load NVMe device info for checking engine %s target disconnected", e.Name)
		}
	}

	return i.NVMeTCPInfo.TransportAddress == "" && i.NVMeTCPInfo.TransportServiceID == "", nil
}

func (e *Engine) reloadDevice() error {
	if e.NvmeTcpFrontend == nil {
		return fmt.Errorf("invalid value for NvmeTcpFrontend: %v", e.NvmeTcpFrontend)
	}
	nvmeTCPInfo := &initiator.NVMeTCPInfo{
		SubsystemNQN: e.NvmeTcpFrontend.Nqn,
	}
	i, err := initiator.NewInitiator(e.VolumeName, initiator.HostProc, nvmeTCPInfo, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to recreate initiator after engine %s target switchover", e.Name)
	}

	if err := i.LoadNVMeDeviceInfo(i.NVMeTCPInfo.TransportAddress, i.NVMeTCPInfo.TransportServiceID, i.NVMeTCPInfo.SubsystemNQN); err != nil {
		return errors.Wrapf(err, "failed to load NVMe device info after engine %s target switchover", e.Name)
	}

	if err := i.ReloadDmDevice(); err != nil {
		return errors.Wrapf(err, "failed to reload device mapper after engine %s target switchover", e.Name)
	}

	return nil
}

func (e *Engine) disconnectTarget(targetAddress string) error {
	if e.NvmeTcpFrontend == nil {
		return fmt.Errorf("invalid value for NvmeTcpFrontend: %v", e.NvmeTcpFrontend)
	}
	nvmeTCPInfo := &initiator.NVMeTCPInfo{
		SubsystemNQN: e.NvmeTcpFrontend.Nqn,
	}
	i, err := initiator.NewInitiator(e.VolumeName, initiator.HostProc, nvmeTCPInfo, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to create initiator for engine %s disconnect target %v", e.Name, targetAddress)
	}

	if err := i.LoadNVMeDeviceInfo(i.NVMeTCPInfo.TransportAddress, i.NVMeTCPInfo.TransportServiceID, i.NVMeTCPInfo.SubsystemNQN); err != nil {
		if !helpertypes.ErrorIsValidNvmeDeviceNotFound(err) {
			return errors.Wrapf(err, "failed to load NVMe device info for engine %s disconnect target %v", e.Name, targetAddress)
		}
	}

	e.log.Infof("Disconnecting from old target %s", targetAddress)
	if err := i.DisconnectNVMeTCPTarget(); err != nil {
		return errors.Wrapf(err, "failed to disconnect from old target %s for engine %s", targetAddress, e.Name)
	}

	// Make sure the old target is disconnected before connecting to the new targets
	if err := i.WaitForNVMeTCPTargetDisconnect(maxNumRetries, retryInterval); err != nil {
		return errors.Wrapf(err, "failed to wait for disconnect from old target %s for engine %s", targetAddress, e.Name)
	}

	e.log.Infof("Disconnected from old target %s", targetAddress)

	return nil
}

func (e *Engine) connectTarget(targetAddress string) error {
	if e.NvmeTcpFrontend == nil {
		return fmt.Errorf("invalid value for NvmeTcpFrontend: %v", e.NvmeTcpFrontend)
	}
	if targetAddress == "" {
		return fmt.Errorf("failed to connect target for engine %s: missing required parameter target address", e.Name)
	}

	targetIP, targetPort, err := splitHostPort(targetAddress)
	if err != nil {
		return errors.Wrapf(err, "failed to split target address %s", targetAddress)
	}

	nvmeTCPInfo := &initiator.NVMeTCPInfo{
		SubsystemNQN: e.NvmeTcpFrontend.Nqn,
	}
	i, err := initiator.NewInitiator(e.VolumeName, initiator.HostProc, nvmeTCPInfo, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to create initiator for engine %s connect target %v:%v", e.Name, targetIP, targetPort)
	}

	if err := i.LoadNVMeDeviceInfo(i.NVMeTCPInfo.TransportAddress, i.NVMeTCPInfo.TransportServiceID, i.NVMeTCPInfo.SubsystemNQN); err != nil {
		if !helpertypes.ErrorIsValidNvmeDeviceNotFound(err) {
			return errors.Wrapf(err, "failed to load NVMe device info for engine %s connect target %v:%v", e.Name, targetIP, targetPort)
		}
	}

	for r := 0; r < maxNumRetries; r++ {
		e.log.Infof("Discovering target %v:%v before target switchover", targetIP, targetPort)
		subsystemNQN, err := i.DiscoverNVMeTCPTarget(targetIP, strconv.Itoa(int(targetPort)))
		if err != nil {
			e.log.WithError(err).Warnf("Failed to discover target %v:%v for target switchover", targetIP, targetPort)
			time.Sleep(retryInterval)
			continue
		}
		i.NVMeTCPInfo.SubsystemNQN = subsystemNQN

		e.log.Infof("Connecting to target %v:%v before target switchover", targetIP, targetPort)
		controllerName, err := i.ConnectNVMeTCPTarget(targetIP, strconv.Itoa(int(targetPort)), e.NvmeTcpFrontend.Nqn)
		if err != nil {
			e.log.WithError(err).Warnf("Failed to connect to target %v:%v for target switchover", targetIP, targetPort)
			time.Sleep(retryInterval)
			continue
		}
		i.NVMeTCPInfo.ControllerName = controllerName
		break
	}

	if i.NVMeTCPInfo.SubsystemNQN == "" || i.NVMeTCPInfo.ControllerName == "" {
		return fmt.Errorf("failed to connect to target %v:%v for engine %v target switchover", targetIP, targetPort, e.Name)
	}

	// Target is switched over, to avoid the error "failed to wait for connect to target",
	// create a new initiator and wait for connect
	nvmeTCPInfo = &initiator.NVMeTCPInfo{
		SubsystemNQN: e.NvmeTcpFrontend.Nqn,
	}
	i, err = initiator.NewInitiator(e.VolumeName, initiator.HostProc, nvmeTCPInfo, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to create initiator for engine %s wait for connect target %v:%v", e.Name, targetIP, targetPort)
	}

	if err := i.WaitForNVMeTCPConnect(maxNumRetries, retryInterval); err == nil {
		return errors.Wrapf(err, "failed to wait for connect to target %v:%v for engine %v target switchover", targetIP, targetPort, e.Name)
	}

	return nil
}

// DeleteTarget deletes the target instance
func (e *Engine) DeleteTarget(spdkClient *spdkclient.Client, superiorPortAllocator *commonbitmap.Bitmap) (err error) {
	if e.NvmeTcpFrontend == nil {
		return fmt.Errorf("invalid value for NvmeTcpFrontend: %v", e.NvmeTcpFrontend)
	}
	e.log.Infof("Deleting target with target port %d and standby target port %d", e.NvmeTcpFrontend.TargetPort, e.NvmeTcpFrontend.StandbyTargetPort)

	err = spdkClient.StopExposeBdev(e.NvmeTcpFrontend.Nqn)
	if err != nil {
		return errors.Wrapf(err, "failed to stop expose bdev while deleting target instance for engine %s", e.Name)
	}

	err = e.releaseTargetAndStandbyTargetPorts(superiorPortAllocator)
	if err != nil {
		return errors.Wrapf(err, "failed to release target and standby target ports while deleting target instance for engine %s", e.Name)
	}

	e.log.Infof("Deleting raid bdev %s while deleting target instance", e.Name)
	_, err = spdkClient.BdevRaidDelete(e.Name)
	if err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "failed to delete raid bdev after engine %s while deleting target instance", e.Name)
	}

	for replicaName, replicaStatus := range e.ReplicaStatusMap {
		e.log.Infof("Disconnecting replica %s while deleting target instance", replicaName)
		err = disconnectNVMfBdev(spdkClient, replicaStatus.BdevName)
		if err != nil {
			e.log.WithError(err).Warnf("Engine failed to disconnect replica %s while deleting target instance, will mark the replica mode from %v to ERR",
				replicaName, replicaStatus.Mode)
			replicaStatus.Mode = types.ModeERR
		}
		replicaStatus.BdevName = ""
	}
	return nil
}

func isSwitchOverTargetRequired(oldTargetAddress, newTargetAddress string) bool {
	return oldTargetAddress != newTargetAddress
}

func (e *Engine) releaseTargetAndStandbyTargetPorts(superiorPortAllocator *commonbitmap.Bitmap) error {
	if e.NvmeTcpFrontend == nil {
		return fmt.Errorf("invalid value for NvmeTcpFrontend: %v", e.NvmeTcpFrontend)
	}
	releaseTargetPortRequired := e.NvmeTcpFrontend.TargetPort != 0
	releaseStandbyTargetPortRequired := e.NvmeTcpFrontend.StandbyTargetPort != 0 && e.NvmeTcpFrontend.StandbyTargetPort != e.NvmeTcpFrontend.TargetPort

	// Release the target port
	if releaseTargetPortRequired {
		if err := superiorPortAllocator.ReleaseRange(e.NvmeTcpFrontend.TargetPort, e.NvmeTcpFrontend.TargetPort); err != nil {
			return errors.Wrapf(err, "failed to release target port %d", e.NvmeTcpFrontend.TargetPort)
		}
	}
	e.NvmeTcpFrontend.TargetPort = 0

	// Release the standby target port
	if releaseStandbyTargetPortRequired {
		if err := superiorPortAllocator.ReleaseRange(e.NvmeTcpFrontend.StandbyTargetPort, e.NvmeTcpFrontend.StandbyTargetPort); err != nil {
			return errors.Wrapf(err, "failed to release standby target port %d", e.NvmeTcpFrontend.StandbyTargetPort)
		}
	}
	e.NvmeTcpFrontend.StandbyTargetPort = 0

	return nil
}
