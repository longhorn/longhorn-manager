package initiator

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	commonns "github.com/longhorn/go-common-libs/ns"
	commontypes "github.com/longhorn/go-common-libs/types"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	"github.com/longhorn/go-spdk-helper/pkg/types"
	"github.com/longhorn/go-spdk-helper/pkg/util"
)

const (
	LockFile    = "/var/run/longhorn-spdk.lock"
	LockTimeout = 120 * time.Second

	HostProc = "/host/proc"

	validateDiskCreationMaxRetries    = 60
	validateDiskCreationRetryInterval = 1 * time.Second

	UnInitializedUblkId      = -1
	MaxUblkId                = 65535
	DefaultUblkQueueDepth    = 128
	DefaultUblkNumberOfQueue = 1

	// LinuxKernelSectorSize is the fixed sector size (512 bytes) used by the
	// Linux kernel for all block layer and Device Mapper table calculations.
	// As defined in the kernel source (include/linux/types.h), "Linux always
	// considers sectors to be 512 bytes long independently of the devices
	// real block size."
	//
	// Refs:
	// - https://github.com/torvalds/linux/blob/master/include/linux/types.h#L130-L138
	// - https://android.googlesource.com/platform/external/lvm2/+/refs/heads/main/tools/dmsetup.c#103
	DmSectorSize = 512
)

const (
	maxConnectTargetRetries    = 15
	retryConnectTargetInterval = 1 * time.Second

	maxWaitDeviceRetries = 60
	waitDeviceInterval   = 1 * time.Second
)

var (
	idGenerator IDGenerator
)

var errDeviceNotReady = errors.New("device is not a block device yet")

type Initiator struct {
	Name     string
	Endpoint string
	dev      *util.LonghornBlockDevice
	isUp     bool

	NVMeTCPInfo *NVMeTCPInfo
	UblkInfo    *UblkInfo

	hostProc string
	executor *commonns.Executor

	logger logrus.FieldLogger
}

type NVMeTCPInfo struct {
	SubsystemNQN       string
	UUID               string
	TransportAddress   string
	TransportServiceID string
	ControllerName     string
	NamespaceName      string
}

type UblkInfo struct {
	// spec
	BdevName          string
	UblkQueueDepth    int32
	UblkNumberOfQueue int32

	// status
	UblkID int32
}

// NewInitiator creates a new initiator
func NewInitiator(name, hostProc string, nvmeTCPInfo *NVMeTCPInfo, ublkInfo *UblkInfo) (*Initiator, error) {
	if name == "" {
		return nil, fmt.Errorf("empty name for initiator creation")
	}
	if (ublkInfo == nil && nvmeTCPInfo == nil) || (ublkInfo != nil && nvmeTCPInfo != nil) {
		return nil, fmt.Errorf("cannot initiator creation because both nvmeTCPInfo and ublkInfo are nil or non-nil: nvmeTCPInfo: %v, ublkInfo: %v", nvmeTCPInfo, ublkInfo)
	}
	if nvmeTCPInfo != nil && nvmeTCPInfo.SubsystemNQN == "" {
		return nil, fmt.Errorf("empty subsystem for NVMe/TCP initiator creation")
	}
	if ublkInfo != nil && ublkInfo.BdevName == "" {
		return nil, fmt.Errorf("empty BdevName for ublk initiator creation")
	}
	// If transportAddress or transportServiceID is empty, the initiator is still valid for stopping
	executor, err := util.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return nil, err
	}

	return &Initiator{
		Name:     name,
		Endpoint: util.GetLonghornDevicePath(name),

		NVMeTCPInfo: nvmeTCPInfo,
		UblkInfo:    ublkInfo,

		hostProc: hostProc,
		executor: executor,

		logger: logrus.WithFields(logrus.Fields{
			"name":        name,
			"nvmeTCPInfo": fmt.Sprintf("%+v", nvmeTCPInfo),
			"ublkInfo":    fmt.Sprintf("%+v", ublkInfo),
		}),
	}, nil
}

func (i *Initiator) newLock() (*commonns.FileLock, error) {
	if i.hostProc != commontypes.HostProcDirectory {
		return nil, fmt.Errorf("invalid host proc path %s for initiator %s, supported path is %s", i.hostProc, i.Name, commontypes.HostProcDirectory)
	}

	lock := commonns.NewLock(LockFile, LockTimeout)
	if err := lock.Lock(); err != nil {
		return nil, errors.Wrapf(err, "failed to get file lock for initiator %s", i.Name)
	}

	return lock, nil
}

// DiscoverNVMeTCPTarget discovers a target
func (i *Initiator) DiscoverNVMeTCPTarget(ip, port string) (string, error) {
	if i.hostProc != "" {
		lock, err := i.newLock()
		if err != nil {
			return "", err
		}
		defer lock.Unlock()
	}

	return DiscoverTarget(ip, port, i.executor)
}

// ConnectNVMeTCPTarget connects to a target
func (i *Initiator) ConnectNVMeTCPTarget(ip, port, nqn string) (string, error) {
	if i.hostProc != "" {
		lock, err := i.newLock()
		if err != nil {
			return "", err
		}
		defer lock.Unlock()
	}

	return ConnectTarget(ip, port, nqn, i.executor)
}

// executeNVMeTCPPathOp validates initiator state, acquires the file lock, and
// delegates to fn. It is the shared skeleton for ConnectNVMeTCPPath and
// ReconnectNVMeTCPPath.
func (i *Initiator) executeNVMeTCPPathOp(transportAddress, transportServiceID, opName string, fn func(string, string) error) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "failed to %s NVMe/TCP path for initiator %s", opName, i.Name)
		}
	}()

	if i.NVMeTCPInfo == nil {
		return fmt.Errorf("nvmeTCPInfo is nil")
	}
	if transportAddress == "" || transportServiceID == "" {
		return fmt.Errorf("invalid transportAddress %s and transportServiceID %s for initiator %s", transportAddress, transportServiceID, i.Name)
	}

	if i.hostProc != "" {
		lock, err := i.newLock()
		if err != nil {
			return err
		}
		defer lock.Unlock()
	}

	return fn(transportAddress, transportServiceID)
}

// ConnectNVMeTCPPath connects an additional NVMe/TCP path without touching dm-linear.
// It is intended for native multipath switchover flows where dm-linear must remain
// intact for later snapshot suspend/resume operations.
func (i *Initiator) ConnectNVMeTCPPath(transportAddress, transportServiceID string) error {
	return i.executeNVMeTCPPathOp(transportAddress, transportServiceID, "connect", i.connectNVMeTCPPathWithoutLock)
}

// ReconnectNVMeTCPPath refreshes the current NVMe/TCP initiator state for the
// specified path without touching dm-linear. It reuses an existing matching
// path when present and otherwise establishes a new multipath connection.
func (i *Initiator) ReconnectNVMeTCPPath(transportAddress, transportServiceID string) error {
	return i.executeNVMeTCPPathOp(transportAddress, transportServiceID, "reconnect", i.ensureNVMeTCPPathWithoutLock)
}

// DisconnectNVMeTCPTarget disconnects a target
func (i *Initiator) DisconnectNVMeTCPTarget() error {
	if i.NVMeTCPInfo == nil {
		return fmt.Errorf("failed to DisconnectNVMeTCPTarget because nvmeTCPInfo is nil")
	}
	if i.hostProc != "" {
		lock, err := i.newLock()
		if err != nil {
			return err
		}
		defer lock.Unlock()
	}

	return DisconnectTarget(i.NVMeTCPInfo.SubsystemNQN, i.executor)
}

func (i *Initiator) connectNVMeTCPPathWithoutLock(transportAddress, transportServiceID string) error {
	if reused, err := i.reuseExistingNVMeTCPPathWithoutLock(transportAddress, transportServiceID); err == nil {
		if reused {
			return nil
		}
	} else {
		i.logger.WithError(err).Debugf("Failed to reuse existing NVMe/TCP path for %s:%s, will attempt fresh connect", transportAddress, transportServiceID)
	}

	// For switchover flows the caller may intentionally keep the newly
	// connected path in ANA inaccessible state until after control-plane
	// coordination completes. In that window the kernel may not expose the
	// new path as the selected namespace device yet, so we only establish the
	// connection here and defer device-info reload to the caller.
	_, _, err := i.discoverAndConnectNVMeTCPTarget(transportAddress, transportServiceID, maxConnectTargetRetries, retryConnectTargetInterval)
	return err
}

// WaitForNVMeTCPConnect waits for the NVMe/TCP initiator to connect and load the device info
func (i *Initiator) WaitForNVMeTCPConnect(maxRetries int, retryInterval time.Duration) (err error) {
	if i.NVMeTCPInfo == nil {
		return fmt.Errorf("failed to WaitForNVMeTCPConnect because nvmeTCPInfo is nil")
	}
	if i.hostProc != "" {
		lock, err := i.newLock()
		if err != nil {
			return err
		}
		defer lock.Unlock()
	}

	err = retry.Do(
		func() error {
			errTest := i.loadNVMeDeviceInfoWithoutLock(
				i.NVMeTCPInfo.TransportAddress,
				i.NVMeTCPInfo.TransportServiceID,
				i.NVMeTCPInfo.SubsystemNQN,
			)

			return errTest
		},
		retry.Attempts(uint(maxRetries)),
		retry.Delay(retryInterval),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			i.logger.WithError(err).Warnf(
				"Retrying waiting for NVMe/TCP connect: address=%s:%s attempt=%d/%d next_wait=%s",
				i.NVMeTCPInfo.TransportAddress, i.NVMeTCPInfo.TransportServiceID, n+1, maxRetries, retryInterval,
			)
		}),
	)

	if err != nil {
		return errors.Wrap(err, "failed to wait for NVMe/TCP connect")
	}

	return nil
}

// WaitForNVMeTCPTargetDisconnect waits for the NVMe/TCP initiator to disconnect
func (i *Initiator) WaitForNVMeTCPTargetDisconnect(maxRetries int, retryInterval time.Duration) (err error) {
	if i.NVMeTCPInfo == nil {
		return fmt.Errorf("failed to WaitForNVMeTCPTargetDisconnect because nvmeTCPInfo is nil")
	}
	if i.hostProc != "" {
		lock, err := i.newLock()
		if err != nil {
			return err
		}
		defer lock.Unlock()
	}

	// Keep original behavior when maxRetries <= 0: do nothing and return current err (nil).
	if maxRetries <= 0 {
		return err
	}

	var (
		found        bool
		lastErr      error
		attemptCount int
		forceRetry   = errors.New("force retry")
	)

	_ = retry.Do(
		func() error {
			// Extra terminal no-op attempt to preserve original "sleep after last failed loop" behavior.
			if attemptCount >= maxRetries {
				return nil
			}

			attemptCount++
			lastErr = i.loadNVMeDeviceInfoWithoutLock(
				i.NVMeTCPInfo.TransportAddress,
				i.NVMeTCPInfo.TransportServiceID,
				i.NVMeTCPInfo.SubsystemNQN,
			)
			if types.ErrorIsValidNvmeDeviceNotFound(lastErr) {
				found = true
				return nil
			}

			// Always continue retrying for exactly maxRetries loop-equivalent attempts,
			// even when lastErr == nil, matching the original for-loop behavior.
			return forceRetry
		},
		retry.Attempts(uint(maxRetries+1)),
		retry.Delay(retryInterval),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
	)

	if found {
		return nil
	}
	return lastErr
}

// Suspend suspends the device mapper device for the NVMe/TCP initiator
func (i *Initiator) Suspend(noflush, nolockfs bool) error {
	if i.hostProc != "" {
		lock, err := i.newLock()
		if err != nil {
			return err
		}
		defer lock.Unlock()
	}

	suspended, err := i.IsSuspended()
	if err != nil {
		return errors.Wrapf(err, "failed to check if linear dm device is suspended for NVMe/TCP initiator %s", i.Name)
	}

	if !suspended {
		if err := i.suspendLinearDmDevice(noflush, nolockfs); err != nil {
			return errors.Wrapf(err, "failed to suspend linear dm device for NVMe/TCP initiator %s", i.Name)
		}
	}

	return nil
}

// Resume resumes the device mapper device for the NVMe/TCP initiator
func (i *Initiator) Resume() error {
	if i.hostProc != "" {
		lock, err := i.newLock()
		if err != nil {
			return err
		}
		defer lock.Unlock()
	}

	if err := i.resumeLinearDmDevice(); err != nil {
		return errors.Wrapf(err, "failed to resume linear dm device for NVMe/TCP initiator %s", i.Name)
	}

	return nil
}

func (i *Initiator) resumeLinearDmDevice() error {
	i.logger.Info("Resuming linear dm device")

	return util.DmsetupResume(i.Name, i.executor)
}

func (i *Initiator) replaceDmDeviceTarget() error {
	suspended, err := i.IsSuspended()
	if err != nil {
		return errors.Wrapf(err, "failed to check if linear dm device is suspended for initiator %s", i.Name)
	}

	if !suspended {
		if err := i.suspendLinearDmDevice(true, false); err != nil {
			return errors.Wrapf(err, "failed to suspend linear dm device for initiator %s", i.Name)
		}
	}

	if err := i.reloadLinearDmDevice(); err != nil {
		return errors.Wrapf(err, "failed to reload linear dm device for initiator %s", i.Name)
	}

	if err := i.resumeLinearDmDevice(); err != nil {
		return errors.Wrapf(err, "failed to resume linear dm device for initiator %s", i.Name)
	}
	return nil
}

// StartNvmeTCPInitiator starts the NVMe/TCP initiator with the given transportAddress and transportServiceID
func (i *Initiator) StartNvmeTCPInitiator(transportAddress, transportServiceID string, dmDeviceAndEndpointCleanupRequired bool, stop bool) (dmDeviceIsBusy bool, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "failed to start NVMe/TCP initiator %s", i.Name)
		}
	}()

	if i.NVMeTCPInfo == nil {
		return false, fmt.Errorf("nvmeTCPInfo is nil")
	}
	if transportAddress == "" || transportServiceID == "" {
		return false, fmt.Errorf("invalid transportAddress %s and transportServiceID %s for starting initiator %s", transportAddress, transportServiceID, i.Name)
	}

	i.logger.WithFields(logrus.Fields{
		"transportAddress":                   transportAddress,
		"transportServiceID":                 transportServiceID,
		"dmDeviceAndEndpointCleanupRequired": dmDeviceAndEndpointCleanupRequired,
	}).Info("Starting NVMe/TCP initiator")

	if i.hostProc != "" {
		lock, err := i.newLock()
		if err != nil {
			return false, err
		}
		defer lock.Unlock()
	}

	// Check if the initiator/NVMe/TCP device is already launched and matches the params.
	if launched, err := i.reuseExistingNVMeTCPPathWithoutLock(transportAddress, transportServiceID); err == nil {
		if launched {
			err = i.LoadEndpointForNvmeTcpFrontend(false)
			if err == nil {
				i.logger.Info("NVMe/TCP initiator is already launched with correct params")
				return false, nil
			}
			i.logger.WithError(err).Warnf("NVMe/TCP initiator is launched with failed to load the endpoint")
		}
	} else {
		i.logger.WithError(err).Warn("Failed to load existing NVMe/TCP path state before starting initiator")
	}
	if i.NVMeTCPInfo.TransportAddress != "" && i.NVMeTCPInfo.TransportServiceID != "" &&
		(i.NVMeTCPInfo.TransportAddress != transportAddress || i.NVMeTCPInfo.TransportServiceID != transportServiceID) {
		i.logger.Warnf("NVMe/TCP initiator is launched but with incorrect address, the required one is %s:%s, will try to stop then relaunch it", transportAddress, transportServiceID)
	}

	if stop {
		i.logger.Info("Stopping NVMe/TCP initiator blindly before starting")
		dmDeviceIsBusy, err = i.stopWithoutLock(nil, dmDeviceAndEndpointCleanupRequired, false, false)
		if err != nil {
			return dmDeviceIsBusy, errors.Wrapf(err, "failed to stop the mismatching NVMe/TCP initiator %s before starting", i.Name)
		}
	} else {
		dmDeviceIsBusy = true
	}

	i.logger.Info("Ensuring NVMe/TCP target path is connected")
	err = i.ensureNVMeTCPPathWithoutLock(transportAddress, transportServiceID)
	if err != nil {
		return dmDeviceIsBusy, errors.Wrapf(err, "failed to ensure device info after connecting target for NVMe/TCP initiator %s", i.Name)
	}

	if dmDeviceAndEndpointCleanupRequired {
		if dmDeviceIsBusy {
			// Endpoint is already created, just replace the target device
			i.logger.Info("Linear dm device is busy, trying the best to replace the target device for NVMe/TCP initiator")
			if err := i.replaceDmDeviceTarget(); err != nil {
				i.logger.WithError(err).Warnf("Failed to replace the target device for NVMe/TCP initiator")
			} else {
				i.logger.Info("Successfully replaced the target device for NVMe/TCP initiator")
				dmDeviceIsBusy = false
			}
		} else {
			i.logger.Info("Creating linear dm device for NVMe/TCP initiator")
			if err := i.createLinearDmDevice(); err != nil {
				return false, errors.Wrapf(err, "failed to create linear dm device for NVMe/TCP initiator %s", i.Name)
			}
		}
	} else {
		i.logger.Info("Skipping creating linear dm device for NVMe/TCP initiator")
		i.dev.Export = i.dev.Source
	}

	i.logger.Infof("Creating endpoint %v", i.Endpoint)
	if err := i.createEndpoint(); err != nil {
		return dmDeviceIsBusy, err
	}

	i.logger.Infof("Launched NVMe/TCP initiator: %+v", i)

	return dmDeviceIsBusy, nil
}

func (i *Initiator) createEndpoint() error {
	exist, err := i.isEndpointExist()
	if err != nil {
		return errors.Wrapf(err, "failed to check if endpoint %v exists for NVMe/TCP initiator %s", i.Endpoint, i.Name)
	}
	if exist {
		i.logger.Infof("Skipping endpoint %v creation for NVMe/TCP initiator", i.Endpoint)
		return nil
	}

	if err := i.makeEndpoint(); err != nil {
		return err
	}
	return nil
}

func (i *Initiator) StartUblkInitiator(spdkClient *client.Client, dmDeviceAndEndpointCleanupRequired bool) (dmDeviceIsBusy bool, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "failed to start ublk initiator %s", i.Name)
		}
	}()
	if i.UblkInfo == nil {
		return false, fmt.Errorf("UblkInfo is nil")
	}

	if i.hostProc != "" {
		lock, err := i.newLock()
		if err != nil {
			return false, err
		}
		defer lock.Unlock()
	}

	ublkDeviceList, err := spdkClient.UblkGetDisks(0)
	if err != nil {
		return false, err
	}

	i.logger.Info("Stopping ublk initiator blindly before starting")
	for _, ublkDevice := range ublkDeviceList {
		if ublkDevice.BdevName == i.UblkInfo.BdevName {
			if err := spdkClient.UblkStopDisk(ublkDevice.ID); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
				return false, err
			}
		}
	}

	dmDeviceIsBusy, err = i.stopWithoutLock(spdkClient, dmDeviceAndEndpointCleanupRequired, false, false)
	if err != nil {
		return dmDeviceIsBusy, errors.Wrapf(err, "failed to stop the ublk initiator %s before starting", i.Name)
	}

	i.logger.Info("Launching ublk initiator")

	ublkDeviceList, err = spdkClient.UblkGetDisks(0)
	if err != nil {
		return false, err
	}
	availableUblkID, err := idGenerator.GetAvailableID(ublkDeviceList)
	if err != nil {
		return false, err
	}

	queueDepth := i.UblkInfo.UblkQueueDepth
	if queueDepth <= 0 {
		i.logger.Infof("Invalid queue depth %d for ublk initiator, using default value %d", queueDepth, DefaultUblkQueueDepth)
		queueDepth = DefaultUblkQueueDepth
	}
	numQueues := i.UblkInfo.UblkNumberOfQueue
	if numQueues <= 0 {
		i.logger.Infof("Invalid number of queues %d for ublk initiator, using default value %d", numQueues, DefaultUblkNumberOfQueue)
		numQueues = DefaultUblkNumberOfQueue
	}

	i.UblkInfo.UblkQueueDepth = queueDepth
	i.UblkInfo.UblkNumberOfQueue = numQueues

	i.logger.Infof("Starting ublk initiator with bdev %s, available UBLK ID %d, queue depth %d, number of queues %d",
		i.UblkInfo.BdevName, availableUblkID, i.UblkInfo.UblkQueueDepth, i.UblkInfo.UblkNumberOfQueue)
	if err := spdkClient.UblkStartDisk(i.UblkInfo.BdevName, availableUblkID, i.UblkInfo.UblkQueueDepth, i.UblkInfo.UblkNumberOfQueue); err != nil {
		return false, err
	}
	i.UblkInfo.UblkID = availableUblkID
	i.logger = i.logger.WithFields(logrus.Fields{
		"ublkInfo": fmt.Sprintf("%+v", i.UblkInfo),
	})

	devicePath, err := spdkClient.FindUblkDevicePath(i.UblkInfo.UblkID)
	if err != nil {
		return false, err
	}

	dev, err := util.DetectDevice(devicePath, i.executor)
	if err != nil {
		return dmDeviceIsBusy, errors.Wrapf(err, "cannot find the device for ublk initiator %s at path %s", i.Name, devicePath)
	}

	i.dev = &util.LonghornBlockDevice{
		Source: *dev,
	}

	if dmDeviceAndEndpointCleanupRequired {
		if dmDeviceIsBusy {
			// Endpoint is already created, just replace the target device
			i.logger.Info("Linear dm device is busy, trying the best to replace the target device for ublk initiator")
			if err := i.replaceDmDeviceTarget(); err != nil {
				i.logger.WithError(err).Warnf("Failed to replace the target device for ublk initiator")
			} else {
				i.logger.Info("Successfully replaced the target device for ublk initiator")
				dmDeviceIsBusy = false
			}
		} else {
			i.logger.Info("Creating linear dm device for ublk initiator")
			if err := i.createLinearDmDevice(); err != nil {
				return false, errors.Wrapf(err, "failed to create linear dm device for ublk initiator %s", i.Name)
			}
		}
	} else {
		i.logger.Info("Skipping creating linear dm device for ublk initiator")
		i.dev.Export = i.dev.Source
	}

	i.logger.Infof("Creating endpoint %v", i.Endpoint)
	exist, err := i.isEndpointExist()
	if err != nil {
		return dmDeviceIsBusy, errors.Wrapf(err, "failed to check if endpoint %v exists for ublk initiator %s", i.Endpoint, i.Name)
	}
	if exist {
		i.logger.Infof("Skipping endpoint %v creation for ublk initiator", i.Endpoint)
	} else {
		if err := i.makeEndpoint(); err != nil {
			return dmDeviceIsBusy, err
		}
	}

	i.logger.Infof("Launched ublk initiator: %+v", i)

	return dmDeviceIsBusy, nil
}

func (i *Initiator) waitAndLoadNVMeDeviceInfoWithoutLock(transportAddress, transportServiceID string) (err error) {
	if i.NVMeTCPInfo == nil {
		return fmt.Errorf("failed to waitAndLoadNVMeDeviceInfoWithoutLock because nvmeTCPInfo is nil")
	}

	err = retry.Do(
		func() error {
			return i.loadNVMeDeviceInfoWithoutLock(
				transportAddress,
				transportServiceID,
				i.NVMeTCPInfo.SubsystemNQN,
			)
		},
		retry.Attempts(uint(maxWaitDeviceRetries)),
		retry.Delay(waitDeviceInterval),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			i.logger.WithError(err).Warnf(
				"Retrying loading NVMe device info for initiator %s: address=%s:%s attempt=%d/%d next_wait=%s",
				i.Name, transportAddress, transportServiceID, n+1, maxWaitDeviceRetries, waitDeviceInterval,
			)
		}),
	)

	if err != nil {
		return errors.Wrap(err, "failed to load NVMe device info")
	}

	return nil
}

func (i *Initiator) reuseExistingNVMeTCPPathWithoutLock(transportAddress, transportServiceID string) (bool, error) {
	if err := i.loadNVMeDeviceInfoWithoutLock(i.NVMeTCPInfo.TransportAddress, i.NVMeTCPInfo.TransportServiceID, i.NVMeTCPInfo.SubsystemNQN); err != nil {
		return false, err
	}
	if i.NVMeTCPInfo.TransportAddress != transportAddress || i.NVMeTCPInfo.TransportServiceID != transportServiceID {
		return false, nil
	}

	i.logger.WithFields(logrus.Fields{
		"transportAddress":   transportAddress,
		"transportServiceID": transportServiceID,
	}).Info("NVMe/TCP path is already connected")

	if err := i.waitAndLoadNVMeDeviceInfoWithoutLock(transportAddress, transportServiceID); err != nil {
		return false, err
	}

	return true, nil
}

func (i *Initiator) ensureNVMeTCPPathWithoutLock(transportAddress, transportServiceID string) error {
	if reused, err := i.reuseExistingNVMeTCPPathWithoutLock(transportAddress, transportServiceID); err == nil {
		if reused {
			return nil
		}
	}

	previousInfo := *i.NVMeTCPInfo
	subsystemNQN, controllerName, err := i.discoverAndConnectNVMeTCPTarget(transportAddress, transportServiceID, maxConnectTargetRetries, retryConnectTargetInterval)
	if err != nil {
		return err
	}

	cleanupConnection := func(reason error) {
		i.logger.WithError(reason).Warnf("Cleaning up orphaned NVMe/TCP connection for %s at %s:%s after post-connect failure", subsystemNQN, transportAddress, transportServiceID)
		if disconnectErr := DisconnectController(subsystemNQN, transportAddress, transportServiceID, i.executor); disconnectErr != nil {
			i.logger.WithError(disconnectErr).Warnf("Failed to disconnect orphaned NVMe/TCP controller for %s at %s:%s", subsystemNQN, transportAddress, transportServiceID)
		}
	}

	if err := i.recordConnectedNVMeTCPInfo(subsystemNQN, controllerName); err != nil {
		cleanupConnection(err)
		*i.NVMeTCPInfo = previousInfo
		return err
	}
	if err := i.waitAndLoadNVMeDeviceInfoWithoutLock(transportAddress, transportServiceID); err != nil {
		cleanupConnection(err)
		*i.NVMeTCPInfo = previousInfo
		return err
	}

	return nil
}

func (i *Initiator) recordConnectedNVMeTCPInfo(subsystemNQN, controllerName string) error {
	if i.NVMeTCPInfo == nil {
		return fmt.Errorf("nvmeTCPInfo is nil")
	}

	// Persist the discovered subsystem immediately after a successful connect.
	// Later reload/load/cleanup paths use SubsystemNQN to locate or disconnect
	// the NVMe device, so it must stay in sync even if controllerName validation fails.
	if subsystemNQN != "" {
		i.NVMeTCPInfo.SubsystemNQN = subsystemNQN
	}
	if controllerName == "" {
		return fmt.Errorf("controller name is empty")
	}

	i.NVMeTCPInfo.ControllerName = controllerName
	return nil
}

func (i *Initiator) discoverAndConnectNVMeTCPTarget(transportAddress, transportServiceID string, maxRetries int, retryInterval time.Duration) (subsystemNQN, controllerName string, err error) {
	if i.NVMeTCPInfo == nil {
		return "", "", fmt.Errorf("nvmeTCPInfo is nil")
	}

	err = retry.Do(
		func() error {
			var e error

			// If SubsystemNQN is already known (e.g. for backup or rebuild
			// initiators), skip the discovery step and connect directly.
			// This avoids "failed to add controller" errors from nvme-cli 2.x
			// when the kernel already has NVMe-oF connections to the same
			// target address with the same hostNQN/hostID.
			if i.NVMeTCPInfo.SubsystemNQN != "" {
				subsystemNQN = i.NVMeTCPInfo.SubsystemNQN
				i.logger.Infof("Using pre-configured SubsystemNQN %s for target %s:%s, skipping discovery",
					subsystemNQN, transportAddress, transportServiceID)
			} else {
				i.logger.Infof("Discovering NVMe/TCP target %s:%s", transportAddress, transportServiceID)
				subsystemNQN, e = DiscoverTarget(transportAddress, transportServiceID, i.executor)
				if e != nil {
					return errors.Wrapf(e, "discover NVMe/TCP target %s:%s failed", transportAddress, transportServiceID)
				}
			}

			i.logger.Infof("Connecting to NVMe/TCP target %s:%s with subsystemNQN %s", transportAddress, transportServiceID, subsystemNQN)
			controllerName, e = ConnectTarget(transportAddress, transportServiceID, subsystemNQN, i.executor)
			if e != nil {
				// "already connected" means the path is present in the kernel
				// but GetDevices() couldn't find a namespace device yet (e.g.
				// multipath ANA inaccessible). Since the goal is to ensure
				// the path is connected, verify via subsystem listing and
				// treat it as success.
				if strings.Contains(strings.ToLower(e.Error()), "already connected") {
					i.logger.Infof("NVMe/TCP target %s:%s is already connected, verifying controller via subsystem listing", transportAddress, transportServiceID)
					if name, verifyErr := i.findControllerBySubsystem(subsystemNQN, transportAddress, transportServiceID); verifyErr == nil {
						controllerName = name
						i.logger.Infof("Verified existing controller %s for %s:%s", controllerName, transportAddress, transportServiceID)
						return nil
					}
					return retry.Unrecoverable(errors.Wrapf(e, "connect NVMe/TCP target %s:%s (nqn=%s) failed", transportAddress, transportServiceID, subsystemNQN))
				}
				return errors.Wrapf(e, "connect NVMe/TCP target %s:%s (nqn=%s) failed", transportAddress, transportServiceID, subsystemNQN)
			}

			return nil
		},
		retry.Attempts(uint(maxRetries)),
		retry.Delay(retryInterval),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			i.logger.WithError(err).Warnf(
				"Retrying NVMe/TCP target connect: addr=%s:%s attempt=%d/%d next_wait=%s",
				transportAddress, transportServiceID, n+1, maxRetries, retryInterval,
			)
		}),
	)

	if err != nil {
		return "", "", errors.Wrapf(err, "failed to discover and connect NVMe/TCP target %s:%s", transportAddress, transportServiceID)
	}

	return subsystemNQN, controllerName, nil
}

// findControllerBySubsystem looks up the controller name for the given NQN
// and transport address via subsystem listing. This is used as a fallback
// when ConnectTarget reports "already connected" but GetDevices cannot find
// a namespace device (e.g. multipath ANA inaccessible state).
func (i *Initiator) findControllerBySubsystem(nqn, transportAddress, transportServiceID string) (string, error) {
	subsystems, err := GetSubsystems(i.executor)
	if err != nil {
		return "", errors.Wrap(err, "failed to list subsystems")
	}
	for _, sys := range subsystems {
		if sys.NQN != nqn {
			continue
		}
		for _, path := range sys.Paths {
			controllerIP, controllerPort := GetIPAndPortFromControllerAddress(path.Address)
			if controllerIP == transportAddress && controllerPort == transportServiceID {
				return path.Name, nil
			}
		}
	}
	return "", fmt.Errorf("no controller found for subsystem %s at %s:%s", nqn, transportAddress, transportServiceID)
}

// Stop stops the NVMe/TCP initiator
func (i *Initiator) Stop(spdkClient *client.Client, dmDeviceAndEndpointCleanupRequired, deferDmDeviceCleanup, returnErrorForBusyDevice bool) (bool, error) {
	if i.hostProc != "" {
		lock, err := i.newLock()
		if err != nil {
			return false, err
		}
		defer lock.Unlock()
	}

	return i.stopWithoutLock(spdkClient, dmDeviceAndEndpointCleanupRequired, deferDmDeviceCleanup, returnErrorForBusyDevice)
}

func (i *Initiator) stopWithoutLock(spdkClient *client.Client, dmDeviceAndEndpointCleanupRequired, deferDmDeviceCleanup, returnErrorForBusyDevice bool) (dmDeviceIsBusy bool, err error) {
	dmDeviceIsBusy = false

	if dmDeviceAndEndpointCleanupRequired {
		err = i.removeLinearDmDevice(false, deferDmDeviceCleanup)
		if err != nil {
			if !os.IsNotExist(err) {
				if types.ErrorIsDeviceOrResourceBusy(err) {
					if returnErrorForBusyDevice {
						return true, err
					}
					dmDeviceIsBusy = true
				} else {
					return false, err
				}
			}
		}

		err = i.removeEndpoint()
		if err != nil {
			return false, err
		}
	}

	// stopping NvmeTcp initiator
	if i.NVMeTCPInfo != nil {
		err = DisconnectTarget(i.NVMeTCPInfo.SubsystemNQN, i.executor)
		if err != nil {
			return dmDeviceIsBusy, errors.Wrapf(err, "failed to disconnect target for NVMe/TCP initiator %s", i.Name)
		}

		i.NVMeTCPInfo.ControllerName = ""
		i.NVMeTCPInfo.NamespaceName = ""
		i.NVMeTCPInfo.TransportAddress = ""
		i.NVMeTCPInfo.TransportServiceID = ""
		return dmDeviceIsBusy, nil
	}

	// stopping ublk initiator
	if spdkClient == nil || i.UblkInfo == nil {
		return dmDeviceIsBusy, fmt.Errorf("failed to stop ublk initiator because spdkClient or UblkInfo is nil: spdkClient: %v, UblkInfo: %v", spdkClient, i.UblkInfo)
	}
	if i.UblkInfo.UblkID == UnInitializedUblkId {
		return dmDeviceIsBusy, nil
	}
	if err := spdkClient.UblkStopDisk(i.UblkInfo.UblkID); err != nil {
		if jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return dmDeviceIsBusy, nil
		}
		return dmDeviceIsBusy, err
	}
	return dmDeviceIsBusy, nil
}

// GetControllerName returns the controller name
func (i *Initiator) GetControllerName() string {
	if i.NVMeTCPInfo == nil {
		return ""
	}
	return i.NVMeTCPInfo.ControllerName
}

// GetNamespaceName returns the namespace name
func (i *Initiator) GetNamespaceName() string {
	if i.NVMeTCPInfo == nil {
		return ""
	}
	return i.NVMeTCPInfo.NamespaceName
}

// GetTransportAddress returns the transport address
func (i *Initiator) GetTransportAddress() string {
	if i.NVMeTCPInfo == nil {
		return ""
	}
	return i.NVMeTCPInfo.TransportAddress
}

// GetTransportServiceID returns the transport service ID
func (i *Initiator) GetTransportServiceID() string {
	if i.NVMeTCPInfo == nil {
		return ""
	}
	return i.NVMeTCPInfo.TransportServiceID
}

// GetEndpoint returns the endpoint
func (i *Initiator) GetEndpoint() string {
	if i.isUp {
		return i.Endpoint
	}
	return ""
}

// WaitForControllerLive waits for the NVMe controller at the given address to
// reach "live" state. This is needed after nvme connect which returns
// immediately while the TCP handshake completes asynchronously.
func (i *Initiator) WaitForControllerLive(transportAddress, transportServiceID string, maxAttempts int, retryInterval time.Duration) error {
	if i.NVMeTCPInfo == nil {
		return fmt.Errorf("nvmeTCPInfo is nil")
	}

	nqn := i.NVMeTCPInfo.SubsystemNQN

	err := retry.Do(
		func() error {
			subsystems, err := GetSubsystems(i.executor)
			if err != nil {
				return errors.Wrap(err, "failed to list subsystems while waiting for controller live state")
			}

			for _, sys := range subsystems {
				if sys.NQN != nqn {
					continue
				}
				for _, path := range sys.Paths {
					controllerIP, controllerPort := GetIPAndPortFromControllerAddress(path.Address)
					if controllerIP == transportAddress && controllerPort == transportServiceID {
						if path.State == "live" {
							i.logger.Infof("NVMe controller %s for %s:%s reached live state",
								path.Name, transportAddress, transportServiceID)
							return nil
						}
					}
				}
			}

			return fmt.Errorf("NVMe controller for %s:%s is not live yet", transportAddress, transportServiceID)
		},
		retry.Attempts(uint(maxAttempts)),
		retry.Delay(retryInterval),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			i.logger.WithError(err).Warnf(
				"Retrying waiting for NVMe controller live: addr=%s:%s attempt=%d/%d next_wait=%s",
				transportAddress, transportServiceID, n+1, maxAttempts, retryInterval,
			)
		}),
	)

	if err != nil {
		return fmt.Errorf("timed out waiting for NVMe controller to become live for %s:%s after %d attempts",
			transportAddress, transportServiceID, maxAttempts)
	}

	return nil
}

// GetDevice returns the device information
func (i *Initiator) LoadNVMeDeviceInfo(transportAddress, transportServiceID, subsystemNQN string) (err error) {
	if i.hostProc != "" {
		lock, err := i.newLock()
		if err != nil {
			return err
		}
		defer lock.Unlock()
	}

	return i.loadNVMeDeviceInfoWithoutLock(transportAddress, transportServiceID, subsystemNQN)
}

func (i *Initiator) loadNVMeDeviceInfoWithoutLock(transportAddress, transportServiceID, subsystemNQN string) error {
	if i.NVMeTCPInfo == nil {
		return fmt.Errorf("failed to loadNVMeDeviceInfoWithoutLock because nvmeTCPInfo is nil")
	}
	nvmeDevices, err := GetDevices(transportAddress, transportServiceID, subsystemNQN, i.executor)
	if err != nil {
		return err
	}
	if len(nvmeDevices) != 1 {
		return fmt.Errorf("found zero or multiple devices NVMe/TCP initiator %s", i.Name)
	}
	if len(nvmeDevices[0].Namespaces) != 1 {
		return fmt.Errorf("found zero or multiple devices for NVMe/TCP initiator %s", i.Name)
	}
	controller, err := selectControllerForNVMeDevice(nvmeDevices[0], transportAddress, transportServiceID, i.NVMeTCPInfo.ControllerName)
	if err != nil {
		return errors.Wrapf(err, "failed to select controller for NVMe/TCP initiator %s", i.Name)
	}

	i.NVMeTCPInfo.ControllerName = controller.Controller
	i.NVMeTCPInfo.NamespaceName = nvmeDevices[0].Namespaces[0].NameSpace
	i.NVMeTCPInfo.TransportAddress, i.NVMeTCPInfo.TransportServiceID = GetIPAndPortFromControllerAddress(controller.Address)
	i.logger = i.logger.WithFields(logrus.Fields{
		"controllerName":     i.NVMeTCPInfo.ControllerName,
		"namespaceName":      i.NVMeTCPInfo.NamespaceName,
		"transportAddress":   i.NVMeTCPInfo.TransportAddress,
		"transportServiceID": i.NVMeTCPInfo.TransportServiceID,
	})

	devPath := filepath.Join("/dev", i.NVMeTCPInfo.NamespaceName)
	dev, err := util.DetectDevice(devPath, i.executor)
	if err != nil {
		return errors.Wrapf(err, "cannot find the device for NVMe/TCP initiator %s with namespace name %s", i.Name, i.NVMeTCPInfo.NamespaceName)
	}

	i.dev = &util.LonghornBlockDevice{
		Source: *dev,
	}
	return nil
}

func selectControllerForNVMeDevice(device Device, transportAddress, transportServiceID, recordedControllerName string) (Controller, error) {
	if len(device.Controllers) == 0 {
		return Controller{}, fmt.Errorf("no NVMe controllers found for subsystem %s", device.SubsystemNQN)
	}

	if transportAddress != "" && transportServiceID != "" {
		for _, controller := range device.Controllers {
			controllerAddress, controllerServiceID := GetIPAndPortFromControllerAddress(controller.Address)
			if controllerAddress == transportAddress && controllerServiceID == transportServiceID {
				return controller, nil
			}
		}
	}

	if recordedControllerName != "" {
		for _, controller := range device.Controllers {
			if controller.Controller == recordedControllerName {
				return controller, nil
			}
		}
	}

	logrus.Warnf("No NVMe controller matched address %s:%s or recorded name %q for subsystem %s, falling back to first controller %s",
		transportAddress, transportServiceID, recordedControllerName, device.SubsystemNQN, device.Controllers[0].Controller)
	return device.Controllers[0], nil
}

func (i *Initiator) isNamespaceExist(devices []string) bool {
	if i.NVMeTCPInfo == nil {
		return false
	}
	for _, device := range devices {
		if device == i.NVMeTCPInfo.NamespaceName {
			return true
		}
	}
	return false
}

func (i *Initiator) findDependentDevices(devName string) ([]string, error) {
	depDevices, err := util.DmsetupDeps(devName, i.executor)
	if err != nil {
		return nil, err
	}
	return depDevices, nil
}

// LoadEndpointForNvmeTcpFrontend loads the endpoint
func (i *Initiator) LoadEndpointForNvmeTcpFrontend(dmDeviceIsBusy bool) error {
	if i.NVMeTCPInfo == nil {
		return fmt.Errorf("failed to LoadEndpointForNvmeTcpFrontend because nvmeTCPInfo is nil")
	}
	dev, err := util.DetectDevice(i.Endpoint, i.executor)
	if err != nil {
		return err
	}

	depDevices, err := i.findDependentDevices(dev.Name)
	if err != nil {
		return err
	}

	if dmDeviceIsBusy {
		i.logger.Debugf("Skipping endpoint %v loading due to device busy", i.Endpoint)
	} else {
		if i.NVMeTCPInfo.NamespaceName != "" && !i.isNamespaceExist(depDevices) {
			return fmt.Errorf("detected device %s name mismatching from endpoint %v for NVMe/TCP initiator %s", dev.Name, i.Endpoint, i.Name)
		}
	}

	newDev := &util.LonghornBlockDevice{
		Export: *dev,
	}
	// Preserve the Source device (e.g. the underlying NVMe namespace) if it
	// was already populated from a previous createLinearDmDevice call.
	// SyncDmDeviceSize relies on Source.Name to locate the physical device.
	if i.dev != nil && i.dev.Source.Name != "" {
		newDev.Source = i.dev.Source
	}
	i.dev = newDev
	i.isUp = true

	return nil
}

func (i *Initiator) isEndpointExist() (bool, error) {
	_, err := os.Stat(i.Endpoint)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (i *Initiator) makeEndpoint() error {
	if err := util.DuplicateDevice(i.dev, i.Endpoint); err != nil {
		return errors.Wrap(err, "failed to duplicate device")
	}
	i.isUp = true
	return nil
}

func (i *Initiator) removeEndpoint() error {
	if i.Endpoint == "" {
		return nil
	}

	if err := util.RemoveDevice(i.Endpoint); err != nil {
		return err
	}
	i.dev = nil
	i.isUp = false
	return nil
}

func (i *Initiator) removeLinearDmDevice(force, deferred bool) error {
	dmDevPath := getDmDevicePath(i.Name)
	if _, err := os.Stat(dmDevPath); err != nil {
		return err
	}

	i.logger.Info("Removing linear dm device")
	return util.DmsetupRemove(i.Name, force, deferred, i.executor)
}

func (i *Initiator) createLinearDmDevice() error {
	if i.dev == nil {
		return fmt.Errorf("found nil device for linear dm device creation")
	}

	nvmeDevPath := fmt.Sprintf("/dev/%s", i.dev.Source.Name)
	sectors, err := util.GetDeviceSectorSize(nvmeDevPath, i.executor)
	if err != nil {
		return err
	}

	// Create a device mapper device with the same size as the original device
	table := fmt.Sprintf("0 %v linear %v 0", sectors, nvmeDevPath)

	i.logger.Infof("Creating linear dm device with table '%s'", table)
	if err := util.DmsetupCreate(i.Name, table, i.executor); err != nil {
		return err
	}

	dmDevPath := getDmDevicePath(i.Name)
	if err := i.validateDiskCreation(dmDevPath, validateDiskCreationMaxRetries, validateDiskCreationRetryInterval); err != nil {
		return err
	}

	// Get the device numbers
	major, minor, err := util.GetDeviceNumbers(dmDevPath, i.executor)
	if err != nil {
		return err
	}

	i.dev.Export.Name = i.Name
	i.dev.Export.Major = major
	i.dev.Export.Minor = minor

	return nil
}

func (i *Initiator) validateDiskCreation(path string, maxRetries int, retryInterval time.Duration) error {
	if maxRetries <= 0 {
		return fmt.Errorf("maxRetries must be > 0")
	}

	err := retry.Do(
		func() error {
			isBlockDev, err := util.IsBlockDevice(path)
			if err != nil {
				return err
			}
			if !isBlockDev {
				return errDeviceNotReady
			}
			return nil
		},
		retry.Attempts(uint(maxRetries)),
		retry.Delay(retryInterval),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			i.logger.WithError(err).Warnf(
				"Retrying device creation validation: path=%s attempt=%d/%d next_wait=%s",
				path, n+1, maxRetries, retryInterval,
			)
		}),
	)

	if err != nil {
		return fmt.Errorf("failed to validate device %s creation: %w", path, err)
	}

	i.logger.Infof("Device %s is created and ready", path)
	return nil
}

func (i *Initiator) suspendLinearDmDevice(noflush, nolockfs bool) error {
	i.logger.Info("Suspending linear dm device")

	return util.DmsetupSuspend(i.Name, noflush, nolockfs, i.executor)
}

// ReloadDmDevice reloads the linear dm device
func (i *Initiator) ReloadDmDevice() (err error) {
	if i.hostProc != "" {
		lock, err := i.newLock()
		if err != nil {
			return err
		}
		defer lock.Unlock()
	}

	return i.reloadLinearDmDevice()
}

func (i *Initiator) SyncDmDeviceSize(expectedSize uint64) error {
	if i.dev == nil || i.dev.Source.Name == "" {
		return fmt.Errorf("initiator device source is not initialized")
	}

	devPath := fmt.Sprintf("/dev/%s", i.dev.Source.Name)
	expectedSectors := int64(expectedSize / DmSectorSize)

	i.logger.Infof("Start reloading dm device %v to expected size %v bytes (%v sectors)", i.Name, expectedSize, expectedSectors)

	var sectors int64

	// Keep original behavior when maxWaitDeviceRetries <= 0:
	// no polling happens, then fall through to the same final check.
	if maxWaitDeviceRetries > 0 {
		const forceRetryMsg = "force retry"
		forceRetryErr := errors.New(forceRetryMsg)

		attempt := 0
		_ = retry.Do(
			func() error {
				// Extra terminal no-op attempt to preserve original behavior:
				// sleep also happens after the last failed polling iteration.
				if attempt >= maxWaitDeviceRetries {
					return nil
				}
				attempt++

				output, err := i.executor.Execute(nil, util.BlockdevBinary, []string{"--getsize", devPath}, types.ExecuteTimeout)
				if err == nil {
					sectors, _ = strconv.ParseInt(strings.TrimSpace(output), 10, 64)
					if sectors >= expectedSectors {
						i.logger.Infof("Kernel updated device %v capacity to %v sectors", devPath, sectors)
						return nil
					}
				}
				return forceRetryErr
			},
			retry.Attempts(uint(maxWaitDeviceRetries+1)),
			retry.Delay(waitDeviceInterval),
			retry.DelayType(retry.FixedDelay),
			retry.LastErrorOnly(true),
		)
	}

	if sectors < expectedSectors {
		return fmt.Errorf("timeout waiting for device %v to reach expected size %v", devPath, expectedSectors)
	}
	return i.reloadLinearDmDevice()
}

// IsSuspended checks if the linear dm device is suspended
func (i *Initiator) IsSuspended() (bool, error) {
	devices, err := util.DmsetupInfo(i.Name, i.executor)
	if err != nil {
		return false, err
	}

	for _, device := range devices {
		if device.Name == i.Name {
			return device.Suspended, nil
		}
	}
	return false, fmt.Errorf("failed to find linear dm device %s", i.Name)
}

func (i *Initiator) reloadLinearDmDevice() error {
	devPath := fmt.Sprintf("/dev/%s", i.dev.Source.Name)

	// Get the size of the device
	opts := []string{
		"--getsize", devPath,
	}
	output, err := i.executor.Execute(nil, util.BlockdevBinary, opts, types.ExecuteTimeout)
	if err != nil {
		return err
	}
	sectors, err := strconv.ParseInt(strings.TrimSpace(output), 10, 64)
	if err != nil {
		return err
	}

	table := fmt.Sprintf("0 %v linear %v 0", sectors, devPath)

	i.logger.Infof("Reloading linear dm device with table '%s'", table)

	err = util.DmsetupReload(i.Name, table, i.executor)
	if err != nil {
		return err
	}

	// Reload the device numbers
	dmDevPath := getDmDevicePath(i.Name)
	major, minor, err := util.GetDeviceNumbers(dmDevPath, i.executor)
	if err != nil {
		return err
	}

	i.dev.Export.Name = i.Name
	i.dev.Export.Major = major
	i.dev.Export.Minor = minor

	return nil
}

func getDmDevicePath(name string) string {
	return filepath.Join("/dev/mapper", name)
}
