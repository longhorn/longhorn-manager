package initiator

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
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

	validateDiskCreationTimeout = 30 // seconds

	UnInitializedUblkId      = -1
	MaxUblkId                = 65535
	DefaultUblkQueueDepth    = 128
	DefaultUblkNumberOfQueue = 1
)

const (
	maxConnectTargetRetries    = 15
	retryConnectTargetInterval = 1 * time.Second

	maxWaitDeviceRetries = 60
	waitDeviceInterval   = 1 * time.Second
)

var (
	idGenerator         IDGenerator
	isUblkTargetCreated = false
)

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
	BdevName string
	UblkID   int32
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
		return nil, fmt.Errorf("empty subsystem for NVMe-oF initiator creation")
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

// WaitForNVMeTCPConnect waits for the NVMe-oF initiator to connect
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

	for r := 0; r < maxRetries; r++ {
		err = i.loadNVMeDeviceInfoWithoutLock(i.NVMeTCPInfo.TransportAddress, i.NVMeTCPInfo.TransportServiceID, i.NVMeTCPInfo.SubsystemNQN)
		if err == nil {
			return nil
		}
		time.Sleep(retryInterval)
	}

	return err
}

// WaitForNVMeTCPTargetDisconnect waits for the NVMe-oF initiator to disconnect
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

	for r := 0; r < maxRetries; r++ {
		err = i.loadNVMeDeviceInfoWithoutLock(i.NVMeTCPInfo.TransportAddress, i.NVMeTCPInfo.TransportServiceID, i.NVMeTCPInfo.SubsystemNQN)
		if types.ErrorIsValidNvmeDeviceNotFound(err) {
			return nil
		}
		time.Sleep(retryInterval)
	}

	return err
}

// Suspend suspends the device mapper device for the NVMe-oF initiator
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
		return errors.Wrapf(err, "failed to check if linear dm device is suspended for NVMe-oF initiator %s", i.Name)
	}

	if !suspended {
		if err := i.suspendLinearDmDevice(noflush, nolockfs); err != nil {
			return errors.Wrapf(err, "failed to suspend linear dm device for NVMe-oF initiator %s", i.Name)
		}
	}

	return nil
}

// Resume resumes the device mapper device for the NVMe-oF initiator
func (i *Initiator) Resume() error {
	if i.hostProc != "" {
		lock, err := i.newLock()
		if err != nil {
			return err
		}
		defer lock.Unlock()
	}

	if err := i.resumeLinearDmDevice(); err != nil {
		return errors.Wrapf(err, "failed to resume linear dm device for NVMe-oF initiator %s", i.Name)
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

// StartNvmeTCPInitiator starts the NVMe-oF initiator with the given transportAddress and transportServiceID
func (i *Initiator) StartNvmeTCPInitiator(transportAddress, transportServiceID string, dmDeviceAndEndpointCleanupRequired bool) (dmDeviceIsBusy bool, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "failed to start NVMe-oF initiator %s", i.Name)
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
	}).Info("Starting NVMe-oF initiator")

	if i.hostProc != "" {
		lock, err := i.newLock()
		if err != nil {
			return false, err
		}
		defer lock.Unlock()
	}

	// Check if the initiator/NVMe-oF device is already launched and matches the params
	err = i.loadNVMeDeviceInfoWithoutLock(i.NVMeTCPInfo.TransportAddress, i.NVMeTCPInfo.TransportServiceID, i.NVMeTCPInfo.SubsystemNQN)
	if err == nil {
		if i.NVMeTCPInfo.TransportAddress == transportAddress && i.NVMeTCPInfo.TransportServiceID == transportServiceID {
			err = i.LoadEndpointForNvmeTcpFrontend(false)
			if err == nil {
				i.logger.Info("NVMe-oF initiator is already launched with correct params")
				return false, nil
			}
			i.logger.WithError(err).Warnf("NVMe-oF initiator is launched with failed to load the endpoint")
		} else {
			i.logger.Warnf("NVMe-oF initiator is launched but with incorrect address, the required one is %s:%s, will try to stop then relaunch it", transportAddress, transportServiceID)
		}
	}

	i.logger.Info("Stopping NVMe-oF initiator blindly before starting")
	dmDeviceIsBusy, err = i.stopWithoutLock(nil, dmDeviceAndEndpointCleanupRequired, false, false)
	if err != nil {
		return dmDeviceIsBusy, errors.Wrapf(err, "failed to stop the mismatching NVMe-oF initiator %s before starting", i.Name)
	}

	i.logger.Info("Launching NVMe-oF initiator")

	i.connectNVMeTCPTarget(transportAddress, transportServiceID, maxConnectTargetRetries, retryConnectTargetInterval)
	if i.NVMeTCPInfo.ControllerName == "" {
		return dmDeviceIsBusy, fmt.Errorf("failed to start NVMe-oF initiator %s within %d * %v sec retries", i.Name, maxConnectTargetRetries, retryConnectTargetInterval.Seconds())
	}

	err = i.waitAndLoadNVMeDeviceInfoWithoutLock(transportAddress, transportServiceID)
	if err != nil {
		return dmDeviceIsBusy, errors.Wrapf(err, "failed to load device info after connecting target for NVMe-oF initiator %s", i.Name)
	}

	if dmDeviceAndEndpointCleanupRequired {
		if dmDeviceIsBusy {
			// Endpoint is already created, just replace the target device
			i.logger.Info("Linear dm device is busy, trying the best to replace the target device for NVMe-oF initiator")
			if err := i.replaceDmDeviceTarget(); err != nil {
				i.logger.WithError(err).Warnf("Failed to replace the target device for NVMe-oF initiator")
			} else {
				i.logger.Info("Successfully replaced the target device for NVMe-oF initiator")
				dmDeviceIsBusy = false
			}
		} else {
			i.logger.Info("Creating linear dm device for NVMe-oF initiator")
			if err := i.createLinearDmDevice(); err != nil {
				return false, errors.Wrapf(err, "failed to create linear dm device for NVMe-oF initiator %s", i.Name)
			}
		}
	} else {
		i.logger.Info("Skipping creating linear dm device for NVMe-oF initiator")
		i.dev.Export = i.dev.Source
	}

	i.logger.Infof("Creating endpoint %v", i.Endpoint)
	exist, err := i.isEndpointExist()
	if err != nil {
		return dmDeviceIsBusy, errors.Wrapf(err, "failed to check if endpoint %v exists for NVMe-oF initiator %s", i.Endpoint, i.Name)
	}
	if exist {
		i.logger.Infof("Skipping endpoint %v creation for NVMe-oF initiator", i.Endpoint)
	} else {
		if err := i.makeEndpoint(); err != nil {
			return dmDeviceIsBusy, err
		}
	}

	i.logger.Infof("Launched NVMe-oF initiator: %+v", i)

	return dmDeviceIsBusy, nil
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

	if !isUblkTargetCreated {
		if err := spdkClient.UblkCreateTarget("", true); err != nil {
			return false, err
		}
		isUblkTargetCreated = true
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
	if err := spdkClient.UblkStartDisk(i.UblkInfo.BdevName, availableUblkID, DefaultUblkQueueDepth, DefaultUblkNumberOfQueue); err != nil {
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
	for r := 0; r < maxWaitDeviceRetries; r++ {
		err = i.loadNVMeDeviceInfoWithoutLock(transportAddress, transportServiceID, i.NVMeTCPInfo.SubsystemNQN)
		if err == nil {
			break
		}
		time.Sleep(waitDeviceInterval)
	}
	return err
}

func (i *Initiator) connectNVMeTCPTarget(transportAddress, transportServiceID string, maxRetries int, retryInterval time.Duration) {
	if i.NVMeTCPInfo == nil {
		logrus.Warnf("Failed to connectTarget because nvmeTCPInfo is nil")
	}
	for r := 0; r < maxRetries; r++ {
		// Rerun this API for a discovered target should be fine
		subsystemNQN, err := DiscoverTarget(transportAddress, transportServiceID, i.executor)
		if err != nil {
			i.logger.WithError(err).Warn("Failed to discover target")
			time.Sleep(retryInterval)
			continue
		}

		controllerName, err := ConnectTarget(transportAddress, transportServiceID, subsystemNQN, i.executor)
		if err != nil {
			i.logger.WithError(err).Warn("Failed to connect target")
			time.Sleep(retryInterval)
			continue
		}

		i.NVMeTCPInfo.SubsystemNQN = subsystemNQN
		i.NVMeTCPInfo.ControllerName = controllerName
		break
	}
}

// Stop stops the NVMe-oF initiator
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
			return dmDeviceIsBusy, errors.Wrapf(err, "failed to disconnect target for NVMe-oF initiator %s", i.Name)
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
		return fmt.Errorf("found zero or multiple devices NVMe-oF initiator %s", i.Name)
	}
	if len(nvmeDevices[0].Namespaces) != 1 {
		return fmt.Errorf("found zero or multiple devices for NVMe-oF initiator %s", i.Name)
	}
	if i.NVMeTCPInfo.ControllerName != "" && i.NVMeTCPInfo.ControllerName != nvmeDevices[0].Controllers[0].Controller {
		return fmt.Errorf("found mismatching between the detected controller name %s and the recorded value %s for NVMe-oF initiator %s", nvmeDevices[0].Controllers[0].Controller, i.NVMeTCPInfo.ControllerName, i.Name)
	}

	i.NVMeTCPInfo.ControllerName = nvmeDevices[0].Controllers[0].Controller
	i.NVMeTCPInfo.NamespaceName = nvmeDevices[0].Namespaces[0].NameSpace
	i.NVMeTCPInfo.TransportAddress, i.NVMeTCPInfo.TransportServiceID = GetIPAndPortFromControllerAddress(nvmeDevices[0].Controllers[0].Address)
	i.logger = i.logger.WithFields(logrus.Fields{
		"controllerName":     i.NVMeTCPInfo.ControllerName,
		"namespaceName":      i.NVMeTCPInfo.NamespaceName,
		"transportAddress":   i.NVMeTCPInfo.TransportAddress,
		"transportServiceID": i.NVMeTCPInfo.TransportServiceID,
	})

	devPath := filepath.Join("/dev", i.NVMeTCPInfo.NamespaceName)
	dev, err := util.DetectDevice(devPath, i.executor)
	if err != nil {
		return errors.Wrapf(err, "cannot find the device for NVMe-oF initiator %s with namespace name %s", i.Name, i.NVMeTCPInfo.NamespaceName)
	}

	i.dev = &util.LonghornBlockDevice{
		Source: *dev,
	}
	return nil
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
			return fmt.Errorf("detected device %s name mismatching from endpoint %v for NVMe-oF initiator %s", dev.Name, i.Endpoint, i.Name)
		}
	}

	i.dev = &util.LonghornBlockDevice{
		Export: *dev,
	}
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
	if err := validateDiskCreation(dmDevPath, validateDiskCreationTimeout); err != nil {
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

func validateDiskCreation(path string, timeout int) error {
	for i := 0; i < timeout; i++ {
		isBlockDev, _ := util.IsBlockDevice(path)
		if isBlockDev {
			return nil
		}
		time.Sleep(time.Second * 1)
	}

	return fmt.Errorf("failed to validate device %s creation", path)
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
