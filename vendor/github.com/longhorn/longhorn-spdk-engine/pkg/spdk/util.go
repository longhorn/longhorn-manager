package spdk

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-spdk-helper/pkg/initiator"
	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"

	commonns "github.com/longhorn/go-common-libs/ns"
	commontypes "github.com/longhorn/go-common-libs/types"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"
)

func connectNVMeTarget(srcIP string, srcPort int32, maxRetries int, retryInterval time.Duration) (string, string, error) {
	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to create executor")
	}

	subsystemNQN := ""
	controllerName := ""
	for r := 0; r < maxRetries; r++ {
		subsystemNQN, err = initiator.DiscoverTarget(srcIP, strconv.Itoa(int(srcPort)), executor)
		if err != nil {
			logrus.WithError(err).Warnf("Failed to discover target for with address %v:%v", srcIP, srcPort)
			time.Sleep(retryInterval)
			continue
		}

		controllerName, err = initiator.ConnectTarget(srcIP, strconv.Itoa(int(srcPort)), subsystemNQN, executor)
		if err != nil {
			logrus.WithError(err).Warnf("Failed to connect target with address %v:%v", srcIP, srcPort)
			time.Sleep(retryInterval)
			continue
		}
		// break when it successfully discover and connect the target
		break
	}
	if subsystemNQN == "" || controllerName == "" {
		return "", "", errors.Wrapf(err, "timeout connecting target with address %v:%v", srcIP, srcPort)
	}
	return subsystemNQN, controllerName, nil
}

func exposeSnapshotLvolBdev(spdkClient *spdkclient.Client, lvsName, lvolName, ip string, port int32, executor *commonns.Executor) (subsystemNQN, controllerName string, err error) {
	bdevLvolList, err := spdkClient.BdevLvolGet(spdktypes.GetLvolAlias(lvsName, lvolName), 0)
	if err != nil {
		return "", "", err
	}
	if len(bdevLvolList) == 0 {
		return "", "", errors.Errorf("cannot find lvol bdev %v for backup", lvolName)
	}

	portStr := strconv.Itoa(int(port))
	err = spdkClient.StartExposeBdev(helpertypes.GetNQN(lvolName), bdevLvolList[0].UUID, generateNGUID(lvolName), ip, portStr)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to expose snapshot lvol bdev %v", lvolName)
	}

	for r := 0; r < maxNumRetries; r++ {
		subsystemNQN, err = initiator.DiscoverTarget(ip, portStr, executor)
		if err != nil {
			logrus.WithError(err).Errorf("Failed to discover target for snapshot lvol bdev %v", lvolName)
			time.Sleep(retryInterval)
			continue
		}

		controllerName, err = initiator.ConnectTarget(ip, portStr, subsystemNQN, executor)
		if err != nil {
			logrus.WithError(err).Errorf("Failed to connect target for snapshot lvol bdev %v", lvolName)
			time.Sleep(retryInterval)
			continue
		}
		// break when it successfully discover and connect the target
		break
	}
	return subsystemNQN, controllerName, nil
}

func splitHostPort(address string) (string, int32, error) {
	if strings.Contains(address, ":") {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return "", 0, errors.Wrapf(err, "failed to split host and port from address %v", address)
		}

		portAsInt := 0
		if port != "" {
			portAsInt, err = strconv.Atoi(port)
			if err != nil {
				return "", 0, errors.Wrapf(err, "failed to parse port %v", port)
			}
		}
		return host, int32(portAsInt), nil
	}

	return address, 0, nil
}

// connectNVMfBdev connects to the NVMe-oF target, which is exposed by a remote lvol bdev.
// controllerName is typically the lvol name, and address is the IP:port of the NVMe-oF target.
func connectNVMfBdev(spdkClient *spdkclient.Client, controllerName, address string, ctrlrLossTimeout, fastIOFailTimeoutSec int) (bdevName string, err error) {
	if controllerName == "" || address == "" {
		return "", fmt.Errorf("controllerName or address is empty")
	}

	defer func() {
		if err != nil {
			if _, detachErr := spdkClient.BdevNvmeDetachController(controllerName); detachErr != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(detachErr) {
				logrus.WithError(detachErr).Errorf("Failed to detach NVMe controller %s after failing at attaching it", controllerName)
			}
		}
	}()

	ip, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}

	// Blindly detach the controller in case of the previous replica connection is not cleaned up correctly
	if _, err := spdkClient.BdevNvmeDetachController(controllerName); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return "", err
	}

	nvmeBdevNameList, err := spdkClient.BdevNvmeAttachController(controllerName, helpertypes.GetNQN(controllerName),
		ip, port, spdktypes.NvmeTransportTypeTCP, spdktypes.NvmeAddressFamilyIPv4,
		int32(ctrlrLossTimeout), replicaReconnectDelaySec, int32(fastIOFailTimeoutSec), replicaMultipath)
	if err != nil {
		return "", err
	}

	if len(nvmeBdevNameList) != 1 {
		return "", fmt.Errorf("got zero or multiple results when attaching lvol %s with address %s as a NVMe bdev: %+v", controllerName, address, nvmeBdevNameList)
	}

	return nvmeBdevNameList[0], nil
}

func disconnectNVMfBdev(spdkClient *spdkclient.Client, bdevName string) error {
	if bdevName == "" {
		return nil
	}
	if _, err := spdkClient.BdevNvmeDetachController(helperutil.GetNvmeControllerNameFromNamespaceName(bdevName)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return err
	}
	return nil
}

func GetSnapXattr(spdkClient *spdkclient.Client, alias, key string) (string, error) {
	value, err := spdkClient.BdevLvolGetXattr(alias, key)
	if err != nil {
		return "", err
	}
	return value, nil
}

func GetLvsNameByUUID(spdkClient *spdkclient.Client, lvsUUID string) (string, error) {
	if lvsUUID == "" {
		return "", fmt.Errorf("empty UUID provided when getting logical volume store name")
	}
	var lvsList []spdktypes.LvstoreInfo
	lvsList, err := spdkClient.BdevLvolGetLvstore("", lvsUUID)
	if err != nil {
		return "", err
	}
	if len(lvsList) != 1 {
		return "", fmt.Errorf("expected exactly one lvstore for UUID %s, but found %d", lvsUUID, len(lvsList))
	}
	return lvsList[0].Name, nil
}

// ExtractBackingImageAndDiskUUID extracts the BackingImageName and DiskUUID from the string pattern "bi-${BackingImageName}-disk-${DiskUUID}"
func ExtractBackingImageAndDiskUUID(lvolName string) (string, string, error) {
	// Define the regular expression pattern
	// This captures the BackingImageName and DiskUUID while allowing for hyphens in both.
	re := regexp.MustCompile(`^bi-([a-zA-Z0-9-]+)-disk-([a-zA-Z0-9-]+)$`)

	// Try to find a match
	matches := re.FindStringSubmatch(lvolName)
	if matches == nil {
		return "", "", fmt.Errorf("lvolName does not match the expected pattern")
	}

	// Extract BackingImageName and DiskUUID from the matches
	backingImageName := matches[1]
	diskUUID := matches[2]

	return backingImageName, diskUUID, nil
}
