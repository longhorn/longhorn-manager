package disk

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
)

type DiskDriver interface {
	DiskCreate(*spdkclient.Client, string, string, uint64) (string, error)
	DiskDelete(*spdkclient.Client, string, string) (bool, error)
	DiskGet(*spdkclient.Client, string, string, uint64) ([]spdktypes.BdevInfo, error)
}

var (
	diskDrivers map[string]DiskDriver
)

func init() {
	diskDrivers = make(map[string]DiskDriver)
}

func RegisterDiskDriver(diskDriver string, ops DiskDriver) {
	diskDrivers[diskDriver] = ops
}

func DiskCreate(spdkClient *spdkclient.Client, diskName, diskPath, diskDriver string, blockSize uint64) (string, error) {
	driver, ok := diskDrivers[diskDriver]
	if !ok {
		return "", fmt.Errorf("disk driver %s is not registered", diskDriver)
	}

	return driver.DiskCreate(spdkClient, diskName, diskPath, blockSize)
}

func DiskDelete(spdkClient *spdkclient.Client, diskName, diskPath, diskDriver string) (bool, error) {
	driver, ok := diskDrivers[diskDriver]
	if !ok {
		return false, fmt.Errorf("disk driver %s is not registered", diskDriver)
	}

	return driver.DiskDelete(spdkClient, diskName, diskPath)
}

func DiskGet(spdkClient *spdkclient.Client, diskName, diskPath, diskDriver string, timeout uint64) ([]spdktypes.BdevInfo, error) {
	if diskDriver == "" {
		if !isBDF(diskPath) {
			return spdkClient.BdevGetBdevs(diskName, 0)
		}
		bdevs, err := spdkClient.BdevGetBdevs("", 0)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get bdevs")
		}
		foundBdevs := []spdktypes.BdevInfo{}
		for _, bdev := range bdevs {
			if bdev.DriverSpecific == nil {
				continue
			}
			if bdev.DriverSpecific.Nvme == nil {
				match, err := IsVirtioScsiBdevOfDisk(bdev.Name, diskName)
				if err != nil {
					logrus.WithError(err).Warnf("Failed to check if bdev %v belongs to disk %v", bdev.Name, diskName)
					continue
				}
				if match {
					foundBdevs = append(foundBdevs, bdev)
				}
			} else {
				nvmes := *bdev.DriverSpecific.Nvme
				for _, nvme := range nvmes {
					if nvme.PciAddress == diskPath {
						foundBdevs = append(foundBdevs, bdev)
					}
				}
			}
		}
		return foundBdevs, nil
	}

	driver, ok := diskDrivers[diskDriver]
	if !ok {
		return nil, fmt.Errorf("disk driver %s is not registered", diskDriver)
	}

	return driver.DiskGet(spdkClient, diskName, diskPath, timeout)
}

// IsVirtioScsiBdevOfDisk reports whether bdevName belongs to the virtio-scsi
// controller named diskName. SPDK names such bdevs "<diskName>t<target>",
// where <target> is a decimal SCSI target number (uint8).
func IsVirtioScsiBdevOfDisk(bdevName, diskName string) (bool, error) {
	if bdevName == diskName {
		return true, nil
	}
	suffix, ok := strings.CutPrefix(bdevName, diskName+"t")
	if !ok || suffix == "" {
		return false, nil
	}
	n, err := strconv.Atoi(suffix) // entire remainder must be decimal
	if err != nil {
		return false, errors.Wrapf(err, "failed to parse virtio-scsi target from bdev name %v", bdevName)
	}
	if n < 0 || n > 255 {
		return false, nil
	}
	return true, nil
}
