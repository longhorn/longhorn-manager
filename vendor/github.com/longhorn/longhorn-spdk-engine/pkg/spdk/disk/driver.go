package disk

import (
	"fmt"

	"github.com/pkg/errors"

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
				if diskName == bdev.Name {
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
