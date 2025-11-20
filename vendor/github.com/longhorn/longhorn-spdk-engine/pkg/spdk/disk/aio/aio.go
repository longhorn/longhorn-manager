package aio

import (
	"fmt"
	"io"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	commontypes "github.com/longhorn/go-common-libs/types"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	spdkutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk"
)

type DiskDriverAio struct {
}

func init() {
	driver := &DiskDriverAio{}
	disk.RegisterDiskDriver(string(commontypes.DiskDriverAio), driver)
}

func (d *DiskDriverAio) DiskCreate(spdkClient *spdkclient.Client, diskName, diskPath string, blockSize uint64, denyInUseDisk bool) (string, error) {
	if err := validateDiskCreation(spdkClient, diskPath, denyInUseDisk); err != nil {
		return "", errors.Wrap(err, "failed to validate disk creation")
	}

	return spdkClient.BdevAioCreate(diskPath, diskName, blockSize)
}

func (d *DiskDriverAio) DiskDelete(spdkClient *spdkclient.Client, diskName, diskPath string) (deleted bool, err error) {
	return spdkClient.BdevAioDelete(diskName)
}

func (d *DiskDriverAio) DiskGet(spdkClient *spdkclient.Client, diskName, diskPath string, timeout uint64) ([]spdktypes.BdevInfo, error) {
	return spdkClient.BdevAioGet(diskName, timeout)
}

func validateDiskCreation(spdkClient *spdkclient.Client, diskPath string, denyInUseDisk bool) error {
	ok, err := spdkutil.IsBlockDevice(diskPath)
	if err != nil {
		return errors.Wrap(err, "failed to check if disk is a block device")
	}
	if !ok {
		return errors.Wrapf(err, "disk %v is not a block device", diskPath)
	}

	size, err := getDiskDeviceSize(diskPath)
	if err != nil {
		return errors.Wrap(err, "failed to get disk size")
	}
	if size == 0 {
		return fmt.Errorf("disk %v size is 0", diskPath)
	}

	if denyInUseDisk {
		executor, err := spdkutil.NewExecutor(commontypes.ProcDirectory)
		if err != nil {
			return errors.Wrapf(err, "failed to get the executor for AIO disk %v", diskPath)
		}
		if spdkutil.IsBlockDeviceInUse(diskPath, executor) {
			return fmt.Errorf("disk %v is in use (filesystem or partition table is detected). Set `deny-adding-in-use-block-disk` to false or wipe all data on the disk", diskPath)
		}
	}

	return nil
}

func getDiskDeviceSize(path string) (int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to open %s", path)
	}
	defer func() {
		if errClose := file.Close(); errClose != nil {
			logrus.WithError(errClose).Errorf("Failed to close disk device %s", path)
		}
	}()

	pos, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to seek %s", path)
	}
	return pos, nil
}
