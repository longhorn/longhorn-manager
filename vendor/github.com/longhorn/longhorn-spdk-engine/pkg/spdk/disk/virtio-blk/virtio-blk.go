package virtioblk

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	commontypes "github.com/longhorn/go-common-libs/types"
	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdksetup "github.com/longhorn/go-spdk-helper/pkg/spdk/setup"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk"
)

type DiskDriverVirtioBlk struct {
}

func init() {
	driver := &DiskDriverVirtioBlk{}
	disk.RegisterDiskDriver(string(commontypes.DiskDriverVirtioBlk), driver)
}

func (d *DiskDriverVirtioBlk) DiskCreate(spdkClient *spdkclient.Client, diskName, diskPath string, blockSize uint64) (string, error) {
	// TODO: validate the diskPath
	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get the executor for virtio-blk disk create %v", diskPath)
	}

	_, err = spdksetup.Bind(diskPath, "", executor)
	if err != nil {
		return "", errors.Wrapf(err, "failed to bind virtio-blk disk %v", diskPath)
	}
	defer func() {
		if err != nil {
			logrus.WithError(err).Warnf("Unbinding virtio-blk disk %v since failed to attach", diskPath)

			_, errUnbind := spdksetup.Unbind(diskPath, executor)
			if errUnbind != nil {
				logrus.WithError(errUnbind).Warnf("Failed to unbind virtio-blk disk %v since failed to attach", diskPath)
			}
		}
	}()

	bdevs, err := spdkClient.BdevVirtioAttachController(diskName, "pci", diskPath, "blk")
	if err != nil {
		return "", errors.Wrapf(err, "failed to attach virtio-blk disk %v", diskPath)
	}
	if len(bdevs) == 0 {
		return "", errors.Errorf("failed to attach virtio-blk disk %v", diskPath)
	}
	return bdevs[0], nil
}

func (d *DiskDriverVirtioBlk) DiskDelete(spdkClient *spdkclient.Client, diskName, diskPath string) (deleted bool, err error) {
	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get the executor for virtio-blk disk %v deletion", diskName)
	}

	_, err = spdkClient.BdevVirtioDetachController(diskName)
	if err != nil {
		if !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return false, errors.Wrapf(err, "failed to detach virtio-blk disk %v", diskName)
		}
	}

	_, err = spdksetup.Unbind(diskPath, executor)
	if err != nil {
		return false, errors.Wrapf(err, "failed to unbind virtio-blk disk %v", diskPath)
	}
	return true, nil
}

func (d *DiskDriverVirtioBlk) DiskGet(spdkClient *spdkclient.Client, diskName, diskPath string, timeout uint64) ([]spdktypes.BdevInfo, error) {
	return spdkClient.BdevGetBdevs(diskName, timeout)
}
