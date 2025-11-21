package virtioscsi

import (
	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"

	commontypes "github.com/longhorn/go-common-libs/types"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdksetup "github.com/longhorn/go-spdk-helper/pkg/spdk/setup"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk"
)

type DiskDriverVirtioScsi struct {
}

func init() {
	driver := &DiskDriverVirtioScsi{}
	disk.RegisterDiskDriver(string(commontypes.DiskDriverVirtioScsi), driver)
}

func (d *DiskDriverVirtioScsi) DiskCreate(spdkClient *spdkclient.Client, diskName, diskPath string, blockSize uint64, denyInUseDisk bool) (string, error) {
	// TODO: validate the diskPath
	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get the executor for virtio-scsi disk create %v", diskPath)
	}

	_, err = spdksetup.Bind(diskPath, "", executor)
	if err != nil {
		return "", errors.Wrapf(err, "failed to bind virtio-scsi disk %v", diskPath)
	}
	defer func() {
		if err != nil {
			logrus.WithError(err).Warnf("Unbinding virtio-scsi disk %v since failed to attach", diskPath)

			_, errUnbind := spdksetup.Unbind(diskPath, executor)
			if errUnbind != nil {
				logrus.WithError(errUnbind).Warnf("Failed to unbind virtio-scsi disk %v since failed to attach", diskPath)
			}
		}
	}()

	bdevs, err := spdkClient.BdevVirtioAttachController(diskName, "pci", diskPath, "scsi")
	if err != nil {
		return "", errors.Wrapf(err, "failed to attach virtio-scsi disk %v", diskPath)
	}
	if len(bdevs) == 0 {
		return "", errors.Errorf("failed to attach virtio-scsi disk %v", diskPath)
	}
	return bdevs[0], nil
}

func (d *DiskDriverVirtioScsi) DiskDelete(spdkClient *spdkclient.Client, diskName, diskPath string) (deleted bool, err error) {
	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get the executor for virtio-scsi disk %v deletion", diskName)
	}

	_, err = spdkClient.BdevVirtioDetachController(diskName)
	if err != nil {
		if !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return false, errors.Wrapf(err, "failed to detach virtio-scsi disk %v", diskName)
		}
	}

	_, err = spdksetup.Unbind(diskPath, executor)
	if err != nil {
		return false, errors.Wrapf(err, "failed to unbind virtio-scsi disk %v", diskPath)
	}
	return true, nil
}

func (d *DiskDriverVirtioScsi) DiskGet(spdkClient *spdkclient.Client, diskName, diskPath string, timeout uint64) ([]spdktypes.BdevInfo, error) {
	return spdkClient.BdevGetBdevs(diskName, timeout)
}
