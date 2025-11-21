package nvme

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	commontypes "github.com/longhorn/go-common-libs/types"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdksetup "github.com/longhorn/go-spdk-helper/pkg/spdk/setup"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk"
)

const (
	// Timeouts for disk bdev
	diskCtrlrLossTimeoutSec  = 30
	diskReconnectDelaySec    = 2
	diskFastIOFailTimeoutSec = 15
	diskTransportAckTimeout  = 10
	diskKeepAliveTimeoutMs   = 10000
	diskMultipath            = "disable"
)

type DiskDriverNvme struct {
}

func init() {
	driver := &DiskDriverNvme{}
	disk.RegisterDiskDriver(string(commontypes.DiskDriverNvme), driver)
}

func (d *DiskDriverNvme) DiskCreate(spdkClient *spdkclient.Client, diskName, diskPath string, blockSize uint64, denyInUseDisk bool) (string, error) {
	// TODO: validate the diskPath
	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get the executor for NVMe disk create %v", diskPath)
	}

	_, err = spdksetup.Bind(diskPath, "", executor)
	if err != nil {
		return "", errors.Wrapf(err, "failed to bind NVMe disk %v", diskPath)
	}
	defer func() {
		if err != nil {
			logrus.WithError(err).Warnf("Unbinding NVMe disk %v since failed to attach", diskPath)

			_, errUnbind := spdksetup.Unbind(diskPath, executor)
			if errUnbind != nil {
				logrus.WithError(errUnbind).Warnf("Failed to unbind NVMe disk %v since failed to attach", diskPath)
			}
		}
	}()
	bdevs, err := spdkClient.BdevNvmeAttachController(diskName, "", diskPath, "", "PCIe", "",
		diskCtrlrLossTimeoutSec, diskReconnectDelaySec, diskFastIOFailTimeoutSec, diskMultipath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to attach NVMe disk %v", diskPath)
	}
	if len(bdevs) == 0 {
		return "", errors.Errorf("failed to attach NVMe disk %v", diskPath)
	}
	return bdevs[0], nil
}

func (d *DiskDriverNvme) DiskDelete(spdkClient *spdkclient.Client, diskName, diskPath string) (deleted bool, err error) {
	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get the executor for NVMe disk %v deletion", diskName)
	}

	controllers, err := spdkClient.BdevNvmeGetControllers("")
	if err != nil {
		return false, errors.Wrap(err, "failed to get NVMe controllers")
	}

	for _, controller := range controllers {
		for _, ctrl := range controller.Ctrlrs {
			if ctrl.Trid.Traddr == diskPath && strings.ToLower(string(ctrl.Trid.Trtype)) == "pcie" {
				logrus.Infof("Detaching NVMe controller %v", controller.Name)
				_, err = spdkClient.BdevNvmeDetachController(controller.Name)
				if err != nil {
					logrus.WithError(err).Warnf("Failed to detach NVMe controller %v", controller.Name)
				}
				break
			}
		}
	}

	_, err = spdksetup.Unbind(diskPath, executor)
	if err != nil {
		logrus.WithError(err).Warnf("Failed to unbind NVMe disk %v", diskPath)
	}
	return true, nil
}

func (d *DiskDriverNvme) DiskGet(spdkClient *spdkclient.Client, diskName, diskPath string, timeout uint64) ([]spdktypes.BdevInfo, error) {
	bdevs, err := spdkClient.BdevGetBdevs("", 0)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get bdevs")
	}
	foundBdevs := []spdktypes.BdevInfo{}
	for _, bdev := range bdevs {
		if bdev.DriverSpecific == nil {
			continue
		}
		if bdev.DriverSpecific.Nvme == nil {
			continue
		}
		nvmes := *bdev.DriverSpecific.Nvme
		for _, nvme := range nvmes {
			if nvme.PciAddress == diskPath {
				foundBdevs = append(foundBdevs, bdev)
			}
		}
	}
	return foundBdevs, nil
}
