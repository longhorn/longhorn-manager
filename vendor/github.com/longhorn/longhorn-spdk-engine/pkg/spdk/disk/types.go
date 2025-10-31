package disk

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"

	commontypes "github.com/longhorn/go-common-libs/types"
	spdksetup "github.com/longhorn/go-spdk-helper/pkg/spdk/setup"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
)

type BlockDiskSubsystem string

const (
	BlockDiskSubsystemVirtio = BlockDiskSubsystem("virtio")
	BlockDiskSubsystemPci    = BlockDiskSubsystem("pci")
	BlockDiskSubsystemNvme   = BlockDiskSubsystem("nvme")
	BlockDiskSubsystemScsi   = BlockDiskSubsystem("scsi")
)

type BlockDiskType string

const (
	BlockDiskTypeDisk = BlockDiskType("disk")
	BlockDiskTypeLoop = BlockDiskType("loop")
)

func GetDiskDriver(diskDriver commontypes.DiskDriver, diskPathOrBdf string) (commontypes.DiskDriver, error) {
	if isBDF(diskPathOrBdf) {
		return getDiskDriverForBDF(diskDriver, diskPathOrBdf)
	}

	return getDiskDriverForPath(diskDriver, diskPathOrBdf)
}

// isVfioPci checks if the given driver is vfio_pci or a variant of it.
func isVfioPci(driver string) bool {
	normalized := strings.ReplaceAll(driver, "-", "_")
	return normalized == string(commontypes.DiskDriverVfioPci)
}

// isUioPciGeneric checks if the given driver is uio_pci_generic or a variant of it.
func isUioPciGeneric(driver string) bool {
	normalized := strings.ReplaceAll(driver, "-", "_")
	return normalized == string(commontypes.DiskDriverUioPciGeneric)
}

func getDiskDriverForBDF(diskDriver commontypes.DiskDriver, bdf string) (commontypes.DiskDriver, error) {
	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get the executor for disk driver detection")
	}

	diskStatus, err := spdksetup.GetDiskStatus(bdf, executor)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get disk status for BDF %s", bdf)
	}

	switch diskDriver {
	case commontypes.DiskDriverAuto:
		diskPath := ""
		if !isVfioPci(diskStatus.Driver) && !isUioPciGeneric(diskStatus.Driver) {
			devName, err := util.GetDevNameFromBDF(bdf)
			if err != nil {
				return "", errors.Wrapf(err, "failed to get device name from BDF %s", bdf)
			}
			diskPath = fmt.Sprintf("/dev/%s", devName)
		}
		return getDriverForAuto(diskStatus, diskPath)
	case commontypes.DiskDriverAio, commontypes.DiskDriverNvme, commontypes.DiskDriverVirtioScsi, commontypes.DiskDriverVirtioBlk:
		return diskDriver, nil
	default:
		return commontypes.DiskDriverNone, fmt.Errorf("unsupported disk driver %s for BDF %s", diskDriver, bdf)
	}
}

func getDriverForAuto(diskStatus *helpertypes.DiskStatus, diskPath string) (commontypes.DiskDriver, error) {
	// SPDK supports various types of disks, including NVMe, virtio-blk, and virtio-scsi.
	//
	// NVMe disks can be managed by either NVMe bdev or AIO bdev.
	// VirtIO disks can be managed by virtio-blk, virtio-scsi, or AIO bdev.
	//
	// To use the correct bdev,  need to identify the disk type.
	// Here's how to identify the disk type:
	// - If a block device uses the subsystems virtio and pci, it's a virtio-blk disk.
	// - If it uses the subsystems virtio, pci, and scsi, it's a virtio-scsi disk.
	switch diskStatus.Driver {
	case string(commontypes.DiskDriverNvme):
		return commontypes.DiskDriverNvme, nil
	case string(commontypes.DiskDriverVirtioPci):
		blockdevice, err := util.GetBlockDevice(diskPath)
		if err != nil {
			return commontypes.DiskDriverNone, errors.Wrapf(err, "failed to get blockdevice info for %s", diskPath)
		}

		if slices.Contains(blockdevice.Subsystems, string(BlockDiskSubsystemVirtio)) && slices.Contains(blockdevice.Subsystems, string(BlockDiskSubsystemPci)) {
			diskDriver := commontypes.DiskDriverVirtioBlk
			if slices.Contains(blockdevice.Subsystems, string(BlockDiskSubsystemScsi)) {
				diskDriver = commontypes.DiskDriverVirtioScsi
			}
			return diskDriver, nil
		}

		return commontypes.DiskDriverNone, fmt.Errorf("unsupported disk driver %s for disk path %s", diskStatus.Driver, diskPath)
	default:
		return commontypes.DiskDriverNone, fmt.Errorf("unsupported disk driver %s for disk path %s", diskStatus.Driver, diskPath)
	}
}

func getDiskDriverForPath(diskDriver commontypes.DiskDriver, diskPath string) (commontypes.DiskDriver, error) {
	switch diskDriver {
	case commontypes.DiskDriverAuto, commontypes.DiskDriverAio:
		return commontypes.DiskDriverAio, nil
	default:
		return commontypes.DiskDriverNone, fmt.Errorf("unsupported disk driver %s for disk path %s", diskDriver, diskPath)
	}
}

func isBDF(addr string) bool {
	bdfFormat := "[a-f0-9]{4}:[a-f0-9]{2}:[a-f0-9]{2}\\.[a-f0-9]{1}"
	bdfPattern := regexp.MustCompile(bdfFormat)
	return bdfPattern.MatchString(addr)
}
