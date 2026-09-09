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

// PCI device types reported by the SPDK setup script for a BDF. They are derived
// from a PCI class/vendor scan, so they stay accurate even after the device has
// been bound to a userspace PCI driver such as vfio-pci.
const (
	PciDeviceTypeNvme   = "NVMe"
	PciDeviceTypeVirtio = "virtio"
)

// PciDriverNone is what the SPDK setup script reports for a device that is not
// bound to any driver.
const PciDriverNone = "-"

// UnbindHintFmt tells the user how to release a device that is still held by a
// userspace PCI driver.
const UnbindHintFmt = "unbind it first with '/usr/src/spdk/scripts/setup.sh unbind %s' or specify the disk driver explicitly instead of using 'auto'"

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

// IsBoundToUserspaceDriver reports whether the device is currently bound to a
// userspace PCI driver. Such a device is invisible to the kernel, so it has
// neither a block device node nor a kernel driver to derive the disk driver from.
func IsBoundToUserspaceDriver(driver string) bool {
	return isVfioPci(driver) || isUioPciGeneric(driver)
}

// IsDetachedFromKernelDriver reports whether the device exposes no block device
// because the kernel does not drive it. An interrupted disk creation leaves the
// device either bound to a userspace PCI driver or, when the userspace bind
// failed after the kernel driver was already released, bound to nothing at all.
func IsDetachedFromKernelDriver(driver string) bool {
	return IsBoundToUserspaceDriver(driver) || driver == "" || driver == PciDriverNone
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
		if IsDetachedFromKernelDriver(diskStatus.Driver) {
			return getDriverForDetachedDevice(diskStatus, bdf)
		}

		devName, err := util.GetDevNameFromBDF(bdf)
		if err != nil {
			return "", errors.Wrapf(err, "failed to get device name from BDF %s", bdf)
		}
		return getDriverForAuto(diskStatus, fmt.Sprintf("/dev/%s", devName))
	case commontypes.DiskDriverAio, commontypes.DiskDriverNvme, commontypes.DiskDriverVirtioScsi, commontypes.DiskDriverVirtioBlk:
		return diskDriver, nil
	default:
		return commontypes.DiskDriverNone, fmt.Errorf("unsupported disk driver %s for BDF %s", diskDriver, bdf)
	}
}

// getDriverForDetachedDevice resolves the disk driver of a device that the
// kernel does not drive, which typically happens when a previous disk creation
// was interrupted while rebinding the device. There is no kernel driver nor
// block device to derive the disk driver from, so the PCI device type reported
// by the SPDK setup script is used instead. Without this, such a device can
// never be resolved back to nvme and the disk stays unusable forever.
func getDriverForDetachedDevice(diskStatus *helpertypes.DiskStatus, bdf string) (commontypes.DiskDriver, error) {
	if strings.EqualFold(diskStatus.Type, PciDeviceTypeNvme) {
		return commontypes.DiskDriverNvme, nil
	}

	// A virtio device without a kernel driver exposes no block device, so
	// virtio-blk and virtio-scsi cannot be told apart here.
	return commontypes.DiskDriverNone, fmt.Errorf("cannot determine the disk driver of device %s of type %q because the kernel does not drive it, current driver %q: %s",
		bdf, diskStatus.Type, diskStatus.Driver, fmt.Sprintf(UnbindHintFmt, bdf))
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
	bdfFormat := "^[a-f0-9]{4}:[a-f0-9]{2}:[a-f0-9]{2}\\.[a-f0-9]{1}$"
	bdfPattern := regexp.MustCompile(bdfFormat)
	return bdfPattern.MatchString(addr)
}
