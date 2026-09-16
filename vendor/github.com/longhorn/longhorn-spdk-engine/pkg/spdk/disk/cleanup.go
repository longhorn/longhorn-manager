package disk

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	commontypes "github.com/longhorn/go-common-libs/types"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdksetup "github.com/longhorn/go-spdk-helper/pkg/spdk/setup"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"
)

// ReleaseOrphanDevice releases the device of a disk whose driver is unknown,
// which happens when a disk creation is interrupted before its metadata is
// persisted. A PCI device left bound to a userspace driver is invisible to the
// kernel, so without releasing it the disk can never be provisioned again.
func ReleaseOrphanDevice(spdkClient *spdkclient.Client, diskName, diskPathOrBdf string) (released bool, err error) {
	if diskPathOrBdf == "" {
		return false, errors.Errorf("disk path is required for releasing the device of disk %v", diskName)
	}

	if !isBDF(diskPathOrBdf) {
		// A non-BDF path can only be backed by an AIO bdev.
		return DiskDelete(spdkClient, diskName, diskPathOrBdf, string(commontypes.DiskDriverAio))
	}

	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return false, errors.Wrap(err, "failed to get the executor for releasing orphan device")
	}

	diskStatus, err := spdksetup.GetDiskStatus(diskPathOrBdf, executor)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get disk status for BDF %s", diskPathOrBdf)
	}

	if !IsDetachedFromKernelDriver(diskStatus.Driver) {
		return false, nil
	}

	// The disk driver only selects the deletion that detaches the bdev before
	// unbinding the device. Unlike disk creation, a virtio device does not have to
	// be told apart here, since detaching a controller that was never attached is
	// tolerated and the unbind is common to both virtio drivers.
	var diskDriver commontypes.DiskDriver
	switch {
	case strings.EqualFold(diskStatus.Type, PciDeviceTypeNvme):
		diskDriver = commontypes.DiskDriverNvme
	case strings.EqualFold(diskStatus.Type, PciDeviceTypeVirtio):
		diskDriver = commontypes.DiskDriverVirtioBlk
	default:
		return false, errors.Errorf("cannot release device %s of disk %v because its type %q is not a disk type: %s",
			diskPathOrBdf, diskName, diskStatus.Type, fmt.Sprintf(UnbindHintFmt, diskPathOrBdf))
	}

	logrus.Infof("Releasing orphan device %s of type %q not driven by the kernel, current driver %q", diskPathOrBdf, diskStatus.Type, diskStatus.Driver)

	return DiskDelete(spdkClient, diskName, diskPathOrBdf, string(diskDriver))
}
