package spdk

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	commontypes "github.com/longhorn/go-common-libs/types"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	spdkutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/util"

	spdkdisk "github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk"

	_ "github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk/aio"
	_ "github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk/nvme"
	_ "github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk/virtio-blk"
	_ "github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk/virtio-scsi"
)

const (
	defaultClusterSize = 1 * 1024 * 1024 // 1MB
	defaultBlockSize   = 4096            // 4KB

	hostPrefix = "/host"
)

type DiskState string

const (
	DiskStateUnknown  = DiskState("")
	DiskStateError    = DiskState("error")
	DiskStateReady    = DiskState("ready")
	DiskStateCreating = DiskState("creating")
)

type Disk struct {
	sync.RWMutex

	Name string
	UUID string

	DiskDriver string
	DiskID     string
	DiskPath   string
	DiskType   string

	TotalSize   int64
	FreeSize    int64
	TotalBlocks int64
	FreeBlocks  int64
	BlockSize   int64
	ClusterSize int64

	State DiskState
}

func NewDisk(diskName, diskUUID, diskPath, diskDriver string, blockSize int64) *Disk {
	return &Disk{
		DiskPath:  diskPath,
		BlockSize: blockSize,
		DiskType:  DiskTypeBlock,
		State:     DiskStateCreating,
	}
}

func (d *Disk) DiskCreate(spdkClient *spdkclient.Client, diskName, diskUUID, diskPath, diskDriver string, blockSize int64) (err error) {
	log := logrus.WithFields(logrus.Fields{
		"diskName":   diskName,
		"diskUUID":   diskUUID,
		"diskPath":   diskPath,
		"blockSize":  blockSize,
		"diskDriver": diskDriver,
	})

	log.Info("Creating disk")

	d.Lock()
	defer func() {
		if err != nil {
			d.State = DiskStateError
			log.WithError(err).Error("Failed to create disk")
		} else {
			d.State = DiskStateReady
			log.Info("Created disk successfully")
		}
		d.Unlock()
	}()

	if diskName == "" || diskPath == "" {
		return grpcstatus.Error(grpccodes.InvalidArgument, "disk name and disk path are required")
	}

	exactDiskDriver, err := spdkdisk.GetDiskDriver(commontypes.DiskDriver(diskDriver), diskPath)
	if err != nil {
		log.WithError(err).Error("Failed to determine disk driver")
		return grpcstatus.Errorf(grpccodes.InvalidArgument, "failed to get disk driver for disk %q: %v", diskName, err)
	}
	d.DiskDriver = string(exactDiskDriver)

	lvstoreUUID, err := addBlockDevice(spdkClient, diskName, diskUUID, diskPath, exactDiskDriver, blockSize)
	if err != nil {
		log.WithError(err).Error("Failed to add block device")
		return grpcstatus.Errorf(grpccodes.Internal, "failed to add disk block device: %v", err)
	}

	diskID, err := getDiskID(diskPath, exactDiskDriver)
	if err != nil {
		log.WithError(err).Error("Failed to get disk ID")
		return grpcstatus.Errorf(grpccodes.Internal, "failed to get disk ID for %q: %v", diskName, err)
	}
	d.DiskID = diskID

	if err := d.lvstoreToDisk(spdkClient, "", lvstoreUUID); err != nil {
		log.WithError(err).Error("Failed to update disk from lvstore")
		return grpcstatus.Errorf(grpccodes.Internal, "failed to update disk from lvstore: %v", err)
	}

	return nil
}

func (d *Disk) DiskDelete(spdkClient *spdkclient.Client, diskName, diskUUID, diskPath, diskDriver string) (ret *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"diskName":   diskName,
		"diskUUID":   diskUUID,
		"diskPath":   diskPath,
		"diskDriver": diskDriver,
	})

	log.Info("Deleting disk")

	d.Lock()
	defer d.Unlock()

	defer func() {
		if err != nil {
			log.WithError(err).Error("Failed to delete disk")
		} else {
			log.Info("Deleted disk")
		}
	}()

	if diskName == "" {
		return &emptypb.Empty{}, grpcstatus.Error(grpccodes.InvalidArgument, "disk name is required")
	}

	if diskUUID != "" {
		lvstores, err := spdkClient.BdevLvolGetLvstore("", diskUUID)
		if err != nil {
			if !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
				return nil, errors.Wrapf(err, "failed to get lvstore with UUID %v", diskUUID)
			}
		}

		if len(lvstores) == 0 {
			log.Infof("Lvstore not found for disk %v (UUID %v); treating as already deleted", diskName, diskUUID)
		} else if lvstores[0].UUID != diskUUID {
			log.Warnf("Lvstore UUID mismatch (expected %v, found %v); proceed with bdev deletion", diskUUID, lvstores[0].UUID)
		}
	} else {
		// The disk is not successfully created in creation stage because the diskUUID is not provided,
		// so we blindly use the diskName as the bdevName here.
		log.Warn("Disk UUID is not provided, blindly delete the disk")
	}

	if diskDriver == "" {
		diskDriver = d.DiskDriver
	}
	if _, err := spdkdisk.DiskDelete(spdkClient, diskName, diskPath, diskDriver); err != nil {
		return nil, errors.Wrapf(err, "failed to delete disk %v", diskName)
	}

	return &emptypb.Empty{}, nil
}

type DeviceInfo struct {
	DeviceDriver string `json:"device_driver"`
}

func (d *Disk) updateDiskFromLvstoreNoLock(spdkClient *spdkclient.Client, diskName, diskPath, diskDriver string) (string, error) {
	bdevs, err := spdkdisk.DiskGet(spdkClient, diskName, diskPath, diskDriver, 0)
	if err != nil {
		if !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return "", grpcstatus.Errorf(grpccodes.Internal, "failed to get bdev with name %q: %v", diskName, err)
		}
	}
	if len(bdevs) == 0 {
		return "", grpcstatus.Errorf(grpccodes.NotFound, "no disk bdev found with name %q", diskName)
	}

	var (
		targetBdev      *spdktypes.BdevInfo
		exactDiskDriver commontypes.DiskDriver
	)

	for i := range bdevs {
		bdev := &bdevs[i]
		switch bdev.ProductName {
		case spdktypes.BdevProductNameAio:
			if bdev.DriverSpecific != nil {
				diskPath = util.RemovePrefix(bdev.DriverSpecific.Aio.FileName, hostPrefix)
				exactDiskDriver = commontypes.DiskDriverAio
				targetBdev = bdev
			}
		case spdktypes.BdevProductNameVirtioBlk:
			exactDiskDriver = commontypes.DiskDriverVirtioBlk
			targetBdev = bdev
		case spdktypes.BdevProductNameVirtioScsi:
			exactDiskDriver = commontypes.DiskDriverVirtioScsi
			targetBdev = bdev
		case spdktypes.BdevProductNameNvme:
			exactDiskDriver = commontypes.DiskDriverNvme
			targetBdev = bdev
		}

		if targetBdev != nil {
			break
		}
	}

	if targetBdev == nil {
		return "", grpcstatus.Errorf(grpccodes.NotFound, "no matching disk bdev found for %q", diskName)
	}

	diskID, err := getDiskID(diskPath, exactDiskDriver)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get disk ID")
	}

	d.DiskID = diskID
	d.DiskDriver = string(exactDiskDriver)

	return targetBdev.Name, nil
}

func (d *Disk) DiskGet(spdkClient *spdkclient.Client, diskName, diskPath, diskDriver string) (ret *spdkrpc.Disk, err error) {
	log := logrus.WithFields(logrus.Fields{
		"diskName":   diskName,
		"diskPath":   diskPath,
		"diskDriver": diskDriver,
	})

	log.Trace("Getting disk info")

	defer func() {
		if err != nil {
			log.WithError(err).Error("Failed to get disk info")
		} else {
			log.Trace("Got disk info")
		}
	}()

	if diskName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name is required")
	}

	d.RLock()
	if d.State == DiskStateCreating || d.State == DiskStateError {
		defer d.RUnlock()
		return &spdkrpc.Disk{
			Id:          d.DiskID,
			Name:        d.Name,
			Uuid:        d.UUID,
			Path:        d.DiskPath,
			Type:        DiskTypeBlock,
			Driver:      d.DiskDriver,
			TotalSize:   d.TotalSize,
			FreeSize:    d.FreeSize,
			TotalBlocks: d.TotalBlocks,
			FreeBlocks:  d.FreeBlocks,
			BlockSize:   d.BlockSize,
			ClusterSize: d.ClusterSize,
			State:       string(d.State),
		}, nil
	}
	d.RUnlock()

	d.Lock()
	diskBdevName, err := d.updateDiskFromLvstoreNoLock(spdkClient, diskName, diskPath, diskDriver)
	if err != nil {
		d.Unlock()
		return nil, err
	}
	if err := d.lvstoreToDisk(spdkClient, diskBdevName, ""); err != nil {
		d.Unlock()
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to update disk from lvstore: %v", err)
	}
	d.Unlock()

	d.RLock()
	defer d.RUnlock()
	return &spdkrpc.Disk{
		Id:          d.DiskID,
		Name:        d.Name,
		Uuid:        d.UUID,
		Path:        d.DiskPath,
		Type:        DiskTypeBlock,
		Driver:      d.DiskDriver,
		TotalSize:   d.TotalSize,
		FreeSize:    d.FreeSize,
		TotalBlocks: d.TotalBlocks,
		FreeBlocks:  d.FreeBlocks,
		BlockSize:   d.BlockSize,
		ClusterSize: d.ClusterSize,
		State:       string(d.State),
	}, nil
}

func getDiskPath(path string) string {
	return filepath.Join(hostPrefix, path)
}

func getDiskIDFromDeviceNumber(filename string) (string, error) {
	executor, err := spdkutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return "", err
	}

	dev, err := spdkutil.DetectDevice(filename, executor)
	if err != nil {
		return "", errors.Wrapf(err, "failed to detect disk device %v", filename)
	}

	return fmt.Sprintf("%d-%d", dev.Major, dev.Minor), nil
}

func validateAioDiskCreation(spdkClient *spdkclient.Client, diskPath string, diskDriver commontypes.DiskDriver) error {
	diskID, err := getDiskIDFromDeviceNumber(getDiskPath(diskPath))
	if err != nil {
		return errors.Wrap(err, "failed to get disk device number")
	}

	bdevs, err := spdkdisk.DiskGet(spdkClient, "", "", string(diskDriver), 0)
	if err != nil {
		return errors.Wrap(err, "failed to get disk bdevs")
	}

	for _, bdev := range bdevs {
		id, err := getDiskIDFromDeviceNumber(bdev.DriverSpecific.Aio.FileName)
		if err != nil {
			return errors.Wrap(err, "failed to get disk device number")
		}

		if id == diskID {
			return fmt.Errorf("disk %v is already used by disk bdev %v", diskPath, bdev.Name)
		}
	}

	return nil
}

func addBlockDevice(spdkClient *spdkclient.Client, diskName, diskUUID, originalDiskPath string, diskDriver commontypes.DiskDriver, blockSize int64) (string, error) {
	log := logrus.WithFields(logrus.Fields{
		"diskName":   diskName,
		"diskUUID":   diskUUID,
		"diskPath":   originalDiskPath,
		"diskDriver": diskDriver,
		"blockSize":  blockSize,
	})

	diskPath := originalDiskPath
	if diskDriver == commontypes.DiskDriverAio {
		if err := validateAioDiskCreation(spdkClient, diskPath, diskDriver); err != nil {
			return "", errors.Wrap(err, "failed to validate disk creation")
		}
		diskPath = getDiskPath(originalDiskPath)
	}

	log.Info("Creating disk bdev")

	bdevName, err := spdkdisk.DiskCreate(spdkClient, diskName, diskPath, string(diskDriver), uint64(blockSize))
	if err != nil {
		if !jsonrpc.IsJSONRPCRespErrorFileExists(err) {
			return "", errors.Wrapf(err, "failed to create disk bdev")
		}
	}

	bdevs, err := spdkdisk.DiskGet(spdkClient, bdevName, diskPath, "", 0)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get disk bdev")
	}
	if len(bdevs) == 0 {
		return "", fmt.Errorf("cannot find disk bdev with name %v", bdevName)
	}
	if len(bdevs) > 1 {
		return "", fmt.Errorf("found multiple disk bdevs with name %v", bdevName)
	}
	bdev := bdevs[0]

	// Name of the lvstore is the same as the name of the disk bdev
	lvstoreName := bdev.Name

	log.Infof("Creating lvstore %v", lvstoreName)

	lvstores, err := spdkClient.BdevLvolGetLvstore("", "")
	if err != nil {
		return "", errors.Wrapf(err, "failed to get lvstores")
	}

	for _, lvstore := range lvstores {
		if lvstore.BaseBdev != bdevName {
			continue
		}

		if diskUUID != "" && diskUUID != lvstore.UUID {
			continue
		}

		log.Infof("Found an existing lvstore %v", lvstore.Name)
		if lvstore.Name == lvstoreName {
			return lvstore.UUID, nil
		}

		// Rename the existing lvstore to the name of the disk bdev if the UUID matches
		log.Infof("Renaming the existing lvstore %v to %v", lvstore.Name, lvstoreName)
		renamed, err := spdkClient.BdevLvolRenameLvstore(lvstore.Name, lvstoreName)
		if err != nil {
			return "", errors.Wrapf(err, "failed to rename lvstore %v to %v", lvstore.Name, lvstoreName)
		}
		if !renamed {
			return "", fmt.Errorf("failed to rename lvstore %v to %v", lvstore.Name, lvstoreName)
		}
		return lvstore.UUID, nil
	}

	if diskUUID == "" {
		log.Infof("Creating a new lvstore %v", lvstoreName)
		return spdkClient.BdevLvolCreateLvstore(bdev.Name, lvstoreName, defaultClusterSize)
	}

	// The lvstore should be created before, but it cannot be found now.
	return "", grpcstatus.Error(grpccodes.NotFound, fmt.Sprintf("cannot find lvstore with UUID %v", diskUUID))
}

func getDiskID(diskPath string, diskDriver commontypes.DiskDriver) (string, error) {
	var err error

	diskID := diskPath
	if diskDriver == commontypes.DiskDriverAio {
		diskID, err = getDiskIDFromDeviceNumber(getDiskPath(diskPath))
		if err != nil {
			return "", errors.Wrapf(err, "failed to get disk ID")
		}
	}
	return diskID, nil
}

func (d *Disk) lvstoreToDisk(spdkClient *spdkclient.Client, lvstoreName, lvstoreUUID string) error {
	lvstores, err := spdkClient.BdevLvolGetLvstore(lvstoreName, lvstoreUUID)
	if err != nil {
		return errors.Wrapf(err, "failed to get lvstore with name %v and UUID %v", lvstoreName, lvstoreUUID)
	}
	lvstore := &lvstores[0]

	d.Name = lvstore.Name
	d.UUID = lvstore.UUID

	d.TotalSize = int64(lvstore.TotalDataClusters * lvstore.ClusterSize)
	d.FreeSize = int64(lvstore.FreeClusters * lvstore.ClusterSize)
	d.TotalBlocks = int64(lvstore.TotalDataClusters * lvstore.ClusterSize / lvstore.BlockSize)
	d.FreeBlocks = int64(lvstore.FreeClusters * lvstore.ClusterSize / lvstore.BlockSize)
	d.BlockSize = int64(lvstore.BlockSize)
	d.ClusterSize = int64(lvstore.ClusterSize)

	return nil
}

func (d *Disk) diskHealthGet(spdkClient *spdkclient.Client, diskName, diskDriver string) (*spdktypes.BdevNvmeControllerHealthInfo, error) {
	if diskName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name is required")
	}

	// Currently, SPDK health info is only available for NVMe bdev controllers.
	if diskDriver != "" && diskDriver != string(commontypes.DiskDriverNvme) {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "disk driver %q does not support health info", diskDriver)
	}

	d.Lock()
	defer d.Unlock()

	healthInfo, err := spdkClient.BdevNvmeGetControllerHealthInfo(diskName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get NVMe bdev controller health info for disk %q", diskName)
	}

	return &healthInfo, nil
}

func isNvmeDriver(diskDriver, diskPath string) bool {
	if diskDriver == string(commontypes.DiskDriverNvme) {
		return true
	}

	exactDiskDriver, err := spdkdisk.GetDiskDriver(commontypes.DiskDriver(diskDriver), diskPath)
	if err != nil {
		logrus.WithError(err).Warnf("failed to get disk driver for driver %v and path %s", diskDriver, diskPath)
		return false
	}

	return exactDiskDriver == commontypes.DiskDriverNvme
}
