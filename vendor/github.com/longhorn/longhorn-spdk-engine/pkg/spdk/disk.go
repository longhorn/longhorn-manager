package spdk

import (
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	commontypes "github.com/longhorn/go-common-libs/types"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	spdkutil "github.com/longhorn/go-spdk-helper/pkg/util"

	spdkdisk "github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"

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
	DiskPaths  []string
	DiskType   string

	TotalSize   int64
	FreeSize    int64
	TotalBlocks int64
	FreeBlocks  int64
	BlockSize   int64
	ClusterSize int64

	State DiskState
}

func NewDisk(diskName, diskUUID, diskDriver string, diskPaths []string, blockSize int64) *Disk {
	return &Disk{
		DiskPaths: diskPaths,
		BlockSize: blockSize,
		DiskType:  DiskTypeBlock,
		State:     DiskStateCreating,
	}
}

func (d *Disk) DiskCreate(spdkClient *spdkclient.Client, diskName, diskUUID, diskDriver string, diskPaths []string, blockSize int64) error {
	log := logrus.WithFields(logrus.Fields{
		"diskName":   diskName,
		"diskUUID":   diskUUID,
		"diskPaths":  diskPaths,
		"blockSize":  blockSize,
		"diskDriver": diskDriver,
	})

	log.Info("Creating disk")

	if diskName == "" || len(diskPaths) == 0 {
		return grpcstatus.Error(grpccodes.InvalidArgument, "disk name and disk path are required")
	}

	// Determine the exact disk driver from the given disk paths
	var exactDiskDriver commontypes.DiskDriver
	for _, diskPath := range diskPaths {
		diskDriver, err := spdkdisk.GetDiskDriver(commontypes.DiskDriver(diskDriver), diskPath)
		if err != nil {
			log.WithError(err).Error("Failed to determine disk driver")
			return grpcstatus.Errorf(grpccodes.InvalidArgument, "failed to get disk driver for disk %q: %v", diskName, err)
		}

		if exactDiskDriver == "" {
			exactDiskDriver = diskDriver
		} else if exactDiskDriver != diskDriver {
			log.Errorf("Inconsistent disk drivers for disk paths: %v and %v", exactDiskDriver, diskDriver)
			return grpcstatus.Errorf(grpccodes.InvalidArgument, "inconsistent disk drivers for disk %q: %v and %v", diskName, exactDiskDriver, diskDriver)
		}
	}

	lvstoreUUID, err := addBlockDevice(spdkClient, diskName, diskUUID, diskPaths, exactDiskDriver, blockSize)
	if err != nil {
		log.WithError(err).Error("Failed to add block device")
		return grpcstatus.Errorf(grpccodes.Internal, "failed to add disk block device: %v", err)
	}

	diskID, err := getDiskID(diskPaths, exactDiskDriver)
	if err != nil {
		log.WithError(err).Error("Failed to get disk ID")
		return grpcstatus.Errorf(grpccodes.Internal, "failed to get disk ID for %q: %v", diskName, err)
	}

	d.Lock()
	defer d.Unlock()

	d.DiskDriver = string(exactDiskDriver)
	d.DiskID = diskID

	if err := d.lvstoreToDisk(spdkClient, "", lvstoreUUID); err != nil {
		d.State = DiskStateError
		log.WithError(err).Error("Failed to update disk from lvstore")
		return grpcstatus.Errorf(grpccodes.Internal, "failed to update disk from lvstore: %v", err)
	}

	d.State = DiskStateReady
	log.Info("Created disk successfully")
	return nil
}

func (d *Disk) DiskDelete(spdkClient *spdkclient.Client, diskName, diskUUID, diskDriver string, diskPaths []string) (ret *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"diskName":   diskName,
		"diskUUID":   diskUUID,
		"diskPaths":  diskPaths,
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

		log.Infof("Deleting lvstore with UUID %v", diskUUID)
		_, err = spdkClient.BdevLvolDeleteLvstore("", diskUUID)
		if err != nil {
			if !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
				log.Warnf("Failed to delete lvstore with UUID %v: %v", diskUUID, err)
			}
		}
	} else {
		// The disk is not successfully created in creation stage because the diskUUID is not provided,
		// so we blindly use the diskName as the bdevName here.
		log.Warn("Disk UUID is not provided, blindly delete the disk")
	}

	if diskDriver == "" {
		diskDriver = d.DiskDriver
	}

	// if only one disk path, delete directly
	if len(diskPaths) == 1 {
		if _, err := spdkdisk.DiskDelete(spdkClient, diskName, diskPaths[0], diskDriver); err != nil {
			if jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
				log.Infof("Disk path %v for disk %v not found; treating as already deleted", diskPaths[0], diskName)
			}
		}

		return &emptypb.Empty{}, nil
	}

	// multiple disk paths, delete one by one
	// the name of each sub-disk is generated from the disk name and disk path
	// not using disk name directly
	log.Infof("Deleting raid bdev %v", diskName)
	_, err = spdkClient.BdevRaidDelete(diskName)
	if err != nil {
		if !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return &emptypb.Empty{}, errors.Wrapf(err, "failed to delete raid bdev %v", diskName)
		}
		log.Infof("Raid bdev %v not found; treating as already deleted", diskName)
	}

	var errs error
	for _, diskPath := range diskPaths {
		subDiskName := makeBaseBdevNameFromPath(diskName, diskPath)
		if _, err := spdkdisk.DiskDelete(spdkClient, subDiskName, diskPath, diskDriver); err != nil {
			if jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
				log.Infof("Disk path %v for disk %v not found; treating as already deleted", diskPath, subDiskName)
				continue
			}
			errs = multierr.Append(errs, errors.Wrapf(err, "failed to delete disk path %v for disk %v", diskPath, subDiskName))
		}
	}
	if errs != nil {
		return nil, errs
	}

	return &emptypb.Empty{}, nil
}

type DeviceInfo struct {
	DeviceDriver string `json:"device_driver"`
}

func (d *Disk) updateDiskFromLvstoreNoLock(spdkClient *spdkclient.Client, diskName, diskDriver string, diskPaths []string) (string, error) {
	if len(diskPaths) == 1 {
		return d.refreshSingleDiskInfo(spdkClient, diskName, diskPaths[0], diskDriver)
	}

	return d.refreshRaidDiskInfo(spdkClient, diskName, diskDriver, diskPaths)
}

func (d *Disk) refreshSingleDiskInfo(spdkClient *spdkclient.Client, diskName, diskPath, diskDriver string) (string, error) {
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

	diskID, err := getDiskID([]string{diskPath}, exactDiskDriver)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get disk ID")
	}

	d.DiskID = diskID
	d.DiskDriver = string(exactDiskDriver)

	return targetBdev.Name, nil
}

func (d *Disk) refreshRaidDiskInfo(spdkClient *spdkclient.Client, diskName, diskDriver string, diskPaths []string) (string, error) {
	raidDisk, err := spdkClient.BdevRaidGet(diskName, 0)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get raid bdev %v", diskName)
	}
	if len(raidDisk) == 0 {
		return "", grpcstatus.Error(grpccodes.NotFound, fmt.Sprintf("cannot find raid bdev with name %v", diskName))
	}
	raidBdev := &raidDisk[0]

	// Determine the exact disk driver from the given disk paths
	var exactDiskDriver commontypes.DiskDriver
	for _, diskPath := range diskPaths {
		diskDriver, err := spdkdisk.GetDiskDriver(commontypes.DiskDriver(diskDriver), diskPath)
		if err != nil {
			return "", errors.Wrapf(err, "failed to get disk driver for disk %q", diskName)
		}

		if exactDiskDriver == "" {
			exactDiskDriver = diskDriver
			break
		}
	}

	diskID, err := getDiskID(diskPaths, exactDiskDriver)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get disk ID")
	}

	d.DiskID = diskID
	d.DiskDriver = string(exactDiskDriver)

	return raidBdev.Name, nil
}

func (d *Disk) DiskGet(spdkClient *spdkclient.Client, diskName, diskDriver string, diskPaths []string) (ret *spdkrpc.Disk, err error) {
	log := logrus.WithFields(logrus.Fields{
		"diskName":   diskName,
		"diskPaths":  diskPaths,
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

	if d.State == DiskStateCreating {
		d.RLock()
		defer d.RUnlock()
		return &spdkrpc.Disk{
			Id:          d.DiskID,
			Name:        d.Name,
			Uuid:        d.UUID,
			Path:        d.DiskPaths,
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

	d.Lock()
	if diskDriver == "" {
		diskDriver = d.DiskDriver
	}

	diskBdevName, err := d.updateDiskFromLvstoreNoLock(spdkClient, diskName, diskDriver, diskPaths)
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
		Path:        d.DiskPaths,
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

func addBlockDevice(spdkClient *spdkclient.Client, diskName, diskUUID string, originalDiskPaths []string, diskDriver commontypes.DiskDriver, blockSize int64) (uuid string, err error) {
	log := logrus.WithFields(logrus.Fields{
		"diskName":   diskName,
		"diskUUID":   diskUUID,
		"diskPaths":  originalDiskPaths,
		"diskDriver": diskDriver,
		"blockSize":  blockSize,
	})

	log.Info("Creating disk bdev")

	bdevInfo := spdktypes.BdevInfo{}
	switch len(originalDiskPaths) {
	case 0:
		return "", fmt.Errorf("no disk paths provided")
	case 1:
		// Single disk path, create a single base bdev

		// Ideally we would keep the storage stack fully consistent, and support using
		// RAID concat even when there is only a single underlying disk.
		// However, for compatibility, existing disks that already contain
		// lvols cannot have their storage stack changed from:
		//     bdev -> lvs
		// to:
		//     bdev -> raid -> lvs
		// Therefore, when there is only one base bdev, we retain the original
		// non-RAID layout. RAID concat is used only when multiple base bdevs are present.
		baseBdevInfo, err := createBaseDiskBdev(spdkClient, diskName, originalDiskPaths[0], diskDriver, blockSize)
		if err != nil {
			return "", errors.Wrapf(err, "failed to create base disk bdev")
		}
		bdevInfo = baseBdevInfo
	default:
		// Multiple disk paths, create multiple base bdevs
		diskBdevs := []string{}
		for _, diskPath := range originalDiskPaths {
			baseDiskName := makeBaseBdevNameFromPath(diskName, diskPath)
			baseBdevInfo, err := createBaseDiskBdev(spdkClient, baseDiskName, diskPath, diskDriver, blockSize)
			if err != nil {
				return "", errors.Wrapf(err, "failed to create base disk bdev")
			}
			diskBdevs = append(diskBdevs, baseBdevInfo.Name)
		}

		raidBdevInfo, err := createRaidBdev(spdkClient, diskName, diskBdevs)
		if err != nil {
			return "", errors.Wrapf(err, "failed to create raid %s with bdevs %v", diskName, diskBdevs)
		}

		bdevInfo = raidBdevInfo
	}

	// Name of the lvstore is the same as the name of the disk bdev
	lvstoreName := bdevInfo.Name

	log.Infof("Creating lvstore %v", lvstoreName)

	lvstores, err := spdkClient.BdevLvolGetLvstore("", "")
	if err != nil {
		return "", errors.Wrapf(err, "failed to get lvstores")
	}

	for _, lvstore := range lvstores {
		if lvstore.BaseBdev != bdevInfo.Name {
			continue
		}

		if diskUUID != "" && diskUUID != lvstore.UUID {
			continue
		}

		log.Infof("Found an existing lvstore %v", lvstore.Name)

		// Validate the existing lvstore block size matches the bdev.
		// If there is a mismatch, the lvstore is incompatible with this base bdev.
		// For data safety, do NOT delete or modify the lvstore here.
		// Caller must decide how to handle this situation.
		if lvstore.BlockSize != uint64(bdevInfo.BlockSize) {
			return "", fmt.Errorf("lvstore %v block size %v does not match bdev %v block size %v", lvstore.Name, lvstore.BlockSize, bdevInfo.Name, bdevInfo.BlockSize)
		}

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
		return spdkClient.BdevLvolCreateLvstore(bdevInfo.Name, lvstoreName, defaultClusterSize)
	}

	// The lvstore should be created before, but it cannot be found now.
	return "", grpcstatus.Error(grpccodes.NotFound, fmt.Sprintf("cannot find lvstore with UUID %v", diskUUID))
}

func createRaidBdev(spdkClient *spdkclient.Client, diskName string, diskBdevs []string) (spdktypes.BdevInfo, error) {
	slices.Sort(diskBdevs)

	if _, err := spdkClient.BdevRaidCreate(diskName, spdktypes.BdevRaidLevelConcat, 4, diskBdevs, ""); err != nil {
		if !jsonrpc.IsJSONRPCRespErrorFileExists(err) {
			return spdktypes.BdevInfo{}, errors.Wrapf(err, "failed to create raid concat bdev %v", diskName)
		}
	}

	raidBdevs, err := spdkClient.BdevRaidGet(diskName, 0)
	if err != nil {
		return spdktypes.BdevInfo{}, errors.Wrapf(err, "failed to get raid bdev %v", diskName)
	}
	if len(raidBdevs) == 0 {
		return spdktypes.BdevInfo{}, fmt.Errorf("cannot find raid bdev with name %v", diskName)
	}

	return raidBdevs[0], nil
}

func createBaseDiskBdev(spdkClient *spdkclient.Client, diskName, originalDiskPath string, diskDriver commontypes.DiskDriver, blockSize int64) (spdktypes.BdevInfo, error) {
	diskPath := originalDiskPath
	if diskDriver == commontypes.DiskDriverAio {
		if err := validateAioDiskCreation(spdkClient, diskPath, diskDriver); err != nil {
			return spdktypes.BdevInfo{}, errors.Wrap(err, "failed to validate disk creation")
		}
		diskPath = getDiskPath(diskPath)
	}

	bdevName, err := spdkdisk.DiskCreate(spdkClient, diskName, diskPath, string(diskDriver), uint64(blockSize))
	if err != nil {
		if !jsonrpc.IsJSONRPCRespErrorFileExists(err) {
			return spdktypes.BdevInfo{}, errors.Wrapf(err, "failed to create disk bdev")
		}
	}

	bdevs, err := spdkdisk.DiskGet(spdkClient, bdevName, diskPath, "", 0)
	if err != nil {
		return spdktypes.BdevInfo{}, errors.Wrapf(err, "failed to get disk bdev")
	}
	if len(bdevs) == 0 {
		return spdktypes.BdevInfo{}, fmt.Errorf("cannot find disk bdev with name %v", bdevName)
	}
	if len(bdevs) > 1 {
		return spdktypes.BdevInfo{}, fmt.Errorf("found multiple disk bdevs with name %v", bdevName)
	}

	return bdevs[0], nil
}

func getDiskID(diskPaths []string, diskDriver commontypes.DiskDriver) (string, error) {
	var err error

	if len(diskPaths) == 0 {
		return "", errors.New("no disk paths provided")
	}

	diskIDs := []string{}
	for _, diskPath := range diskPaths {
		diskID := ""
		if diskDriver == commontypes.DiskDriverAio {
			diskID, err = getDiskIDFromDeviceNumber(getDiskPath(diskPath))
			if err != nil {
				return "", errors.Wrapf(err, "failed to get disk ID")
			}
		} else {
			diskID = diskPath
		}

		diskIDs = append(diskIDs, diskID)
	}

	if len(diskIDs) == 1 {
		return diskIDs[0], nil
	}

	sort.Strings(diskIDs)
	return strings.Join(diskIDs, "-"), nil
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

func (d *Disk) diskHealthGet(spdkClient *spdkclient.Client, diskName, diskPath string, diskDriver string) (*spdktypes.BdevNvmeControllerHealthInfo, error) {
	if diskName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name is required")
	}

	// Currently, SPDK health info is only available for NVMe bdev controllers.
	if diskDriver != "" && diskDriver != string(commontypes.DiskDriverNvme) {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "disk driver %q does not support health info", diskDriver)
	}

	d.Lock()
	defer d.Unlock()

	// Attempt to get RAID bdev for this disk
	raidBdevs, err := spdkClient.BdevRaidGet(diskName, 0)
	if err != nil {
		if jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			// No RAID → diskName is the NVMe base bdev
			health, err := spdkClient.BdevNvmeGetControllerHealthInfo(diskName)
			if err != nil {
				return nil, errors.Wrapf(
					err, "failed to get NVMe controller health info for disk %q", diskName)
			}
			return &health, nil
		}

		return nil, errors.Wrapf(err, "failed to get RAID bdev for %q", diskName)
	}

	// RAID exists
	if len(raidBdevs) != 1 {
		return nil, fmt.Errorf("unexpected number of RAID bdevs for %q: %d", diskName, len(raidBdevs))
	}

	// Get base NVMe bdev name under RAID
	baseBdevControllerName := makeBaseBdevNameFromPath(diskName, diskPath)

	healthInfo, err := spdkClient.BdevNvmeGetControllerHealthInfo(baseBdevControllerName)
	if err != nil {
		return nil, errors.Wrapf(
			err, "failed to get NVMe controller health info for base bdev %q of disk %q", baseBdevControllerName, diskName,
		)
	}

	return &healthInfo, nil
}

// makeBaseBdevNameFromPath returns a valid SPDK bdev name by combining the disk
// name with a sanitized version of the disk path. The disk path is cleaned so
// that only SPDK-safe characters remain (e.g., "0000:00:1e.0" → "0000-00-1e-0", "/dev/sda -> <diskName>-dev-sda'").
func makeBaseBdevNameFromPath(diskName, diskPath string) string {
	return fmt.Sprintf("%s-%s", diskName, sanitizeBdevComponent(diskPath))
}

func sanitizeBdevComponent(s string) string {
	var allowed = func(r rune) bool {
		return (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '-' || r == '_'
	}

	b := strings.Builder{}
	for _, r := range s {
		if allowed(r) {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	// collapse repeated '-'
	result := regexp.MustCompile(`-+`).ReplaceAllString(b.String(), "-")
	return strings.Trim(result, "-")
}
