package monitor

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	lhns "github.com/longhorn/go-common-libs/ns"
	lhtypes "github.com/longhorn/go-common-libs/types"

	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	defaultBlockSize = 4096
)

// GetDiskStat returns the disk stat of the given directory
func getDiskStat(diskType longhorn.DiskType, diskName, diskPath string, diskDriver longhorn.DiskDriver, client *DiskServiceClient) (stat *lhtypes.DiskStat, err error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return lhns.GetDiskStat(diskPath)
	case longhorn.DiskTypeBlock:
		return getBlockTypeDiskStat(client, diskName, diskPath, diskDriver)
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func getBlockTypeDiskStat(client *DiskServiceClient, diskName, diskPath string, diskDriver longhorn.DiskDriver) (stat *lhtypes.DiskStat, err error) {
	if client == nil || client.c == nil {
		return nil, errors.New("disk service client is nil")
	}

	info, err := client.c.DiskGet(string(longhorn.DiskTypeBlock), diskName, diskPath, string(diskDriver))
	if err != nil {
		return nil, err
	}

	return &lhtypes.DiskStat{
		DiskID:           info.ID,
		Name:             info.Name,
		Path:             info.Path,
		Type:             info.Type,
		Driver:           lhtypes.DiskDriver(info.Driver),
		TotalBlocks:      info.TotalBlocks,
		FreeBlocks:       info.FreeBlocks,
		BlockSize:        info.BlockSize,
		StorageMaximum:   info.TotalSize,
		StorageAvailable: info.FreeSize,
	}, nil
}

// getDiskConfig returns the disk config of the given directory
func getDiskConfig(diskType longhorn.DiskType, diskName, diskPath string, diskDriver longhorn.DiskDriver, client *DiskServiceClient) (*util.DiskConfig, error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return getFilesystemTypeDiskConfig(diskPath)
	case longhorn.DiskTypeBlock:
		return getBlockTypeDiskConfig(client, diskName, diskPath, diskDriver)
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func getFilesystemTypeDiskConfig(path string) (*util.DiskConfig, error) {
	var err error
	defer func() {
		err = errors.Wrapf(err, "failed to get disk config for %v", path)
	}()

	diskCfgFilePath := filepath.Join(path, util.DiskConfigFile)
	output, err := lhns.ReadFileContent(diskCfgFilePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read host disk config file %v", util.DiskConfigFile)
	}

	cfg := &util.DiskConfig{}
	if err := json.Unmarshal([]byte(output), cfg); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal host %v content: %v", diskCfgFilePath, output)
	}
	return cfg, nil
}

func getBlockTypeDiskConfig(client *DiskServiceClient, diskName, diskPath string, diskDriver longhorn.DiskDriver) (config *util.DiskConfig, err error) {
	if client == nil || client.c == nil {
		return nil, errors.New("disk service client is nil")
	}

	info, err := client.c.DiskGet(string(longhorn.DiskTypeBlock), diskName, diskPath, string(diskDriver))
	if err != nil {
		if grpcstatus.Code(err) == grpccodes.NotFound {
			return nil, errors.Wrapf(err, "cannot find disk info")
		}
		return nil, err
	}

	return &util.DiskConfig{
		DiskName:   info.Name,
		DiskUUID:   info.UUID,
		DiskDriver: longhorn.DiskDriver(info.Driver),
	}, nil
}

// GenerateDiskConfig generates a disk config for the given directory
func generateDiskConfig(diskType longhorn.DiskType, diskName, diskUUID, diskPath, diskDriver string, client *DiskServiceClient) (*util.DiskConfig, error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return generateFilesystemTypeDiskConfig(diskPath)
	case longhorn.DiskTypeBlock:
		return generateBlockTypeDiskConfig(client, diskName, diskUUID, diskPath, diskDriver, defaultBlockSize)
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func generateFilesystemTypeDiskConfig(diskPath string) (*util.DiskConfig, error) {
	var err error
	defer func() {
		err = errors.Wrapf(err, "failed to generate disk config for %v", diskPath)
	}()

	cfg := &util.DiskConfig{
		DiskName: "",
		DiskUUID: util.UUID(),
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal %+v", cfg)
	}

	diskCfgFilePath := filepath.Join(diskPath, util.DiskConfigFile)
	if _, err := lhns.GetFileInfo(diskCfgFilePath); err == nil {
		return nil, errors.Wrapf(err, "disk cfg on %v exists, cannot override", diskCfgFilePath)
	}

	defer func() {
		if err != nil {
			if derr := util.DeleteDiskPathReplicaSubdirectoryAndDiskCfgFile(diskPath); derr != nil {
				err = errors.Wrapf(err, "cleaning up disk config path %v failed with error: %v", diskPath, derr)
			}
		}
	}()

	if err := lhns.WriteFile(diskCfgFilePath, string(encoded)); err != nil {
		return nil, err
	}

	if _, err := lhns.CreateDirectory(filepath.Join(diskPath, util.ReplicaDirectory), time.Now()); err != nil {
		return nil, errors.Wrapf(err, "failed to create replica subdirectory %v", diskPath)
	}

	if err := lhns.SyncFile(diskCfgFilePath); err != nil {
		return nil, err
	}

	return cfg, nil
}

func generateBlockTypeDiskConfig(client *DiskServiceClient, diskName, diskUUID, diskPath, diskDriver string, blockSize int64) (*util.DiskConfig, error) {
	if client == nil || client.c == nil {
		return nil, errors.New("disk service client is nil")
	}

	info, err := client.c.DiskCreate(string(longhorn.DiskTypeBlock), diskName, diskUUID, diskPath, diskDriver, blockSize)
	if err != nil {
		return nil, err
	}

	return &util.DiskConfig{
		DiskName:   info.Name,
		DiskUUID:   info.UUID,
		DiskDriver: longhorn.DiskDriver(info.Driver),
	}, nil
}

// DeleteDisk deletes the disk with the given name, uuid, path and driver
func DeleteDisk(diskType longhorn.DiskType, diskName, diskUUID, diskPath, diskDriver string, client *engineapi.DiskService) error {
	if client == nil {
		return errors.New("disk service client is nil")
	}

	return client.DiskDelete(string(diskType), diskName, diskUUID, diskPath, diskDriver)
}

// getSpdkReplicaInstanceNames returns the replica lvol names of the given disk
func getSpdkReplicaInstanceNames(client *DiskServiceClient, diskType, diskName, diskDriver string) (map[string]string, error) {
	if client == nil || client.c == nil {
		return nil, errors.New("disk service client is nil")
	}

	instances, err := client.c.DiskReplicaInstanceList(diskType, diskName, diskDriver)
	if err != nil {
		return nil, err
	}

	instanceNames := map[string]string{}
	for name := range instances {
		instanceNames[name] = name
	}

	return instanceNames, nil
}
