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
func getDiskStat(diskType longhorn.DiskType, name, path string, client *DiskServiceClient) (stat *lhtypes.DiskStat, err error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return lhns.GetDiskStat(path)
	case longhorn.DiskTypeBlock:
		return getBlockTypeDiskStat(client, name, path)
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func getBlockTypeDiskStat(client *DiskServiceClient, name, path string) (stat *lhtypes.DiskStat, err error) {
	if client == nil || client.c == nil {
		return nil, errors.New("disk service client is nil")
	}

	info, err := client.c.DiskGet(string(longhorn.DiskTypeBlock), name, path)
	if err != nil {
		return nil, err
	}
	return &lhtypes.DiskStat{
		DiskID:           info.ID,
		Path:             info.Path,
		Type:             info.Type,
		TotalBlocks:      info.TotalBlocks,
		FreeBlocks:       info.FreeBlocks,
		BlockSize:        info.BlockSize,
		StorageMaximum:   info.TotalSize,
		StorageAvailable: info.FreeSize,
	}, nil
}

// GetDiskConfig returns the disk config of the given directory
func getDiskConfig(diskType longhorn.DiskType, name, path string, client *DiskServiceClient) (*util.DiskConfig, error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return getFilesystemTypeDiskConfig(path)
	case longhorn.DiskTypeBlock:
		return getBlockTypeDiskConfig(client, name, path)
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

func getBlockTypeDiskConfig(client *DiskServiceClient, name, path string) (config *util.DiskConfig, err error) {
	if client == nil || client.c == nil {
		return nil, errors.New("disk service client is nil")
	}

	info, err := client.c.DiskGet(string(longhorn.DiskTypeBlock), name, path)
	if err != nil {
		if grpcstatus.Code(err) == grpccodes.NotFound {
			return nil, errors.Wrapf(err, "cannot find disk info")
		}
		return nil, err
	}
	return &util.DiskConfig{
		DiskUUID: info.UUID,
	}, nil
}

// GenerateDiskConfig generates a disk config for the given directory
func generateDiskConfig(diskType longhorn.DiskType, name, uuid, path string, client *DiskServiceClient) (*util.DiskConfig, error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return generateFilesystemTypeDiskConfig(path)
	case longhorn.DiskTypeBlock:
		return generateBlockTypeDiskConfig(client, name, uuid, path, defaultBlockSize)
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func generateFilesystemTypeDiskConfig(path string) (*util.DiskConfig, error) {
	var err error
	defer func() {
		err = errors.Wrapf(err, "failed to generate disk config for %v", path)
	}()

	cfg := &util.DiskConfig{
		DiskUUID: util.UUID(),
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("BUG: Cannot marshal %+v: %v", cfg, err)
	}

	diskCfgFilePath := filepath.Join(path, util.DiskConfigFile)
	if _, err := lhns.GetFileInfo(diskCfgFilePath); err == nil {
		return nil, fmt.Errorf("disk cfg on %v exists, cannot override", diskCfgFilePath)
	}

	defer func() {
		if err != nil {
			if derr := util.DeleteDiskPathReplicaSubdirectoryAndDiskCfgFile(path); derr != nil {
				err = errors.Wrapf(err, "cleaning up disk config path %v failed with error: %v", path, derr)
			}
		}
	}()

	if err := lhns.WriteFile(diskCfgFilePath, string(encoded)); err != nil {
		return nil, err
	}

	if _, err := lhns.CreateDirectory(filepath.Join(path, util.ReplicaDirectory), time.Now()); err != nil {
		return nil, errors.Wrapf(err, "failed to create replica subdirectory %v", path)
	}

	if err := lhns.SyncFile(diskCfgFilePath); err != nil {
		return nil, err
	}

	return cfg, nil
}

func generateBlockTypeDiskConfig(client *DiskServiceClient, name, uuid, path string, blockSize int64) (*util.DiskConfig, error) {
	if client == nil || client.c == nil {
		return nil, errors.New("disk service client is nil")
	}

	info, err := client.c.DiskCreate(string(longhorn.DiskTypeBlock), name, uuid, path, blockSize)
	if err != nil {
		return nil, err
	}
	return &util.DiskConfig{
		DiskUUID: info.UUID,
	}, nil
}

// DeleteDisk deletes the disk with the given name and uuid
func DeleteDisk(diskType longhorn.DiskType, diskName, diskUUID string, client *engineapi.DiskService) error {
	if client == nil {
		return errors.New("disk service client is nil")
	}

	return client.DiskDelete(string(diskType), diskName, diskUUID)
}

// getSpdkReplicaInstanceNames returns the replica lvol names of the given disk
func getSpdkReplicaInstanceNames(client *DiskServiceClient, diskType, diskName string) (map[string]string, error) {
	if client == nil || client.c == nil {
		return nil, errors.New("disk service client is nil")
	}

	instances, err := client.c.DiskReplicaInstanceList(diskType, diskName)
	if err != nil {
		return nil, err
	}

	instanceNames := map[string]string{}
	for name := range instances {
		instanceNames[name] = name
	}

	return instanceNames, nil
}
