package monitor

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/go-common-libs/multierr"
	lhns "github.com/longhorn/go-common-libs/ns"
	lhtypes "github.com/longhorn/go-common-libs/types"

	imapi "github.com/longhorn/longhorn-instance-manager/pkg/api"
	spdkdisk "github.com/longhorn/longhorn-spdk-engine/pkg/spdk"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	defaultBlockSize      = 512
	uuidGenerationRetries = 20
)

// GetDiskStat returns the disk stat of the given directory
func getDiskStat(diskType longhorn.DiskType, diskName, diskPath string, diskDriver longhorn.DiskDriver, client *DiskServiceClient) (stat *lhtypes.DiskStat, err error) {
	paths := util.SplitPaths(diskPath)

	switch diskType {
	case longhorn.DiskTypeFilesystem:
		if len(paths) != 1 {
			return nil, fmt.Errorf("filesystem type disk should have only one path, got %v", paths)
		}

		return lhns.GetDiskStat(diskPath)
	case longhorn.DiskTypeBlock:
		return getBlockTypeDiskStat(client, diskName, paths, diskDriver)
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func getBlockTypeDiskStat(client *DiskServiceClient, diskName string, diskPath []string, diskDriver longhorn.DiskDriver) (stat *lhtypes.DiskStat, err error) {
	if client == nil || client.c == nil {
		return nil, errors.New("disk service client is nil")
	}

	info, err := client.c.DiskGet(string(longhorn.DiskTypeBlock), diskName, string(diskDriver), diskPath)
	if err != nil {
		return nil, err
	}

	return &lhtypes.DiskStat{
		DiskID:           info.ID,
		Name:             info.Name,
		Path:             util.JoinPaths(info.Path),
		Type:             info.Type,
		Driver:           lhtypes.DiskDriver(info.Driver),
		TotalBlocks:      info.TotalBlocks,
		FreeBlocks:       info.FreeBlocks,
		BlockSize:        info.BlockSize,
		StorageMaximum:   info.TotalSize,
		StorageAvailable: info.FreeSize,
	}, nil
}

func getDiskHealth(diskType longhorn.DiskType, diskName, diskPath string, diskDriver longhorn.DiskDriver, lastCollectedAt time.Time, client *DiskServiceClient, logger logrus.FieldLogger) (map[string]map[string]longhorn.HealthData, time.Time, error) {
	// Skip if health data was collected recently.
	if time.Since(lastCollectedAt) < HealthDataUpdateInterval {
		return nil, lastCollectedAt, nil
	}

	paths := util.SplitPaths(diskPath)

	logger.WithField("diskType", diskType).Debugf("Collecting health data for disk %s", diskName)

	switch diskType {
	case longhorn.DiskTypeFilesystem:
		if len(paths) != 1 {
			return nil, lastCollectedAt, fmt.Errorf("filesystem type disk should have only one path, got %v", paths)
		}

		return getHealthDataFromMountPath(diskName, diskPath, lastCollectedAt, logger)
	case longhorn.DiskTypeBlock:
		return getBlockDiskHealth(diskName, paths, diskDriver, lastCollectedAt, client, logger)
	default:
		return nil, lastCollectedAt, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func getBlockDiskHealth(diskName string, diskPaths []string, diskDriver longhorn.DiskDriver, lastCollectedAt time.Time, client *DiskServiceClient, logger logrus.FieldLogger) (map[string]map[string]longhorn.HealthData, time.Time, error) {
	// diskHealthData -> disk Name -> disk Path -> HealthData
	diskHealthData := map[string]map[string]longhorn.HealthData{
		diskName: {},
	}

	errs := multierr.NewMultiError()
	collectAt := lastCollectedAt
	for _, diskPath := range diskPaths {
		var (
			healthData map[string]longhorn.HealthData
			err        error
		)

		switch diskDriver {
		case longhorn.DiskDriverAio:
			// AIO driver doesn't claim device exclusively, so smartctl can access it directly
			healthData, collectAt, err = getHealthDataFromBlockDevice(diskName, diskPath, lastCollectedAt, logger)
		case longhorn.DiskDriverNvme:
			// NVME driver claims device exclusively, use SPDK health info instead
			healthData, collectAt, err = getHealthDataFromControllerHealthInfo(diskName, diskPath, client, lastCollectedAt, logger)
		default:
			return nil, lastCollectedAt, fmt.Errorf("unknown block disk driver %v", diskDriver)
		}

		if err != nil {
			errs.Append("errors", errors.Wrap(err, fmt.Sprintf("failed to get health data for disk %s at path %s", diskName, diskPath)))
			continue
		}

		diskHealthData[diskName][diskPath] = healthData[diskName]
	}

	if len(errs) > 0 {
		return diskHealthData, collectAt, errs
	}

	return diskHealthData, collectAt, nil
}

func getHealthDataFromMountPath(diskName, diskPath string, lastCollectedAt time.Time, logger logrus.FieldLogger) (map[string]map[string]longhorn.HealthData, time.Time, error) {
	// Collect SMART data - resolves mount path to physical device(s)
	healthData, err := util.CollectHealthDataFromMountPath(diskPath, diskName, logger)
	if err != nil {
		return nil, lastCollectedAt, err
	}

	diskHealthData := map[string]map[string]longhorn.HealthData{
		diskName: {
			diskPath: healthData[diskName],
		},
	}

	return diskHealthData, time.Now(), nil
}

func getHealthDataFromBlockDevice(diskName, diskPath string, lastCollectedAt time.Time, logger logrus.FieldLogger) (map[string]longhorn.HealthData, time.Time, error) {
	// Collect health data directly from the block device without mount path resolution
	healthData, err := util.CollectHealthDataForBlockDevice(diskPath, diskName, logger)
	if err != nil {
		return nil, lastCollectedAt, err
	}

	return healthData, time.Now(), nil
}

func getHealthDataFromControllerHealthInfo(diskName, diskPath string, client *DiskServiceClient, lastCollectedAt time.Time, logger logrus.FieldLogger) (map[string]longhorn.HealthData, time.Time, error) {
	if client == nil || client.c == nil {
		return nil, lastCollectedAt, errors.New("disk service client is nil")
	}

	log := logger.WithFields(logrus.Fields{
		"diskName": diskName,
		"diskPath": diskPath,
	})
	log.Debugf("Collecting controller health info for NVME disk")

	// Get health data from SPDK via disk service
	health, err := client.c.DiskHealthGet(string(longhorn.DiskTypeBlock), diskName, diskPath, string(longhorn.DiskDriverNvme))
	if err != nil {
		log.WithError(err).Warnf("Failed to get controller health info")
		return nil, lastCollectedAt, err
	}

	// Map SPDK NVMe health data to SmartData format
	// Clamp temperature to non-negative and drop fractional part for schema compatibility
	temp := health.TemperatureCelsius
	if temp < 0 {
		temp = 0
	}
	healthData := map[string]longhorn.HealthData{
		diskName: {
			Source:          longhorn.HealthDataSourceSPDK,
			ModelName:       health.ModelNumber,
			SerialNumber:    health.SerialNumber,
			FirmwareVersion: health.FirmwareRevision,
			DiskName:        diskName,
			DiskType:        "nvme",
			Temperature:     uint8(temp),
			HealthStatus:    determineHealthStatus(health),
			Attributes:      convertNvmeHealthToAttributes(health),
		},
	}

	return healthData, time.Now(), nil
}

// determineHealthStatus evaluates NVMe health data to determine overall health status
func determineHealthStatus(health *imapi.DiskHealth) longhorn.HealthDataHealthStatus {
	// Critical warning bits indicate various health issues
	if health.CriticalWarning != 0 {
		return longhorn.HealthDataStatusFailed
	}

	// Check available spare threshold
	if health.AvailableSparePercentage < health.AvailableSpareThresholdPercentage {
		return longhorn.HealthDataStatusFailed
	}

	// Check media errors
	if health.MediaErrors > 0 {
		return longhorn.HealthDataStatusWarning
	}

	return longhorn.HealthDataStatusPassed
}

// convertNvmeHealthToAttributes converts NVMe health metrics to SMART attribute format
func convertNvmeHealthToAttributes(health *imapi.DiskHealth) []*longhorn.HealthAttribute {
	// TemperatureCelsius from SPDK is a float64 and may be negative when unavailable.
	// The CRD schema expects an integer (int64) encoded value for RawValue. Casting a
	// negative float directly to uint64 would overflow into a huge 1844.. number, which
	// then fails Kubernetes validation (reported as type number vs integer). We clamp
	// negatives to 0 and truncate fractional values toward zero.
	tempVal := health.TemperatureCelsius
	if tempVal < 0 {
		tempVal = 0
	}
	temperatureCelsius := uint64(tempVal)

	logrus.Infof("Converting NVMe health data to SMART attributes: %+v", health)

	attributes := []*longhorn.HealthAttribute{
		{
			Name:     "Critical Warning",
			RawValue: uint64(health.CriticalWarning),
		},
		{
			Name:     "Temperature Celsius",
			RawValue: temperatureCelsius,
			RawString: func() string {
				if health.TemperatureCelsius < 0 {
					return "unknown"
				}
				// We intentionally drop any fractional part.
				return fmt.Sprintf("%d Celsius", temperatureCelsius)
			}(),
		},
		{
			Name:     "Available Spare Percentage",
			RawValue: uint64(health.AvailableSparePercentage),
		},
		{
			Name:     "Available Spare Threshold Percentage",
			RawValue: uint64(health.AvailableSpareThresholdPercentage),
		},
		{
			Name:     "Percentage Used",
			RawValue: uint64(health.PercentageUsed),
		},
		{
			Name:     "Data Units Read",
			RawValue: health.DataUnitsRead,
		},
		{
			Name:     "Data Units Written",
			RawValue: health.DataUnitsWritten,
		},
		{
			Name:     "Host Read Commands",
			RawValue: health.HostReadCommands,
		},
		{
			Name:     "Host Write Commands",
			RawValue: health.HostWriteCommands,
		},
		{
			Name:     "Controller Busy Time",
			RawValue: health.ControllerBusyTime,
		},
		{
			Name:     "Power Cycles",
			RawValue: health.PowerCycles,
		},
		{
			Name:     "Power On Hours",
			RawValue: health.PowerOnHours,
		},
		{
			Name:     "Unsafe Shutdowns",
			RawValue: health.UnsafeShutdowns,
		},
		{
			Name:     "Media Errors",
			RawValue: health.MediaErrors,
		},
		{
			Name:     "Number of Error Log Entries",
			RawValue: health.NumErrLogEntries,
		},
		{
			Name:     "Warning Temperature Time Minutes",
			RawValue: health.WarningTemperatureTimeMinutes,
		},
		{
			Name:     "Critical Composite Temperature Time Minutes",
			RawValue: health.CriticalCompositeTemperatureTimeMinutes,
		},
	}

	return attributes
}

// getDiskConfig returns the disk config of the given directory
func getDiskConfig(diskType longhorn.DiskType, diskName string, diskPath string, diskDriver longhorn.DiskDriver, client *DiskServiceClient) (*util.DiskConfig, error) {
	paths := util.SplitPaths(diskPath)

	switch diskType {
	case longhorn.DiskTypeFilesystem:
		if len(paths) != 1 {
			return nil, fmt.Errorf("filesystem type disk should have only one path, got %v", paths)
		}

		return getFilesystemTypeDiskConfig(diskPath)
	case longhorn.DiskTypeBlock:
		return getBlockTypeDiskConfig(client, diskName, paths, diskDriver)
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

	// Filesystem type disk is always ready if the config file exists
	cfg.State = string(spdkdisk.DiskStateReady)

	return cfg, nil
}

func getBlockTypeDiskConfig(client *DiskServiceClient, diskName string, diskPath []string, diskDriver longhorn.DiskDriver) (config *util.DiskConfig, err error) {
	if client == nil || client.c == nil {
		return nil, errors.New("disk service client is nil")
	}

	info, err := client.c.DiskGet(string(longhorn.DiskTypeBlock), diskName, string(diskDriver), diskPath)
	if err != nil {
		if grpcstatus.Code(err) == grpccodes.NotFound {
			return nil, errors.Wrapf(err, "cannot find disk info")
		}
		return nil, err
	}

	getDiskName := func() string {
		if info.Name != "" {
			return info.Name
		}
		return diskName
	}

	return &util.DiskConfig{
		DiskName:   getDiskName(),
		DiskUUID:   info.UUID,
		DiskDriver: longhorn.DiskDriver(info.Driver),
		State:      info.State,
	}, nil
}

// GenerateDiskConfig generates a disk config for the given directory
func generateDiskConfig(diskType longhorn.DiskType, diskName, diskUUID, diskPath, diskDriver string, client *DiskServiceClient, ds *datastore.DataStore) (*util.DiskConfig, error) {
	paths := util.SplitPaths(diskPath)

	switch diskType {
	case longhorn.DiskTypeFilesystem:
		if len(paths) != 1 {
			return nil, fmt.Errorf("filesystem type disk should have only one path, got %v", paths)
		}

		return generateFilesystemTypeDiskConfig(diskName, diskPath, ds)
	case longhorn.DiskTypeBlock:
		return generateBlockTypeDiskConfig(client, diskName, diskUUID, paths, diskDriver, defaultBlockSize)
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func generateFilesystemTypeDiskConfig(diskName, diskPath string, ds *datastore.DataStore) (*util.DiskConfig, error) {
	var err error
	defer func() {
		err = errors.Wrapf(err, "failed to generate disk config for %v", diskPath)
	}()

	allDiskUUIDFirstFourCharSet, err := ds.GetAllDiskUUIDFirstFourChar()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get all diskUUIDs first four char set")
	}

	uuid, err := generateUniqueFirstFourCharUUID(allDiskUUIDFirstFourCharSet)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to generate unique disk UUID")
	}

	cfg := &util.DiskConfig{
		DiskName: diskName,
		DiskUUID: uuid,
		State:    string(spdkdisk.DiskStateReady),
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

func generateBlockTypeDiskConfig(client *DiskServiceClient, diskName, diskUUID string, diskPath []string, diskDriver string, blockSize int64) (*util.DiskConfig, error) {
	if client == nil || client.c == nil {
		return nil, errors.New("disk service client is nil")
	}

	info, err := client.c.DiskCreate(string(longhorn.DiskTypeBlock), diskName, diskUUID, diskDriver, diskPath, blockSize)
	if err != nil {
		return nil, err
	}

	getDiskName := func() string {
		if info.Name != "" {
			return info.Name
		}
		return diskName
	}

	return &util.DiskConfig{
		DiskName:   getDiskName(),
		DiskUUID:   info.UUID,
		DiskDriver: longhorn.DiskDriver(info.Driver),
		State:      info.State,
	}, nil
}

// DeleteDisk deletes the disk with the given name, uuid, path and driver
func DeleteDisk(diskType longhorn.DiskType, diskName, diskUUID string, diskPath []string, diskDriver string, client *engineapi.DiskService) error {
	if client == nil {
		return errors.New("disk service client is nil")
	}

	return client.DiskDelete(string(diskType), diskName, diskUUID, diskDriver, diskPath)
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

func generateUniqueFirstFourCharUUID(allDiskUUIDFirstFourCharSet map[string]bool) (string, error) {
	for i := 0; i < uuidGenerationRetries; i++ {
		uuid := util.UUID()
		prefix := uuid[:4]
		if !allDiskUUIDFirstFourCharSet[prefix] {
			return uuid, nil
		}
	}
	return "", fmt.Errorf("failed to generate UUID with unique prefix after %v attempts", uuidGenerationRetries)
}
