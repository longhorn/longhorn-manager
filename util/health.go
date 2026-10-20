package util

import (
	"encoding/json"
	"errors"
	"path/filepath"

	"github.com/henrygd/beszel/agent"
	"github.com/sirupsen/logrus"

	commonns "github.com/longhorn/go-common-libs/ns"
	commonsys "github.com/longhorn/go-common-libs/sys"
	commonutils "github.com/longhorn/go-common-libs/utils"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// CollectHealthDataFromMountPath collects health data by resolving a mount path to its physical device
// This is used for V1 filesystem-type disks where the disk path is a mount point.
// Returns nil if health data cannot be collected (e.g., device doesn't support health monitoring)
func CollectHealthDataFromMountPath(mountPath, diskName string, logger logrus.FieldLogger) (map[string]longhorn.HealthData, error) {
	physicalDevice, err := resolveMountPathToPhysicalDeviceWithDeps(mountPath)
	if err != nil {
		return nil, err
	}

	return collectSmartDataForDevice(physicalDevice, diskName, logger)
}

// CollectHealthDataForBlockDevice collects health data directly from a block device
// without resolving through mount paths. This is used for V2 block-type disks.
func CollectHealthDataForBlockDevice(devicePath, diskName string, logger logrus.FieldLogger) (map[string]longhorn.HealthData, error) {
	physicalDevice, err := commonns.ResolveBlockDeviceToPhysicalDevice(devicePath)
	if err != nil {
		return nil, err
	}

	return collectSmartDataForDevice(physicalDevice, diskName, logger)
}

// CollectHealthDataForBlockDevices collects health data for a disk made of
// several block devices, such as an lvm disk. Each entry is keyed by the disk
// name and the physical device, so the devices coexist in the disk status. A
// path that does not resolve to a block device (a volume group name, or a
// device that is gone) is skipped; collection fails only when no device
// could be sampled.
func CollectHealthDataForBlockDevices(devicePaths []string, diskName string, logger logrus.FieldLogger) (map[string]longhorn.HealthData, error) {
	log := logger.WithField("diskName", diskName)
	devices := map[string]string{}
	var errs []error
	for _, devicePath := range devicePaths {
		physicalDevice, err := commonns.ResolveBlockDeviceToPhysicalDevice(devicePath)
		if err != nil {
			log.WithError(err).Debugf("Skipping health collection for %v", devicePath)
			errs = append(errs, err)
			continue
		}
		devices[diskName+"/"+filepath.Base(physicalDevice)] = physicalDevice
	}
	if len(devices) == 0 {
		if len(errs) > 0 {
			return nil, errors.Join(errs...)
		}
		return map[string]longhorn.HealthData{}, nil
	}
	return collectSmartDataForDevices(devices, logger)
}

// collectSmartDataForDevice collects the SMART data of one device under the
// disk name.
func collectSmartDataForDevice(devicePath, diskName string, logger logrus.FieldLogger) (map[string]longhorn.HealthData, error) {
	return collectSmartDataForDevices(map[string]string{diskName: devicePath}, logger)
}

// collectSmartDataForDevices collects SMART data for the physical devices of
// one disk, keyed as given, with one SMART manager for all of them.
func collectSmartDataForDevices(devices map[string]string, logger logrus.FieldLogger) (map[string]longhorn.HealthData, error) {
	log := logger.WithField("devices", devices)
	log.Debugf("Collecting SMART data for devices")

	smartMgr, err := agent.NewSmartManager()
	if err != nil {
		return nil, err
	}

	refreshed := 0
	var errs []error
	for _, devicePath := range devices {
		if err := smartMgr.RefreshDevice(false, devicePath); err != nil {
			log.WithError(err).Warnf("Failed to refresh SMART data of %v", devicePath)
			errs = append(errs, err)
			continue
		}
		refreshed++
	}
	if refreshed == 0 {
		return nil, errors.Join(errs...)
	}

	data := smartMgr.GetCurrentData()

	// Marshal to JSON using beszel's short keys
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	// Unmarshal to a generic map to handle key conversion
	var beszelMap map[string]any
	err = json.Unmarshal(jsonBytes, &beszelMap)
	if err != nil {
		return nil, err
	}

	// Convert the map structure from beszel format to longhorn.HealthData.
	// Ref: https://github.com/henrygd/beszel/blob/4d05bfdff0ec90b68e820ad5dc32a5c4bccf8f0f/internal/entities/smart/smart.go#L507-L518
	keys := make(map[string]string, len(devices))
	for key, devicePath := range devices {
		keys[devicePath] = key
	}
	result := make(map[string]longhorn.HealthData)
	for _, diskValue := range beszelMap {
		diskMap, ok := diskValue.(map[string]any)
		if !ok {
			continue
		}

		physicalDiskName := commonutils.GetStringFromMap(diskMap, "dn")
		key, wanted := keys[physicalDiskName]
		if !wanted {
			log.Debugf("Skipping SMART data for device %s, which is not one of the disk's devices", physicalDiskName)
			continue
		}

		log.Debugf("Converting SMART data for disk: %+v", diskMap)
		healthData := longhorn.HealthData{
			Source:          longhorn.HealthDataSourceSMART,
			ModelName:       commonutils.GetStringFromMap(diskMap, "mn"),
			SerialNumber:    commonutils.GetStringFromMap(diskMap, "sn"),
			FirmwareVersion: commonutils.GetStringFromMap(diskMap, "fv"),
			Capacity:        commonutils.GetNumberFromMap[uint64](diskMap, "c"),
			HealthStatus:    longhorn.HealthDataHealthStatus(commonutils.GetStringFromMap(diskMap, "s")),
			DiskName:        physicalDiskName,
			DiskType:        commonutils.GetStringFromMap(diskMap, "dt"),
			Temperature:     commonutils.GetNumberFromMap[uint8](diskMap, "t"),
		}

		// Convert attributes
		if attrs, ok := diskMap["a"].([]any); ok {
			healthData.Attributes = make([]*longhorn.HealthAttribute, 0, len(attrs))
			for _, attr := range attrs {
				if attrMap, ok := attr.(map[string]any); ok {
					healthAttr := &longhorn.HealthAttribute{
						ID:         commonutils.GetNumberFromMap[uint16](attrMap, "id"),
						Name:       commonutils.GetStringFromMap(attrMap, "n"),
						Value:      commonutils.GetNumberFromMap[uint16](attrMap, "v"),
						Worst:      commonutils.GetNumberFromMap[uint16](attrMap, "w"),
						Threshold:  commonutils.GetNumberFromMap[uint16](attrMap, "t"),
						RawValue:   commonutils.GetNumberFromMap[uint64](attrMap, "rv"),
						RawString:  commonutils.GetStringFromMap(attrMap, "rs"),
						WhenFailed: commonutils.GetStringFromMap(attrMap, "wf"),
					}
					healthData.Attributes = append(healthData.Attributes, healthAttr)
				}
			}
		}

		result[key] = healthData
	}

	if len(result) == 0 {
		log.Debugf("No health data found for the devices")
	}

	return result, nil
}

// resolveMountPathToPhysicalDeviceWithDeps returns the top-level physical device
// (e.g., /dev/nvme0 for NVMe, /dev/sda for SATA) corresponding to the given
// mount path (e.g., /var/lib/longhorn).
func resolveMountPathToPhysicalDeviceWithDeps(mountPath string) (string, error) {
	blockDevice, err := commonsys.FindBlockDeviceForMount(mountPath)
	if err != nil {
		return "", err
	}

	return commonns.ResolveBlockDeviceToPhysicalDevice(blockDevice)
}
