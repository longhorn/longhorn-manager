package util

import (
	"encoding/json"

	"github.com/henrygd/beszel/agent"
	"github.com/sirupsen/logrus"

	commonsys "github.com/longhorn/go-common-libs/sys"
	commonutils "github.com/longhorn/go-common-libs/utils"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// CollectHealthDataFromMountPath collects health data by resolving a mount path to its physical device
// This is used for V1 filesystem-type disks where the disk path is a mount point.
// Returns nil if health data cannot be collected (e.g., device doesn't support health monitoring)
func CollectHealthDataFromMountPath(mountPath, diskName string, logger logrus.FieldLogger) (map[string]longhorn.HealthData, error) {
	physicalDevice, err := commonsys.ResolveMountPathToPhysicalDevice(mountPath)
	if err != nil {
		return nil, err
	}

	return collectSmartDataForDevice(physicalDevice, diskName, logger)
}

// CollectHealthDataForBlockDevice collects health data directly from a block device
// without resolving through mount paths. This is used for V2 block-type disks.
func CollectHealthDataForBlockDevice(devicePath, diskName string, logger logrus.FieldLogger) (map[string]longhorn.HealthData, error) {
	physicalDevice, err := commonsys.ResolveBlockDeviceToPhysicalDevice(devicePath)
	if err != nil {
		return nil, err
	}

	return collectSmartDataForDevice(physicalDevice, diskName, logger)
}

// collectSmartDataForDevice is the common implementation for collecting SMART data
func collectSmartDataForDevice(devicePath, diskName string, logger logrus.FieldLogger) (map[string]longhorn.HealthData, error) {
	log := logger.WithFields(logrus.Fields{
		"devicePath": devicePath,
		"diskName":   diskName,
	})
	log.Debugf("Collecting SMART data for device")

	smartMgr, err := agent.NewSmartManager()
	if err != nil {
		return nil, err
	}

	err = smartMgr.Refresh(false)
	if err != nil {
		return nil, err
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
	result := make(map[string]longhorn.HealthData)
	for _, diskValue := range beszelMap {
		diskMap, ok := diskValue.(map[string]any)
		if !ok {
			continue
		}

		physicalDiskName := commonutils.GetStringFromMap(diskMap, "dn")
		if physicalDiskName != devicePath {
			log.Debugf("Skipping SMART data for device %s (looking for %s)", physicalDiskName, devicePath)
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

		result[devicePath] = healthData
		break // Found the device, no need to continue
	}

	if len(result) == 0 {
		log.Debugf("No health data found for device %s", devicePath)
	}

	return result, nil
}
