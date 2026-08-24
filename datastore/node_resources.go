package datastore

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// getNodeV2DataEngineResources returns the node's v2 data engine resource overrides, or nil.
// A missing node has no overrides, so callers fall back to the global settings.
func (s *DataStore) getNodeV2DataEngineResources(nodeName string) (*longhorn.NodeV2DataEngineResources, error) {
	if nodeName == "" {
		return nil, nil
	}
	node, err := s.GetNodeRO(nodeName)
	if err != nil {
		if ErrorIsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if node.Spec.DataEngineResources == nil {
		return nil, nil
	}
	return node.Spec.DataEngineResources.V2, nil
}

// GetNodeEffectiveSettingAsIntByDataEngine returns the node's override when set, otherwise the global setting value.
func (s *DataStore) GetNodeEffectiveSettingAsIntByDataEngine(settingName types.SettingName, dataEngine longhorn.DataEngineType, nodeName string) (int64, error) {
	if types.IsDataEngineV2(dataEngine) {
		resources, err := s.getNodeV2DataEngineResources(nodeName)
		if err != nil {
			return -1, err
		}
		if resources != nil {
			switch settingName {
			case types.SettingNameDataEngineNumberOfCPUCores:
				if resources.NumberOfCPUCores != nil {
					return *resources.NumberOfCPUCores, nil
				}
			case types.SettingNameDataEngineMemorySize:
				if resources.MemorySizeMiB != nil {
					return *resources.MemorySizeMiB, nil
				}
			case types.SettingNameDataEngineIobufSmallPoolSize:
				if resources.IobufSmallPoolSize != nil {
					return *resources.IobufSmallPoolSize, nil
				}
			case types.SettingNameDataEngineIobufLargePoolSize:
				if resources.IobufLargePoolSize != nil {
					return *resources.IobufLargePoolSize, nil
				}
			}
		}
	}
	return s.GetSettingAsIntByDataEngine(settingName, dataEngine)
}

// GetNodeEffectiveSettingAsBoolByDataEngine returns the node's override when set, otherwise the global setting value.
func (s *DataStore) GetNodeEffectiveSettingAsBoolByDataEngine(settingName types.SettingName, dataEngine longhorn.DataEngineType, nodeName string) (bool, error) {
	if types.IsDataEngineV2(dataEngine) && settingName == types.SettingNameDataEngineHugepageEnabled {
		resources, err := s.getNodeV2DataEngineResources(nodeName)
		if err != nil {
			return false, err
		}
		if resources != nil && resources.HugepageEnabled != nil {
			return *resources.HugepageEnabled, nil
		}
	}
	return s.GetSettingAsBoolByDataEngine(settingName, dataEngine)
}

// GetNodeEffectiveCPUMask returns the node's CPU mask override when set, otherwise the global setting value.
func (s *DataStore) GetNodeEffectiveCPUMask(dataEngine longhorn.DataEngineType, nodeName string) (string, error) {
	if types.IsDataEngineV2(dataEngine) {
		resources, err := s.getNodeV2DataEngineResources(nodeName)
		if err != nil {
			return "", err
		}
		// The global setting is normalized on write; the node override is stored as written.
		if resources != nil && resources.CPUMask != nil {
			return types.NormalizeCPUMask(*resources.CPUMask)
		}
	}
	return s.GetSettingValueExistedByDataEngine(types.SettingNameDataEngineCPUMask, dataEngine)
}

// GetNodeInstanceManagerResources returns the node's instance manager pod resources override, or nil.
// A non-nil override replaces the derived pod resources as a whole and disables dynamic CPU pinning.
func (s *DataStore) GetNodeInstanceManagerResources(dataEngine longhorn.DataEngineType, nodeName string) (*corev1.ResourceRequirements, error) {
	if !types.IsDataEngineV2(dataEngine) || nodeName == "" {
		return nil, nil
	}
	node, err := s.GetNodeRO(nodeName)
	if err != nil {
		if ErrorIsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if node.Spec.InstanceManagerResources == nil {
		return nil, nil
	}
	return node.Spec.InstanceManagerResources.V2, nil
}
