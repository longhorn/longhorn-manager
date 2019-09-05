package types

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/longhorn/longhorn-manager/util"
)

type Setting struct {
	Value string `json:"value"`
}

type SettingType string

const (
	SettingTypeString = SettingType("string")
	SettingTypeInt    = SettingType("int")
	SettingTypeBool   = SettingType("bool")
)

type SettingName string

const (
	SettingNameBackupTarget                      = SettingName("backup-target")
	SettingNameBackupTargetCredentialSecret      = SettingName("backup-target-credential-secret")
	SettingNameCreateDefaultDiskLabeledNodes     = SettingName("create-default-disk-labeled-nodes")
	SettingNameDefaultDataPath                   = SettingName("default-data-path")
	SettingNameDefaultEngineImage                = SettingName("default-engine-image")
	SettingNameReplicaSoftAntiAffinity           = SettingName("replica-soft-anti-affinity")
	SettingNameStorageOverProvisioningPercentage = SettingName("storage-over-provisioning-percentage")
	SettingNameStorageMinimalAvailablePercentage = SettingName("storage-minimal-available-percentage")
	SettingNameUpgradeChecker                    = SettingName("upgrade-checker")
	SettingNameLatestLonghornVersion             = SettingName("latest-longhorn-version")
	SettingNameDefaultReplicaCount               = SettingName("default-replica-count")
	SettingNameGuaranteedEngineCPU               = SettingName("guaranteed-engine-cpu")
	SettingNameDefaultLonghornStaticStorageClass = SettingName("default-longhorn-static-storage-class")
	SettingNameBackupstorePollInterval           = SettingName("backupstore-poll-interval")
)

var (
	SettingNameList = []SettingName{
		SettingNameBackupTarget,
		SettingNameBackupTargetCredentialSecret,
		SettingNameCreateDefaultDiskLabeledNodes,
		SettingNameDefaultDataPath,
		SettingNameDefaultEngineImage,
		SettingNameReplicaSoftAntiAffinity,
		SettingNameStorageOverProvisioningPercentage,
		SettingNameStorageMinimalAvailablePercentage,
		SettingNameUpgradeChecker,
		SettingNameLatestLonghornVersion,
		SettingNameDefaultReplicaCount,
		SettingNameGuaranteedEngineCPU,
		SettingNameDefaultLonghornStaticStorageClass,
		SettingNameBackupstorePollInterval,
	}
)

type SettingCategory string

const (
	SettingCategoryGeneral    = SettingCategory("general")
	SettingCategoryBackup     = SettingCategory("backup")
	SettingCategoryScheduling = SettingCategory("scheduling")
)

type SettingDefinition struct {
	DisplayName string          `json:"displayName"`
	Description string          `json:"description"`
	Category    SettingCategory `json:"category"`
	Type        SettingType     `json:"type"`
	Required    bool            `json:"required"`
	ReadOnly    bool            `json:"readOnly"`
	Default     string          `json:"default"`
}

var (
	SettingDefinitions = map[SettingName]SettingDefinition{
		SettingNameBackupTarget:                      SettingDefinitionBackupTarget,
		SettingNameBackupTargetCredentialSecret:      SettingDefinitionBackupTargetCredentialSecret,
		SettingNameCreateDefaultDiskLabeledNodes:     SettingDefinitionCreateDefaultDiskLabeledNodes,
		SettingNameDefaultDataPath:                   SettingDefinitionDefaultDataPath,
		SettingNameDefaultEngineImage:                SettingDefinitionDefaultEngineImage,
		SettingNameReplicaSoftAntiAffinity:           SettingDefinitionReplicaSoftAntiAffinity,
		SettingNameStorageOverProvisioningPercentage: SettingDefinitionStorageOverProvisioningPercentage,
		SettingNameStorageMinimalAvailablePercentage: SettingDefinitionStorageMinimalAvailablePercentage,
		SettingNameUpgradeChecker:                    SettingDefinitionUpgradeChecker,
		SettingNameLatestLonghornVersion:             SettingDefinitionLatestLonghornVersion,
		SettingNameDefaultReplicaCount:               SettingDefinitionDefaultReplicaCount,
		SettingNameGuaranteedEngineCPU:               SettingDefinitionGuaranteedEngineCPU,
		SettingNameDefaultLonghornStaticStorageClass: SettingDefinitionDefaultLonghornStaticStorageClass,
		SettingNameBackupstorePollInterval:           SettingDefinitionBackupstorePollInterval,
	}

	SettingDefinitionBackupTarget = SettingDefinition{
		DisplayName: "Backup Target",
		Description: "The target used for backup. Support NFS or S3.",
		Category:    SettingCategoryBackup,
		Type:        SettingTypeString,
		Required:    false,
		ReadOnly:    false,
	}

	SettingDefinitionBackupTargetCredentialSecret = SettingDefinition{
		DisplayName: "Backup Target Credential Secret",
		Description: "The Kubernetes secret associated with the backup target.",
		Category:    SettingCategoryBackup,
		Type:        SettingTypeString,
		Required:    false,
		ReadOnly:    false,
	}

	SettingDefinitionBackupstorePollInterval = SettingDefinition{
		DisplayName: "Backupstore Poll Interval",
		Description: "In seconds. The interval to poll the backup store for updating volumes' Last Backup field.",
		Category:    SettingCategoryBackup,
		Type:        SettingTypeInt,
		Required:    true,
		ReadOnly:    false,
		Default:     "300",
	}

	SettingDefinitionCreateDefaultDiskLabeledNodes = SettingDefinition{
		DisplayName: "Create Default Disk on Labeled Nodes",
		Description: "Create default Disk automatically only on Nodes with the label " +
			"\"node.longhorn.io/create-default-disk=true\" if no other Disks exist. If disabled, default Disk will " +
			"be created on all new Nodes (only on first add).",
		Category: SettingCategoryGeneral,
		Type:     SettingTypeBool,
		Required: true,
		ReadOnly: false,
		Default:  "false",
	}

	SettingDefinitionDefaultDataPath = SettingDefinition{
		DisplayName: "Default Data Path",
		Description: "Default path to use for storing data on a host",
		Category:    SettingCategoryGeneral,
		Type:        SettingTypeString,
		Required:    true,
		ReadOnly:    false,
		Default:     "/var/lib/rancher/longhorn/",
	}

	SettingDefinitionDefaultEngineImage = SettingDefinition{
		DisplayName: "Default Engine Image",
		Description: "The default engine image used by the manager. Can be changed on the manager starting command line only",
		Category:    SettingCategoryGeneral,
		Type:        SettingTypeString,
		Required:    true,
		ReadOnly:    true,
	}

	SettingDefinitionReplicaSoftAntiAffinity = SettingDefinition{
		DisplayName: "Replica Soft Anti-Affinity",
		Description: "Allow scheduling on nodes with existing healthy replicas of the same volume",
		Category:    SettingCategoryScheduling,
		Type:        SettingTypeBool,
		Required:    true,
		ReadOnly:    false,
		Default:     "true",
	}

	SettingDefinitionStorageOverProvisioningPercentage = SettingDefinition{
		DisplayName: "Storage Over Provisioning Percentage",
		Description: "The over-provisioning percentage defines how much storage can be allocated relative to the hard drive's capacity",
		Category:    SettingCategoryScheduling,
		Type:        SettingTypeInt,
		Required:    true,
		ReadOnly:    false,
		Default:     "500",
	}

	SettingDefinitionStorageMinimalAvailablePercentage = SettingDefinition{
		DisplayName: "Storage Minimal Available Percentage",
		Description: "If one disk's available capacity to it's maximum capacity in % is less than the minimal available percentage, the disk would become unschedulable until more space freed up.",
		Category:    SettingCategoryScheduling,
		Type:        SettingTypeInt,
		Required:    true,
		ReadOnly:    false,
		Default:     "10",
	}

	SettingDefinitionUpgradeChecker = SettingDefinition{
		DisplayName: "Enable Upgrade Checker",
		Description: "Upgrade Checker will check for new Longhorn version periodically. When there is a new version available, it will notify the user using UI",
		Category:    SettingCategoryGeneral,
		Type:        SettingTypeBool,
		Required:    true,
		ReadOnly:    false,
		Default:     "true",
	}

	SettingDefinitionLatestLonghornVersion = SettingDefinition{
		DisplayName: "Latest Longhorn Version",
		Description: "The latest version of Longhorn available. Update by Upgrade Checker automatically",
		Category:    SettingCategoryGeneral,
		Type:        SettingTypeString,
		Required:    false,
		ReadOnly:    true,
	}

	SettingDefinitionDefaultReplicaCount = SettingDefinition{
		DisplayName: "Default Replica Count",
		Description: "The default number of replicas when creating the volume from Longhorn UI. For Kubernetes, update the `numberOfReplicas` in the StorageClass",
		Category:    SettingCategoryGeneral,
		Type:        SettingTypeInt,
		Required:    true,
		ReadOnly:    false,
		Default:     "3",
	}

	SettingDefinitionGuaranteedEngineCPU = SettingDefinition{
		DisplayName: "Guaranteed Engine CPU",
		Description: "(EXPERIMENTAL FEATURE) Allow Longhorn Engine to have guaranteed CPU allocation. The value is " +
			"how many CPUs should be reserved for each Engine/Replica Manager Pod created by Longhorn. For example, " +
			"0.1 means one-tenth of a CPU. This will help maintain engine stability during high node workload. It " +
			"only applies to the Engine/Replica Manager Pods created after the setting took effect. WARNING: " +
			"Attaching of the volume may fail or stuck while using this feature due to the resource constraint. " +
			"Disabled (\"0\") by default.",
		Category: SettingCategoryGeneral,
		Type:     SettingTypeInt,
		Required: true,
		ReadOnly: false,
		Default:  "0",
	}

	SettingDefinitionDefaultLonghornStaticStorageClass = SettingDefinition{
		DisplayName: "Default Longhorn Static StorageClass Name",
		Description: "The 'storageClassName' is for PV/PVC when creating PV/PVC for an existing Longhorn volume. Notice that it's unnecessary for users create the related StorageClass object in Kubernetes since the StorageClass would only be used as matching labels for PVC bounding purpose. By default 'longhorn-static'.",
		Category:    SettingCategoryGeneral,
		Type:        SettingTypeString,
		Required:    false,
		ReadOnly:    false,
		Default:     "longhorn-static",
	}
)

func ValidateInitSetting(name, value string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "value %v of settings %v is invalid", value, name)
	}()
	sName := SettingName(name)

	definition, ok := SettingDefinitions[sName]
	if !ok {
		return fmt.Errorf("setting %v is not supported", sName)
	}
	if definition.Required == true && value == "" {
		return fmt.Errorf("required setting %v shouldn't be empty", sName)
	}

	switch sName {
	case SettingNameBackupTarget:
		// check whether have $ or , have been set in BackupTarget
		regStr := `[\$\,]`
		reg := regexp.MustCompile(regStr)
		findStr := reg.FindAllString(value, -1)
		if len(findStr) != 0 {
			return fmt.Errorf("value %s, contains %v", value, strings.Join(findStr, " or "))
		}
	case SettingNameCreateDefaultDiskLabeledNodes:
		fallthrough
	case SettingNameReplicaSoftAntiAffinity:
		fallthrough
	case SettingNameUpgradeChecker:
		if value != "true" && value != "false" {
			return fmt.Errorf("value %v of setting %v should be true or false", value, sName)
		}
	case SettingNameStorageOverProvisioningPercentage:
		if _, err := strconv.Atoi(value); err != nil {
			return fmt.Errorf("value %v is not a number", value)
		}
		// additional check whether over provisioning percentage is positive
		value, err := util.ConvertSize(value)
		if err != nil || value < 0 {
			return fmt.Errorf("value %v should be positive", value)
		}
	case SettingNameStorageMinimalAvailablePercentage:
		if _, err := strconv.Atoi(value); err != nil {
			return fmt.Errorf("value %v is not a number", value)
		}
		// additional check whether minimal available percentage is between 0 to 100
		value, err := util.ConvertSize(value)
		if err != nil || value < 0 || value > 100 {
			return fmt.Errorf("value %v should between 0 to 100", value)
		}
	case SettingNameDefaultReplicaCount:
		c, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("value %v is not int: %v", SettingNameDefaultReplicaCount, err)
		}
		if err := ValidateReplicaCount(c); err != nil {
			return fmt.Errorf("value %v: %v", c, err)
		}
	case SettingNameGuaranteedEngineCPU:
		if _, err := resource.ParseQuantity(value); err != nil {
			return errors.Wrapf(err, "invalid value %v as CPU resource", value)
		}
	case SettingNameBackupstorePollInterval:
		interval, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("value of %v is not int: %v", SettingNameBackupstorePollInterval, err)
		}
		if interval < 0 {
			return fmt.Errorf("backupstore poll interval %v shouldn't be less than 0", value)
		}
	}
	return nil
}
