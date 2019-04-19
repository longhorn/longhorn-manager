package types

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
	SettingNameDefaultEngineImage                = SettingName("default-engine-image")
	SettingNameStorageOverProvisioningPercentage = SettingName("storage-over-provisioning-percentage")
	SettingNameStorageMinimalAvailablePercentage = SettingName("storage-minimal-available-percentage")
	SettingNameUpgradeChecker                    = SettingName("upgrade-checker")
	SettingNameLatestLonghornVersion             = SettingName("latest-longhorn-version")
	SettingNameDefaultReplicaCount               = SettingName("default-replica-count")
	SettingNameGuaranteedEngineCPU               = SettingName("guaranteed-engine-cpu")
)

var (
	SettingNameList = []SettingName{
		SettingNameBackupTarget,
		SettingNameBackupTargetCredentialSecret,
		SettingNameDefaultEngineImage,
		SettingNameStorageOverProvisioningPercentage,
		SettingNameStorageMinimalAvailablePercentage,
		SettingNameUpgradeChecker,
		SettingNameLatestLonghornVersion,
		SettingNameDefaultReplicaCount,
		SettingNameGuaranteedEngineCPU,
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
		SettingNameDefaultEngineImage:                SettingDefinitionDefaultEngineImage,
		SettingNameStorageOverProvisioningPercentage: SettingDefinitionStorageOverProvisioningPercentage,
		SettingNameStorageMinimalAvailablePercentage: SettingDefinitionStorageMinimalAvailablePercentage,
		SettingNameUpgradeChecker:                    SettingDefinitionUpgradeChecker,
		SettingNameLatestLonghornVersion:             SettingDefinitionLatestLonghornVersion,
		SettingNameDefaultReplicaCount:               SettingDefinitionDefaultReplicaCount,
		SettingNameGuaranteedEngineCPU:               SettingDefinitionGuaranteedEngineCPU,
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

	SettingDefinitionDefaultEngineImage = SettingDefinition{
		DisplayName: "Default Engine Image",
		Description: "The default engine image used by the manager. Can be changed on the manager starting command line only",
		Category:    SettingCategoryGeneral,
		Type:        SettingTypeString,
		Required:    true,
		ReadOnly:    true,
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
		Description: "(EXPERIMENTAL FEATURE) Allow Longhorn Engine to have guaranteed CPU allocation. The value is how many CPUs should be reserved for one Longhorn engine or replica. For example, 0.1 means one-tenth of a CPU. This will help the engine stability during the high node workload. It only applies to the volumes attached after the setting took effect. WARNING: Attaching of the volume may fail or stuck while using this feature due to the resource constraint. Disabled (\"0\") by default.",
		Category:    SettingCategoryGeneral,
		Type:        SettingTypeInt,
		Required:    true,
		ReadOnly:    false,
		Default:     "0",
	}
)
