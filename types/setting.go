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
	SettingNameBackupTarget                 = SettingName("backup-target")
	SettingNameBackupTargetCredentialSecret = SettingName("backup-target-credential-secret")
	SettingNameDefaultEngineImage           = SettingName("default-engine-image")
)

type SettingDefinition struct {
	DisplayName string      `json:"displayName"`
	Description string      `json:"description"`
	Type        SettingType `json:"type"`
	Nullable    bool        `json:"nullable"`
	ReadOnly    bool        `json:"readOnly"`
	Default     string      `json:"default"`
}

var (
	SettingDefinitions = map[SettingName]SettingDefinition{
		SettingNameBackupTarget:                 SettingDefinitionBackupTarget,
		SettingNameBackupTargetCredentialSecret: SettingDefinitionBackupTargetCredentialSecret,
		SettingNameDefaultEngineImage:           SettingDefinitionDefaultEngineImage,
	}

	SettingDefinitionBackupTarget = SettingDefinition{
		DisplayName: "Backup Target",
		Description: "The target used for backup. Support NFS or S3.",
		Type:        SettingTypeString,
		Nullable:    true,
		ReadOnly:    false,
	}

	SettingDefinitionBackupTargetCredentialSecret = SettingDefinition{
		DisplayName: "Backup Target Credential Secret",
		Description: "The Kubernetes secret associated with the backup target.",
		Type:        SettingTypeString,
		Nullable:    true,
		ReadOnly:    false,
	}

	SettingDefinitionDefaultEngineImage = SettingDefinition{
		DisplayName: "Default Engine Image",
		Description: "The default engine image used by the manager. Can be changed on the manager starting command line only",
		Type:        SettingTypeString,
		Nullable:    false,
		ReadOnly:    true,
	}
)
