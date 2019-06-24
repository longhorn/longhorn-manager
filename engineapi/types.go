package engineapi

import (
	"fmt"
	"strings"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	// CurrentCLIVersion indicates the API version manager used to talk with the
	// engine, including `longhorn-engine` and `longhorn-engine-launcher`
	CurrentCLIVersion = 1

	ControllerDefaultPort     = "9501"
	EngineLauncherDefaultPort = "9510"
	ReplicaDefaultPort        = "9502"

	DefaultISCSIPort = "3260"
	DefaultISCSILUN  = "1"

	FrontendISCSI    = "tgt-iscsi"
	FrontendBlockDev = "tgt-blockdev"
)

type Replica struct {
	URL  string
	Mode types.ReplicaMode
}

type Controller struct {
	URL    string
	NodeID string
}

type EngineClient interface {
	Name() string
	Endpoint() (string, error)
	Version(clientOnly bool) (*EngineVersion, error)
	Upgrade(binary string, replicaURLs []string) error

	Info() (*Volume, error)

	ReplicaList() (map[string]*Replica, error)
	ReplicaAdd(url string) error
	ReplicaRemove(url string) error

	SnapshotCreate(name string, labels map[string]string) (string, error)
	SnapshotList() (map[string]*Snapshot, error)
	SnapshotGet(name string) (*Snapshot, error)
	SnapshotDelete(name string) error
	SnapshotRevert(name string) error
	SnapshotPurge() error
	SnapshotBackup(snapName, backupTarget string, labels map[string]string, credential map[string]string) error
	SnapshotBackupStatus() (map[string]*types.BackupStatus, error)

	BackupRestoreIncrementally(backupTarget, backupName, backupVolume, lastBackup string) error
	BackupRestore(backup string) error
	BackupRestoreStatus() ([]*types.RestoreStatus, error)
}

type EngineClientRequest struct {
	VolumeName  string
	EngineImage string
	IP          string
}

type EngineClientCollection interface {
	NewEngineClient(request *EngineClientRequest) (EngineClient, error)
}

type Volume struct {
	Name          string `json:"name"`
	ReplicaCount  int    `json:"replicaCount"`
	Endpoint      string `json:"endpoint"`
	Frontend      string `json:"frontend"`
	FrontendState string `json:"frontendState"`
	IsRestoring   bool   `json:"isRestoring"`
	LastRestored  string `json:"lastRestored"`
}

type Snapshot struct {
	Name        string            `json:"name"`
	Parent      string            `json:"parent"`
	Children    map[string]bool   `json:"children"`
	Removed     bool              `json:"removed"`
	UserCreated bool              `json:"usercreated"`
	Created     string            `json:"created"`
	Size        string            `json:"size"`
	Labels      map[string]string `json:"labels"`
}

type BackupVolume struct {
	Name           string                             `json:"name"`
	Size           string                             `json:"size"`
	Created        string                             `json:"created"`
	LastBackupName string                             `json:"lastBackupName"`
	LastBackupAt   string                             `json:"lastBackupAt"`
	DataStored     string                             `json:"dataStored"`
	Messages       map[backupstore.MessageType]string `json:"messages"`
	Backups        map[string]*Backup                 `json:"backups"`
	BaseImage      string                             `json:"baseImage"`
}

type Backup struct {
	Name            string            `json:"name"`
	URL             string            `json:"url"`
	SnapshotName    string            `json:"snapshotName"`
	SnapshotCreated string            `json:"snapshotCreated"`
	Created         string            `json:"created"`
	Size            string            `json:"size"`
	Labels          map[string]string `json:"labels"`
	VolumeName      string            `json:"volumeName"`
	VolumeSize      string            `json:"volumeSize"`
	VolumeCreated   string            `json:"volumeCreated"`
}

type LauncherVolumeInfo struct {
	Volume   string `json:"volume,omitempty"`
	Frontend string `json:"frontend,omitempty"`
	Endpoint string `json:"endpoint,omitempty"`
}

type EngineVersion struct {
	ClientVersion *types.EngineVersionDetails `json:"clientVersion"`
	ServerVersion *types.EngineVersionDetails `json:"serverVersion"`
}

func GetControllerDefaultURL(ip string) string {
	if ip == "" {
		return ""
	}
	return "http://" + ip + ":" + ControllerDefaultPort
}

func GetEngineLauncherDefaultURL(ip string) string {
	if ip == "" {
		return ""
	}
	return ip + ":" + EngineLauncherDefaultPort
}

func GetReplicaDefaultURL(ip string) string {
	return "tcp://" + ip + ":" + ReplicaDefaultPort
}

func GetIPFromURL(url string) string {
	// tcp, \/\/<address>, 9502
	return strings.TrimPrefix(strings.Split(url, ":")[1], "//")
}

func ValidateReplicaURL(url string) error {
	if !strings.HasPrefix(url, "tcp://") {
		return fmt.Errorf("invalid replica url %v", url)
	}
	return nil
}

func CheckCLICompatibilty(cliVersion, cliMinVersion int) error {
	if CurrentCLIVersion > cliVersion || CurrentCLIVersion < cliMinVersion {
		return fmt.Errorf("Current CLI version %v is not compatible with CLIVersion %v and CLIMinVersion %v", CurrentCLIVersion, cliVersion, cliMinVersion)
	}
	return nil
}
