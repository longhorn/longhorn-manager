package engineapi

import (
	"fmt"
	"strings"
	"time"

	devtypes "github.com/longhorn/go-iscsi-helper/types"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	// CurrentCLIVersion indicates the API version manager used to talk with the
	// engine, including `longhorn-engine` and `longhorn-instance-manager`
	CurrentCLIVersion = 3

	InstanceManagerDefaultPort = 8500

	DefaultISCSIPort = "3260"
	DefaultISCSILUN  = "1"

	commonTimeout = 1 * time.Minute

	BackupStateComplete   = "complete"
	BackupStateError      = "error"
	BackupStateInProgress = "in_progress"
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
	Version(clientOnly bool) (*EngineVersion, error)

	Info() (*Volume, error)
	Endpoint() (string, error)
	Expand(size int64) error

	FrontendStart(volumeFrontend types.VolumeFrontend) error
	FrontendShutdown() error

	ReplicaList() (map[string]*Replica, error)
	ReplicaAdd(url string) error
	ReplicaRemove(url string) error
	ReplicaRebuildStatus() (map[string]*types.RebuildStatus, error)

	SnapshotCreate(name string, labels map[string]string) (string, error)
	SnapshotList() (map[string]*types.Snapshot, error)
	SnapshotGet(name string) (*types.Snapshot, error)
	SnapshotDelete(name string) error
	SnapshotRevert(name string) error
	SnapshotPurge() error
	SnapshotPurgeStatus() (map[string]*types.PurgeStatus, error)
	SnapshotBackup(snapName, backupTarget string, labels map[string]string, credential map[string]string) (string, error)
	SnapshotBackupStatus() (map[string]*types.BackupStatus, error)

	BackupRestore(backupTarget, backupName, backupVolume, lastRestored string, credential map[string]string) error
	BackupRestoreStatus() (map[string]*types.RestoreStatus, error)
}

type EngineClientRequest struct {
	VolumeName  string
	EngineImage string
	IP          string
	Port        int
}

type EngineClientCollection interface {
	NewEngineClient(request *EngineClientRequest) (EngineClient, error)
}

type Volume struct {
	Name                  string `json:"name"`
	Size                  int64  `json:"size"`
	ReplicaCount          int    `json:"replicaCount"`
	Endpoint              string `json:"endpoint"`
	Frontend              string `json:"frontend"`
	FrontendState         string `json:"frontendState"`
	IsExpanding           bool   `json:"isExpanding"`
	LastExpansionError    string `json:"lastExpansionError"`
	LastExpansionFailedAt string `json:"lastExpansionFailedAt"`
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
	Name            string                             `json:"name"`
	URL             string                             `json:"url"`
	SnapshotName    string                             `json:"snapshotName"`
	SnapshotCreated string                             `json:"snapshotCreated"`
	Created         string                             `json:"created"`
	Size            string                             `json:"size"`
	Labels          map[string]string                  `json:"labels"`
	VolumeName      string                             `json:"volumeName"`
	VolumeSize      string                             `json:"volumeSize"`
	VolumeCreated   string                             `json:"volumeCreated"`
	Messages        map[backupstore.MessageType]string `json:"messages"`
}

type BackupCreateInfo struct {
	BackupID      string
	IsIncremental bool
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

type TaskError struct {
	ReplicaErrors []ReplicaError
}

type ReplicaError struct {
	Address string
	Message string
}

func (e TaskError) Error() string {
	var errs []string
	for _, re := range e.ReplicaErrors {
		errs = append(errs, re.Error())
	}

	if errs == nil {
		return "Unknown"
	}
	if len(errs) == 1 {
		return errs[0]
	}
	return strings.Join(errs, "; ")
}

func (e ReplicaError) Error() string {
	return fmt.Sprintf("%v: %v", e.Address, e.Message)
}

func GetBackendReplicaURL(address string) string {
	return "tcp://" + address
}

func GetAddressFromBackendReplicaURL(url string) string {
	// tcp://<address>:<Port>
	return strings.TrimPrefix(url, "tcp://")
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

func GetEngineProcessFrontend(volumeFrontend types.VolumeFrontend) (string, error) {
	frontend := ""
	if volumeFrontend == types.VolumeFrontendBlockDev {
		frontend = string(devtypes.FrontendTGTBlockDev)
	} else if volumeFrontend == types.VolumeFrontendISCSI {
		frontend = string(devtypes.FrontendTGTISCSI)
	} else if volumeFrontend == types.VolumeFrontend("") {
		frontend = ""
	} else {
		return "", fmt.Errorf("unknown volume frontend %v", volumeFrontend)
	}

	return frontend, nil
}
