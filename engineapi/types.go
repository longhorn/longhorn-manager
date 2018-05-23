package engineapi

import (
	"fmt"
	"strings"

	"github.com/rancher/longhorn-manager/types"
)

const (
	ControllerDefaultPort     = "9501"
	EngineLauncherDefaultPort = "9510"
	ReplicaDefaultPort        = "9502"
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
	Endpoint() string
	Upgrade(binary string, replicaURLs []string) error

	ReplicaList() (map[string]*Replica, error)
	ReplicaAdd(url string) error
	ReplicaRemove(url string) error

	SnapshotCreate(name string, labels map[string]string) (string, error)
	SnapshotList() (map[string]*Snapshot, error)
	SnapshotGet(name string) (*Snapshot, error)
	SnapshotDelete(name string) error
	SnapshotRevert(name string) error
	SnapshotPurge() error
	SnapshotBackup(snapName, backupTarget string, labels map[string]string) error
}

type EngineClientRequest struct {
	VolumeName        string
	ControllerURL     string
	EngineLauncherURL string
}

type EngineClientCollection interface {
	NewEngineClient(request *EngineClientRequest) (EngineClient, error)
}

type Volume struct {
	Name         string `json:"name"`
	ReplicaCount int    `json:"replicaCount"`
	Endpoint     string `json:"endpoint"`
}

type Snapshot struct {
	Name        string              `json:"name"`
	Parent      string              `json:"parent"`
	Children    map[string]struct{} `json:"children"`
	Removed     bool                `json:"removed"`
	UserCreated bool                `json:"usercreated"`
	Created     string              `json:"created"`
	Size        string              `json:"size"`
	Labels      map[string]string   `json:"labels"`
}

type BackupVolume struct {
	Name           string `json:"name"`
	Size           string `json:"size"`
	Created        string `json:"created"`
	LastBackupName string
	SpaceUsage     string
	Backups        map[string]*Backup
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

func GetControllerDefaultURL(ip string) string {
	return "http://" + ip + ":" + ControllerDefaultPort
}

func GetEngineLauncherDefaultURL(ip string) string {
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
