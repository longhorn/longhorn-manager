package engineapi

import (
	"fmt"
	"strings"
)

type ReplicaMode string

const (
	ControllerDefaultPort = "9501"
	ReplicaDefaultPort    = "9502"

	ReplicaModeRW  = ReplicaMode("RW")
	ReplicaModeWO  = ReplicaMode("WO")
	ReplicaModeERR = ReplicaMode("ERR")
)

type Replica struct {
	URL  string
	Mode ReplicaMode
}

type Controller struct {
	URL    string
	NodeID string
}

type EngineClient interface {
	Name() string
	Endpoint() string

	ReplicaList() (map[string]*Replica, error)
	ReplicaAdd(url string) error
	ReplicaRemove(url string) error

	SnapshotCreate(name string, labels map[string]string) (string, error)
	SnapshotList() (map[string]*Snapshot, error)
	SnapshotGet(name string) (*Snapshot, error)
	SnapshotDelete(name string) error
	SnapshotRevert(name string) error
	SnapshotPurge() error
	SnapshotBackup(snapName, backupTarget string) error
}

type EngineClientRequest struct {
	VolumeName    string
	ControllerURL string
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
	Name        string            `json:"name"`
	Parent      string            `json:"parent"`
	Children    []string          `json:"children"`
	Removed     bool              `json:"removed"`
	UserCreated bool              `json:"usercreated"`
	Created     string            `json:"created"`
	Size        string            `json:"size"`
	Labels      map[string]string `json:"labels"`
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
	Name            string `json:"name,omitempty"`
	URL             string `json:"url,omitempty"`
	SnapshotName    string `json:"snapshotName,omitempty"`
	SnapshotCreated string `json:"snapshotCreated,omitempty"`
	Created         string `json:"created,omitempty"`
	Size            string `json:"size,omitempty"`
	VolumeName      string `json:"volumeName,omitempty"`
	VolumeSize      string `json:"volumeSize,omitempty"`
	VolumeCreated   string `json:"volumeCreated,omitempty"`
}

func GetControllerDefaultURL(ip string) string {
	return "http://" + ip + ":" + ControllerDefaultPort
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
