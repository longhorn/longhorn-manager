package client

import (
	"time"

	"google.golang.org/grpc"

	"github.com/longhorn/types/pkg/generated/spdkrpc"
)

const (
	GRPCServiceTimeout     = 3 * time.Minute
	GRPCServiceMedTimeout  = 24 * time.Hour
	GRPCServiceLongTimeout = 72 * time.Hour
)

type SPDKServiceContext struct {
	cc      *grpc.ClientConn
	service spdkrpc.SPDKServiceClient
}

type SPDKClient struct {
	serviceURL string
	SPDKServiceContext
}

type BackupCreateRequest struct {
	BackupName           string
	SnapshotName         string
	VolumeName           string
	EngineName           string
	ReplicaName          string
	Size                 uint64
	BackupTarget         string
	StorageClassName     string
	BackingImageName     string
	BackingImageChecksum string
	CompressionMethod    string
	ConcurrentLimit      int32
	Labels               []string
	Credential           map[string]string
}

type BackupRestoreRequest struct {
	BackupUrl       string
	EngineName      string
	ReplicaName     string
	SnapshotName    string
	Credential      map[string]string
	ConcurrentLimit int32
}
