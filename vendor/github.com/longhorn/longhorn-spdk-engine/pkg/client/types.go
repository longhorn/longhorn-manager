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
	// Parameters carries well-known backup parameters (e.g. "backup-mode") that
	// backupstore's DeltaBackupConfig needs but that the v2 spdkrpc.BackupCreateRequest
	// proto message has no dedicated field for. The client wrappers
	// (EngineBackupCreate / ReplicaBackupCreate) smuggle these through the
	// Labels field on the wire; the server side extracts them again before
	// they can be persisted as user-visible backup labels. See
	// types.EncodeBackupParametersIntoLabels / ExtractBackupParametersFromLabels
	// for the recognised keys and the encoder/decoder pair.
	Parameters map[string]string
	Credential map[string]string
}

type BackupRestoreRequest struct {
	BackupUrl       string
	EngineName      string
	ReplicaName     string
	SnapshotName    string
	Credential      map[string]string
	ConcurrentLimit int32
}
