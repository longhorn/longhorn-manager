package types

import (
	"fmt"
	"strings"

	"github.com/longhorn/types/pkg/generated/spdkrpc"
)

const (
	MetadataDir = "/metadata"
)

type Mode string

const (
	ModeWO  = Mode("WO")
	ModeRW  = Mode("RW")
	ModeERR = Mode("ERR")
)

const (
	FrontendSPDKTCPNvmf     = "spdk-tcp-nvmf"
	FrontendSPDKTCPBlockdev = "spdk-tcp-blockdev"
	FrontendUBLK            = "ublk"
	FrontendEmpty           = ""
)

type InstanceState string

const (
	InstanceStatePending     = "pending"
	InstanceStateStopped     = "stopped"
	InstanceStateRunning     = "running"
	InstanceStateTerminating = "terminating"
	InstanceStateError       = "error"
	InstanceStateSuspended   = "suspended"
)

type InstanceType string

const (
	InstanceTypeReplica        = InstanceType("replica")
	InstanceTypeEngine         = InstanceType("engine")
	InstanceTypeEngineFrontend = InstanceType("engine-frontend")
	InstanceTypeBackingImage   = InstanceType("backingImage")
)

type BackingImageState string

const (
	BackingImageStatePending    = BackingImageState("pending")
	BackingImageStateStarting   = BackingImageState("starting")
	BackingImageStateReady      = BackingImageState("ready")
	BackingImageStateInProgress = BackingImageState("in-progress")
	BackingImageStateFailed     = BackingImageState("failed")
	BackingImageStateUnknown    = BackingImageState("unknown")
)

const (
	BackingImagePortCount = 1
)

const VolumeHead = "volume-head"

const SPDKServicePort = 8504

const (
	DefaultUblkQueueDepth    = 128
	DefaultUblkNumberOfQueue = 1
)

func IsUblkFrontend(frontend string) bool {
	return frontend == FrontendUBLK
}

// IsFrontendSupported returns true if the given frontend type is one of the
// supported frontends (spdk-tcp-blockdev, spdk-tcp-nvmf, ublk, or empty).
func IsFrontendSupported(frontend string) bool {
	switch frontend {
	case FrontendSPDKTCPBlockdev, FrontendSPDKTCPNvmf, FrontendUBLK, FrontendEmpty:
		return true
	default:
		return false
	}
}

func ReplicaModeToGRPCReplicaMode(mode Mode) spdkrpc.ReplicaMode {
	switch mode {
	case ModeWO:
		return spdkrpc.ReplicaMode_WO
	case ModeRW:
		return spdkrpc.ReplicaMode_RW
	case ModeERR:
		return spdkrpc.ReplicaMode_ERR
	}
	return spdkrpc.ReplicaMode_ERR
}

func GRPCReplicaModeToReplicaMode(replicaMode spdkrpc.ReplicaMode) Mode {
	switch replicaMode {
	case spdkrpc.ReplicaMode_WO:
		return ModeWO
	case spdkrpc.ReplicaMode_RW:
		return ModeRW
	case spdkrpc.ReplicaMode_ERR:
		return ModeERR
	}
	return ModeERR
}

const (
	ProgressStateError      = "error"
	ProgressStateComplete   = "complete"
	ProgressStateInProgress = "in_progress"
	ProgressStateStarting   = "starting"

	// SPDKShallowCopyStateNew is the state returned from spdk_tgt. There is no underscore in the string.
	SPDKShallowCopyStateInProgress = "in progress"
	SPDKDeepCopyStateInProgress    = "in progress"
)

// Longhorn defined snapshot attributes
const (
	LonghornBackingImageSnapshotAttrChecksum     = "longhorn_backing_image_checksum"
	LonghornBackingImageSnapshotAttrUUID         = "longhorn_backing_image_uuid"
	LonghornBackingImageSnapshotAttrPrepareState = "longhorn_backing_image_prepare_state"
)

// Backing image related utility functions
const (
	BackingImageTempHeadLvolSuffix = "temp-head"
)

func IsBackingImageSnapLvolName(lvolName string) bool {
	return strings.HasPrefix(lvolName, "bi-") && !strings.HasSuffix(lvolName, BackingImageTempHeadLvolSuffix)
}

func GetBackingImageSnapLvolNameFromTempHeadLvolName(lvolName string) string {
	return strings.TrimSuffix(lvolName, fmt.Sprintf("-%s", BackingImageTempHeadLvolSuffix))
}

func IsBackingImageTempHead(lvolName string) bool {
	return strings.HasSuffix(lvolName, BackingImageTempHeadLvolSuffix)
}
