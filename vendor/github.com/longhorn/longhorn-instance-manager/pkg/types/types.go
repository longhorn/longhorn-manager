package types

import (
	"time"
)

const (
	GRPCServiceTimeout = 3 * time.Minute

	ProcessStateRunning  = "running"
	ProcessStateStarting = "starting"
	ProcessStateStopped  = "stopped"
	ProcessStateStopping = "stopping"
	ProcessStateError    = "error"
)

var (
	WaitInterval = 100 * time.Millisecond
	WaitCount    = 600
)

const (
	RetryInterval = 3 * time.Second
	RetryCounts   = 3
)

const (
	InstanceTypeEngine  = "engine"
	InstanceTypeReplica = "replica"
)

const (
	GlobalMountPathPattern = "/host/var/lib/kubelet/plugins/kubernetes.io/csi/driver.longhorn.io/*/globalmount"

	EngineConditionFilesystemReadOnly = "FilesystemReadOnly"
)
