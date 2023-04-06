package types

import (
	"path"
	"path/filepath"
	"time"
)

const (
	GRPCServiceTimeout = 1 * time.Minute

	DevPath       = "/dev"
	MapperDevPath = "/dev/mapper"

	ExportPath = "/export"
)

type ProgressState string

const (
	ProgressStatePending    = ProgressState("pending")
	ProgressStateStarting   = ProgressState("starting")
	ProgressStateInProgress = ProgressState("in_progress")
	ProgressStateComplete   = ProgressState("complete")
	ProgressStateError      = ProgressState("error")
)

func GetVolumeDevicePath(volumeName string, encryptedDevice bool) string {
	if encryptedDevice {
		return path.Join(MapperDevPath, volumeName)
	}
	return filepath.Join(DevPath, "longhorn", volumeName)
}

func GetMountPath(volumeName string) string {
	return filepath.Join(ExportPath, volumeName)
}
