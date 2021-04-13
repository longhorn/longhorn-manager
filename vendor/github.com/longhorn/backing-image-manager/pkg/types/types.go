package types

import (
	"time"
)

const (
	BackingImageManagerDirectoryName = "backing-images"
	DiskPathInContainer              = "/data/"

	DefaultPort = 8000

	GRPCServiceTimeout     = 3 * time.Minute
	FileValidationInterval = 5 * time.Second
	FileSyncTimeout        = 120

	SendingLimit = 3

	DownloadStatePending     = "pending"
	DownloadStateDownloading = "downloading"
	DownloadStateDownloaded  = "downloaded"
	DownloadStateFailed      = "failed"

	BackingImageTmpFileName = "backing.tmp"
	BackingImageFileName    = "backing"
)
