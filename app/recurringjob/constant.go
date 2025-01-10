package recurringjob

import "time"

const (
	HTTPClientTimout = 1 * time.Minute

	SnapshotPurgeStatusInterval = 5 * time.Second
	// SnapshotPurgeStatusTimeout is set to 24 hours because we don't know the appropriate value.
	SnapshotPurgeStatusTimeout = 24 * time.Hour

	WaitInterval              = 5 * time.Second
	DetachingWaitInterval     = 10 * time.Second
	VolumeAttachTimeout       = 300 // 5 minutes
	BackupProcessStartTimeout = 90  // 1.5 minutes
	SnapshotReadyTimeout      = 390 // 6.5 minutes
)
