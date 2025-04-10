package types

import (
	"time"
)

const (
	BinaryCryptsetup = "cryptsetup"
	BinaryFstrim     = "fstrim"
)

const (
	ExecuteNoTimeout            = time.Duration(-1)
	ExecuteDefaultTimeout       = time.Minute * 5  // Increase default timeout for execute commands to 5'
	BackupExecuteDefaultTimeout = time.Minute * 30 // Increase default timeout for backup execute commands to 30'
)
