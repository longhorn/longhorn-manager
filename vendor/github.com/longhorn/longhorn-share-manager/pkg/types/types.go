package types

import (
	"time"
)

const (
	GRPCServiceTimeout = 3 * time.Minute
)

var (
	WaitInterval = 100 * time.Millisecond
	WaitCount    = 600
)

const (
	RetryInterval = 3 * time.Second
	RetryCounts   = 3
)

type ShareState string

const (
	ShareStateReady   = ShareState("ready")
	ShareStateError   = ShareState("error")
	ShareStatePending = ShareState("pending")
	ShareStateUnknown = ShareState("unknown")
	ShareStateDeleted = ShareState("deleted")
)

type Share struct {
	Volume   string     `json:"volume"`
	ExportID uint16     `json:"exportID"`
	State    ShareState `json:"state"`
	Error    string     `json:"error"`
}
