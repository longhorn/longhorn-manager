package controller

const (
	EventReasonCreate         = "Create"
	EventReasonFailedCreating = "FailedCreating"
	EventReasonDelete         = "Delete"
	EventReasonFailedDeleting = "FailedDeleting"
	EventReasonStart          = "Start"
	EventReasonFailedStarting = "FailedStarting"
	EventReasonStop           = "Stop"
	EventReasonFailedStopping = "FailedStopping"

	EventReasonRebuilded        = "Rebuilded"
	EventReasonRebuilding       = "Rebuilding"
	EventReasonFailedRebuilding = "FailedRebuilding"

	EventReasonFailedRestore = "FailedRestore"

	EventReasonFailedExpansion    = "FailedExpansion"
	EventReasonSucceededExpansion = "SucceededExpansion"
	EventReasonCanceledExpansion  = "CanceledExpansion"

	EventReasonAttached = "Attached"
	EventReasonDetached = "Detached"
	EventReasonHealthy  = "Healthy"
	EventReasonFaulted  = "Faulted"
	EventReasonDegraded = "Degraded"
	EventReasonOrphaned = "Orphaned"
	EventReasonUnknown  = "Unknown"

	EventReasonDetachedUnexpectly = "DetachedUnexpectly"
	EventReasonRemount            = "Remount"
	EventReasonAutoSalvaged       = "AutoSalvaged"
)
