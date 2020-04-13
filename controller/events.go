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

	EventReasonFailedExpansion    = "FailedExpansion"
	EventReasonSucceededExpansion = "SucceededExpansion"
	EventReasonCanceledExpansion  = "CanceledExpansion"

	EventReasonAttached = "Attached"
	EventReasonDetached = "Detached"
	EventReasonHealthy  = "Healthy"
	EventReasonFaulted  = "Faulted"
	EventReasonDegraded = "Degraded"
	EventReasonOrphaned = "Orphaned"

	EventReasonDetachedUnexpectly = "DetachedUnexpectly"
	EventReasonRemount            = "Remount"
	EventReasonAutoSalvaged       = "AutoSalvaged"
)
