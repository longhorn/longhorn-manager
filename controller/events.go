package controller

const (
	EventReasonCreate = "Create"
	EventReasonDelete = "Delete"
	EventReasonStart  = "Start"
	EventReasonStop   = "Stop"

	EventReasonFailedCreating = "FailedCreating"
	EventReasonFailedDeleting = "FailedDeleting"
	EventReasonFailedStarting = "FailedStarting"
	EventReasonFailedStopping = "FailedStopping"

	EventReasonEngineRemoveReplica       = "EngineRemoveReplica"
	EventReasonFailedEngineRemoveReplica = "FailedEngineRemoveReplica"

	EventReasonEngineStartRebuild  = "EngineStartRebuild"
	EventReasonFailedEngineRebuild = "FailedEngineRebuild"
)
