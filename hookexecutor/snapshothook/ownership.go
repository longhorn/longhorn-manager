package snapshothook

import (
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// SnapshotGroup status has two writers, and each one writes the whole
// status object. The controller owns the phase and the member records; the
// hook executor owns the hook record. This only works while each writer
// carries the other's fields unchanged from the latest copy it read.

// controllerOwnedStatusFields are set only by the snapshot group controller.
var controllerOwnedStatusFields = []string{
	"OwnerID",
	"Phase",
	"Members",
	"ReadyToUse",
	"CreationTime",
	"Error",
	"Conditions",
}

// executorOwnedStatusFields are set only by the hook executor.
var executorOwnedStatusFields = []string{
	"PreHooksDone",
	"HooksStartedAt",
	"PreHooksDoneAt",
	"PostHooksDoneAt",
	"HookProgress",
	"Hooks",
}

// AdoptExecutorStatus copies the executor-owned fields from src into dst.
func AdoptExecutorStatus(dst, src *longhorn.SnapshotGroupStatus) {
	dst.PreHooksDone = src.PreHooksDone
	dst.HooksStartedAt = src.HooksStartedAt
	dst.PreHooksDoneAt = src.PreHooksDoneAt
	dst.PostHooksDoneAt = src.PostHooksDoneAt
	dst.HookProgress = src.HookProgress
	dst.Hooks = src.Hooks
}
