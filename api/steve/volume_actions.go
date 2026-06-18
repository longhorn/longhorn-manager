package steve

import (
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// volumeActionsForState returns the set of action names that are valid for a
// volume in the given state and robustness. It mirrors the dynamic action
// selection that the legacy /v1 API performs in api.toVolumeResource, so the
// Steve API exposes exactly the same state-aware action list and the frontend
// can render buttons purely from the backend-provided actions map.
//
// attach and detach are always allowed: the volume manager is responsible for
// handling them appropriately regardless of the current state.
func volumeActionsForState(state, robustness string) []string {
	actions := map[string]struct{}{
		"attach": {},
		"detach": {},
	}

	if robustness == string(longhorn.VolumeRobustnessFaulted) {
		actions["salvage"] = struct{}{}
	} else {
		actions["snapshotCRCreate"] = struct{}{}
		actions["snapshotCRGet"] = struct{}{}
		actions["snapshotCRList"] = struct{}{}
		actions["snapshotCRDelete"] = struct{}{}
		actions["snapshotBackup"] = struct{}{}

		switch longhorn.VolumeState(state) {
		case longhorn.VolumeStateDetached:
			actions["activate"] = struct{}{}
			actions["expand"] = struct{}{}
			actions["cancelExpansion"] = struct{}{}
			actions["offlineReplicaRebuilding"] = struct{}{}
			actions["replicaRemove"] = struct{}{}
			actions["engineUpgrade"] = struct{}{}
			actions["pvCreate"] = struct{}{}
			actions["pvcCreate"] = struct{}{}
			actions["updateDataLocality"] = struct{}{}
			actions["updateAccessMode"] = struct{}{}
			actions["updateReplicaAutoBalance"] = struct{}{}
			actions["updateRebuildConcurrentSyncLimit"] = struct{}{}
			actions["updateUnmapMarkSnapChainRemoved"] = struct{}{}
			actions["updateSnapshotDataIntegrity"] = struct{}{}
			actions["updateSnapshotMaxCount"] = struct{}{}
			actions["updateSnapshotMaxSize"] = struct{}{}
			actions["updateReplicaRebuildingBandwidthLimit"] = struct{}{}
			actions["updateUblkQueueDepth"] = struct{}{}
			actions["updateUblkNumberOfQueue"] = struct{}{}
			actions["updateBackupCompressionMethod"] = struct{}{}
			actions["updateReplicaSoftAntiAffinity"] = struct{}{}
			actions["updateReplicaZoneSoftAntiAffinity"] = struct{}{}
			actions["updateReplicaDiskSoftAntiAffinity"] = struct{}{}
			actions["updateFreezeFilesystemForSnapshot"] = struct{}{}
			actions["updateBackupTargetName"] = struct{}{}
			actions["recurringJobAdd"] = struct{}{}
			actions["recurringJobDelete"] = struct{}{}
			actions["recurringJobList"] = struct{}{}
		case longhorn.VolumeStateAttaching:
			actions["cancelExpansion"] = struct{}{}
			actions["offlineReplicaRebuilding"] = struct{}{}
			actions["recurringJobAdd"] = struct{}{}
			actions["recurringJobDelete"] = struct{}{}
			actions["recurringJobList"] = struct{}{}
		case longhorn.VolumeStateAttached:
			actions["activate"] = struct{}{}
			actions["expand"] = struct{}{}
			actions["snapshotPurge"] = struct{}{}
			// Deprecated, replaced by snapshotCRCreate
			actions["snapshotCreate"] = struct{}{}
			actions["snapshotList"] = struct{}{}
			actions["snapshotGet"] = struct{}{}
			actions["snapshotDelete"] = struct{}{}
			actions["snapshotRevert"] = struct{}{}
			actions["replicaRemove"] = struct{}{}
			actions["engineUpgrade"] = struct{}{}
			actions["updateReplicaCount"] = struct{}{}
			actions["updateDataLocality"] = struct{}{}
			actions["updateReplicaAutoBalance"] = struct{}{}
			actions["updateRebuildConcurrentSyncLimit"] = struct{}{}
			actions["updateUnmapMarkSnapChainRemoved"] = struct{}{}
			actions["updateSnapshotDataIntegrity"] = struct{}{}
			actions["updateSnapshotMaxCount"] = struct{}{}
			actions["updateSnapshotMaxSize"] = struct{}{}
			actions["updateReplicaRebuildingBandwidthLimit"] = struct{}{}
			actions["updateBackupCompressionMethod"] = struct{}{}
			actions["updateReplicaSoftAntiAffinity"] = struct{}{}
			actions["updateReplicaZoneSoftAntiAffinity"] = struct{}{}
			actions["updateReplicaDiskSoftAntiAffinity"] = struct{}{}
			actions["updateFreezeFilesystemForSnapshot"] = struct{}{}
			actions["updateBackupTargetName"] = struct{}{}
			actions["pvCreate"] = struct{}{}
			actions["pvcCreate"] = struct{}{}
			actions["cancelExpansion"] = struct{}{}
			actions["offlineReplicaRebuilding"] = struct{}{}
			actions["trimFilesystem"] = struct{}{}
			actions["recurringJobAdd"] = struct{}{}
			actions["recurringJobDelete"] = struct{}{}
			actions["recurringJobList"] = struct{}{}
		}
	}

	names := make([]string, 0, len(actions))
	for name := range actions {
		names = append(names, name)
	}
	return names
}
