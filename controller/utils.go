package controller

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/flowcontrol"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	podRecreateInitBackoff = 1 * time.Second
	podRecreateMaxBackoff  = 120 * time.Second
	backoffGCPeriod        = 12 * time.Hour
)

// newBackoff returns a flowcontrol.Backoff and starts a background GC loop.
func newBackoff(ctx context.Context) *flowcontrol.Backoff {
	backoff := flowcontrol.NewBackOff(podRecreateInitBackoff, podRecreateMaxBackoff)

	go func() {
		ticker := time.NewTicker(backoffGCPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				backoff.GC()
			}
		}
	}()

	return backoff
}

func hasReplicaEvictionRequested(rs map[string]*longhorn.Replica) bool {
	for _, r := range rs {
		if r.Spec.EvictionRequested {
			return true
		}
	}

	return false
}

func isVolumeUpgrading(v *longhorn.Volume) bool {
	return v.Status.CurrentImage != v.Spec.Image
}

// isTargetVolumeOfAnActiveCloning checks if the input volume is the target volume of an on-going cloning process
func isTargetVolumeOfAnActiveCloning(v *longhorn.Volume) bool {
	isCloningDesired := types.IsDataFromVolume(v.Spec.DataSource)
	isCloningCompletedOrFailed := v.Status.CloneStatus.State == longhorn.VolumeCloneStateCompleted ||
		v.Status.CloneStatus.State == longhorn.VolumeCloneStateFailed
	return isCloningDesired && !isCloningCompletedOrFailed
}

// isCloningRequiredAndNotCompleted returns true if the volume requires cloning and the cloning hasn't completed
func isCloningRequiredAndNotCompleted(v *longhorn.Volume) bool {
	isCloningDesired := types.IsDataFromVolume(v.Spec.DataSource)
	isCloningCompleted := v.Status.CloneStatus.State == longhorn.VolumeCloneStateCompleted
	return isCloningDesired && !isCloningCompleted
}

func isVolumeFullyDetached(vol *longhorn.Volume) bool {
	return vol.Spec.NodeID == "" &&
		vol.Spec.MigrationNodeID == "" &&
		vol.Status.CurrentMigrationNodeID == "" &&
		vol.Status.State == longhorn.VolumeStateDetached
}

func createOrUpdateAttachmentTicket(va *longhorn.VolumeAttachment, ticketID, nodeID, disableFrontend string, attacherType longhorn.AttacherType) {
	attachmentTicket, ok := va.Spec.AttachmentTickets[ticketID]
	if !ok {
		// Create new one
		attachmentTicket = &longhorn.AttachmentTicket{
			ID:     ticketID,
			Type:   attacherType,
			NodeID: nodeID,
			Parameters: map[string]string{
				longhorn.AttachmentParameterDisableFrontend: disableFrontend,
			},
		}
	}
	if attachmentTicket.NodeID != nodeID {
		attachmentTicket.NodeID = nodeID
	}
	va.Spec.AttachmentTickets[attachmentTicket.ID] = attachmentTicket
}

func handleReconcileErrorLogging(logger logrus.FieldLogger, err error, mesg string) {
	if types.ErrorIsInvalidState(err) {
		logger.WithError(err).Trace(mesg)
		return
	}

	if apierrors.IsConflict(err) {
		logger.WithError(err).Warn(mesg)
	} else {
		logger.WithError(err).Error(mesg)
	}
}

// r.Spec.FailedAt and r.Spec.LastFailedAt should both be set when a replica failure occurs.
// r.Spec.FailedAt may be cleared (before rebuilding), but r.Spec.LastFailedAt must not be.
func setReplicaFailedAt(r *longhorn.Replica, timestamp string) {
	r.Spec.FailedAt = timestamp
	if timestamp != "" {
		r.Spec.LastFailedAt = timestamp
	}
}

func isRegularRWXVolume(v *longhorn.Volume) bool {
	if v == nil {
		return false
	}
	return v.Spec.AccessMode == longhorn.AccessModeReadWriteMany && !v.Spec.Migratable
}

func checkIfRemoteDataCleanupIsNeeded(obj runtime.Object, bt *longhorn.BackupTarget) (bool, error) {
	if obj == nil || bt == nil {
		return false, nil
	}
	exists, err := datastore.IsLabelLonghornDeleteCustomResourceOnlyExisting(obj)
	if err != nil {
		return false, err
	}

	return !exists && isBackupTargetAvailable(bt), nil
}

// isBackupTargetAvailable returns a boolean that the backup target is available for true and not available for false
func isBackupTargetAvailable(backupTarget *longhorn.BackupTarget) bool {
	return backupTarget != nil &&
		backupTarget.DeletionTimestamp == nil &&
		backupTarget.Spec.BackupTargetURL != "" &&
		backupTarget.Status.Available
}

// isSnapshotExistInEngine checks if a snapshot with the given name exists in the specified engine.
// It returns true if the snapshot is found, otherwise false.
func isSnapshotExistInEngine(snapshotName string, engine *longhorn.Engine) bool {
	if engine == nil {
		return false
	}

	if engine.Status.Snapshots == nil {
		return false
	}

	for name := range engine.Status.Snapshots {
		if name == snapshotName {
			return true
		}
	}
	return false
}

func newReplicaCR(v *longhorn.Volume, e *longhorn.Engine, hardNodeAffinity string) *longhorn.Replica {
	return &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name:            types.GenerateReplicaNameForVolume(v.Name),
			OwnerReferences: datastore.GetOwnerReferencesForVolume(v),
		},
		Spec: longhorn.ReplicaSpec{
			InstanceSpec: longhorn.InstanceSpec{
				VolumeName:  v.Name,
				VolumeSize:  v.Spec.Size,
				Image:       v.Status.CurrentImage,
				DataEngine:  v.Spec.DataEngine,
				DesireState: longhorn.InstanceStateStopped,
			},
			EngineName:                       e.Name,
			Active:                           true,
			BackingImage:                     v.Spec.BackingImage,
			HardNodeAffinity:                 hardNodeAffinity,
			RevisionCounterDisabled:          v.Spec.RevisionCounterDisabled,
			UnmapMarkDiskChainRemovedEnabled: e.Spec.UnmapMarkSnapChainRemovedEnabled,
			SnapshotMaxCount:                 v.Spec.SnapshotMaxCount,
			SnapshotMaxSize:                  v.Spec.SnapshotMaxSize,
		},
	}
}
