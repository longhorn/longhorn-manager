package controller

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

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

// The flow is: VolumeCloneStateEmpty => VolumeCloneStateInitiated => VolumeCloneStateCopyCompletedAwaitingHealthy => VolumeCloneStateFailed or VolumeCloneStateCompleted
// isCloneTargetCopyInProgress returns true if the volume is a clone target and has not reached copy-complete yet
func isCloneTargetCopyInProgress(v *longhorn.Volume) bool {
	isCloneTarget := types.IsDataFromVolume(v.Spec.DataSource)
	reachedOrAfterCopyComplete := v.Status.CloneStatus.State == longhorn.VolumeCloneStateCopyCompletedAwaitingHealthy ||
		v.Status.CloneStatus.State == longhorn.VolumeCloneStateFailed ||
		v.Status.CloneStatus.State == longhorn.VolumeCloneStateCompleted
	return isCloneTarget && !reachedOrAfterCopyComplete
}

// isCloneTargetActive returns true if the volume is a clone target and has not reached a terminal state
func isCloneTargetActive(v *longhorn.Volume) bool {
	isCloneTarget := types.IsDataFromVolume(v.Spec.DataSource)
	isTerminal := v.Status.CloneStatus.State == longhorn.VolumeCloneStateFailed ||
		v.Status.CloneStatus.State == longhorn.VolumeCloneStateCompleted
	return isCloneTarget && !isTerminal
}

// isCloneTargetNotCompletedAndNotCopyCompleted returns true if v is a clone target and not in copy-complete or completed state
func isCloneTargetNotCompletedAndNotCopyCompleted(v *longhorn.Volume) bool {
	isCloneTarget := types.IsDataFromVolume(v.Spec.DataSource)
	completedOrCopyCompleted := v.Status.CloneStatus.State == longhorn.VolumeCloneStateCopyCompletedAwaitingHealthy ||
		v.Status.CloneStatus.State == longhorn.VolumeCloneStateCompleted
	return isCloneTarget && !completedOrCopyCompleted
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

func enqueueAfterDelay(queue workqueue.TypedRateLimitingInterface[any], obj interface{}, delay time.Duration) error {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		return errors.Wrapf(err, "couldn't get key for object %v", obj)
	}
	queue.AddAfter(key, delay)
	return nil
}
