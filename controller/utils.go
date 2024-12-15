package controller

import (
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func hasReplicaEvictionRequested(rs map[string]*longhorn.Replica) bool {
	for _, r := range rs {
		if r.Spec.EvictionRequested {
			return true
		}
	}

	return false
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

	return !exists && bt.Spec.BackupTargetURL != "", nil
}
