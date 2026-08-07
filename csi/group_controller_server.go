package csi

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/rest"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	bsutil "github.com/longhorn/backupstore/util"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhtypedv1beta2 "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/typed/longhorn/v1beta2"
)

const (
	timeoutSnapshotGroupCreation = 90 * time.Second
	tickSnapshotGroupCreation    = 2 * time.Second
	timeoutSnapshotGroupDeletion = 90 * time.Second
	tickSnapshotGroupDeletion    = 2 * time.Second
)

// GroupControllerServer serves the CSI GroupController service backed by the
// Longhorn SnapshotGroup CRD. Unlike the other CSI servers, it talks to the
// Kubernetes API directly instead of the Longhorn REST API: the SnapshotGroup
// reconcile is asynchronous and the CR status is the single source of truth
// the server needs.
type GroupControllerServer struct {
	csi.UnimplementedGroupControllerServer
	enabled     bool
	lhClient    lhclientset.Interface
	lhNamespace string
	caps        []*csi.GroupControllerServiceCapability
	groupLocks  snapshotGroupLockTable
	log         *logrus.Entry
}

func NewGroupControllerServer(enabled bool) (*GroupControllerServer, error) {
	log := logrus.StandardLogger().WithField("component", "csi-group-controller-server")

	// When the volume group snapshot toggle is off, serve the GroupController
	// service with no advertised capabilities, so probing callers see group
	// snapshot support as unavailable.
	if !enabled {
		return &GroupControllerServer{log: log}, nil
	}

	lhNamespace := os.Getenv(types.EnvPodNamespace)
	if lhNamespace == "" {
		return nil, fmt.Errorf("failed to detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get client config")
	}

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get longhorn clientset")
	}

	return &GroupControllerServer{
		enabled:     true,
		lhClient:    lhClient,
		lhNamespace: lhNamespace,
		caps: getGroupControllerServiceCapabilities(
			[]csi.GroupControllerServiceCapability_RPC_Type{
				csi.GroupControllerServiceCapability_RPC_CREATE_DELETE_GET_VOLUME_GROUP_SNAPSHOT,
			}),
		log: log,
	}, nil
}

func (srv *GroupControllerServer) snapshotGroups() lhtypedv1beta2.SnapshotGroupInterface {
	return srv.lhClient.LonghornV1beta2().SnapshotGroups(srv.lhNamespace)
}

func (srv *GroupControllerServer) backups() lhtypedv1beta2.BackupInterface {
	return srv.lhClient.LonghornV1beta2().Backups(srv.lhNamespace)
}

func (srv *GroupControllerServer) volumes() lhtypedv1beta2.VolumeInterface {
	return srv.lhClient.LonghornV1beta2().Volumes(srv.lhNamespace)
}

func (srv *GroupControllerServer) GroupControllerGetCapabilities(ctx context.Context, req *csi.GroupControllerGetCapabilitiesRequest) (*csi.GroupControllerGetCapabilitiesResponse, error) {
	return &csi.GroupControllerGetCapabilitiesResponse{
		Capabilities: srv.caps,
	}, nil
}

func (srv *GroupControllerServer) CreateVolumeGroupSnapshot(ctx context.Context, req *csi.CreateVolumeGroupSnapshotRequest) (*csi.CreateVolumeGroupSnapshotResponse, error) {
	if !srv.enabled {
		return nil, status.Error(codes.Unimplemented, "volume group snapshot support is disabled")
	}

	log := srv.log.WithFields(logrus.Fields{"function": "CreateVolumeGroupSnapshot"})

	groupName := req.GetName()
	if len(groupName) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume group snapshot name must be provided")
	}
	volumeNames := req.GetSourceVolumeIds()
	if len(volumeNames) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume group snapshot source volume ids must be provided")
	}

	params, err := parseVolumeGroupSnapshotParameters(req.GetParameters())
	if err != nil {
		return nil, err
	}

	if err := srv.ensureSnapshotGroupExists(ctx, log, groupName, params, volumeNames); err != nil {
		return nil, err
	}

	snapshotGroup, err := srv.waitForSnapshotGroupTerminalPhase(ctx, groupName)
	if err != nil {
		return nil, err
	}
	if snapshotGroup.Status.Phase == longhorn.SnapshotGroupPhaseFailed {
		// Delete the failed group before returning the error: an error
		// response records no handle, so the sidecar could never delete the
		// group, and the next retry starts fresh with a new deadline window.
		failureMessage := snapshotGroup.Status.Error
		if err := srv.deleteSnapshotGroupAndWait(ctx, groupName); err != nil {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "volume group snapshot %v failed: %v", groupName, failureMessage)
	}

	if params.csiSnapshotType == csiSnapshotTypeLonghornBackup {
		groupSnapshot, err := srv.ensureVolumeGroupSnapshotBackedUp(ctx, log, snapshotGroup, params.backupMode)
		if err != nil {
			return nil, err
		}
		return &csi.CreateVolumeGroupSnapshotResponse{GroupSnapshot: groupSnapshot}, nil
	}

	return &csi.CreateVolumeGroupSnapshotResponse{
		GroupSnapshot: toCSIVolumeGroupSnapshot(snapshotGroup),
	}, nil
}

// volumeGroupSnapshotParameters is the validated form of the group snapshot
// class parameters.
type volumeGroupSnapshotParameters struct {
	csiSnapshotType string
	backupMode      longhorn.BackupMode
	snapshotLabels  map[string]string
}

// parseVolumeGroupSnapshotParameters validates the class parameters. The type
// must explicitly name snap (in-cluster group snapshot) or bak (group snapshot
// followed by a per-member backup upload): group snapshots are new, so there
// is no backward-compatible default to honor.
func parseVolumeGroupSnapshotParameters(parameters map[string]string) (*volumeGroupSnapshotParameters, error) {
	// The raw parameter is validated: the deprecated per-volume alias for
	// bak exists only in legacy snapshot handles, never in a group class.
	csiSnapshotType := parameters["type"]
	if !isVolumeGroupSnapshotType(csiSnapshotType) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid CSI volume group snapshot type: %q. Must be %v or %v", parameters["type"], csiSnapshotTypeLonghornSnapshot, csiSnapshotTypeLonghornBackup)
	}

	// backupMode configures the bak member backups; a valid backupMode on a
	// snap group is ignored.
	backupMode := longhorn.BackupMode(parameters["backupMode"])
	if backupMode == "" {
		backupMode = longhorn.BackupModeIncremental
	}
	if backupMode != longhorn.BackupModeIncremental && backupMode != longhorn.BackupModeFull {
		return nil, status.Errorf(codes.InvalidArgument, "invalid backup mode: %q. Must be %v or %v", parameters["backupMode"], longhorn.BackupModeIncremental, longhorn.BackupModeFull)
	}

	// The reserved keys type and backupMode are stripped before parameters
	// become snapshot labels. This deliberately diverges from the per-volume
	// path, which forwards raw parameters into snapshot and backup labels.
	snapshotLabels := make(map[string]string)
	for key, value := range parameters {
		if key == "type" || key == "backupMode" {
			continue
		}
		snapshotLabels[key] = value
	}

	return &volumeGroupSnapshotParameters{
		csiSnapshotType: csiSnapshotType,
		backupMode:      backupMode,
		snapshotLabels:  snapshotLabels,
	}, nil
}

// ensureSnapshotGroupExists creates the SnapshotGroup if it does not exist.
func (srv *GroupControllerServer) ensureSnapshotGroupExists(ctx context.Context, log *logrus.Entry, groupName string, params *volumeGroupSnapshotParameters, volumeNames []string) error {
	// The sidecar retries this call until the group is ready, so the group
	// usually already exists. Get first keeps that case to one cheap read;
	// Create would run the whole admission chain before failing on the
	// existing name.
	snapshotGroup, err := srv.snapshotGroups().Get(ctx, groupName, metav1.GetOptions{})
	if err == nil {
		existingType := snapshotGroupCSIType(snapshotGroup)
		if existingType == "" {
			return status.Errorf(codes.AlreadyExists, "volume group snapshot %v already exists and was not created through CSI", groupName)
		}
		if existingType != params.csiSnapshotType {
			return status.Errorf(codes.AlreadyExists, "volume group snapshot %v already exists with type %v", groupName, existingType)
		}
		if len(snapshotGroup.Spec.Volumes) != len(volumeNames) || !sets.New(snapshotGroup.Spec.Volumes...).Equal(sets.New(volumeNames...)) {
			return status.Errorf(codes.AlreadyExists, "volume group snapshot %v already exists with different source volumes %v", groupName, snapshotGroup.Spec.Volumes)
		}
		if mismatch := snapshotGroupParametersMismatch(snapshotGroup, params); mismatch != "" {
			return status.Errorf(codes.AlreadyExists, "volume group snapshot %v already exists with different %v", groupName, mismatch)
		}
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return status.Error(codes.Internal, err.Error())
	}

	recordedParameters, err := json.Marshal(recordedSnapshotGroupCSIParameters{
		BackupMode: params.backupMode,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal CSI parameters of volume group snapshot %v: %v", groupName, err)
	}
	snapshotGroup = &longhorn.SnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: groupName,
			Labels: map[string]string{
				types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupCSIType): params.csiSnapshotType,
			},
			Annotations: map[string]string{
				types.SnapshotGroupAnnotationCSIParameters: string(recordedParameters),
			},
		},
		Spec: longhorn.SnapshotGroupSpec{
			Volumes: volumeNames,
			Labels:  params.snapshotLabels,
		},
	}
	log.Infof("Creating SnapshotGroup %v for volumes %v", groupName, volumeNames)
	if _, err := srv.snapshotGroups().Create(ctx, snapshotGroup, metav1.CreateOptions{}); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return srv.ensureSnapshotGroupExists(ctx, log, groupName, params, volumeNames)
		}
		if apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		return status.Error(codes.Internal, err.Error())
	}
	return nil
}

// recordedSnapshotGroupCSIParameters is the persisted form of the class
// parameters that have no home in the group spec: only the backup mode. The
// type lives in its own label and the snapshot labels in the immutable spec.
type recordedSnapshotGroupCSIParameters struct {
	BackupMode longhorn.BackupMode `json:"backupMode"`
}

// snapshotGroupParametersMismatch compares the requested parameters with the
// group and names the first difference, or returns empty on a match. The
// snapshot labels are compared against the immutable spec, so the comparison
// does not depend on the record; the backup mode is compared against the
// record stamped at creation, only for bak groups. A bak group without a
// readable record is incompatible: CSI creation always stamps it.
func snapshotGroupParametersMismatch(snapshotGroup *longhorn.SnapshotGroup, params *volumeGroupSnapshotParameters) string {
	if !maps.Equal(snapshotGroup.Spec.Labels, params.snapshotLabels) {
		return fmt.Sprintf("snapshot labels: recorded %v, requested %v", snapshotGroup.Spec.Labels, params.snapshotLabels)
	}
	if params.csiSnapshotType != csiSnapshotTypeLonghornBackup {
		return ""
	}
	recordedBackupMode, err := recordedSnapshotGroupBackupMode(snapshotGroup)
	if err != nil {
		return "backup mode: the group has no readable record"
	}
	if recordedBackupMode != params.backupMode {
		return fmt.Sprintf("backup mode: recorded %v, requested %v", recordedBackupMode, params.backupMode)
	}
	return ""
}

// recordedSnapshotGroupBackupMode returns the backup mode recorded at group
// creation. CSI creation always stamps the record, and Get creates missing
// member backups with this mode, so an unreadable record is an error rather
// than a guessed mode: a guess could mix upload modes within one group.
func recordedSnapshotGroupBackupMode(snapshotGroup *longhorn.SnapshotGroup) (longhorn.BackupMode, error) {
	var recorded recordedSnapshotGroupCSIParameters
	if recordedJSON := snapshotGroup.Annotations[types.SnapshotGroupAnnotationCSIParameters]; recordedJSON != "" {
		if err := json.Unmarshal([]byte(recordedJSON), &recorded); err != nil {
			return "", status.Errorf(codes.Internal, "volume group snapshot %v has an unreadable backup mode record: %v", snapshotGroup.Name, err)
		}
	}
	if recorded.BackupMode == "" {
		return "", status.Errorf(codes.Internal, "volume group snapshot %v has no recorded backup mode; restore the %v annotation or delete and recreate the group", snapshotGroup.Name, types.SnapshotGroupAnnotationCSIParameters)
	}
	return recorded.BackupMode, nil
}

// waitForSnapshotGroupTerminalPhase polls the SnapshotGroup until it reaches
// Ready or Failed. The group deadline is enforced by the SnapshotGroup
// controller; the poll timeout here only bounds this RPC, and the call is
// idempotent so the external snapshotter simply retries after DEADLINE_EXCEEDED.
func (srv *GroupControllerServer) waitForSnapshotGroupTerminalPhase(ctx context.Context, groupName string) (*longhorn.SnapshotGroup, error) {
	var snapshotGroup *longhorn.SnapshotGroup
	err := pollUntil(ctx, timeoutSnapshotGroupCreation, tickSnapshotGroupCreation,
		fmt.Sprintf("volume group snapshot %v to become ready", groupName),
		func() (bool, error) {
			var err error
			snapshotGroup, err = srv.snapshotGroups().Get(ctx, groupName, metav1.GetOptions{})
			if err != nil {
				return false, status.Error(codes.Internal, err.Error())
			}
			switch snapshotGroup.Status.Phase {
			case longhorn.SnapshotGroupPhaseReady, longhorn.SnapshotGroupPhaseFailed:
				return true, nil
			}
			return false, nil
		})
	if err != nil {
		return nil, err
	}
	return snapshotGroup, nil
}

func (srv *GroupControllerServer) DeleteVolumeGroupSnapshot(ctx context.Context, req *csi.DeleteVolumeGroupSnapshotRequest) (*csi.DeleteVolumeGroupSnapshotResponse, error) {
	if !srv.enabled {
		return nil, status.Error(codes.Unimplemented, "volume group snapshot support is disabled")
	}

	log := srv.log.WithFields(logrus.Fields{"function": "DeleteVolumeGroupSnapshot"})

	if len(req.GetGroupSnapshotId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume group snapshot id must be provided")
	}
	csiSnapshotType, groupName := decodeSnapshotGroupID(req.GetGroupSnapshotId())
	if len(groupName) == 0 {
		// An undecodable handle names nothing this driver created; treat the
		// deletion as already done.
		return &csi.DeleteVolumeGroupSnapshotResponse{}, nil
	}

	// Delete shares the fan-out's per-group lock: a Create in this process
	// can no longer create a backup after the sweep below saw none.
	lock := srv.lockSnapshotGroup(groupName)
	defer lock.Unlock()

	// The requested type decides whether the member backups are deleted, so
	// it must match the type recorded on the group.
	snapshotGroup, err := srv.snapshotGroups().Get(ctx, groupName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	createdOutsideCSI := false
	if err == nil {
		recordedType := snapshotGroupCSIType(snapshotGroup)
		if recordedType != "" && recordedType != csiSnapshotType {
			return nil, status.Errorf(codes.InvalidArgument, "volume group snapshot %v is of type %v, not %v", groupName, recordedType, csiSnapshotType)
		}
		createdOutsideCSI = recordedType == ""
	}

	// For bak groups, delete the member backups before the group CR. The
	// group CR names the members, so keeping it until last lets a retried
	// Delete find the surviving backups from any partial state.
	if csiSnapshotType == csiSnapshotTypeLonghornBackup {
		if err := srv.deleteSnapshotGroupMemberBackups(ctx, log, groupName); err != nil {
			return nil, err
		}
	}

	// The requested CSI group was deleted and its name is now reused by a
	// group created outside CSI. Do not delete that group; the backup
	// deletion above already removed what the old group left behind.
	if createdOutsideCSI {
		return &csi.DeleteVolumeGroupSnapshotResponse{}, nil
	}

	if err := srv.deleteSnapshotGroupAndWait(ctx, groupName); err != nil {
		return nil, err
	}
	return &csi.DeleteVolumeGroupSnapshotResponse{}, nil
}

// deleteSnapshotGroupAndWait requests the group's deletion and waits for the
// finalizer removal. The SnapshotGroup controller requests deletion of all
// member snapshots before releasing the CR; the actual purge is handled by
// the snapshot controller, so this wait is short.
func (srv *GroupControllerServer) deleteSnapshotGroupAndWait(ctx context.Context, groupName string) error {
	err := srv.snapshotGroups().Delete(ctx, groupName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return status.Error(codes.Internal, err.Error())
	}
	return pollUntil(ctx, timeoutSnapshotGroupDeletion, tickSnapshotGroupDeletion,
		fmt.Sprintf("volume group snapshot %v to be deleted", groupName),
		func() (bool, error) {
			_, err := srv.snapshotGroups().Get(ctx, groupName, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			if err != nil {
				return false, status.Error(codes.Internal, err.Error())
			}
			return false, nil
		})
}

func (srv *GroupControllerServer) GetVolumeGroupSnapshot(ctx context.Context, req *csi.GetVolumeGroupSnapshotRequest) (*csi.GetVolumeGroupSnapshotResponse, error) {
	if !srv.enabled {
		return nil, status.Error(codes.Unimplemented, "volume group snapshot support is disabled")
	}

	if len(req.GetGroupSnapshotId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume group snapshot id must be provided")
	}
	csiSnapshotType, groupName := decodeSnapshotGroupID(req.GetGroupSnapshotId())
	if len(groupName) == 0 {
		return nil, status.Errorf(codes.NotFound, "volume group snapshot %v not found", req.GetGroupSnapshotId())
	}

	snapshotGroup, err := srv.snapshotGroups().Get(ctx, groupName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "volume group snapshot %v not found", groupName)
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	// This is not the requested CSI group: its recorded type differs, or it
	// was created outside CSI.
	if snapshotGroupCSIType(snapshotGroup) != csiSnapshotType {
		return nil, status.Errorf(codes.NotFound, "volume group snapshot %v with type %v not found", groupName, csiSnapshotType)
	}

	// A Failed group never becomes ready. Only a statically provisioned
	// content reaches this: Create deletes the groups it fails.
	if snapshotGroup.Status.Phase == longhorn.SnapshotGroupPhaseFailed {
		return nil, status.Errorf(codes.Internal, "volume group snapshot %v failed: %v", groupName, snapshotGroup.Status.Error)
	}

	if csiSnapshotType == csiSnapshotTypeLonghornBackup {
		// A recorded group is served without the fan-out lock; nothing about
		// it changes anymore.
		if snapshotGroupBackupsRecorded(snapshotGroup) {
			backups, _, err := srv.listSnapshotGroupMemberBackups(ctx, groupName, string(snapshotGroup.UID), recordedSnapshotGroupMemberBackupNames(snapshotGroup))
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			return &csi.GetVolumeGroupSnapshotResponse{
				GroupSnapshot: toCSIVolumeGroupSnapshotBackup(snapshotGroup, backups),
			}, nil
		}
		// The sidecar polls a dynamically provisioned content by retrying
		// Create and a statically provisioned one through Get, so Get drives
		// the same fan-out as Create: it recreates missing member backups,
		// surfaces failed uploads, and stamps completion. The backup mode
		// comes from the parameters recorded at group creation.
		log := srv.log.WithFields(logrus.Fields{"function": "GetVolumeGroupSnapshot"})
		backupMode, err := recordedSnapshotGroupBackupMode(snapshotGroup)
		if err != nil {
			return nil, err
		}
		snapshotGroup, backups, err := srv.reconcileSnapshotGroupBackups(ctx, log, groupName, backupMode)
		if err != nil {
			return nil, err
		}
		return &csi.GetVolumeGroupSnapshotResponse{
			GroupSnapshot: toCSIVolumeGroupSnapshotBackup(snapshotGroup, backups),
		}, nil
	}

	return &csi.GetVolumeGroupSnapshotResponse{
		GroupSnapshot: toCSIVolumeGroupSnapshot(snapshotGroup),
	}, nil
}

// toCSIVolumeGroupSnapshot maps a SnapshotGroup CR to the CSI representation.
// Member snapshot IDs use the per-volume snap encoding so members stay
// individually restorable through the existing CreateVolume source paths.
func toCSIVolumeGroupSnapshot(snapshotGroup *longhorn.SnapshotGroup) *csi.VolumeGroupSnapshot {
	groupSnapshotID := encodeSnapshotGroupID(csiSnapshotTypeLonghornSnapshot, snapshotGroup.Name)
	creationTime := parseSnapshotGroupTime(snapshotGroup.Status.CreationTime, snapshotGroup.Name)

	// A Ready group reports every member ready: the controller later flips a
	// lost member's entry to false as a loss marker, and CSI must not report
	// a finished snapshot as still in progress.
	groupIsReady := snapshotGroup.Status.Phase == longhorn.SnapshotGroupPhaseReady

	statusMembers := memberStatusesBySnapshotName(snapshotGroup)
	snapshots := make([]*csi.Snapshot, 0, len(snapshotGroup.Spec.Members))
	for _, member := range snapshotGroup.Spec.Members {
		memberStatus := statusMembers[member.SnapshotName]
		memberCreationTime := parseSnapshotGroupTime(memberStatus.CreationTime, snapshotGroup.Name)
		snapshots = append(snapshots, &csi.Snapshot{
			SnapshotId:      encodeSnapshotID(csiSnapshotTypeLonghornSnapshot, member.VolumeName, member.SnapshotName),
			SourceVolumeId:  member.VolumeName,
			GroupSnapshotId: groupSnapshotID,
			CreationTime:    memberCreationTime,
			ReadyToUse:      groupIsReady || memberStatus.ReadyToUse,
		})
	}

	return &csi.VolumeGroupSnapshot{
		GroupSnapshotId: groupSnapshotID,
		Snapshots:       snapshots,
		CreationTime:    creationTime,
		ReadyToUse:      snapshotGroup.Status.ReadyToUse,
	}
}

// memberStatusesBySnapshotName indexes the mirrored member statuses. The
// converters list members from the immutable spec and join the status here:
// a restored group carries its members in the spec before the controller
// rebuilds the status, and a ready response must never miss a member.
func memberStatusesBySnapshotName(snapshotGroup *longhorn.SnapshotGroup) map[string]longhorn.SnapshotGroupMemberStatus {
	statusMembers := make(map[string]longhorn.SnapshotGroupMemberStatus, len(snapshotGroup.Status.Members))
	for _, member := range snapshotGroup.Status.Members {
		statusMembers[member.SnapshotName] = member
	}
	return statusMembers
}

// toCSIVolumeGroupSnapshotBackup maps a bak-type group to CSI. Member snapshot
// IDs use the per-volume backup encoding, so members restore and delete
// through the existing per-volume CSI code. Before every backup completes,
// readiness is computed live from the backups' current states. Once the
// backups-completed annotation records that every upload finished, readiness
// stays true and every member handle keeps the backup name recorded with
// the stamp, even when the backup is later deleted.
// Create and Get stamp the annotation when they observe completion. Times
// are the snapshot creation times, never upload times.
func toCSIVolumeGroupSnapshotBackup(snapshotGroup *longhorn.SnapshotGroup, backups map[string]*longhorn.Backup) *csi.VolumeGroupSnapshot {
	groupSnapshotID := encodeSnapshotGroupID(csiSnapshotTypeLonghornBackup, snapshotGroup.Name)
	creationTime := parseSnapshotGroupTime(snapshotGroup.Status.CreationTime, snapshotGroup.Name)
	recordedBackupNames := recordedSnapshotGroupMemberBackupNames(snapshotGroup)
	backupsCompleted := snapshotGroupBackupsRecorded(snapshotGroup) ||
		allSnapshotGroupMemberBackupsCompleted(snapshotGroup, backups)

	statusMembers := memberStatusesBySnapshotName(snapshotGroup)
	snapshots := make([]*csi.Snapshot, 0, len(snapshotGroup.Spec.Members))
	for _, member := range snapshotGroup.Spec.Members {
		memberStatus := statusMembers[member.SnapshotName]
		backupName := recordedBackupNames[member.SnapshotName]
		backupCompleted := false
		if backup := backups[member.SnapshotName]; backup != nil {
			if backupName == "" {
				backupName = backup.Name
			}
			backupCompleted = backup.Status.State == longhorn.BackupStateCompleted
		}
		// No backup exists for this member yet, so it has no handle; a
		// made-up one would be malformed. Only a not-ready view can be
		// missing members: the fan-out creates every backup before a group
		// reports ready.
		if backupName == "" {
			continue
		}
		snapshots = append(snapshots, &csi.Snapshot{
			SnapshotId:      encodeSnapshotID(csiSnapshotTypeLonghornBackup, member.VolumeName, backupName),
			SourceVolumeId:  member.VolumeName,
			GroupSnapshotId: groupSnapshotID,
			CreationTime:    parseSnapshotGroupTime(memberStatus.CreationTime, snapshotGroup.Name),
			ReadyToUse:      backupsCompleted || backupCompleted,
		})
	}

	return &csi.VolumeGroupSnapshot{
		GroupSnapshotId: groupSnapshotID,
		Snapshots:       snapshots,
		CreationTime:    creationTime,
		ReadyToUse:      backupsCompleted,
	}
}

// ensureVolumeGroupSnapshotBackedUp runs the bak upload fan-out for Create.
// An in-progress upload is reported as not ready instead of blocking the
// RPC; uploads have no deadline, like any per-volume backup.
func (srv *GroupControllerServer) ensureVolumeGroupSnapshotBackedUp(ctx context.Context, log *logrus.Entry, snapshotGroup *longhorn.SnapshotGroup, backupMode longhorn.BackupMode) (*csi.VolumeGroupSnapshot, error) {
	snapshotGroup, backups, err := srv.reconcileSnapshotGroupBackups(ctx, log, snapshotGroup.Name, backupMode)
	if err != nil {
		return nil, err
	}
	return toCSIVolumeGroupSnapshotBackup(snapshotGroup, backups), nil
}

// reconcileSnapshotGroupBackups drives the bak upload fan-out: find or create
// one Backup per member, fail on member backup errors, and stamp the
// backups-completed annotation once every upload completes. The sidecar
// polls a dynamically provisioned content by retrying Create and a
// statically provisioned one through Get, so both RPCs drive this same
// fan-out; every call is stateless and idempotent. A recorded group is
// returned as is, and a member backup deleted after the stamp is not
// recreated. The fan-out only runs on a Ready group: a statically
// provisioned content can poll a group that is still taking snapshots or
// has failed.
func (srv *GroupControllerServer) reconcileSnapshotGroupBackups(ctx context.Context, log *logrus.Entry, groupName string, backupMode longhorn.BackupMode) (*longhorn.SnapshotGroup, map[string]*longhorn.Backup, error) {
	lock := srv.lockSnapshotGroup(groupName)
	defer lock.Unlock()

	// Re-read under the lock: a completion record may have landed after
	// this RPC fetched its group copy.
	snapshotGroup, backups, err := srv.refreshSnapshotGroupBackupsLocked(ctx, log, groupName)
	if err != nil {
		return nil, nil, err
	}
	if snapshotGroupBackupsRecorded(snapshotGroup) || snapshotGroup.Status.Phase != longhorn.SnapshotGroupPhaseReady {
		return snapshotGroup, backups, nil
	}

	for _, member := range snapshotGroup.Spec.Members {
		if _, exists := backups[member.SnapshotName]; exists {
			continue
		}
		backup, err := srv.createSnapshotGroupMemberBackup(ctx, log, snapshotGroup, member, backupMode)
		if err != nil {
			return nil, nil, err
		}
		backups[member.SnapshotName] = backup
	}

	if failedVolumes := failedSnapshotGroupMemberBackupVolumes(snapshotGroup, backups); len(failedVolumes) > 0 {
		return nil, nil, status.Errorf(codes.Internal, "volume group snapshot %v member backups failed for volumes %v; delete and recreate the failed backups to recover", groupName, failedVolumes)
	}

	if allSnapshotGroupMemberBackupsCompleted(snapshotGroup, backups) {
		if snapshotGroup, err = srv.stampSnapshotGroupBackupsCompleted(ctx, log, snapshotGroup, backups); err != nil {
			return nil, nil, err
		}
	}

	return snapshotGroup, backups, nil
}

// lockSnapshotGroup locks the group's backup fan-out and deletion, and
// returns the held lock. Overlapping Create retries would otherwise race the
// list-then-create and create duplicate member backups, and a Delete could
// sweep while a fan-out creates.
func (srv *GroupControllerServer) lockSnapshotGroup(groupName string) *heldSnapshotGroupLock {
	return srv.groupLocks.lock(groupName)
}

// snapshotGroupLockTable hands out one mutex per group name and forgets the
// name once the last holder or waiter releases it, so the table holds the
// groups with an RPC in flight instead of every group name ever seen.
type snapshotGroupLockTable struct {
	mu      sync.Mutex
	entries map[string]*snapshotGroupLockEntry
}

type snapshotGroupLockEntry struct {
	mutex    sync.Mutex
	refCount int
}

// heldSnapshotGroupLock is a held per-group lock; Unlock releases it.
type heldSnapshotGroupLock struct {
	table     *snapshotGroupLockTable
	groupName string
	entry     *snapshotGroupLockEntry
}

func (table *snapshotGroupLockTable) lock(groupName string) *heldSnapshotGroupLock {
	table.mu.Lock()
	if table.entries == nil {
		table.entries = map[string]*snapshotGroupLockEntry{}
	}
	entry := table.entries[groupName]
	if entry == nil {
		entry = &snapshotGroupLockEntry{}
		table.entries[groupName] = entry
	}
	entry.refCount++
	table.mu.Unlock()

	entry.mutex.Lock()
	return &heldSnapshotGroupLock{table: table, groupName: groupName, entry: entry}
}

func (held *heldSnapshotGroupLock) Unlock() {
	held.entry.mutex.Unlock()
	held.table.mu.Lock()
	held.entry.refCount--
	if held.entry.refCount == 0 {
		delete(held.table.entries, held.groupName)
	}
	held.table.mu.Unlock()
}

// refreshSnapshotGroupBackupsLocked re-reads the group and its member
// backups, and deletes duplicate backups. Callers hold the group lock, so
// the fresh read sees a completion record another caller just stamped.
func (srv *GroupControllerServer) refreshSnapshotGroupBackupsLocked(ctx context.Context, log *logrus.Entry, groupName string) (*longhorn.SnapshotGroup, map[string]*longhorn.Backup, error) {
	snapshotGroup, err := srv.snapshotGroups().Get(ctx, groupName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, status.Errorf(codes.NotFound, "volume group snapshot %v not found", groupName)
		}
		return nil, nil, status.Error(codes.Internal, err.Error())
	}
	backups, duplicates, err := srv.listSnapshotGroupMemberBackups(ctx, groupName, string(snapshotGroup.UID), recordedSnapshotGroupMemberBackupNames(snapshotGroup))
	if err != nil {
		return nil, nil, status.Error(codes.Internal, err.Error())
	}
	// Duplicates can only appear when two plugin processes run the fan-out
	// at the same time; the lock cannot reach across processes. Deleting the
	// extras leaves one backup per member.
	for _, duplicate := range duplicates {
		log.Warnf("Deleting duplicate Backup %v for snapshot %v of volume group snapshot %v", duplicate.Name, duplicate.Spec.SnapshotName, groupName)
		if err := srv.backups().Delete(ctx, duplicate.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return nil, nil, status.Error(codes.Internal, err.Error())
		}
	}
	return snapshotGroup, backups, nil
}

func (srv *GroupControllerServer) createSnapshotGroupMemberBackup(ctx context.Context, log *logrus.Entry, snapshotGroup *longhorn.SnapshotGroup, member longhorn.SnapshotGroupMember, backupMode longhorn.BackupMode) (*longhorn.Backup, error) {
	volume, err := srv.volumes().Get(ctx, member.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get volume %v to back up volume group snapshot %v member %v: %v", member.VolumeName, snapshotGroup.Name, member.SnapshotName, err)
	}

	groupLabelKey := types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroup)
	// The group label also goes into spec.labels so it reaches the backup
	// metadata on the backup target.
	backupLabels := make(map[string]string, len(snapshotGroup.Spec.Labels)+1)
	maps.Copy(backupLabels, snapshotGroup.Spec.Labels)
	backupLabels[groupLabelKey] = snapshotGroup.Name

	backup := &longhorn.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name: bsutil.GenerateName("backup"),
			Labels: map[string]string{
				types.LonghornLabelBackupTarget: volume.Spec.BackupTargetName,
				types.LonghornLabelBackupVolume: member.VolumeName,
				groupLabelKey:                   snapshotGroup.Name,
				types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupUID): string(snapshotGroup.UID),
			},
		},
		Spec: longhorn.BackupSpec{
			SnapshotName: member.SnapshotName,
			Labels:       backupLabels,
			BackupMode:   backupMode,
		},
	}
	log.Infof("Creating Backup %v for volume group snapshot %v member snapshot %v on volume %v", backup.Name, snapshotGroup.Name, member.SnapshotName, member.VolumeName)
	backup, err = srv.backups().Create(ctx, backup, metav1.CreateOptions{})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return backup, nil
}

// listSnapshotGroupMemberBackups returns the group's member backups, indexed
// by the snapshot they upload. A group UID limits the match to backups that
// group created, so a group does not adopt backups from an earlier group
// with the same name; an empty UID matches by name alone. When more than one
// backup uploads the same snapshot, the better one wins and the rest are
// returned as duplicates.
func (srv *GroupControllerServer) listSnapshotGroupMemberBackups(ctx context.Context, groupName, groupUID string, recordedBackupNames map[string]string) (map[string]*longhorn.Backup, []*longhorn.Backup, error) {
	selectorLabels := labels.Set{types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroup): groupName}
	if groupUID != "" {
		selectorLabels[types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupUID)] = groupUID
	}
	selector := selectorLabels.String()
	backupList, err := srv.backups().List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, nil, err
	}
	backups := make(map[string]*longhorn.Backup, len(backupList.Items))
	var duplicates []*longhorn.Backup
	for i := range backupList.Items {
		backup := &backupList.Items[i]
		if existing := backups[backup.Spec.SnapshotName]; existing != nil {
			winner := preferredMemberBackup(existing, backup, recordedBackupNames)
			if winner == existing {
				duplicates = append(duplicates, backup)
				continue
			}
			duplicates = append(duplicates, existing)
		}
		backups[backup.Spec.SnapshotName] = backup
	}
	return backups, duplicates, nil
}

// preferredMemberBackup picks which of two backups of the same snapshot to
// keep: a backup named in the completion record wins, then a completed
// backup over an unfinished one, then an unfinished one over a failed one.
// Equal states fall back to name order so every caller picks the same
// winner.
func preferredMemberBackup(a, b *longhorn.Backup, recordedBackupNames map[string]string) *longhorn.Backup {
	if recordedBackupNames[a.Spec.SnapshotName] == a.Name {
		return a
	}
	if recordedBackupNames[b.Spec.SnapshotName] == b.Name {
		return b
	}
	rank := func(backup *longhorn.Backup) int {
		switch backup.Status.State {
		case longhorn.BackupStateCompleted:
			return 2
		case longhorn.BackupStateError, longhorn.BackupStateUnknown:
			return 0
		default:
			return 1
		}
	}
	if rank(a) != rank(b) {
		if rank(a) > rank(b) {
			return a
		}
		return b
	}
	if a.Name > b.Name {
		return a
	}
	return b
}

func failedSnapshotGroupMemberBackupVolumes(snapshotGroup *longhorn.SnapshotGroup, backups map[string]*longhorn.Backup) []string {
	var failedVolumes []string
	for _, member := range snapshotGroup.Spec.Members {
		backup := backups[member.SnapshotName]
		if backup == nil {
			continue
		}
		// Unknown is terminal like Error: the upload will never complete.
		if backup.Status.State == longhorn.BackupStateError || backup.Status.State == longhorn.BackupStateUnknown || backup.Status.Error != "" {
			failedVolumes = append(failedVolumes, member.VolumeName)
		}
	}
	return failedVolumes
}

func allSnapshotGroupMemberBackupsCompleted(snapshotGroup *longhorn.SnapshotGroup, backups map[string]*longhorn.Backup) bool {
	for _, member := range snapshotGroup.Spec.Members {
		backup := backups[member.SnapshotName]
		// A backup being deleted must not count as completed, or the stamp
		// could freeze a member handle that is about to disappear.
		if backup == nil || !backup.DeletionTimestamp.IsZero() || backup.Status.State != longhorn.BackupStateCompleted {
			return false
		}
	}
	return true
}

// stampSnapshotGroupBackupsCompleted stamps the backups-completed annotation
// with the member backup names as its value. A stamped value that does not
// pass snapshotGroupBackupsRecorded is rewritten, so a damaged annotation
// heals on the next retry.
func (srv *GroupControllerServer) stampSnapshotGroupBackupsCompleted(ctx context.Context, log *logrus.Entry, snapshotGroup *longhorn.SnapshotGroup, backups map[string]*longhorn.Backup) (*longhorn.SnapshotGroup, error) {
	if snapshotGroupBackupsRecorded(snapshotGroup) {
		return snapshotGroup, nil
	}
	memberBackupNames := make(map[string]string, len(snapshotGroup.Spec.Members))
	for _, member := range snapshotGroup.Spec.Members {
		backup := backups[member.SnapshotName]
		if backup == nil {
			return nil, status.Errorf(codes.Internal, "missing backup for member snapshot %v of volume group snapshot %v", member.SnapshotName, snapshotGroup.Name)
		}
		memberBackupNames[member.SnapshotName] = backup.Name
	}
	recordedBackupNames, err := json.Marshal(memberBackupNames)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal member backup names of volume group snapshot %v: %v", snapshotGroup.Name, err)
	}
	updated := snapshotGroup.DeepCopy()
	if updated.Annotations == nil {
		updated.Annotations = map[string]string{}
	}
	updated.Annotations[types.SnapshotGroupAnnotationBackupsCompleted] = string(recordedBackupNames)
	log.Infof("All member backups of volume group snapshot %v completed", snapshotGroup.Name)
	updated, err = srv.snapshotGroups().Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		// The next sidecar retry re-derives completion and stamps again.
		return nil, status.Errorf(codes.Internal, "failed to record backup completion on volume group snapshot %v: %v", snapshotGroup.Name, err)
	}
	return updated, nil
}

// deleteSnapshotGroupMemberBackups requests deletion of every member backup
// and waits until they are gone. An unreachable backup target blocks the wait,
// matching the per-volume deletion semantics; the sidecar retries the RPC.
func (srv *GroupControllerServer) deleteSnapshotGroupMemberBackups(ctx context.Context, log *logrus.Entry, groupName string) error {
	return pollUntil(ctx, timeoutSnapshotGroupDeletion, tickSnapshotGroupDeletion,
		fmt.Sprintf("member backups of volume group snapshot %v to be deleted", groupName),
		func() (bool, error) {
			backups, _, err := srv.listSnapshotGroupMemberBackups(ctx, groupName, "", nil)
			if err != nil {
				return false, status.Error(codes.Internal, err.Error())
			}
			if len(backups) == 0 {
				return true, nil
			}
			for _, backup := range backups {
				if backup.DeletionTimestamp != nil {
					continue
				}
				log.Infof("Deleting Backup %v of volume group snapshot %v", backup.Name, groupName)
				if err := srv.backups().Delete(ctx, backup.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
					return false, status.Error(codes.Internal, err.Error())
				}
			}
			return false, nil
		})
}

// snapshotGroupBackupsRecorded reports whether the completion annotation
// names a backup for every spec member.
func snapshotGroupBackupsRecorded(snapshotGroup *longhorn.SnapshotGroup) bool {
	recordedBackupNames := recordedSnapshotGroupMemberBackupNames(snapshotGroup)
	if len(recordedBackupNames) == 0 {
		return false
	}
	for _, member := range snapshotGroup.Spec.Members {
		if recordedBackupNames[member.SnapshotName] == "" {
			return false
		}
	}
	return true
}

// recordedSnapshotGroupMemberBackupNames returns the member backup names
// recorded when completion was stamped, keyed by member snapshot name. Empty
// before completion.
func recordedSnapshotGroupMemberBackupNames(snapshotGroup *longhorn.SnapshotGroup) map[string]string {
	recorded := snapshotGroup.Annotations[types.SnapshotGroupAnnotationBackupsCompleted]
	if recorded == "" {
		return nil
	}
	backupNames := map[string]string{}
	if err := json.Unmarshal([]byte(recorded), &backupNames); err != nil {
		logrus.WithError(err).Errorf("Failed to parse recorded member backup names of CSI volume group snapshot %v", snapshotGroup.Name)
		return nil
	}
	return backupNames
}

// encodeSnapshotGroupID builds the group snapshot handle. Get and Delete
// carry only the handle, not the class parameters, so the prefix encodes the
// snapshot type.
func encodeSnapshotGroupID(csiSnapshotType, groupName string) string {
	return fmt.Sprintf("%s://%s", csiSnapshotType, groupName)
}

// decodeSnapshotGroupID returns the snapshot type and SnapshotGroup name from
// a group snapshot handle, or empty values when the handle is not a snap:// or
// bak:// group handle. A name containing "/" is a per-volume snapshot handle,
// not a group.
func decodeSnapshotGroupID(groupSnapshotID string) (csiSnapshotType, groupName string) {
	csiSnapshotType, groupName, found := strings.Cut(groupSnapshotID, "://")
	if !found || strings.Contains(groupName, "/") {
		return "", ""
	}
	csiSnapshotType = normalizeCSISnapshotType(csiSnapshotType)
	if !isVolumeGroupSnapshotType(csiSnapshotType) {
		return "", ""
	}
	return csiSnapshotType, groupName
}

// snapshotGroupCSIType returns the group's recorded snapshot type, or empty
// when the group was not created through CSI.
func snapshotGroupCSIType(snapshotGroup *longhorn.SnapshotGroup) string {
	return snapshotGroup.Labels[types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupCSIType)]
}

// isVolumeGroupSnapshotType reports whether csiSnapshotType is a snapshot
// type supported for volume group snapshots.
func isVolumeGroupSnapshotType(csiSnapshotType string) bool {
	return csiSnapshotType == csiSnapshotTypeLonghornSnapshot || csiSnapshotType == csiSnapshotTypeLonghornBackup
}

// pollUntil re-runs poll every tick until it reports done or fails, the
// timeout passes, or the RPC context ends. waitingFor names the awaited
// outcome in the returned errors.
func pollUntil(ctx context.Context, timeout, tick time.Duration, waitingFor string, poll func() (done bool, err error)) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		done, err := poll()
		if err != nil {
			// A poll canceled mid-request returns its own wrapped context
			// error; translate it so the RPC keeps the context's code.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return status.Errorf(status.FromContextError(ctxErr).Code(), "gave up waiting for %s: %v", waitingFor, ctxErr)
			}
			return err
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			// The context ends as Canceled or DeadlineExceeded; keep the
			// matching gRPC code.
			return status.Errorf(status.FromContextError(ctx.Err()).Code(), "gave up waiting for %s: %v", waitingFor, ctx.Err())
		case <-timer.C:
			return status.Errorf(codes.DeadlineExceeded, "timed out waiting for %s", waitingFor)
		case <-ticker.C:
		}
	}
}

// parseSnapshotGroupTime converts a mirrored creation time to a proto
// timestamp. The time is empty until the member snapshot is taken, so empty
// is not an error.
func parseSnapshotGroupTime(creationTime, groupName string) *timestamppb.Timestamp {
	if creationTime == "" {
		return nil
	}
	parsed, err := toProtoTimestamp(creationTime)
	if err != nil {
		logrus.WithError(err).Errorf("Failed to parse creation time %v for CSI volume group snapshot %v", creationTime, groupName)
		return nil
	}
	return parsed
}

func getGroupControllerServiceCapabilities(rpcTypes []csi.GroupControllerServiceCapability_RPC_Type) []*csi.GroupControllerServiceCapability {
	var caps []*csi.GroupControllerServiceCapability
	for _, rpcType := range rpcTypes {
		caps = append(caps, &csi.GroupControllerServiceCapability{
			Type: &csi.GroupControllerServiceCapability_Rpc{
				Rpc: &csi.GroupControllerServiceCapability_RPC{
					Type: rpcType,
				},
			},
		})
	}
	return caps
}
