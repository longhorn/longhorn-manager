package csi

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/rest"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

	// Only in-cluster snapshots are supported for group snapshots. There is no
	// backward-compatible default to honor here, so an explicit type is
	// required to keep the class self-describing.
	parameters := req.GetParameters()
	if csiSnapshotType := normalizeCSISnapshotType(parameters["type"]); csiSnapshotType != csiSnapshotTypeLonghornSnapshot {
		return nil, status.Errorf(codes.InvalidArgument, "invalid CSI volume group snapshot type: %q. Must be %v", parameters["type"], csiSnapshotTypeLonghornSnapshot)
	}
	snapshotLabels := make(map[string]string)
	for key, value := range parameters {
		if key == "type" {
			continue
		}
		snapshotLabels[key] = value
	}

	snapshotGroup, err := srv.getOrCreateSnapshotGroup(ctx, log, groupName, volumeNames, snapshotLabels)
	if err != nil {
		return nil, err
	}

	snapshotGroup, err = srv.waitForSnapshotGroupTerminalPhase(ctx, groupName)
	if err != nil {
		return nil, err
	}
	if snapshotGroup.Status.Phase == longhorn.SnapshotGroupPhaseFailed {
		return nil, status.Errorf(codes.Internal, "volume group snapshot %v failed: %v", groupName, snapshotGroup.Status.Error)
	}

	return &csi.CreateVolumeGroupSnapshotResponse{
		GroupSnapshot: toCSIVolumeGroupSnapshot(snapshotGroup),
	}, nil
}

// getOrCreateSnapshotGroup returns the existing SnapshotGroup for groupName or
// creates one for volumeNames. Per the CSI spec, an existing group with the
// same name but a different source volume set fails with ALREADY_EXISTS.
func (srv *GroupControllerServer) getOrCreateSnapshotGroup(ctx context.Context, log *logrus.Entry, groupName string, volumeNames []string, snapshotLabels map[string]string) (*longhorn.SnapshotGroup, error) {
	snapshotGroup, err := srv.snapshotGroups().Get(ctx, groupName, metav1.GetOptions{})
	if err == nil {
		if !sets.New(snapshotGroup.Spec.Volumes...).Equal(sets.New(volumeNames...)) {
			return nil, status.Errorf(codes.AlreadyExists, "volume group snapshot %v already exists with different source volumes %v", groupName, snapshotGroup.Spec.Volumes)
		}
		return snapshotGroup, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}

	snapshotGroup = &longhorn.SnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: groupName,
		},
		Spec: longhorn.SnapshotGroupSpec{
			Volumes: volumeNames,
			Labels:  snapshotLabels,
		},
	}
	log.Infof("Creating SnapshotGroup %v for volumes %v", groupName, volumeNames)
	snapshotGroup, err = srv.snapshotGroups().Create(ctx, snapshotGroup, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return snapshotGroup, nil
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
	groupName := decodeSnapshotGroupID(req.GetGroupSnapshotId())
	if len(groupName) == 0 {
		// An undecodable handle names nothing this driver created; treat the
		// deletion as already done.
		return &csi.DeleteVolumeGroupSnapshotResponse{}, nil
	}

	err := srv.snapshotGroups().Delete(ctx, groupName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Wait for the finalizer removal. The SnapshotGroup controller requests
	// deletion of all member snapshots before releasing the CR; the actual
	// purge is handled by the snapshot controller, so this wait is short.
	err = pollUntil(ctx, timeoutSnapshotGroupDeletion, tickSnapshotGroupDeletion,
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
	if err != nil {
		return nil, err
	}
	return &csi.DeleteVolumeGroupSnapshotResponse{}, nil
}

func (srv *GroupControllerServer) GetVolumeGroupSnapshot(ctx context.Context, req *csi.GetVolumeGroupSnapshotRequest) (*csi.GetVolumeGroupSnapshotResponse, error) {
	if !srv.enabled {
		return nil, status.Error(codes.Unimplemented, "volume group snapshot support is disabled")
	}

	if len(req.GetGroupSnapshotId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume group snapshot id must be provided")
	}
	groupName := decodeSnapshotGroupID(req.GetGroupSnapshotId())
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

	return &csi.GetVolumeGroupSnapshotResponse{
		GroupSnapshot: toCSIVolumeGroupSnapshot(snapshotGroup),
	}, nil
}

// toCSIVolumeGroupSnapshot maps a SnapshotGroup CR to the CSI representation.
// Member snapshot IDs use the per-volume snap encoding so members stay
// individually restorable through the existing CreateVolume source paths.
func toCSIVolumeGroupSnapshot(snapshotGroup *longhorn.SnapshotGroup) *csi.VolumeGroupSnapshot {
	groupSnapshotID := encodeSnapshotGroupID(snapshotGroup.Name)
	creationTime := parseSnapshotGroupTime(snapshotGroup.Status.CreationTime, snapshotGroup.Name)

	// A Ready group reports every member ready: the controller later flips a
	// lost member's entry to false as a loss marker, and CSI must not report
	// a finished snapshot as still in progress.
	groupIsReady := snapshotGroup.Status.Phase == longhorn.SnapshotGroupPhaseReady

	snapshots := make([]*csi.Snapshot, 0, len(snapshotGroup.Status.Members))
	for _, member := range snapshotGroup.Status.Members {
		memberCreationTime := parseSnapshotGroupTime(member.CreationTime, snapshotGroup.Name)
		snapshots = append(snapshots, &csi.Snapshot{
			SnapshotId:      encodeSnapshotID(csiSnapshotTypeLonghornSnapshot, member.VolumeName, member.SnapshotName),
			SourceVolumeId:  member.VolumeName,
			GroupSnapshotId: groupSnapshotID,
			CreationTime:    memberCreationTime,
			ReadyToUse:      groupIsReady || member.ReadyToUse,
		})
	}

	return &csi.VolumeGroupSnapshot{
		GroupSnapshotId: groupSnapshotID,
		Snapshots:       snapshots,
		CreationTime:    creationTime,
		ReadyToUse:      snapshotGroup.Status.ReadyToUse,
	}
}

// encodeSnapshotGroupID builds the group snapshot handle. Get and Delete
// carry only the handle, not the class parameters, so the prefix encodes the
// snapshot type.
func encodeSnapshotGroupID(groupName string) string {
	return fmt.Sprintf("%s://%s", csiSnapshotTypeLonghornSnapshot, groupName)
}

// decodeSnapshotGroupID returns the SnapshotGroup name from a group snapshot
// handle, or empty when the handle is not a snap:// group handle. A name
// containing "/" is a per-volume snapshot handle, not a group.
func decodeSnapshotGroupID(groupSnapshotID string) string {
	csiSnapshotType, groupName, found := strings.Cut(groupSnapshotID, "://")
	if !found || normalizeCSISnapshotType(csiSnapshotType) != csiSnapshotTypeLonghornSnapshot || strings.Contains(groupName, "/") {
		return ""
	}
	return groupName
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
