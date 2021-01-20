package csi

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	putil "sigs.k8s.io/sig-storage-lib-external-provisioner/util"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

const (
	timeoutAttachDetach     = 120 * time.Second
	tickAttachDetach        = 2 * time.Second
	timeoutBackupInitiation = 60 * time.Second
	tickBackupInitiation    = 5 * time.Second
)

type ControllerServer struct {
	apiClient   *longhornclient.RancherClient
	nodeID      string
	caps        []*csi.ControllerServiceCapability
	accessModes []*csi.VolumeCapability_AccessMode
}

func NewControllerServer(apiClient *longhornclient.RancherClient, nodeID string) *ControllerServer {
	return &ControllerServer{
		apiClient: apiClient,
		nodeID:    nodeID,
		caps: getControllerServiceCapabilities(
			[]csi.ControllerServiceCapability_RPC_Type{
				csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
				csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
				csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
				csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
			}),
		accessModes: getVolumeCapabilityAccessModes(
			[]csi.VolumeCapability_AccessMode_Mode{
				csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			}),
	}
}

func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	logrus.Infof("ControllerServer create volume req: %v", req)
	if err := cs.validateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		logrus.Errorf("CreateVolume: invalid create volume req: %v", req)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// Check request parameters like Name and Volume Capabilities
	if len(req.GetName()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume Name cannot be empty")
	}
	volumeCaps := req.GetVolumeCapabilities()
	if err := cs.validateVolumeCapabilities(volumeCaps); err != nil {
		return nil, err
	}
	volumeParameters := req.GetParameters()

	// check if we need to restore from a csi snapshot
	// we don't support volume cloning at the moment
	if req.VolumeContentSource != nil && req.VolumeContentSource.GetSnapshot() != nil {
		snapshot := req.VolumeContentSource.GetSnapshot()
		_, volumeName, backupName := decodeSnapshotID(snapshot.SnapshotId)
		bv, err := cs.apiClient.BackupVolume.ById(volumeName)
		if err != nil {
			msg := fmt.Sprintf("CreateVolume: cannot restore snapshot %v backupvolume not available", snapshot.SnapshotId)
			logrus.Error(msg)
			return nil, status.Error(codes.NotFound, msg)
		}

		backup, err := cs.apiClient.BackupVolume.ActionBackupGet(bv, &longhornclient.BackupInput{Name: backupName})
		if err != nil {
			msg := fmt.Sprintf("CreateVolume: cannot restore snapshot %v backup not available", snapshot.SnapshotId)
			logrus.Error(msg)
			return nil, status.Error(codes.NotFound, msg)
		}

		// use the fromBackup method for the csi snapshot restores as well
		// the same parameter was previously only used for restores based on the storage class
		if volumeParameters == nil {
			volumeParameters = map[string]string{}
		}
		volumeParameters["fromBackup"] = backup.Url
	}

	// check for already existing volume name
	// ID and name are same in longhorn API
	existVol, err := cs.apiClient.Volume.ById(req.GetName())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if existVol != nil && existVol.Name == req.GetName() {
		logrus.Debugf("CreateVolume: got an exist volume: %s", existVol.Name)
		exVolSize, err := util.ConvertSize(existVol.Size)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		// pass through the volume content source in case this volume is in the process of being created
		rsp := &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      existVol.Id,
				CapacityBytes: exVolSize,
				VolumeContext: volumeParameters,
				ContentSource: req.VolumeContentSource,
			},
		}

		return rsp, nil
	}

	// irregardless of the used storage class, if this is requested in rwx mode
	// we need to mark the volume as a shared volume
	for _, cap := range volumeCaps {
		if requiresSharedAccess(nil, cap) {
			if volumeParameters == nil {
				volumeParameters = map[string]string{}
			}
			volumeParameters["share"] = "true"
			break
		}
	}

	vol, err := getVolumeOptions(volumeParameters)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if vol.BackingImage != "" {
		if _, err := cs.apiClient.BackingImage.ById(vol.BackingImage); err != nil {
			msg := fmt.Sprintf("CreateVolume: cannot find backing image %v for volume %v", vol.BackingImage, req.Name)
			logrus.Error(msg)
			return nil, status.Error(codes.NotFound, msg)
		}
	}

	vol.Name = req.Name

	volSizeBytes := int64(util.MinimalVolumeSize)
	if req.GetCapacityRange() != nil {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}

	if volSizeBytes < util.MinimalVolumeSize {
		logrus.Warnf("Request volume %v size %v is smaller than minimal size %v, set it to minimal size.", vol.Name, volSizeBytes, util.MinimalVolumeSize)
		volSizeBytes = util.MinimalVolumeSize
	}

	// Round up to multiple of 2 * 1024 * 1024
	volSizeBytes = util.RoundUpSize(volSizeBytes)

	if volSizeBytes >= putil.GiB {
		vol.Size = fmt.Sprintf("%.2fGi", float64(volSizeBytes)/float64(putil.GiB))
	} else {
		vol.Size = fmt.Sprintf("%dMi", putil.RoundUpSize(volSizeBytes, putil.MiB))
	}

	logrus.Infof("CreateVolume: creating a volume by API client, name: %s, size: %s accessMode: %v", vol.Name, vol.Size, vol.AccessMode)
	resVol, err := cs.apiClient.Volume.Create(vol)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if !cs.waitForVolumeState(resVol.Id, string(types.VolumeStateDetached), isVolumeDetached, true, false) {
		return nil, status.Error(codes.DeadlineExceeded, "cannot wait for volume creation to complete")
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      resVol.Id,
			CapacityBytes: volSizeBytes,
			VolumeContext: volumeParameters,
			ContentSource: req.VolumeContentSource,
		},
	}, nil
}

func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	logrus.Infof("ControllerServer delete volume req: %v", req)

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if err := cs.validateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		logrus.Errorf("DeleteVolume: invalid delete volume req: %v", req)
		return nil, status.Error(codes.Internal, err.Error())
	}

	existVol, err := cs.apiClient.Volume.ById(req.GetVolumeId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if existVol == nil {
		logrus.Warnf("DeleteVolume: volume %s not exists", req.GetVolumeId())
		return &csi.DeleteVolumeResponse{}, nil
	}

	logrus.Debugf("DeleteVolume: volume %s exists", req.GetVolumeId())
	if err = cs.apiClient.Volume.Delete(existVol); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: cs.caps,
	}, nil
}

func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	logrus.Infof("ControllerServer ValidateVolumeCapabilities req: %v", req)
	if err := cs.validateVolumeCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, err
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeContext:      req.GetVolumeContext(),
			VolumeCapabilities: req.GetVolumeCapabilities(),
			Parameters:         req.GetParameters(),
		},
	}, nil
}

func (cs *ControllerServer) isNodeReady(nodeID string) bool {
	requestedNode, err := cs.apiClient.Node.ById(nodeID)
	if err != nil || requestedNode == nil {
		return false
	}

	if requestedNode.Conditions[string(types.NodeConditionTypeReady)] != nil {
		condition := requestedNode.Conditions[string(types.NodeConditionTypeReady)].(map[string]interface{})
		if condition != nil &&
			condition["status"] != nil &&
			condition["status"].(string) == string(types.ConditionStatusTrue) {
			return true
		}
	}

	// a non existing status is the same as node NotReady
	return false
}

// ControllerPublishVolume will attach the volume to the specified node
func (cs *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	logrus.Infof("ControllerServer ControllerPublishVolume req: %v", req)
	existVol, err := cs.apiClient.Volume.ById(req.GetVolumeId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if existVol == nil {
		msg := fmt.Sprintf("ControllerPublishVolume: the volume %s not exists", req.GetVolumeId())
		logrus.Warn(msg)
		return nil, status.Error(codes.NotFound, msg)
	}

	volumeCapability := req.GetVolumeCapability()
	if volumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not provided")
	}

	if !cs.isNodeReady(req.GetNodeId()) {
		msg := fmt.Sprintf("ControllerPublishVolume: the volume %s cannot be attached to `NotReady` node %s",
			req.GetVolumeId(), req.GetNodeId())
		logrus.Warn(msg)
		return nil, status.Error(codes.NotFound, msg)
	}

	if existVol.RestoreRequired {
		return nil, status.Errorf(codes.Aborted, "The volume %s is restoring backup", req.GetVolumeId())
	}

	if existVol.State == string(types.VolumeStateAttaching) || existVol.State == string(types.VolumeStateDetaching) {
		return nil, status.Errorf(codes.Aborted, "The volume %s is %s", req.GetVolumeId(), existVol.State)
	}

	// Check volume frontend settings
	if existVol.Frontend != string(types.VolumeFrontendBlockDev) {
		return nil, status.Errorf(codes.InvalidArgument, "ControllerPublishVolume: there is no block device frontend for volume %s", req.GetVolumeId())
	}

	if requiresSharedAccess(existVol, volumeCapability) {

		// we check for volume readiness before potentially messing with the access mode
		if !existVol.Ready || len(existVol.Controllers) == 0 || existVol.Controllers[0].Endpoint == "" {
			return nil, status.Errorf(codes.Aborted,
				"The volume %s is not ready for workloads", req.GetVolumeId())
		}

		if existVol.AccessMode != string(types.AccessModeReadWriteMany) {
			input := &longhornclient.UpdateAccessModeInput{
				AccessMode: string(types.AccessModeReadWriteMany),
			}

			if existVol, err = cs.apiClient.Volume.ActionUpdateAccessMode(existVol, input); err != nil {
				logrus.WithError(err).Errorf("Failed to change Volume %s access mode to RWX", req.GetVolumeId())
				return nil, status.Error(codes.Internal, err.Error())
			}

			logrus.Infof("Changed Volume %s access mode to RWX", req.GetVolumeId())
		}

		if !cs.waitForVolumeState(req.GetVolumeId(), "share available", isVolumeShareAvailable, false, false) {
			return nil, status.Errorf(codes.DeadlineExceeded, "Failed to wait for volume %v share available", req.GetVolumeId())
		}

		logrus.Infof("Volume %s shared to %s", req.GetVolumeId(), req.GetNodeId())
		return &csi.ControllerPublishVolumeResponse{}, nil
	}

	// the volume is already attached make sure it's to the same node as this
	if existVol.State == string(types.VolumeStateAttached) {
		if !existVol.Ready || len(existVol.Controllers) == 0 || existVol.Controllers[0].Endpoint == "" {
			return nil, status.Errorf(codes.Aborted,
				"The volume %s is already attached but it is not ready for workloads", req.GetVolumeId())
		}

		if existVol.Controllers[0].HostId != req.GetNodeId() {
			return nil, status.Errorf(codes.FailedPrecondition,
				"The volume %s cannot be attached to the node %s since it is already attached to the node %s",
				req.GetVolumeId(), req.GetNodeId(), existVol.Controllers[0].HostId)
		}

		// TODO: add comparison for capabilities, to do so we need to pass the volume context
		//  as part of the volume creation (this is for the Volume published but is incompatible case of the csi spec)
		logrus.Infof("ControllerPublishVolume: no need to attach volume %s since it's already attached to the correct node %s",
			req.GetVolumeId(), req.GetNodeId())
	} else {
		logrus.Debugf("ControllerPublishVolume: volume %s is ready to be attached, and the preferred nodeID is %s", req.GetVolumeId(), req.GetNodeId())
		// attach longhorn volume with frontend enabled
		input := &longhornclient.AttachInput{
			HostId:          req.GetNodeId(),
			DisableFrontend: false,
		}
		existVol, err = cs.apiClient.Volume.ActionAttach(existVol, input)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		logrus.Debugf("ControllerPublishVolume: succeed to send an attach request for volume %s", req.GetVolumeId())
	}

	if !cs.waitForVolumeState(req.GetVolumeId(), string(types.VolumeStateAttached), isVolumeAttached, false, false) {
		return nil, status.Errorf(codes.Aborted, "Attaching volume %s on node %s failed",
			req.GetVolumeId(), req.GetNodeId())
	}
	logrus.Infof("Volume %s attached on %s", req.GetVolumeId(), req.GetNodeId())

	return &csi.ControllerPublishVolumeResponse{}, nil
}

// ControllerUnpublishVolume will detach the volume
func (cs *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	logrus.Infof("ControllerServer ControllerUnpublishVolume req: %v", req)

	existVol, err := cs.apiClient.Volume.ById(req.GetVolumeId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// VOLUME_NOT_FOUND is no longer the ControllerUnpublishVolume error
	// See https://github.com/container-storage-interface/spec/issues/382 for details
	if existVol == nil {
		msg := fmt.Sprintf("ControllerUnpublishVolume: the volume %s not exists", req.GetVolumeId())
		logrus.Warn(msg)
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	if existVol.State == string(types.VolumeStateDetaching) {
		return nil, status.Errorf(codes.Aborted, "The volume %s is detaching", req.GetVolumeId())
	}

	needToDetach := false
	if existVol.State == string(types.VolumeStateAttached) || existVol.State == string(types.VolumeStateAttaching) {

		// try to wait till we are attached, but if we fail to attach we need to process the detach
		// irregardless of the node we are on otherwise we might end up, allowing a bugged volume to remain in the attaching state forever
		// NOTE: this is a tradeoff and it would be better if we could verify that we are actually stuck attaching to the requested node
		if existVol.State == string(types.VolumeStateAttaching) {
			if cs.waitForVolumeState(req.GetVolumeId(), string(types.VolumeStateAttached), isVolumeAttached, false, true) {
				existVol, err = cs.apiClient.Volume.ById(req.GetVolumeId())
				if err != nil {
					return nil, status.Error(codes.Internal, err.Error())
				}
			} else {
				logrus.Warnf("Volume %s stuck in attaching state, processing detach request for node %s "+
					"even though we don't know which node we are attaching on", req.GetVolumeId(), req.GetNodeId())
			}
		}

		// for shared volumes we don't need to call detach since the share manager handles the volume attachment logic
		if requiresSharedAccess(existVol, nil) {
			logrus.Infof("Requesting shared volume %s detach from node %s", req.GetVolumeId(), req.GetNodeId())
			needToDetach = false
		} else if existVol.Controllers[0].HostId == req.GetNodeId() {
			logrus.Infof("Requesting volume %s detach from node %s", req.GetVolumeId(), req.GetNodeId())
			needToDetach = true
		} else if len(existVol.Controllers) == 0 || existVol.Controllers[0].HostId == "" {
			logrus.Warnf("Processing volume %s detach request for node %s "+
				"even though we don't know which node we are attached on", req.GetVolumeId(), req.GetNodeId())
			needToDetach = true
		}
	}

	if needToDetach {
		logrus.Debugf("requesting Volume %s detachment for %s", req.GetVolumeId(), req.GetNodeId())
		_, err = cs.apiClient.Volume.ActionDetach(existVol)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else if requiresSharedAccess(existVol, nil) {
		logrus.Infof("don't need to detach shared Volume %s from node %s", req.GetVolumeId(), req.GetNodeId())
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	} else {
		logrus.Infof("don't need to detach Volume %s since we are already detached from node %s",
			req.GetVolumeId(), req.GetNodeId())
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	if !cs.waitForVolumeState(req.GetVolumeId(), string(types.VolumeStateDetached), isVolumeDetached, false, true) {
		return nil, status.Errorf(codes.Aborted, "Failed to detach volume %s from node %s", req.GetVolumeId(), req.GetNodeId())
	}

	logrus.Debugf("Volume %s detached on %s", req.GetVolumeId(), req.GetNodeId())
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *ControllerServer) ListVolumes(context.Context, *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) GetCapacity(context.Context, *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	logrus.Debugf("ControllerServer CreateSnapshot req: %v", req)
	csiLabels := req.Parameters
	csiSnapshotName := req.GetName()
	csiVolumeName := req.GetSourceVolumeId()
	if len(csiVolumeName) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume name must be provided")
	} else if len(csiSnapshotName) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot name must be provided")
	}

	// we check for backup existence first, since it's possible that
	// the actual volume is no longer available but the backup still is.
	backupVolume, err := cs.apiClient.BackupVolume.ById(csiVolumeName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	backupListOutput, err := cs.apiClient.BackupVolume.ActionBackupList(backupVolume)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// NOTE: csi-snapshots assume a 1 to 1 relationship, longhorn allows for multiple backups of a snapshot
	var backup *longhornclient.Backup
	for _, b := range backupListOutput.Data {
		if b.SnapshotName == csiSnapshotName {
			backup = &b
			break
		}
	}

	// since there is a backup file for this on the backupstore we can assume successful completion
	// since the backup.cfg only gets written after all the blocks have been transferred
	if backup != nil {
		rsp := createSnapshotResponse(backup.VolumeName, backup.Name, backup.SnapshotCreated, backup.VolumeSize, 100)
		logrus.Infof("ControllerServer CreateSnapshot rsp: %v", rsp)
		return rsp, nil
	}

	existVol, err := cs.apiClient.Volume.ById(csiVolumeName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if existVol == nil {
		msg := fmt.Sprintf("CreateSnapshot: the volume %s doesn't exist", csiVolumeName)
		logrus.Warn(msg)
		return nil, status.Error(codes.NotFound, msg)
	}

	var snapshot *longhornclient.Snapshot
	snapshotListOutput, err := cs.apiClient.Volume.ActionSnapshotList(existVol)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	for _, snap := range snapshotListOutput.Data {
		if snap.Name == csiSnapshotName {
			snapshot = &snap
			break
		}
	}

	// if a backup has been deleted from the backupstore but the runtime information is still present in the volume
	// we want to create a new backup same as if the backup operation has failed
	backupStatus, err := cs.getBackupStatus(csiVolumeName, csiSnapshotName)
	if err != nil {
		return nil, err
	}

	if backupStatus != nil && backupStatus.Progress != 100 && backupStatus.Error == "" {
		var creationTime string
		if snapshot != nil {
			creationTime = snapshot.Created
		}

		rsp := createSnapshotResponse(csiVolumeName, backupStatus.Id, creationTime, existVol.Size, int(backupStatus.Progress))
		logrus.Infof("ControllerServer CreateSnapshot rsp: %v", rsp)
		return rsp, nil
	}

	// no existing backup and no local snapshot, create a new one
	if snapshot == nil {
		snapshot, err = cs.apiClient.Volume.ActionSnapshotCreate(existVol, &longhornclient.SnapshotInput{
			Labels: csiLabels,
			Name:   csiSnapshotName,
		})

		// failed to create snapshot, so there is no way to backup
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	// create backup based on local volume snapshot
	existVol, err = cs.apiClient.Volume.ActionSnapshotBackup(existVol, &longhornclient.SnapshotInput{
		Labels: csiLabels,
		Name:   csiSnapshotName,
	})

	// failed to kick off backup
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// we need to wait for backup initiation since we only know the backupID after the fact
	backupStatus, err = cs.waitForBackupInitiation(csiVolumeName, csiSnapshotName)
	if err != nil {
		return nil, err
	}

	rsp := createSnapshotResponse(existVol.Name, backupStatus.Id, snapshot.Created, existVol.Size, int(backupStatus.Progress))
	logrus.Debugf("ControllerServer CreateSnapshot rsp: %v", rsp)
	return rsp, nil
}

func createSnapshotResponse(volumeName, backupName, snapshotTime, volumeSize string, progress int) *csi.CreateSnapshotResponse {
	creationTime, err := toProtoTimestamp(snapshotTime)
	if err != nil {
		logrus.Errorf("Failed to parse creation time %v for backup %v", snapshotTime, backupName)
	}

	size, _ := util.ConvertSize(volumeSize)
	size = util.RoundUpSize(size)
	snapshotID := encodeSnapshotID(volumeName, backupName)
	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SizeBytes:      size,
			SnapshotId:     snapshotID,
			SourceVolumeId: volumeName,
			CreationTime:   creationTime,
			ReadyToUse:     progress == 100,
		},
	}
}

// encodeSnapshotID encodes the backup volume as part of the snapshotID
// so we don't need to iterate over all backup volumes,
// when trying to find a backup for deletion or restoration
func encodeSnapshotID(volumeName, backupName string) string {
	return fmt.Sprintf("bs://%s/%s", volumeName, backupName)
}

// decodeSnapshotID splits up the snapshotID back into it's components
// backupType will be used once we implement the VolumeBackup crd
func decodeSnapshotID(snapshotID string) (backupType, volumeName, backupName string) {
	split := strings.Split(snapshotID, "://")
	if len(split) < 2 {
		return "", "", snapshotID
	}
	backupType = split[0]

	split = strings.Split(split[1], "/")
	volumeName = split[0]
	backupName = split[1]

	return backupType, volumeName, backupName
}

func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if len(req.GetSnapshotId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot ID must be provided")
	}

	_, volumeName, backupName := decodeSnapshotID(req.SnapshotId)
	backupVolume, err := cs.apiClient.BackupVolume.ById(volumeName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if backupVolume != nil {
		backupVolume, err = cs.apiClient.BackupVolume.ActionBackupDelete(backupVolume, &longhornclient.BackupInput{Name: backupName})
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

func (cs *ControllerServer) ListSnapshots(context.Context, *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	logrus.Infof("ControllerServer ControllerExpandVolume req: %v", req)
	existVol, err := cs.apiClient.Volume.ById(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	if existVol == nil {
		msg := fmt.Sprintf("ControllerExpandVolume: the volume %s not exists", req.GetVolumeId())
		logrus.Warn(msg)
		return nil, status.Errorf(codes.NotFound, msg)
	}
	if len(existVol.Controllers) != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "There should be only one controller for volume %s", req.GetVolumeId())
	}
	// Support offline expansion only
	if existVol.State != string(types.VolumeStateDetached) {
		return nil, status.Errorf(codes.FailedPrecondition, "Invalid volume state %v for expansion", existVol.State)
	}
	volumeSize, err := strconv.ParseInt(existVol.Size, 10, 64)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	requestedSize := req.CapacityRange.GetRequiredBytes()
	if requestedSize <= volumeSize {
		return nil, status.Errorf(codes.OutOfRange, "The requested size %v is smaller than or the same as the size %v of volume %v", requestedSize, volumeSize, existVol.Name)
	}

	if _, err = cs.apiClient.Volume.ActionExpand(existVol, &longhornclient.ExpandInput{
		Size: strconv.FormatInt(requestedSize, 10),
	}); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         req.CapacityRange.GetRequiredBytes(),
		NodeExpansionRequired: false,
	}, nil
}

func isVolumeDetached(vol *longhornclient.Volume) bool {
	return vol.State == string(types.VolumeStateDetached)
}

func isVolumeAttached(vol *longhornclient.Volume) bool {
	return vol.State == string(types.VolumeStateAttached) && vol.Controllers[0].Endpoint != ""
}

func isVolumeShareAvailable(vol *longhornclient.Volume) bool {
	return vol.ShareState == string(types.ShareManagerStateRunning) && vol.ShareEndpoint != ""
}

func (cs *ControllerServer) waitForVolumeState(volumeID string, stateDescription string,
	predicate func(vol *longhornclient.Volume) bool, notFoundRetry, notFoundReturn bool) bool {
	timer := time.NewTimer(timeoutAttachDetach)
	defer timer.Stop()
	timeout := timer.C

	ticker := time.NewTicker(tickAttachDetach)
	defer ticker.Stop()
	tick := ticker.C

	for {
		select {
		case <-timeout:
			logrus.Warnf("waitForVolumeState: timeout while waiting for volume %s state %s", volumeID, stateDescription)
			return false
		case <-tick:
			logrus.Debugf("Polling volume %s state for %s at %s", volumeID, stateDescription, time.Now().String())
			existVol, err := cs.apiClient.Volume.ById(volumeID)
			if err != nil {
				logrus.Warnf("waitForVolumeState: error while waiting for volume %s state %s error %s", volumeID, stateDescription, err)
				continue
			}
			if existVol == nil {
				logrus.Warnf("waitForVolumeState: volume %s does not exist", volumeID)
				if notFoundRetry {
					continue
				}
				return notFoundReturn
			}
			if predicate(existVol) {
				return true
			}
		}
	}
}

// waitForBackupInitiation polls the volumes backup status till there is a backup in progress
// this is necessary since the backup name is only known after the backup is initiated
func (cs *ControllerServer) waitForBackupInitiation(volumeName, snapshotName string) (*longhornclient.BackupStatus, error) {
	timer := time.NewTimer(timeoutBackupInitiation)
	defer timer.Stop()
	timeout := timer.C

	ticker := time.NewTicker(tickBackupInitiation)
	defer ticker.Stop()
	tick := ticker.C

	for {
		select {
		case <-timeout:
			msg := fmt.Sprintf("waitForBackupInitiation: timeout while waiting for backup initiation for volume %s for snapshot %s", volumeName, snapshotName)
			logrus.Warn(msg)
			return nil, status.Error(codes.DeadlineExceeded, msg)
		case <-tick:

			backupStatus, err := cs.getBackupStatus(volumeName, snapshotName)
			if err != nil {
				return nil, err
			}

			if backupStatus != nil {
				logrus.Infof("Backup %v initiated for volume %v for snapshot %v", backupStatus.Id, volumeName, snapshotName)
				return backupStatus, nil
			}
		}
	}
}

func (cs *ControllerServer) getBackupStatus(volumeName, snapshotName string) (*longhornclient.BackupStatus, error) {
	existVol, err := cs.apiClient.Volume.ById(volumeName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if existVol == nil {
		msg := fmt.Sprintf("could not retrieve backup status the volume %s doesn't exist", volumeName)
		logrus.Warn(msg)
		return nil, status.Error(codes.NotFound, msg)
	}

	var backupStatus *longhornclient.BackupStatus
	for _, status := range existVol.BackupStatus {
		if status.Snapshot == snapshotName {
			backupStatus = &status
			break
		}
	}

	return backupStatus, nil
}

func (cs *ControllerServer) validateControllerServiceRequest(c csi.ControllerServiceCapability_RPC_Type) error {
	if c == csi.ControllerServiceCapability_RPC_UNKNOWN {
		return nil
	}

	for _, cap := range cs.caps {
		if c == cap.GetRpc().GetType() {
			return nil
		}
	}
	return status.Errorf(codes.InvalidArgument, "unsupported capability %s", c)
}

func (cs *ControllerServer) validateVolumeCapabilities(volumeCaps []*csi.VolumeCapability) error {
	if volumeCaps == nil {
		return status.Error(codes.InvalidArgument, "Volume Capabilities cannot be empty")
	}

	for _, cap := range volumeCaps {
		if cap.GetMount() == nil && cap.GetBlock() == nil {
			return status.Error(codes.InvalidArgument, "cannot have both mount and block access type be undefined")
		}
		if cap.GetMount() != nil && cap.GetBlock() != nil {
			return status.Error(codes.InvalidArgument, "cannot have both block and mount access type")
		}

		supportedMode := false
		for _, m := range cs.accessModes {
			if cap.GetAccessMode().GetMode() == m.GetMode() {
				supportedMode = true
				break
			}
		}
		if !supportedMode {
			return status.Errorf(codes.InvalidArgument, "access mode %v is not supported", cap.GetAccessMode().Mode.String())
		}
	}

	return nil
}

func getControllerServiceCapabilities(cl []csi.ControllerServiceCapability_RPC_Type) []*csi.ControllerServiceCapability {
	var cscs []*csi.ControllerServiceCapability

	for _, cap := range cl {
		logrus.Infof("Enabling controller service capability: %v", cap.String())
		cscs = append(cscs, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		})
	}

	return cscs
}

func getVolumeCapabilityAccessModes(vc []csi.VolumeCapability_AccessMode_Mode) []*csi.VolumeCapability_AccessMode {
	var vca []*csi.VolumeCapability_AccessMode
	for _, c := range vc {
		logrus.Infof("Enabling volume access mode: %v", c.String())
		vca = append(vca, &csi.VolumeCapability_AccessMode{Mode: c})
	}
	return vca
}

func toProtoTimestamp(s string) (*timestamp.Timestamp, error) {
	t, err := util.ParseTimeZ(s)
	if err != nil {
		return nil, err
	}

	return ptypes.TimestampProto(t)
}
