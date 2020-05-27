package csi

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	putil "sigs.k8s.io/sig-storage-lib-external-provisioner/util"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

const (
	timeoutAttachDetach = 120 * time.Second
	tickAttachDetach    = 2 * time.Second
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
			}),
		accessModes: getVolumeCapabilityAccessModes(
			[]csi.VolumeCapability_AccessMode_Mode{
				csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
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
	if volumeCaps == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities cannot be empty")
	}

	if err := cs.validateVolumeCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, err
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
		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      existVol.Id,
				CapacityBytes: exVolSize,
				VolumeContext: req.GetParameters(),
			},
		}, nil
	}

	vol, err := getVolumeOptions(req.GetParameters())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	vol.Name = req.Name

	volSizeBytes := int64(putil.GiB)
	if req.GetCapacityRange() != nil {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeGiB := putil.RoundUpToGiB(volSizeBytes)
	vol.Size = fmt.Sprintf("%dGi", volSizeGiB)

	logrus.Infof("CreateVolume: creating a volume by API client, name: %s, size: %s", vol.Name, vol.Size)
	resVol, err := cs.apiClient.Volume.Create(vol)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if !cs.waitForVolumeState(resVol.Id, types.VolumeStateDetached, true, false) {
		return nil, status.Error(codes.Internal, "cannot wait for volume creation to complete")
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      resVol.Id,
			CapacityBytes: int64(volSizeGiB * putil.GiB),
			VolumeContext: req.GetParameters(),
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

	if !cs.isNodeReady(req.GetNodeId()) {
		msg := fmt.Sprintf("ControllerPublishVolume: the volume %s cannot be attached to `NotReady` node %s",
			req.GetVolumeId(), req.GetNodeId())
		logrus.Warn(msg)
		return nil, status.Error(codes.NotFound, msg)
	}

	if existVol.InitialRestorationRequired {
		return nil, status.Errorf(codes.Aborted, "The volume %s is restoring backup", req.GetVolumeId())
	}

	if existVol.State == string(types.VolumeStateAttaching) || existVol.State == string(types.VolumeStateDetaching) {
		return nil, status.Errorf(codes.Aborted, "The volume %s is %s", req.GetVolumeId(), existVol.State)
	}

	// the volume is already attached make sure it's to the same node as this
	if existVol.State == string(types.VolumeStateAttached) {
		if !existVol.Ready || len(existVol.Controllers) == 0 {
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
		logrus.Debugf("ControllerPublishVolume: current nodeID %s", req.GetNodeId())
		// attach longhorn volume with frontend enabled
		input := &longhornclient.AttachInput{
			HostId:          req.GetNodeId(),
			DisableFrontend: false,
		}
		existVol, err = cs.apiClient.Volume.ActionAttach(existVol, input)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	if !cs.waitForVolumeState(req.GetVolumeId(), types.VolumeStateAttached, false, false) {
		return nil, status.Errorf(codes.Aborted, "Attaching volume %s on node %s failed",
			req.GetVolumeId(), req.GetNodeId())
	}
	logrus.Debugf("Volume %s attached on %s", req.GetVolumeId(), req.GetNodeId())

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
			if cs.waitForVolumeState(req.GetVolumeId(), types.VolumeStateAttached, false, true) {
				existVol, err = cs.apiClient.Volume.ById(req.GetVolumeId())
				if err != nil {
					return nil, status.Error(codes.Internal, err.Error())
				}
			} else {
				logrus.Warnf("Volume %s stuck in attaching state, processing detach request for node %s "+
					"even though we don't know which node we are attaching on", req.GetVolumeId(), req.GetNodeId())
			}
		}

		// only detach if we are actually attached to the requested node or we don't know where we are attached
		if len(existVol.Controllers) == 0 || existVol.Controllers[0].HostId == "" {
			logrus.Warnf("Processing volume %s detach request for node %s "+
				"even though we don't know which node we are attached on", req.GetVolumeId(), req.GetNodeId())
			needToDetach = true
		} else if existVol.Controllers[0].HostId == req.GetNodeId() {
			logrus.Infof("Requesting volume %s detach from node %s", req.GetVolumeId(), req.GetNodeId())
			needToDetach = true
		}
	}

	if needToDetach {
		// detach longhorn volume
		logrus.Debugf("requesting Volume %s detachment for %s", req.GetVolumeId(), req.GetNodeId())
		_, err = cs.apiClient.Volume.ActionDetach(existVol)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else {
		logrus.Infof("don't need to detach Volume %s since we are already detached from node %s",
			req.GetVolumeId(), req.GetNodeId())
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	if !cs.waitForVolumeState(req.GetVolumeId(), types.VolumeStateDetached, false, true) {
		return nil, status.Errorf(codes.Aborted, "Detaching volume %s failed", req.GetVolumeId())
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

func (cs *ControllerServer) CreateSnapshot(context.Context, *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) DeleteSnapshot(context.Context, *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
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

func (cs *ControllerServer) waitForVolumeState(volumeID string, state types.VolumeState, notFoundRetry, notFoundReturn bool) bool {
	timeout := time.After(timeoutAttachDetach)
	ticker := time.NewTicker(tickAttachDetach)
	defer ticker.Stop()
	tick := ticker.C
	for {
		select {
		case <-timeout:
			logrus.Warnf("waitForVolumeState: timeout to wait for volume %s become %s", volumeID, state)
			return false
		case <-tick:
			logrus.Debugf("Polling %s state for %s at %s", volumeID, state, time.Now().String())
			existVol, err := cs.apiClient.Volume.ById(volumeID)
			if err != nil {
				logrus.Warnf("waitForVolumeState: wait for %s state %s: %s", volumeID, state, err)
				continue
			}
			if existVol == nil {
				logrus.Warnf("waitForVolumeState: volume %s not exist", volumeID)
				if notFoundRetry {
					continue
				}
				return notFoundReturn
			}
			if existVol.State == string(state) {
				return true
			}
		}
	}
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
