package csi

import (
	"context"
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"

	longhornclient "github.com/rancher/longhorn-manager/client"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

const (
	timeoutAttachDetach = 120 * time.Second
	tickAttachDetach    = 2 * time.Second
)

type ControllerServer struct {
	*csicommon.DefaultControllerServer
	apiClient *longhornclient.RancherClient
}

func NewControllerServer(d *csicommon.CSIDriver, apiClient *longhornclient.RancherClient) *ControllerServer {
	return &ControllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d),
		apiClient:               apiClient,
	}
}

func (cs *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	logrus.Infof("ControllerServer create volume req: %v", req)
	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		logrus.Errorf("CreateVolume: invalid create volume req: %v", req)
		return nil, err
	}
	// Check sanity of request Name, Volume Capabilities
	if len(req.GetName()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume Name cannot be empty")
	}
	if req.GetVolumeCapabilities() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities cannot be empty")
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
				Id:            existVol.Id,
				CapacityBytes: exVolSize,
				Attributes:    req.GetParameters(),
			},
		}, nil
	}

	vol, err := getVolumeOptions(req.GetParameters())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	vol.Name = req.Name

	volSizeBytes := int64(volumeutil.GIB)
	if req.GetCapacityRange() != nil {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeGiB := volumeutil.RoundUpSize(volSizeBytes, volumeutil.GIB)
	vol.Size = fmt.Sprintf("%dGi", volSizeGiB)

	logrus.Infof("CreateVolume: creating a volume by API client, name: %s, size: %s", vol.Name, vol.Size)
	resVol, err := cs.apiClient.Volume.Create(vol)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			Id:            resVol.Id,
			CapacityBytes: int64(volSizeGiB * volumeutil.GIB),
			Attributes:    req.GetParameters(),
		},
	}, nil
}

func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	logrus.Infof("ControllerServer delete volume req: %v", req)
	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		logrus.Errorf("DeleteVolume: invalid delete volume req: %v", req)
		return nil, err
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

func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	logrus.Infof("ControllerServer ValidateVolumeCapabilities req: %v", req)
	for _, cap := range req.GetVolumeCapabilities() {
		if cap.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
			return &csi.ValidateVolumeCapabilitiesResponse{Supported: false, Message: ""}, nil
		}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Supported: true, Message: ""}, nil
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
	if existVol.State == string(types.VolumeStateAttaching) || existVol.State == string(types.VolumeStateDetaching) {
		return nil, status.Errorf(codes.Aborted, "The volume %s is %s", req.GetVolumeId(), existVol.State)
	}

	needToAttach := false
	if existVol.State == string(types.VolumeStateDetached) {
		needToAttach = true
	}

	logrus.Debugf("ControllerPublishVolume: current nodeID %s", req.GetNodeId())
	if needToAttach {
		// attach longhorn volume
		input := &longhornclient.AttachInput{HostId: req.GetNodeId()}
		existVol, err = cs.apiClient.Volume.ActionAttach(existVol, input)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else {
		// return Aborted let CSI retry ControllerPublishVolume
		logrus.Infof("ControllerPublishVolume: no need to attach volume %s", req.GetVolumeId())
		return nil, status.Errorf(codes.Aborted, "The volume %s is %s", req.GetVolumeId(), existVol.State)
	}

	if !cs.waitForAttach(req.GetVolumeId()) {
		return nil, status.Errorf(codes.Aborted, "Attaching volume %s failed", req.GetVolumeId())
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
	if existVol == nil {
		logrus.Warnf("ControllerUnpublishVolume: the volume %s not exists", req.GetVolumeId())
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	if existVol.State == string(types.VolumeStateDetaching) {
		return nil, status.Errorf(codes.Aborted, "The volume %s is detaching", req.GetVolumeId())
	}

	needToDetach := false
	if existVol.State == string(types.VolumeStateAttached) {
		needToDetach = true
	}

	if needToDetach {
		// detach longhorn volume
		_, err = cs.apiClient.Volume.ActionDetach(existVol)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else {
		// return Aborted let CSI retry ControllerUnpublishVolume
		logrus.Infof("ControllerUnpublishVolume: no need to detach volume %s", req.GetVolumeId())
		return nil, status.Errorf(codes.Aborted, "The volume %s is %s", req.GetVolumeId(), existVol.State)
	}

	if !cs.waitForDetach(req.GetVolumeId()) {
		return nil, status.Errorf(codes.Aborted, "Detaching volume %s failed", req.GetVolumeId())
	}
	logrus.Debugf("Volume %s detached on %s", req.GetVolumeId(), req.GetNodeId())

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *ControllerServer) waitForAttach(volumeID string) (attached bool) {
	timeout := time.After(timeoutAttachDetach)
	tick := time.Tick(tickAttachDetach)
	for {
		select {
		case <-timeout:
			logrus.Warnf("waitForAttach: timeout to attach volume %s", volumeID)
			return false
		case <-tick:
			logrus.Debugf("Trying to get %s attach status at %s", volumeID, time.Now().String())
			existVol, err := cs.apiClient.Volume.ById(volumeID)
			if err != nil {
				logrus.Warnf("waitForAttach: %s", err)
				continue
			}
			if existVol == nil {
				logrus.Warnf("waitForAttach: volume %s not exist", volumeID)
				return false
			}
			if existVol.State == string(types.VolumeStateAttached) {
				return true
			}
		}
	}
}

func (cs *ControllerServer) waitForDetach(volumeID string) (attached bool) {
	timeout := time.After(timeoutAttachDetach)
	tick := time.Tick(tickAttachDetach)
	for {
		select {
		case <-timeout:
			logrus.Warnf("waitForDetach: timeout to dettach volume %s", volumeID)
			return false
		case <-tick:
			logrus.Debugf("Trying to get %s dettach status at %s", volumeID, time.Now().String())
			existVol, err := cs.apiClient.Volume.ById(volumeID)
			if err != nil {
				logrus.Warnf("waitForDetach: %s", err)
				continue
			}
			if existVol == nil {
				// volume is not exist so I mark it as detached
				logrus.Warnf("waitForDetach: volume %s not exist", volumeID)
				return true
			}
			if existVol.State == string(types.VolumeStateDetached) {
				return true
			}
		}
	}
}
