package csi

import (
	"context"
	"fmt"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/kubernetes/pkg/util/mount"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
)

type NodeServer struct {
	*csicommon.DefaultNodeServer
}

func NewNodeServer(d *csicommon.CSIDriver) *NodeServer {
	return &NodeServer{
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d),
	}
}

// NodePublishVolume will mount the volume /dev/longhorn/<volume_name> to target_path
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	logrus.Infof("NodeServer NodePublishVolume req: %v", req)

	readOnly := req.GetReadonly()
	if readOnly {
		return nil, status.Error(codes.FailedPrecondition, "Not support readOnly")
	}

	targetPath := req.GetTargetPath()

	notMnt, err := isLikelyNotMountPointAttach(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !notMnt {
		logrus.Debugf("NodePublishVolume: the volume %s has been mounted", req.GetVolumeId())
		return &csi.NodePublishVolumeResponse{}, nil
	}

	fsType := req.GetVolumeCapability().GetMount().GetFsType()
	devicePath := fmt.Sprintf("/dev/longhorn/%s", req.GetVolumeId())

	options := []string{}
	mountFlags := req.GetVolumeCapability().GetMount().GetMountFlags()
	options = append(options, mountFlags...)

	diskMounter := &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: mount.NewOsExec()}
	if err := diskMounter.FormatAndMount(devicePath, targetPath, fsType, options); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	logrus.Debugf("NodePublishVolume: done %s", req.GetVolumeId())

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	logrus.Infof("NodeServer NodeUnpublishVolume req: %v", req)

	targetPath := req.GetTargetPath()

	notMnt, err := isLikelyNotMountPointDetach(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if notMnt {
		return nil, status.Error(codes.NotFound, "Volume not mounted")
	}

	err = volumeutil.UnmountPath(req.GetTargetPath(), mount.New(""))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	logrus.Debugf("NodeUnpublishVolume: done %s", req.GetVolumeId())

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *NodeServer) NodeStageVolume(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest) (
	*csi.NodeStageVolumeResponse, error) {

	return nil, status.Error(codes.Unimplemented, "")
}

func (ns *NodeServer) NodeUnstageVolume(
	ctx context.Context,
	req *csi.NodeUnstageVolumeRequest) (
	*csi.NodeUnstageVolumeResponse, error) {

	return nil, status.Error(codes.Unimplemented, "")
}
