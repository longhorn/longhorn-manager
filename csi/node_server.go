package csi

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/kubernetes/pkg/util/mount"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

type NodeServer struct {
	apiClient *longhornclient.RancherClient
	nodeID    string
	caps      []*csi.NodeServiceCapability
}

func NewNodeServer(apiClient *longhornclient.RancherClient, nodeID string) *NodeServer {
	return &NodeServer{
		apiClient: apiClient,
		nodeID:    nodeID,
		caps: getNodeServiceCapabilities(
			[]csi.NodeServiceCapability_RPC_Type{
				csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
			}),
	}
}

// NodePublishVolume will mount the volume /dev/longhorn/<volume_name> to target_path
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	logrus.Infof("NodeServer NodePublishVolume req: %v", req)

	existVol, err := ns.apiClient.Volume.ById(req.GetVolumeId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if existVol == nil {
		msg := fmt.Sprintf("NodePublishVolume: the volume %s not exists", req.GetVolumeId())
		logrus.Warn(msg)
		return nil, status.Error(codes.NotFound, msg)
	}

	if len(existVol.Controllers) != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "There should be only one controller for volume %s", req.GetVolumeId())
	}
	if existVol.State != string(types.VolumeStateAttached) || existVol.DisableFrontend ||
		existVol.Frontend != string(types.VolumeFrontendBlockDev) || existVol.Controllers[0].Endpoint == "" {
		return nil, status.Errorf(codes.InvalidArgument, "There is no block device frontend for volume %s", req.GetVolumeId())
	}

	readOnly := req.GetReadonly()
	if readOnly {
		return nil, status.Error(codes.FailedPrecondition, "Not support readOnly")
	}

	targetPath := req.GetTargetPath()
	devicePath := existVol.Controllers[0].Endpoint
	diskMounter := &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: mount.NewOsExec()}

	vc := req.GetVolumeCapability()
	if vc == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not provided")
	}

	if blkCapability := vc.GetBlock(); blkCapability != nil {
		return ns.nodePublishBlockVolume(req.GetVolumeId(), devicePath, targetPath, diskMounter)
	} else if mntCapability := vc.GetMount(); mntCapability != nil {
		return ns.nodePublishMountVolume(req.GetVolumeId(), devicePath, targetPath,
			vc.GetMount().GetFsType(), vc.GetMount().GetMountFlags(), diskMounter)
	}

	return nil, status.Error(codes.InvalidArgument, "Invalid volume capability, neither Mount nor Block")
}

func (ns *NodeServer) nodePublishMountVolume(volumeName, devicePath, targetPath, fsType string, mountFlags []string, mounter *mount.SafeFormatAndMount) (*csi.NodePublishVolumeResponse, error) {
	// It's used to check if a directory is a mount point and it will create the directory if not exist. Hence this target path cannot be used for block volume.
	notMnt, err := isLikelyNotMountPointAttach(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !notMnt {
		logrus.Debugf("NodePublishVolume: the volume %s has been mounted", volumeName)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	if err := mounter.FormatAndMount(devicePath, targetPath, fsType, mountFlags); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	logrus.Debugf("NodePublishVolume: done MountVolume %s", volumeName)

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *NodeServer) nodePublishBlockVolume(volumeName, devicePath, targetPath string, mounter *mount.SafeFormatAndMount) (*csi.NodePublishVolumeResponse, error) {
	targetDir := filepath.Dir(targetPath)
	exists, err := mounter.ExistsPath(targetDir)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !exists {
		if err := mounter.MakeDir(targetDir); err != nil {
			return nil, status.Errorf(codes.Internal, "Could not create dir %q: %v", targetDir, err)
		}
	}
	if err = mounter.MakeFile(targetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "Error in making file %v", err)
	}

	if err := mounter.Mount(devicePath, targetPath, "", []string{"bind"}); err != nil {
		if removeErr := os.Remove(targetPath); removeErr != nil {
			return nil, status.Errorf(codes.Internal, "Could not remove mount target %q: %v", targetPath, err)
		}
		return nil, status.Errorf(codes.Internal, "Could not mount %q at %q: %v", devicePath, targetPath, err)
	}
	logrus.Debugf("NodePublishVolume: done BlockVolume %s", volumeName)

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

	mounter := mount.New("")
	for {
		if err := mounter.Unmount(targetPath); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		notMnt, err := mounter.IsLikelyNotMountPoint(targetPath)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if notMnt {
			break
		}
		logrus.Debugf("There are multiple mount layers on mount point %v, will unmount all mount layers for this mount point", targetPath)
	}

	if err := mount.CleanupMountPoint(targetPath, mounter, false); err != nil {
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

func (ns *NodeServer) NodeGetVolumeStats(ctx context.Context, in *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "")
}

// NodeExpandVolume is designed to expand the file system for ONLINE expansion.
// However, once the block device expansion complete, Longhorn will try to expand the file system even if there is no NodeExpandVolume invocation.
// Longhorn cannot guarantee if the file system expansion succeeds since some volumes may not contain file system or they are not block devices.
// This means the function NodeExpandVolume in Longhorn is actually used to verify if the ONLINE expansion complete.
func (ns *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	logrus.Infof("NodeServer NodeExpandVolume req: %v", req)

	requestedSize := req.CapacityRange.GetRequiredBytes()
	mountPath := req.VolumePath
	mounter := &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: mount.NewOsExec()}
	exists, err := mounter.ExistsPath(mountPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	if !exists {
		return nil, status.Errorf(codes.Internal, "Invalid volume path %v for volume %v", mountPath, req.GetVolumeId())
	}
	existVol, err := ns.apiClient.Volume.ById(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	if existVol == nil {
		msg := fmt.Sprintf("NodeExpandVolume: the volume %s not exists", req.GetVolumeId())
		logrus.Warn(msg)
		return nil, status.Errorf(codes.NotFound, msg)
	}
	if len(existVol.Controllers) != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "There should be only one controller for volume %s", req.GetVolumeId())
	}
	// If the volume is not attached/in maintenance mode/using iSCSI frontend, this function shouldn't been invoked.
	if existVol.State != string(types.VolumeStateAttached) {
		return nil, status.Errorf(codes.FailedPrecondition, "Invalid volume state %v for expansion", existVol.State)
	}
	if existVol.DisableFrontend || existVol.Frontend == string(types.VolumeFrontendISCSI) {
		return nil, status.Errorf(codes.FailedPrecondition, "Don't need to call NodeExpandVolume to expand the file system for volume %v", existVol.Name)
	}
	volumeSize, err := strconv.ParseInt(existVol.Size, 10, 64)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	currentSize, err := strconv.ParseInt(existVol.Controllers[0].Size, 10, 64)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	if requestedSize != volumeSize {
		return nil, status.Errorf(codes.OutOfRange, "The size %v of volume %v is not equal to the requested size %v.", volumeSize, req.GetVolumeId(), requestedSize)
	}
	if requestedSize != currentSize {
		return nil, status.Errorf(codes.Aborted, "Waiting for the block device expansion of volume %s complete", req.GetVolumeId())
	}

	devicePath := existVol.Controllers[0].Endpoint
	fsType, err := detectFileSystem(devicePath)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "Failed to detect the file system for volume %v: %v", existVol.Name, err)
	}
	if !util.IsSupportedFileSystem(fsType) {
		return nil, status.Errorf(codes.FailedPrecondition, "Unsupported file system %v for volume %v", fsType, existVol.Name)
	}

	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: requestedSize,
	}, nil
}

func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: ns.nodeID,
	}, nil
}

func (ns *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: ns.caps,
	}, nil
}

func getNodeServiceCapabilities(cs []csi.NodeServiceCapability_RPC_Type) []*csi.NodeServiceCapability {
	var nscs []*csi.NodeServiceCapability

	for _, cap := range cs {
		logrus.Infof("Enabling node service capability: %v", cap.String())
		nscs = append(nscs, &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: cap,
				},
			},
		})
	}

	return nscs
}
