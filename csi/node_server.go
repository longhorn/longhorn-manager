package csi

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/client-go/rest"
	"k8s.io/mount-utils"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	utilexec "k8s.io/utils/exec"

	"github.com/longhorn/longhorn-manager/csi/crypto"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

	lhns "github.com/longhorn/go-common-libs/ns"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

const (
	defaultFsType = "ext4"
)

type fsParameters struct {
	formatParameters string
}

var supportedFs = map[string]fsParameters{
	"ext4": {
		formatParameters: "-b4096",
	},
	"xfs": {
		formatParameters: "-ssize=4096 -bsize=4096",
	},
}

type NodeServer struct {
	csi.UnimplementedNodeServer
	apiClient   *longhornclient.RancherClient
	nodeID      string
	caps        []*csi.NodeServiceCapability
	log         *logrus.Entry
	lhNamespace string
	kubeClient  *clientset.Clientset
	lhClient    *lhclientset.Clientset
}

func NewNodeServer(apiClient *longhornclient.RancherClient, nodeID string) (*NodeServer, error) {
	lhNamespace := os.Getenv(types.EnvPodNamespace)
	if lhNamespace == "" {
		return nil, fmt.Errorf("failed to detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get k8s client")
	}

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get longhorn clientset")
	}

	return &NodeServer{
		apiClient: apiClient,
		nodeID:    nodeID,
		caps: getNodeServiceCapabilities(
			[]csi.NodeServiceCapability_RPC_Type{
				csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
				csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
				csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
			}),
		log:         logrus.StandardLogger().WithField("component", "csi-node-server"),
		lhNamespace: lhNamespace,
		kubeClient:  kubeClient,
		lhClient:    lhClient,
	}, nil
}

// NodePublishVolume will mount the volume /dev/longhorn/<volume_name> to target_path
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	log := ns.log.WithFields(logrus.Fields{"function": "NodePublishVolume"})

	log.Infof("NodePublishVolume is called with req %+v", req)

	targetPath := req.GetTargetPath()
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path missing in request")
	}

	stagingTargetPath := req.GetStagingTargetPath()
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path missing in request")
	}

	volumeCapability := req.GetVolumeCapability()
	if volumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability missing in request")
	}

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id missing in request")
	}

	volume, err := ns.apiClient.Volume.ById(volumeID)
	if err != nil {
		return nil, status.Error(codes.Internal, errors.Wrapf(err, "failed to get volume %s for publishing volume", volumeID).Error())
	}
	if volume == nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", volumeID)
	}

	mounter, err := ns.getMounter(volume, volumeCapability, req.VolumeContext)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// For mounting volumes, we don't want multiple controllers for a volume, since the filesystem could get messed up
	if len(volume.Controllers) == 0 || (len(volume.Controllers) > 1 && volumeCapability.GetBlock() == nil) {
		return nil, status.Errorf(codes.InvalidArgument, "volume %s has invalid controller count %v", volumeID, len(volume.Controllers))
	}

	if volume.DisableFrontend {
		return nil, status.Errorf(codes.InvalidArgument, "volume %s frontend is disabled", volumeID)
	}

	if volume.Frontend != string(longhorn.VolumeFrontendBlockDev) &&
		volume.Frontend != string(longhorn.VolumeFrontendUblk) {
		return nil, status.Errorf(codes.InvalidArgument, "volume %s has invalid frontend type %v", volumeID, volume.Frontend)
	}

	// Check volume attachment status
	if types.IsDataEngineV1(longhorn.DataEngineType(volume.DataEngine)) {
		if volume.State != string(longhorn.VolumeStateAttached) || volume.Controllers[0].Endpoint == "" {
			log.WithField("state", volume.State).Infof("Volume %v hasn't been attached yet, unmounting potential mount point %v", volumeID, targetPath)
			if err := unmount(targetPath, mounter); err != nil {
				log.WithError(err).Warnf("Failed to unmount targetPath %v", targetPath)
			}
			return nil, status.Errorf(codes.InvalidArgument, "volume %s hasn't been attached yet", volumeID)
		}
	}

	if !volume.Ready {
		return nil, status.Errorf(codes.Aborted, "volume %s is not ready for workloads", volumeID)
	}

	podsStatus := ns.collectWorkloadPodsStatus(volume, log)
	if len(podsStatus[corev1.PodPending]) == 0 && len(podsStatus[corev1.PodRunning]) != len(volume.KubernetesStatus.WorkloadsStatus) {
		return nil, status.Errorf(codes.Aborted, "no %v workload pods for volume %v to be mounted: %+v", corev1.PodPending, volumeID, podsStatus)
	}

	// It may be necessary to restage the volume before we can publish it. For example, sometimes kubelet calls
	// NodePublishVolume without calling NodeStageVolume. According to the CSI spec, we should be able to respond with
	// FailedPrecondition and expect kubelet to call NodeStageVolume again, but as of Kubernetes v1.27 it does not.
	isBlock := volumeCapability.GetBlock() != nil

	storageNetworkSetting, err := ns.apiClient.Setting.ById(string(types.SettingNameStorageNetwork))
	if err != nil {
		log.WithError(err).Warnf("Skipping restaging condition check for storage network setting")
	}

	restageRequired, err := restageRequired(volume, volumeID, stagingTargetPath, mounter, isBlock, storageNetworkSetting.Value != "")
	if restageRequired {
		msg := fmt.Sprintf("Staging target path %v is no longer valid for volume %v", stagingTargetPath, volumeID)
		log.WithError(err).Warn(msg)

		log.Warnf("Calling NodeUnstageVolume for volume %v", volumeID)
		_, _ = ns.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
			VolumeId:          volumeID,
			StagingTargetPath: stagingTargetPath,
		})

		log.Warnf("Calling NodeStageVolume for volume %v", volumeID)
		_, err := ns.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
			VolumeId:          volumeID,
			PublishContext:    req.PublishContext,
			StagingTargetPath: stagingTargetPath,
			VolumeCapability:  volumeCapability,
			Secrets:           req.Secrets,
			VolumeContext:     req.VolumeContext,
		})
		if err != nil {
			log.WithError(err).Errorf("Failed NodeStageVolume staging path is still in a bad state for volume %v", volumeID)
			return nil, status.Error(codes.FailedPrecondition, msg)
		}
	}

	if isBlock {
		devicePath := getStageBlockVolumePath(stagingTargetPath, volumeID)
		_, err := os.Stat(devicePath)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, status.Errorf(codes.Internal, "failed to stat device %s: %v", devicePath, err)
			}
		}

		if err := ns.nodePublishBlockVolume(volumeID, devicePath, targetPath, mounter); err != nil {
			log.WithError(err).Errorf("Failed to publish BlockVolume %s", volumeID)
			return nil, err
		}

		log.Infof("Published BlockVolume %s", volumeID)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	isMnt, err := ensureMountPoint(targetPath, mounter)
	if err != nil {
		msg := fmt.Sprintf("Failed to prepare mount point for volume %v error %v", volumeID, err)
		log.WithError(err).Error(msg)
		return nil, status.Error(codes.Internal, msg)
	}
	if isMnt {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	mountOptions := []string{"bind"}
	if req.GetReadonly() {
		mountOptions = append(mountOptions, "ro")
	}
	mountOptions = append(mountOptions, volumeCapability.GetMount().GetMountFlags()...)

	if err := mounter.Mount(stagingTargetPath, targetPath, "", mountOptions); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to bind mount volume %v", volumeID)
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *NodeServer) collectWorkloadPodsStatus(volume *longhornclient.Volume, log *logrus.Entry) map[corev1.PodPhase][]string {
	podsStatus := map[corev1.PodPhase][]string{}

	for _, workload := range volume.KubernetesStatus.WorkloadsStatus {
		phase := corev1.PodPhase(workload.PodStatus)
		podsStatus[phase] = append(podsStatus[phase], workload.PodName)
	}

	return podsStatus
}

func (ns *NodeServer) nodeStageSharedVolume(volumeID, shareEndpoint, targetPath string, mounter mount.Interface, customMountOptions []string) error {
	log := ns.log.WithFields(logrus.Fields{"function": "nodeStageSharedVolume"})

	isMnt, err := ensureMountPoint(targetPath, mounter)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to prepare mount point for shared volume %v: %v", volumeID, err)
	}
	if isMnt {
		return nil
	}

	uri, err := url.Parse(shareEndpoint)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid share endpoint %v for volume %v: %v", shareEndpoint, volumeID, err)
	}

	// share endpoint is of the form nfs://server/export
	fsType := uri.Scheme
	if fsType != "nfs" {
		return status.Errorf(codes.InvalidArgument, "unsupported share fsType %v for volume %v share endpoint %v", fsType, volumeID, shareEndpoint)
	}

	server := uri.Host
	exportPath := uri.Path
	export := fmt.Sprintf("%s:%s", server, exportPath)

	defaultMountOptions := []string{
		"vers=4.1",
		"noresvport",
		//"sync",    // sync mode is prohibitively expensive on the client, so we allow for host defaults
		//"intr",
		//"hard",
		//"softerr", // for this release we use soft mode, so we can always cleanup mount points
		"timeo=600", // This is tenths of a second, so a 60 second timeout, each retrans the timeout will be linearly increased, 60s, 120s, 240s, 480s, 600s(max)
		"retrans=5", // We try the io operation for a total of 5 times, before failing
	}

	mountOptions := append(defaultMountOptions, []string{"softerr"}...)
	if len(customMountOptions) != 0 {
		mountOptions = customMountOptions
	}

	log.Infof("Mounting shared volume %v on node %v via share endpoint %v with mount options %v", volumeID, ns.nodeID, shareEndpoint, mountOptions)
	if err := mounter.Mount(export, targetPath, fsType, mountOptions); err != nil {
		if len(customMountOptions) == 0 && strings.Contains(err.Error(), "an incorrect mount option was specified") {
			log.WithError(err).Warnf("Failed to mount volume %v with default mount options, retrying with soft mount", volumeID)
			mountOptions = append(defaultMountOptions, []string{"soft"}...)
			err = mounter.Mount(export, targetPath, fsType, mountOptions)
			if err == nil {
				return nil
			}
		}
		// Log with mounting node and kernel version for possible troubleshooting.  Don't step on actual mount error.
		kernelRelease, err1 := lhns.GetKernelRelease()
		if err1 != nil {
			kernelRelease = err1.Error()
		}
		osDistro, err2 := lhns.GetOSDistro()
		if err2 != nil {
			osDistro = err2.Error()
		}
		log.WithError(err).Warnf("Failed to mount volume %v on node %s with kernel release %s, os distro %s", volumeID, ns.nodeID, kernelRelease, osDistro)
		return status.Error(codes.Internal, err.Error())
	}

	return nil
}

func (ns *NodeServer) nodeStageMountVolume(volumeID, devicePath, stagingTargetPath, fsType string, mountFlags []string, mounter *mount.SafeFormatAndMount) error {
	log := ns.log.WithFields(logrus.Fields{"function": "NodePublishVolume"})

	isMnt, err := ensureMountPoint(stagingTargetPath, mounter)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to prepare mount point %v for volume %v: %v", stagingTargetPath, volumeID, err)
	}
	if isMnt {
		return nil
	}

	if _, err := os.Stat(devicePath); err != nil {
		return status.Error(codes.Internal, errors.Wrapf(err, "failed to check if device %v exists", devicePath).Error())
	}

	log.Infof("Formatting device %v with fsType %v and mounting at %v with mount flags %v", devicePath, fsType, stagingTargetPath, mountFlags)
	if err := mounter.FormatAndMount(devicePath, stagingTargetPath, fsType, mountFlags); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	return nil
}

// nodeStageBlockVolume utilizes the stagingTargetPath to create a volumeID file to bind mount the devicePath
// this is valid since the csi plugin is in control of the staging path
func (ns *NodeServer) nodeStageBlockVolume(volumeID, devicePath, stagingTargetPath string, mounter mount.Interface) error {
	path := getStageBlockVolumePath(stagingTargetPath, volumeID)
	return ns.nodePublishBlockVolume(volumeID, devicePath, path, mounter)
}

func (ns *NodeServer) nodePublishBlockVolume(volumeID, devicePath, targetPath string, mounter mount.Interface) error {
	log := ns.log.WithFields(logrus.Fields{"function": "nodePublishBlockVolume"})

	// we ensure the parent directory exists and is valid
	if _, err := ensureDirectory(filepath.Dir(targetPath)); err != nil {
		return status.Errorf(codes.Internal, "failed to prepare mount point for block device %v: %v", devicePath, err)
	}

	// create file where we can bind mount the device to
	if err := makeFile(targetPath); err != nil {
		return status.Errorf(codes.Internal, "failed to create file %v: %v", targetPath, err)
	}

	log.Infof("Bind mounting device %v at %v", devicePath, targetPath)
	if err := mounter.Mount(devicePath, targetPath, "", []string{"bind"}); err != nil {
		if removeErr := os.Remove(targetPath); removeErr != nil {
			return status.Errorf(codes.Internal, "failed to remove mount target %q: %v", targetPath, removeErr)
		}
		return status.Errorf(codes.Internal, "failed to bind mount %q at %q: %v", devicePath, targetPath, err)
	}
	return nil
}

func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	log := ns.log.WithFields(logrus.Fields{"function": "NodeUnpublishVolume"})

	log.Infof("NodeUnpublishVolume is called with req %+v", req)

	targetPath := req.GetTargetPath()
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path missing in request")
	}

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id missing in request")
	}

	if err := unmountAndCleanupMountPoint(targetPath, mount.New("")); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to cleanup volume %s mount point %v: %v", volumeID, targetPath, err)
	}

	log.Infof("Volume %s unmounted from path %s", volumeID, targetPath)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *NodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	log := ns.log.WithFields(logrus.Fields{"function": "NodeStageVolume"})

	log.Infof("NodeStageVolume is called with req %+v", req)

	stagingTargetPath := req.GetStagingTargetPath()
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path missing in request")
	}

	volumeCapability := req.GetVolumeCapability()
	if volumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability missing in request")
	}

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id missing in request")
	}

	volume, err := ns.apiClient.Volume.ById(volumeID)
	if err != nil {
		return nil, status.Error(codes.Internal, errors.Wrapf(err, "failed to get volume %s for staging volume", volumeID).Error())
	}
	if volume == nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", volumeID)
	}

	mounter, err := ns.getMounter(volume, volumeCapability, req.VolumeContext)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// For mounting volumes, we don't want multiple controllers for a volume, since the filesystem could get messed up
	if len(volume.Controllers) == 0 || (len(volume.Controllers) > 1 && volumeCapability.GetBlock() == nil) {
		return nil, status.Errorf(codes.InvalidArgument, "volume %s has invalid controller count %v", volumeID, len(volume.Controllers))
	}

	if volume.DisableFrontend {
		return nil, status.Errorf(codes.InvalidArgument, "volume %s frontend is disabled", volumeID)
	}

	if volume.Frontend != string(longhorn.VolumeFrontendBlockDev) &&
		volume.Frontend != string(longhorn.VolumeFrontendUblk) {
		return nil, status.Errorf(codes.InvalidArgument, "volume %s has invalid frontend type %v", volumeID, volume.Frontend)
	}

	// Check volume attachment status
	if volume.State != string(longhorn.VolumeStateAttached) || volume.Controllers[0].Endpoint == "" {
		log.Infof("Volume %v hasn't been attached yet, unmounting potential mount point %v", volumeID, stagingTargetPath)
		if err := unmount(stagingTargetPath, mounter); err != nil {
			log.WithError(err).Warnf("Failed to unmount stagingTargetPath %v", stagingTargetPath)
		}
		return nil, status.Errorf(codes.InvalidArgument, "volume %s hasn't been attached yet", volumeID)
	}

	if !volume.Ready {
		return nil, status.Errorf(codes.Aborted, "volume %s is not ready for workloads", volumeID)
	}

	if requiresSharedAccess(volume, volumeCapability) && !volume.Migratable {
		if volume.AccessMode != string(longhorn.AccessModeReadWriteMany) {
			return nil, status.Errorf(codes.FailedPrecondition, "volume %s requires shared access but is not marked for shared use", volumeID)
		}

		if !isVolumeShareAvailable(volume) {
			return nil, status.Errorf(codes.Aborted, "volume %s share not yet available", volumeID)
		}

		// undocumented field to allow testing different nfs mount options
		// this can be used to enable the default host (ubuntu) client async mode
		var mountOptions []string
		if len(req.VolumeContext["nfsOptions"]) > 0 {
			mountOptions = strings.Split(req.VolumeContext["nfsOptions"], ",")
		}

		if err := ns.nodeStageSharedVolume(volumeID, volume.ShareEndpoint, stagingTargetPath, mounter, mountOptions); err != nil {
			return nil, err
		}

		log.Infof("Mounted shared volume %v on node %v via share endpoint %v", volumeID, ns.nodeID, volume.ShareEndpoint)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	devicePath := volume.Controllers[0].Endpoint
	diskFormat, err := getDiskFormat(devicePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to evaluate device filesystem %v format: %v", devicePath, err)
	}

	dataEngine := volume.DataEngine
	log.Infof("Volume %v (%v) device %v contains filesystem of format %v", volumeID, dataEngine, devicePath, diskFormat)

	if volume.Encrypted {
		secrets := req.GetSecrets()
		keyProvider := secrets[types.CryptoKeyProvider]
		passphrase := secrets[types.CryptoKeyValue]
		if keyProvider != "" && keyProvider != "secret" {
			return nil, status.Errorf(codes.InvalidArgument, "unsupported key provider %v for encrypted volume %v", keyProvider, volumeID)
		}

		if len(passphrase) == 0 {
			return nil, status.Errorf(codes.InvalidArgument, "missing passphrase for encrypted volume %v", volumeID)
		}

		if diskFormat != "" && diskFormat != "crypto_LUKS" {
			return nil, status.Errorf(codes.InvalidArgument, "unsupported disk encryption format %v", diskFormat)
		}

		cryptoParams := crypto.NewEncryptParams(keyProvider, secrets[types.CryptoKeyCipher], secrets[types.CryptoKeyHash], secrets[types.CryptoKeySize], secrets[types.CryptoPBKDF])

		// initial setup of longhorn device for crypto
		if diskFormat == "" {
			if err := crypto.EncryptVolume(devicePath, passphrase, cryptoParams); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
		}

		cryptoDevice := crypto.VolumeMapper(volumeID, dataEngine)
		log.Infof("Volume %s requires crypto device %s", volumeID, cryptoDevice)

		// check if the crypto device is open at the null path.
		// this will happen if the crypto device is not closed properly and a new attaching request is made on the same node.
		// reference issue: https://github.com/longhorn/longhorn/issues/9385
		if mappedToNullPath, err := crypto.IsDeviceMappedToNullPath(cryptoDevice); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to check if the crypto device %s for volume %s is mapped to the null path: %v", cryptoDevice, volumeID, err.Error())
		} else if mappedToNullPath {
			log.Warnf("Closing active crypto device %s for volume %s since the volume is not closed properly before", cryptoDevice, volumeID)
			if err := crypto.CloseVolume(volumeID, dataEngine); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to close active crypto device %s for volume %s: %v ", cryptoDevice, volumeID, err.Error())
			}
		}

		if err := crypto.OpenVolume(volumeID, dataEngine, devicePath, passphrase); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		// update the device path to point to the new crypto device
		devicePath = cryptoDevice
	}

	if volumeCapability.GetBlock() != nil {
		if err := ns.nodeStageBlockVolume(volumeID, devicePath, stagingTargetPath, mounter); err != nil {
			return nil, err
		}

		logrus.Infof("Volume %v device %v available for usage as block device", volumeID, devicePath)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	options := volumeCapability.GetMount().GetMountFlags()
	fsType := volumeCapability.GetMount().GetFsType()
	if fsType == "" {
		fsType = defaultFsType
	}
	if fsType == "xfs" {
		// By default, xfs does not allow mounting of two volumes with the same filesystem uuid.
		// Force ignore this uuid to be able to mount volume + its clone / restored snapshot on the same node.
		options = append(options, "nouuid")
	}

	formatMounter, ok := mounter.(*mount.SafeFormatAndMount)
	if !ok {
		return nil, status.Errorf(codes.Internal, "volume %v cannot get format mounter that support filesystem %v creation", volumeID, fsType)
	}

	if err := ns.nodeStageMountVolume(volumeID, devicePath, stagingTargetPath, fsType, options, formatMounter); err != nil {
		return nil, err
	}

	// check if we need to resize the fs
	// this is important since cloned volumes of bigger size don't trigger NodeExpandVolume
	// therefore NodeExpandVolume is kind of redundant since we have to do this anyway
	// some refs below for more details
	// https://github.com/kubernetes/kubernetes/issues/94929
	// https://github.com/kubernetes-sigs/aws-ebs-csi-driver/pull/753
	resizer := mount.NewResizeFs(utilexec.New())
	if needsResize, err := resizer.NeedResize(devicePath, stagingTargetPath); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	} else if needsResize {
		if resized, err := resizer.Resize(devicePath, stagingTargetPath); err != nil {
			log.WithError(err).Errorf("Mounted volume %v on node %v failed required filesystem resize", volumeID, ns.nodeID)
			return nil, status.Error(codes.Internal, err.Error())
		} else if resized {
			log.Infof("Mounted volume %v on node %v successfully resized filesystem after mount", volumeID, ns.nodeID)
		} else {
			log.Infof("Mounted volume %v on node %v already has correct filesystem size", volumeID, ns.nodeID)
		}
	} else {
		log.Infof("Mounted volume %v on node %v does not require filesystem resize", volumeID, ns.nodeID)
	}

	log.Infof("Mounted volume %v on node %v via device %v", volumeID, ns.nodeID, devicePath)
	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *NodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	log := ns.log.WithFields(logrus.Fields{"function": "NodeUnstageVolume"})

	log.Infof("NodeUnstageVolume is called with req %+v", req)

	stagingTargetPath := req.GetStagingTargetPath()
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path missing in request")
	}

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id missing in request")
	}

	mounter := mount.New("")

	// CO owns the staging_path so we only unmount but not remove the path
	if err := unmount(stagingTargetPath, mounter); err != nil {
		return nil, status.Error(codes.Internal, errors.Wrapf(err, "failed to unmount volume %s mount point %v", volumeID, stagingTargetPath).Error())
	}

	// For block mode we use the staging path as parent, so we have to do additional cleanup of the subfolder/files
	// we should transition the regular fs mounts to also use the same sub folder, this allows us to store additional
	// metadata as well as do more forcefully removals since we no longer share the control of the staging_path with kubernetes
	//
	// The unmount of the parent is a no op for block mode, this is also important for backwards compatibility of the existing block devices.
	deviceFilePath := getStageBlockVolumePath(stagingTargetPath, volumeID)
	if err := unmountAndCleanupMountPoint(deviceFilePath, mounter); err != nil {
		return nil, status.Error(codes.Internal, errors.Wrapf(err, "failed to clean up volume %s device mount point %v", volumeID, deviceFilePath).Error())
	}

	// optionally try to retrieve the volume and check if it's an RWX volume
	// if it is we let the share-manager clean up the crypto device
	volume, _ := ns.apiClient.Volume.ById(volumeID)
	dataEngine := string(longhorn.DataEngineTypeV1)
	if volume != nil {
		dataEngine = volume.DataEngine
	}
	sharedAccess := requiresSharedAccess(volume, nil)
	cleanupCryptoDevice := !sharedAccess || (sharedAccess && volume.Migratable)
	if cleanupCryptoDevice {
		cryptoDevice := crypto.VolumeMapper(volumeID, dataEngine)
		if isOpen, err := crypto.IsDeviceOpen(cryptoDevice); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		} else if isOpen {
			log.Infof("Volume %s closing active crypto device %s", volumeID, cryptoDevice)
			if err := crypto.CloseVolume(volumeID, dataEngine); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
		}
	}

	log.Infof("Volume %s unmounted from node path %s", volumeID, stagingTargetPath)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *NodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	volumePath := req.GetVolumePath()
	if volumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume path missing in request")
	}

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id missing in request")
	}

	existVol, err := ns.apiClient.Volume.ById(volumeID)
	if err != nil {
		return nil, status.Error(codes.Internal, errors.Wrapf(err, "failed to get volume %s for volume statistics", volumeID).Error())
	}
	if existVol == nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", volumeID)
	}

	isBlockVolume, err := isBlockDevice(volumePath)
	if err != nil {
		// ENOENT means the volumePath does not exist
		// See https://man7.org/linux/man-pages/man2/stat.2.html for details.
		if errors.Is(err, unix.ENOENT) {
			return nil, status.Errorf(codes.NotFound, "volume %v is not mounted on path %v", volumeID, volumePath)
		}
		return nil, status.Errorf(codes.Internal, "failed to check volume mode for volume path %v: %v", volumePath, err)
	}

	if isBlockVolume {
		volCapacity, err := strconv.ParseInt(existVol.Size, 10, 64)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to convert volume size %v for volume %v: %v", existVol.Size, volumeID, err)
		}
		return &csi.NodeGetVolumeStatsResponse{
			Usage: []*csi.VolumeUsage{
				&csi.VolumeUsage{
					Total: volCapacity,
					Unit:  csi.VolumeUsage_BYTES,
				},
			},
		}, nil
	}

	stats, err := getFilesystemStatistics(volumePath)
	if err != nil {
		// ENOENT means the volumePath does not exist
		// See http://man7.org/linux/man-pages/man2/statfs.2.html for details.
		if errors.Is(err, unix.ENOENT) {
			return nil, status.Errorf(codes.NotFound, "volume %v is not mounted on path %v", volumeID, volumePath)
		}
		return nil, status.Errorf(codes.Internal, "failed to retrieve capacity statistics for volume path %v for volume %v: %v", volumePath, volumeID, err)
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			&csi.VolumeUsage{
				Available: stats.availableBytes,
				Total:     stats.totalBytes,
				Used:      stats.usedBytes,
				Unit:      csi.VolumeUsage_BYTES,
			},
			&csi.VolumeUsage{
				Available: stats.availableInodes,
				Total:     stats.totalInodes,
				Used:      stats.usedInodes,
				Unit:      csi.VolumeUsage_INODES,
			},
		},
	}, nil
}

// NodeExpandShared Volume is designed to expand the file system in an RWX volume for ONLINE expansion.
// It does so with a gRPC call into the share-manager pod.
func (ns *NodeServer) NodeExpandSharedVolume(volumeName string) error {
	log := ns.log.WithFields(logrus.Fields{"function": "NodeExpandSharedVolume"})

	sm, err := ns.lhClient.LonghornV1beta2().ShareManagers(ns.lhNamespace).Get(context.TODO(), volumeName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "failed to get ShareManager CR")
	}

	podName := types.GetShareManagerPodNameFromShareManagerName(sm.Name)
	pod, err := ns.kubeClient.CoreV1().Pods(ns.lhNamespace).Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "failed to get ShareManager pod")
	}

	client, err := engineapi.NewShareManagerClient(sm, pod)
	if err != nil {
		return errors.Wrapf(err, "failed to launch gRPC client for share manager before resizing volume %v", volumeName)
	}
	defer func(client io.Closer) {
		if closeErr := client.Close(); closeErr != nil {
			ns.log.WithError(closeErr).Warn("Failed to close share manager client")
		}
	}(client)

	// Each node with a workload pod will send an RPC request.  The first will win, and the others are no-ops.
	err = client.FilesystemResize()
	if status.Code(err) == codes.Unimplemented {
		// This is a downrev longhorn-share-manager image.  It will be necessary either to kill the share-manager pod
		// and let it restart with the new image, or scale the workload down and back up to accomplish the same thing.
		// It might be tempting to delete the pod here, but there will be one of these calls for each workload pod,
		// and it would be messy to have multiple kill requests at once.  So the kill will need to be done by the user.
		log.WithError(err).Warn("Share Manager image is down-rev, does not implement RPC FilesystemResize.  Share Manager pod must be restarted with current image.")
	}

	return errors.Wrapf(err, "failed to expand shared volume %v", volumeName)
}

// NodeExpandVolume is designed to expand the file system for ONLINE expansion,
func (ns *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	log := ns.log.WithFields(logrus.Fields{"function": "NodeExpandVolume"})

	log.Infof("NodeExpandVolume is called with req %+v", req)

	if req.CapacityRange == nil {
		return nil, status.Error(codes.InvalidArgument, "capacity range missing in request")
	}
	requestedSize := req.CapacityRange.GetRequiredBytes()

	volumeCapability := req.GetVolumeCapability()
	if volumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability missing in request")
	}

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id missing in request")
	}

	if req.VolumeCapability.GetBlock() != nil {
		log.Infof("Volume %v on node %v does not require filesystem resize/node expansion since it is access mode Block", volumeID, ns.nodeID)
		return &csi.NodeExpandVolumeResponse{}, nil
	}

	volume, err := ns.apiClient.Volume.ById(volumeID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	if volume == nil {
		return nil, status.Errorf(codes.NotFound, "volume %s missing", volumeID)
	}
	if len(volume.Controllers) != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid controller count %v for volume %v node expansion", len(volume.Controllers), volumeID)
	}
	if volume.State != string(longhorn.VolumeStateAttached) {
		return nil, status.Errorf(codes.FailedPrecondition, "invalid state %v for volume %v node expansion", volume.State, volumeID)
	}

	if requiresSharedAccess(volume, volumeCapability) && !volume.Migratable {
		if volume.AccessMode != string(longhorn.AccessModeReadWriteMany) {
			return nil, status.Errorf(codes.FailedPrecondition, "volume %s requires shared access but is not marked for shared use", volumeID)
		}

		if err := ns.NodeExpandSharedVolume(volumeID); err != nil {
			log.WithError(err).Errorf("failed to expand shared volume %v", volumeID)
			return nil, err
		}

		return &csi.NodeExpandVolumeResponse{CapacityBytes: requestedSize}, nil
	}

	devicePath := volume.Controllers[0].Endpoint

	mounter := &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: utilexec.New()}
	diskFormat, err := mounter.GetDiskFormat(devicePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to evaluate device filesystem format for volume %v node expansion", volumeID)
	}
	if diskFormat == "" {
		return nil, fmt.Errorf("unknown filesystem type for volume %v node expansion", volumeID)
	}

	dataEngine := volume.DataEngine
	devicePath, err = func() (string, error) {
		if !volume.Encrypted {
			return devicePath, nil
		}
		if diskFormat != "crypto_LUKS" {
			return "", status.Errorf(codes.InvalidArgument, "unsupported disk encryption format %v", diskFormat)
		}
		devicePath = crypto.VolumeMapper(volumeID, dataEngine)

		// Need to enable feature gate in v1.25:
		// https://github.com/kubernetes/enhancements/issues/3107
		// https://kubernetes.io/blog/2022/09/21/kubernetes-1-25-use-secrets-while-expanding-csi-volumes-on-node-alpha/
		secrets := req.GetSecrets()
		if len(secrets) == 0 {
			log.Infof("Skip encrypto device resizing for volume %v node expansion since the secret empty, maybe the related feature gate is not enabled", volumeID)
			return devicePath, nil
		}
		keyProvider := secrets[types.CryptoKeyProvider]
		passphrase := secrets[types.CryptoKeyValue]
		if keyProvider != "" && keyProvider != "secret" {
			return "", status.Errorf(codes.InvalidArgument, "unsupported key provider %v for encrypted volume %v", keyProvider, volumeID)
		}
		if len(passphrase) == 0 {
			return "", status.Errorf(codes.InvalidArgument, "missing passphrase for encrypted volume %v", volumeID)
		}

		// blindly resize the encrypto device
		if err := crypto.ResizeEncryptoDevice(volumeID, dataEngine, passphrase); err != nil {
			return "", status.Errorf(codes.InvalidArgument, "failed to resize crypto device %v for volume %v node expansion: %v", devicePath, volumeID, err)
		}

		return devicePath, nil
	}()
	if err != nil {
		return nil, err
	}

	resizer := mount.NewResizeFs(utilexec.New())
	if needsResize, err := resizer.NeedResize(devicePath, req.StagingTargetPath); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	} else if needsResize {
		if resized, err := resizer.Resize(devicePath, req.StagingTargetPath); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		} else if resized {
			log.Infof("Volume %v on node %v successfully resized filesystem after mount", volumeID, ns.nodeID)
		} else {
			log.Infof("Volume %v on node %v already has correct filesystem size", volumeID, ns.nodeID)
		}
	} else {
		log.Infof("Volume %v on node %v does not require filesystem resize", volumeID, ns.nodeID)
	}

	return &csi.NodeExpandVolumeResponse{CapacityBytes: requestedSize}, nil
}

func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId:            ns.nodeID,
		MaxVolumesPerNode: 0, // technically the scsi kernel limit is the max limit of volumes
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{
				nodeTopologyKey: ns.nodeID,
			},
		},
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

func (ns *NodeServer) requireHostNamespaceMounter(volume *longhornclient.Volume, volumeCapability *csi.VolumeCapability) (bool, error) {
	if !requiresSharedAccess(volume, volumeCapability) {
		return false, nil
	}
	if volume.Migratable {
		return false, nil
	}

	storageNetworkSetting, err := ns.apiClient.Setting.ById(string(types.SettingNameStorageNetwork))
	if err != nil {
		return false, err
	}
	storageNetworkForRWXVolumeEnabledSetting, err := ns.apiClient.Setting.ById(string(types.SettingNameStorageNetworkForRWXVolumeEnabled))
	if err != nil {
		return false, err
	}
	storageNetworkForRWXVolumeEnabled, err := strconv.ParseBool(storageNetworkForRWXVolumeEnabledSetting.Value)
	if err != nil {
		return false, err
	}

	if storageNetworkSetting.Value != "" && storageNetworkForRWXVolumeEnabled {
		return false, nil
	}

	return true, nil
}

func (ns *NodeServer) getMounter(volume *longhornclient.Volume, volumeCapability *csi.VolumeCapability, volumeContext map[string]string) (mount.Interface, error) {
	if volumeCapability.GetBlock() != nil {
		return mount.New(""), nil
	}

	// HACK: to nsenter host namespaces for the nfs mounts to stay available after csi plugin dies.
	// Only do this for regular RWX volume with false storageNetworkForRWXVolumeEnabledSetting
	requireHostNamespaceMounter, err := ns.requireHostNamespaceMounter(volume, volumeCapability)
	if err != nil {
		return nil, err
	}
	if requireHostNamespaceMounter {
		return mount.New("/usr/local/sbin/nsmounter"), nil
	}

	// mounter that can format and use hard coded filesystem params
	if volumeCapability.GetMount() != nil {
		fsType := volumeCapability.GetMount().GetFsType()
		if fsType == "" {
			fsType = defaultFsType
		}

		// To allow users to override the default block size,
		// put the default block size in front of other user-defined parameters.
		params := ""
		if fsParams, ok := supportedFs[fsType]; ok {
			params += fsParams.formatParameters
		}

		//If the user specifies parameters in the storage class, the parameters are appended after the default value.
		if mkfsParams, ok := volumeContext["mkfsParams"]; ok && mkfsParams != "" {
			params += " " + mkfsParams
		}

		mounter := &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: utilexec.New()}
		if _, ok := supportedFs[fsType]; ok {
			logrus.Infof("Volume %v using user and longhorn provided %v fs creation params: %s", volume.Name, fsType, params)
			cmdParamMapping := map[string]string{"mkfs." + fsType: params}
			mounter = &mount.SafeFormatAndMount{
				Interface: mount.New(""),
				Exec:      NewForcedParamsExec(cmdParamMapping),
			}
		} else {
			logrus.Warnf("Volume %v with unsupported filesystem %v, use default fs creation params", volume.Name, fsType)
		}

		return mounter, nil
	}

	return nil, fmt.Errorf("failed to get mounter for volume %v unsupported volume capability %v", volume.Name, volumeCapability.GetAccessType())
}

// restageRequired determines whether it is necessary to manually call NodeUnstageVolume and NodeStageVolume again
// before publishing. If it returns true, it may also return an error containing the underlying reason restaging is
// required. restageRequired has side effects for v1 mount volumes due to its use of ensureMountPoint. These side
// effects are neither harmful nor helpful, as ensureMountPoint will be called again in the restage flow.
func restageRequired(volume *longhornclient.Volume,
	volumeID, stagingTargetPath string,
	mounter mount.Interface,
	isBlock, isStorageNetworkConfigured bool) (bool, error) {

	if volume.DataEngine == string(longhorn.DataEngineTypeV2) {
		return true, fmt.Errorf("always unstage v2 volume %v", volumeID)
	}

	if isStorageNetworkConfigured && volume.AccessMode == string(longhorn.AccessModeReadWriteMany) {
		return true, fmt.Errorf("always unstage RWX volume %v when storage network is configured", volumeID)
	}

	if isBlock {
		stageBlockVolumePath := getStageBlockVolumePath(stagingTargetPath, volumeID)
		isStaged, err := mounter.IsMountPoint(stageBlockVolumePath)
		// Before v1.6.0, NodeStageVolume was a no-op for block volumes. Instead, we directly bind mounted the
		// device from /dev/longhorn to targetPath. It is possible that we are responding to a NodePublishVolume request
		// for a volume that was "staged" using the old flow. If we are, nothing exists at stageBlockVolumePath, and we
		// will return restageRequired == true. This is fine, because:
		// - NodeUnstageVolume will do nothing for a volume staged with this flow.
		// - NodeStageVolume will add an additional bind mount at stageBlockVolumePath. This additional bind mount will
		//   not affect the original direct bind mount.
		return !isStaged, err
	}
	isStaged, err := ensureMountPoint(stagingTargetPath, mounter)
	return !isStaged, err
}
