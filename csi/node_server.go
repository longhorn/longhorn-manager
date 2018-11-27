package csi

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/Sirupsen/logrus"
	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/pkg/util/mount"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"

	"github.com/rancher/longhorn-manager/datastore"
	lhclientset "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned"
	"github.com/rancher/longhorn-manager/types"
)

type NodeServer struct {
	*csicommon.DefaultNodeServer
	lhClient  lhclientset.Interface
	namespace string
}

func NewNodeServer(d *csicommon.CSIDriver) *NodeServer {
	config, err := rest.InClusterConfig()
	if err != nil {
		logrus.Warningf("NewNodeServer failed to create in-cluster config: %v", err)
	}

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		logrus.Warningf("NewNodeServer failed to create Longhorn client: %v", err)
	}

	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		logrus.Warnf("Cannot detect pod namespace, environment variable %v is missing, "+
			"using default namespace", types.EnvPodNamespace)
		namespace = corev1.NamespaceDefault
	}

	return &NodeServer{
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d),
		lhClient:          lhClient,
		namespace:         namespace,
	}
}

func getVolumeSelector(volumeName string) (labels.Selector, error) {
	return metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: map[string]string{
			datastore.LonghornVolumeKey: volumeName,
		},
	})
}

func (ns *NodeServer) getISCSITargetByVolumeName(name string) (string, error) {
	selector, err := getVolumeSelector(name)
	if err != nil {
		return "", err
	}
	opts := metav1.ListOptions{
		LabelSelector: selector.String(),
	}
	engineList, err := ns.lhClient.LonghornV1alpha1().Engines(ns.namespace).List(opts)
	if err != nil {
		return "", err
	}

	// TODO consider migration scenario
	for _, engine := range engineList.Items {
		if engine.Status.Endpoint != "" {
			return engine.Status.Endpoint, nil
		}
	}

	return "", errors.New("Couldn't find iSCSI target")
}

// NodePublishVolume will mount the volume /dev/longhorn/<volume_name> to target_path
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	logrus.Infof("NodeServer NodePublishVolume req: %v", req)

	readOnly := req.GetReadonly()
	if readOnly {
		return nil, status.Error(codes.FailedPrecondition, "Not support readOnly")
	}

	targetPath := req.GetTargetPath()

	if frontend, ok := req.VolumeAttributes["frontend"]; ok && frontend == "iscsi" {
		iscsiTarget, err := ns.getISCSITargetByVolumeName(req.VolumeId)
		if err != nil {
			return nil, err
		}

		if err := ioutil.WriteFile(targetPath+"/iscsi-target", []byte(iscsiTarget), 0644); err != nil {
			return nil, err
		}
		return &csi.NodePublishVolumeResponse{}, nil
	}

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
