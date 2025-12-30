package csi

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/rest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	"github.com/longhorn/longhorn-manager/types"
)

type SnapshotMetadataServer struct {
	csi.UnimplementedSnapshotMetadataServer
	apiClient   *longhornclient.RancherClient
	nodeID      string
	log         *logrus.Entry
	lhClient    lhclientset.Interface
	lhNamespace string
}

func NewSnapshotMetadataServer(apiClient *longhornclient.RancherClient, nodeID string) (*SnapshotMetadataServer, error) {
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

	return &SnapshotMetadataServer{
		apiClient:   apiClient,
		nodeID:      nodeID,
		log:         logrus.StandardLogger().WithField("component", "csi-snapshot-metadata-server"),
		lhClient:    lhClient,
		lhNamespace: lhNamespace,
	}, nil
}

func (sms *SnapshotMetadataServer) GetMetadataAllocated(req *csi.GetMetadataAllocatedRequest, stream csi.SnapshotMetadata_GetMetadataAllocatedServer) error {
	ctx := stream.Context()

	log := sms.log.WithFields(logrus.Fields{"function": "GetMetadataAllocated"})
	log.Infof("GetMetadataAllocated is called with req %+v", req)

	snapshotID := req.GetSnapshotId()
	log = log.WithField("snapshotID", snapshotID)

	_, snapshotName, parseErr := parseSnapshotID(snapshotID)
	if parseErr != nil {
		log.WithError(parseErr).Error("Failed to parse snapshot ID")
		return parseErr
	}

	snapshot, snapshotErr := sms.getLhSnapshot(ctx, log, snapshotName)
	if snapshotErr != nil {
		return snapshotErr
	}

	// TODO: interact with engines to fetch & calculate the block information
	err := stream.Send(&csi.GetMetadataAllocatedResponse{
		BlockMetadataType:   csi.BlockMetadataType_VARIABLE_LENGTH,
		VolumeCapacityBytes: snapshot.Status.RestoreSize,
		BlockMetadata: []*csi.BlockMetadata{
			{
				ByteOffset: 0,
				SizeBytes:  100,
			},
			{
				ByteOffset: 105,
				SizeBytes:  200,
			},
		},
	})
	if err != nil {
		log.WithError(err).Errorf("failed to send GetMetadataAllocated response")
		return err
	}

	return nil
}

func (sms *SnapshotMetadataServer) GetMetadataDelta(req *csi.GetMetadataDeltaRequest, stream csi.SnapshotMetadata_GetMetadataDeltaServer) error {
	ctx := stream.Context()

	log := sms.log.WithFields(logrus.Fields{"function": "GetMetadataDelta"})
	log.Infof("GetMetadataDelta is called with req %+v", req)

	baseSnapshotID := req.GetBaseSnapshotId()
	targetSnapshotID := req.GetTargetSnapshotId()
	log = log.WithFields(logrus.Fields{
		"baseSnapshotID":   baseSnapshotID,
		"targetSnapshotID": targetSnapshotID,
	})

	baseSnapshot, baseSnapshotErr := sms.getLhSnapshot(ctx, log, baseSnapshotID)
	if baseSnapshotErr != nil {
		return baseSnapshotErr
	}
	targetSnapshot, targetSnapshotErr := sms.getLhSnapshot(ctx, log, targetSnapshotID)
	if targetSnapshotErr != nil {
		return targetSnapshotErr
	}

	if baseSnapshot.Status.RestoreSize != targetSnapshot.Status.RestoreSize {
		log.Warnf("base snapshot size %v does not match target snapshot size %v", baseSnapshot.Status.RestoreSize, targetSnapshot.Status.RestoreSize)
	}

	_, baseSnapshotName, parseErr := parseSnapshotID(baseSnapshotID)
	if parseErr != nil {
		log.WithError(parseErr).Error("Failed to parse base snapshot ID")
		return parseErr
	}
	_, targetSnapshotName, parseErr := parseSnapshotID(targetSnapshotID)
	if parseErr != nil {
		log.WithError(parseErr).Error("Failed to parse target snapshot ID")
		return parseErr
	}
	log.Infof("base snapshot %v, target snapshot %v", baseSnapshotName, targetSnapshotName)

	// TODO: implement difference calculation
	err := stream.Send(&csi.GetMetadataDeltaResponse{
		BlockMetadataType:   csi.BlockMetadataType_VARIABLE_LENGTH,
		VolumeCapacityBytes: baseSnapshot.Status.RestoreSize,
		BlockMetadata: []*csi.BlockMetadata{
			{
				ByteOffset: 0,
				SizeBytes:  100,
			},
			{
				ByteOffset: 105,
				SizeBytes:  200,
			},
		},
	})
	if err != nil {
		log.WithError(err).Errorf("failed to send GetMetadataDelta response")
		return err
	}

	return nil
}

func (sms *SnapshotMetadataServer) getLhSnapshot(ctx context.Context, log *logrus.Entry, name string) (*longhorn.Snapshot, error) {
	snapshot, err := sms.lhClient.LonghornV1beta2().Snapshots(sms.lhNamespace).Get(ctx, name, metav1.GetOptions{})
	if datastore.ErrorIsNotFound(err) || snapshot == nil {
		log.Errorf("snapshot %v not found", name)
		return nil, status.Errorf(codes.NotFound, "snapshot %v not found", name)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get snapshot %v: %v", name, err)
	}
	return snapshot, nil
}

func parseSnapshotID(id string) (volumeName, snapshotName string, err error) {
	const prefix = "snap://"

	if !strings.HasPrefix(id, prefix) {
		return "", "", fmt.Errorf("invalid snapshot ID %q: missing %q prefix", id, prefix)
	}

	parts := strings.Split(strings.TrimPrefix(id, prefix), "/")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid snapshot ID %q: expected format snap://<volume_name>/<snapshot_name>", id)
	}

	volumeName = parts[0]
	snapshotName = parts[1]

	if volumeName == "" {
		return "", "", fmt.Errorf("invalid snapshot ID %q: empty volume name", id)
	}
	if snapshotName == "" {
		return "", "", fmt.Errorf("invalid snapshot ID %q: empty snapshot name", id)
	}

	return volumeName, snapshotName, nil
}
