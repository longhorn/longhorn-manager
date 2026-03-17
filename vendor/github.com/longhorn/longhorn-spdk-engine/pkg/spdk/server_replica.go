package spdk

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	butil "github.com/longhorn/backupstore/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
)

func (s *Server) ReplicaCreate(ctx context.Context, req *spdkrpc.ReplicaCreateRequest) (ret *spdkrpc.Replica, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.LvsName == "" && req.LvsUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "either lvstore name or UUID is required")
	}

	r, err := s.newReplica(req)
	if err != nil {
		return nil, err
	}

	defer func() {
		// Always update the replica map
		s.Lock()
		s.replicaMap[req.Name] = r
		s.Unlock()
	}()

	s.RLock()
	spdkClient := s.spdkClient
	s.RUnlock()

	var backingImage *BackingImage
	if req.BackingImageName != "" {
		backingImage, err = s.getBackingImage(req.BackingImageName, req.LvsUuid)
		if err != nil {
			return nil, err
		}
	}

	return r.Create(spdkClient, req.PortCount, s.portAllocator, backingImage)
}

// ReplicaDelete deletes a replica
func (s *Server) ReplicaDelete(ctx context.Context, req *spdkrpc.ReplicaDeleteRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	defer func() {
		if err == nil && req.CleanupRequired {
			s.Lock()
			delete(s.replicaMap, req.Name)
			s.Unlock()
		}
	}()

	if r != nil {
		if err := r.Delete(spdkClient, req.CleanupRequired, s.portAllocator); err != nil {
			return nil, err
		}
	}

	return &emptypb.Empty{}, nil
}

// ReplicaGet returns a specific replica
func (s *Server) ReplicaGet(ctx context.Context, req *spdkrpc.ReplicaGetRequest) (ret *spdkrpc.Replica, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %v", req.Name)
	}

	return r.Get(), nil
}

// ReplicaExpand expands a replica
func (s *Server) ReplicaExpand(ctx context.Context, req *spdkrpc.ReplicaExpandRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %v", req.Name)
	}

	if err := r.Expand(spdkClient, req.Size); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// ReplicaList returns all replicas
func (s *Server) ReplicaList(ctx context.Context, req *emptypb.Empty) (*spdkrpc.ReplicaListResponse, error) {
	replicaMap := map[string]*Replica{}
	res := map[string]*spdkrpc.Replica{}

	s.RLock()
	for k, v := range s.replicaMap {
		replicaMap[k] = v
	}
	s.RUnlock()

	for replicaName, r := range replicaMap {
		res[replicaName] = r.Get()
	}

	return &spdkrpc.ReplicaListResponse{Replicas: res}, nil
}

// ReplicaWatch returns a stream of replica updates
func (s *Server) ReplicaWatch(req *emptypb.Empty, srv spdkrpc.SPDKService_ReplicaWatchServer) error {
	responseCh, err := s.Subscribe(types.InstanceTypeReplica)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			logrus.WithError(err).Error("SPDK service replica watch errored out")
		} else {
			logrus.Info("SPDK service replica watch ended successfully")
		}
	}()
	logrus.Info("Started new SPDK service replica update watch")

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("spdk gRPC server: stopped replica watch due to the context done")
			done = true
		case <-responseCh:
			if err := srv.Send(&emptypb.Empty{}); err != nil {
				return err
			}
		}
		if done {
			break
		}
	}

	return nil
}

// ReplicaSnapshotCreate creates a snapshot for a replica
func (s *Server) ReplicaSnapshotCreate(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *spdkrpc.Replica, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "snapshot name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during snapshot create", req.Name)
	}

	opts := &api.SnapshotOptions{
		UserCreated: req.UserCreated,
		Timestamp:   req.SnapshotTimestamp,
	}

	return r.SnapshotCreate(spdkClient, req.SnapshotName, opts)
}

// ReplicaSnapshotDelete deletes a snapshot for a replica
func (s *Server) ReplicaSnapshotDelete(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "snapshot name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during snapshot delete", req.Name)
	}

	_, err = r.SnapshotDelete(spdkClient, req.SnapshotName)
	return &emptypb.Empty{}, err
}

// ReplicaSnapshotRevert reverts a snapshot for a replica
func (s *Server) ReplicaSnapshotRevert(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and snapshot name are required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during snapshot revert", req.Name)
	}

	_, err = r.SnapshotRevert(spdkClient, req.SnapshotName)
	return &emptypb.Empty{}, err
}

// ReplicaSnapshotPurge purges all snapshots for a replica
func (s *Server) ReplicaSnapshotPurge(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during snapshot purge", req.Name)
	}

	err = r.SnapshotPurge(spdkClient)
	return &emptypb.Empty{}, err
}

// ReplicaSnapshotHash hashes a snapshot for a replica
func (s *Server) ReplicaSnapshotHash(ctx context.Context, req *spdkrpc.SnapshotHashRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during snapshot hash", req.Name)
	}

	err = r.SnapshotHash(spdkClient, req.SnapshotName, req.Rehash)
	return &emptypb.Empty{}, err
}

// ReplicaSnapshotHashStatus returns the hash status of a snapshot for a replica
func (s *Server) ReplicaSnapshotHashStatus(ctx context.Context, req *spdkrpc.SnapshotHashStatusRequest) (ret *spdkrpc.ReplicaSnapshotHashStatusResponse, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s for snapshot hash status", req.Name)
	}

	state, checksum, errMsg, silentlyCorrupted, err := r.SnapshotHashStatus(req.SnapshotName)
	return &spdkrpc.ReplicaSnapshotHashStatusResponse{
		State:             state,
		Checksum:          checksum,
		Error:             errMsg,
		SilentlyCorrupted: silentlyCorrupted,
	}, err
}

// ReplicaSnapshotRangeHashGet returns the range hash of a snapshot for a replica
func (s *Server) ReplicaSnapshotRangeHashGet(ctx context.Context, req *spdkrpc.ReplicaSnapshotRangeHashGetRequest) (ret *spdkrpc.ReplicaSnapshotRangeHashGetResponse, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and snapshot name are required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s for snapshot range hash get", req.Name)
	}

	rangeHashMap, err := r.SnapshotRangeHashGet(spdkClient, req.SnapshotName, req.ClusterStartIndex, req.ClusterCount)
	return &spdkrpc.ReplicaSnapshotRangeHashGetResponse{
		RangeHashMap: rangeHashMap,
	}, err
}

// ReplicaSnapshotCloneDstStart starts a clone for a snapshot for a replica
func (s *Server) ReplicaSnapshotCloneDstStart(ctx context.Context, req *spdkrpc.ReplicaSnapshotCloneDstStartRequest) (ret *emptypb.Empty, err error) {
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: req.Name},
		util.Param{Name: "snapshotName", Value: req.SnapshotName},
		util.Param{Name: "srcReplicaName", Value: req.SrcReplicaName},
		util.Param{Name: "srcReplicaAddress", Value: req.SrcReplicaAddress},
	); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "%v", err)
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during ReplicaSnapshotCloneDstStart", req.Name)
	}

	if err := r.SnapshotCloneDstStart(spdkClient, req.SnapshotName, req.SrcReplicaName, req.SrcReplicaAddress, req.CloneMode); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to do SnapshotCloneDstStart during ReplicaSnapshotCloneDstStart")
	}
	return &emptypb.Empty{}, nil
}

// ReplicaSnapshotCloneDstStatusCheck checks the status of a clone for a snapshot for a replica
func (s *Server) ReplicaSnapshotCloneDstStatusCheck(ctx context.Context, req *spdkrpc.ReplicaSnapshotCloneDstStatusCheckRequest) (ret *spdkrpc.ReplicaSnapshotCloneDstStatusCheckResponse, err error) {
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: req.Name},
	); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "%v", err)
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during ReplicaSnapshotCloneDstStatusCheck", req.Name)
	}

	return r.SnapshotCloneDstStatusCheck()
}

// ReplicaSnapshotCloneSrcStart starts a clone for a snapshot for a replica
func (s *Server) ReplicaSnapshotCloneSrcStart(ctx context.Context, req *spdkrpc.ReplicaSnapshotCloneSrcStartRequest) (ret *emptypb.Empty, err error) {
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: req.Name},
		util.Param{Name: "snapshotName", Value: req.SnapshotName},
		util.Param{Name: "dstReplicaName", Value: req.DstReplicaName},
	); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "%v", err)
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during ReplicaSnapshotCloneSrcStart", req.Name)
	}

	if err := r.SnapshotCloneSrcStart(spdkClient, req.SnapshotName, req.DstReplicaName, req.DstCloningLvolAddress, req.CloneMode); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ReplicaSnapshotCloneSrcStatusCheck checks the status of a clone for a snapshot for a replica
func (s *Server) ReplicaSnapshotCloneSrcStatusCheck(ctx context.Context, req *spdkrpc.ReplicaSnapshotCloneSrcStatusCheckRequest) (ret *spdkrpc.ReplicaSnapshotCloneSrcStatusCheckResponse, err error) {
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: req.Name},
		util.Param{Name: "snapshotName", Value: req.SnapshotName},
		util.Param{Name: "dstReplicaName", Value: req.DstReplicaName},
	); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "%v", err)
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during ReplicaSnapshotCloneSrcStatusCheck", req.Name)
	}

	return r.SnapshotCloneSrcStatusCheck(spdkClient, req.SnapshotName, req.DstReplicaName)
}

// ReplicaSnapshotCloneSrcFinish finishes a clone for a snapshot for a replica
func (s *Server) ReplicaSnapshotCloneSrcFinish(ctx context.Context, req *spdkrpc.ReplicaSnapshotCloneSrcFinishRequest) (ret *emptypb.Empty, err error) {
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: req.Name},
		util.Param{Name: "dstReplicaName", Value: req.DstReplicaName},
	); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "%v", err)
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during ReplicaSnapshotCloneSrcFinish", req.Name)
	}

	if err := r.SnapshotCloneSrcFinish(spdkClient, req.DstReplicaName); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ReplicaRebuildingSrcStart starts a rebuilding for a replica
func (s *Server) ReplicaRebuildingSrcStart(ctx context.Context, req *spdkrpc.ReplicaRebuildingSrcStartRequest) (ret *spdkrpc.ReplicaRebuildingSrcStartResponse, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.DstReplicaName == "" || req.DstReplicaAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "dst replica name and address are required")
	}
	if req.ExposedSnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "src replica exposed snapshot name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding src start", req.Name)
	}

	exposedSnapshotLvolAddress, err := r.RebuildingSrcStart(spdkClient, req.DstReplicaName, req.DstReplicaAddress, req.ExposedSnapshotName)
	if err != nil {
		return nil, err
	}
	return &spdkrpc.ReplicaRebuildingSrcStartResponse{ExposedSnapshotLvolAddress: exposedSnapshotLvolAddress}, nil
}

// ReplicaRebuildingSrcFinish finishes a rebuilding for a replica
func (s *Server) ReplicaRebuildingSrcFinish(ctx context.Context, req *spdkrpc.ReplicaRebuildingSrcFinishRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.DstReplicaName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "dst replica name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding src finish", req.Name)
	}

	if err = r.RebuildingSrcFinish(spdkClient, req.DstReplicaName); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ReplicaRebuildingSrcShallowCopyStart starts a shallow copy for a rebuilding for a replica
func (s *Server) ReplicaRebuildingSrcShallowCopyStart(ctx context.Context, req *spdkrpc.ReplicaRebuildingSrcShallowCopyStartRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica snapshot name is required")
	}
	if req.DstRebuildingLvolAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "dst rebuilding lvol address is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding src snapshot %s shallow copy start", req.Name, req.SnapshotName)
	}

	if err := r.RebuildingSrcShallowCopyStart(spdkClient, req.SnapshotName, req.DstRebuildingLvolAddress); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// ReplicaRebuildingSrcRangeShallowCopyStart starts a range shallow copy for a rebuilding for a replica
func (s *Server) ReplicaRebuildingSrcRangeShallowCopyStart(ctx context.Context, req *spdkrpc.ReplicaRebuildingSrcRangeShallowCopyStartRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica snapshot name is required")
	}
	if req.DstRebuildingLvolAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "dst rebuilding lvol address is required")
	}
	if len(req.MismatchingClusterList) < 1 {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "mismatching cluster list is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding src snapshot %s range shallow copy start", req.Name, req.SnapshotName)
	}

	if err := r.RebuildingSrcRangeShallowCopyStart(spdkClient, req.SnapshotName, req.DstRebuildingLvolAddress, req.MismatchingClusterList); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// ReplicaRebuildingSrcShallowCopyCheck checks the shallow copy for a rebuilding for a replica
func (s *Server) ReplicaRebuildingSrcShallowCopyCheck(ctx context.Context, req *spdkrpc.ReplicaRebuildingSrcShallowCopyCheckRequest) (ret *spdkrpc.ReplicaRebuildingSrcShallowCopyCheckResponse, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica snapshot name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding src snapshot %s shallow copy check", req.Name, req.SnapshotName)
	}

	return r.RebuildingSrcShallowCopyCheck(req.SnapshotName)
}

// ReplicaRebuildingDstStart starts a rebuilding for a replica
func (s *Server) ReplicaRebuildingDstStart(ctx context.Context, req *spdkrpc.ReplicaRebuildingDstStartRequest) (ret *spdkrpc.ReplicaRebuildingDstStartResponse, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.SrcReplicaName == "" || req.SrcReplicaAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "src replica name and address are required")
	}
	if req.ExternalSnapshotName == "" || req.ExternalSnapshotAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "external external snapshot name and address are required")
	}
	if req.RebuildingSnapshotList == nil {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "rebuilding snapshot list is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding dst start", req.Name)
	}

	var rebuildingSnapshotList []*api.Lvol
	for _, snapshot := range req.RebuildingSnapshotList {
		rebuildingSnapshotList = append(rebuildingSnapshotList, api.ProtoLvolToLvol(snapshot))
	}
	address, err := r.RebuildingDstStart(spdkClient, req.SrcReplicaName, req.SrcReplicaAddress, req.ExternalSnapshotName, req.ExternalSnapshotAddress, rebuildingSnapshotList)
	if err != nil {
		return nil, err
	}
	return &spdkrpc.ReplicaRebuildingDstStartResponse{DstHeadLvolAddress: address}, nil
}

// ReplicaRebuildingDstFinish finishes a rebuilding for a replica
func (s *Server) ReplicaRebuildingDstFinish(ctx context.Context, req *spdkrpc.ReplicaRebuildingDstFinishRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding dst finish", req.Name)
	}

	if err = r.RebuildingDstFinish(spdkClient); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ReplicaRebuildingDstShallowCopyStart starts a shallow copy for a rebuilding for a replica
func (s *Server) ReplicaRebuildingDstShallowCopyStart(ctx context.Context, req *spdkrpc.ReplicaRebuildingDstShallowCopyStartRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica snapshot name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding dst snapshot %s shallow copy start", req.Name, req.SnapshotName)
	}

	if err = r.RebuildingDstShallowCopyStart(spdkClient, req.SnapshotName, req.FastSync); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ReplicaRebuildingDstShallowCopyCheck checks the shallow copy for a rebuilding for a replica
func (s *Server) ReplicaRebuildingDstShallowCopyCheck(ctx context.Context, req *spdkrpc.ReplicaRebuildingDstShallowCopyCheckRequest) (ret *spdkrpc.ReplicaRebuildingDstShallowCopyCheckResponse, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding dst snapshot shallow copy check", req.Name)
	}

	return r.RebuildingDstShallowCopyCheck(spdkClient)
}

// ReplicaRebuildingDstSnapshotCreate creates a snapshot for a rebuilding for a replica
func (s *Server) ReplicaRebuildingDstSnapshotCreate(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and snapshot name are required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding dst snapshot create", req.Name)
	}

	opts := &api.SnapshotOptions{
		UserCreated: req.UserCreated,
		Timestamp:   req.SnapshotTimestamp,
	}

	if err = r.RebuildingDstSnapshotCreate(spdkClient, req.SnapshotName, opts); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ReplicaRebuildingDstSetQosLimit sets the QoS limit for a rebuilding for a replica
func (s *Server) ReplicaRebuildingDstSetQosLimit(
	ctx context.Context,
	req *spdkrpc.ReplicaRebuildingDstSetQosLimitRequest,
) (*emptypb.Empty, error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.QosLimitMbps < 0 {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "QoS limit must not be negative, got %d", req.QosLimitMbps)
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s for QoS setting", req.Name)
	}

	if err := r.RebuildingDstSetQos(spdkClient, req.QosLimitMbps); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to set QoS limit on replica %s: %v", req.Name, err)
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) ReplicaBackupCreate(ctx context.Context, req *spdkrpc.BackupCreateRequest) (ret *spdkrpc.BackupCreateResponse, err error) {
	backupName := req.BackupName

	backupType, err := butil.CheckBackupType(req.BackupTarget)
	if err != nil {
		return nil, err
	}

	err = butil.SetupCredential(backupType, req.Credential)
	if err != nil {
		err = errors.Wrapf(err, "failed to setup credential of backup target %v for backup %v", req.BackupTarget, backupName)
		return nil, grpcstatus.Errorf(grpccodes.Internal, "%v", err)
	}

	var labelMap map[string]string
	if req.Labels != nil {
		labelMap, err = util.ParseLabels(req.Labels)
		if err != nil {
			err = errors.Wrapf(err, "failed to parse backup labels for backup %v", backupName)
			return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "%v", err)
		}
	}

	s.Lock()
	defer s.Unlock()

	if _, ok := s.backupMap[backupName]; ok {
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "backup %v already exists", backupName)
	}

	replica, ok := s.replicaMap[req.ReplicaName]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %v for volume %v backup creation", req.ReplicaName, req.VolumeName)
	}

	backup, err := NewBackup(s.spdkClient, backupName, req.VolumeName, req.SnapshotName, replica, s.portAllocator)
	if err != nil {
		err = errors.Wrapf(err, "failed to create backup instance %v for volume %v", backupName, req.VolumeName)
		return nil, grpcstatus.Errorf(grpccodes.Internal, "%v", err)
	}

	config := &backupstore.DeltaBackupConfig{
		BackupName:      backupName,
		ConcurrentLimit: req.ConcurrentLimit,
		Volume: &backupstore.Volume{
			Name:                 req.VolumeName,
			Size:                 req.Size,
			Labels:               labelMap,
			BackingImageName:     req.BackingImageName,
			BackingImageChecksum: req.BackingImageChecksum,
			CompressionMethod:    req.CompressionMethod,
			StorageClassName:     req.StorageClassName,
			CreatedTime:          util.Now(),
			DataEngine:           string(backupstore.DataEngineV2),
		},
		Snapshot: &backupstore.Snapshot{
			Name:        req.SnapshotName,
			CreatedTime: util.Now(),
		},
		DestURL:  req.BackupTarget,
		DeltaOps: backup,
		Labels:   labelMap,
	}

	s.backupMap[backupName] = backup
	if err := backup.BackupCreate(config); err != nil {
		delete(s.backupMap, backupName)
		err = errors.Wrapf(err, "failed to create backup %v for volume %v", backupName, req.VolumeName)
		return nil, grpcstatus.Errorf(grpccodes.Internal, "%v", err)
	}

	return &spdkrpc.BackupCreateResponse{
		Backup:        backup.Name,
		IsIncremental: backup.IsIncremental,
	}, nil
}

func (s *Server) ReplicaBackupStatus(ctx context.Context, req *spdkrpc.BackupStatusRequest) (ret *spdkrpc.BackupStatusResponse, err error) {
	s.RLock()
	defer s.RUnlock()

	backup, ok := s.backupMap[req.Backup]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find backup %v", req.Backup)
	}

	replicaAddress := ""
	if backup.replica != nil {
		replicaAddress = fmt.Sprintf("tcp://%s", backup.replica.GetAddress())
	}

	return &spdkrpc.BackupStatusResponse{
		Progress:       int32(backup.Progress),
		BackupUrl:      backup.BackupURL,
		Error:          backup.Error,
		SnapshotName:   backup.SnapshotName,
		State:          string(backup.State),
		ReplicaAddress: replicaAddress,
	}, nil
}

func (s *Server) ReplicaBackupRestore(ctx context.Context, req *spdkrpc.ReplicaBackupRestoreRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	replica := s.replicaMap[req.ReplicaName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if replica == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %v for restoring backup %v", req.ReplicaName, req.BackupUrl)
	}

	err = replica.BackupRestore(spdkClient, req.BackupUrl, req.SnapshotName, req.Credential, req.ConcurrentLimit)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) ReplicaRestoreStatus(ctx context.Context, req *spdkrpc.ReplicaRestoreStatusRequest) (ret *spdkrpc.ReplicaRestoreStatusResponse, err error) {
	s.RLock()
	defer s.RUnlock()

	replica, ok := s.replicaMap[req.ReplicaName]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %v", req.ReplicaName)
	}

	if replica.restore == nil {
		return &spdkrpc.ReplicaRestoreStatusResponse{
			ReplicaName: replica.Name,
			IsRestoring: false,
		}, nil
	}

	return &spdkrpc.ReplicaRestoreStatusResponse{
		ReplicaName:            replica.Name,
		ReplicaAddress:         net.JoinHostPort(replica.restore.ip, strconv.Itoa(int(replica.restore.port))),
		IsRestoring:            replica.isRestoring,
		LastRestored:           replica.restore.LastRestored,
		Progress:               int32(replica.restore.Progress),
		Error:                  replica.restore.Error,
		DestFileName:           replica.restore.LvolName,
		State:                  string(replica.restore.State),
		BackupUrl:              replica.restore.BackupURL,
		CurrentRestoringBackup: replica.restore.CurrentRestoringBackup,
	}, nil
}
