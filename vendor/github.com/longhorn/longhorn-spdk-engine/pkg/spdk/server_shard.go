package spdk

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
)

func (s *Server) ShardCreate(ctx context.Context, req *spdkrpc.ShardCreateRequest) (ret *spdkrpc.Shard, err error) {
	if req.VolumeName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "volume name is required")
	}
	if req.SizeBytes == 0 {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "size is required")
	}

	shard, err := s.getOrCreateShard(req)
	if err != nil {
		return nil, err
	}

	defer func() {
		s.Lock()
		s.shardMap[shard.Name] = shard
		s.shardMapGen++
		s.Unlock()
	}()

	s.RLock()
	spdkClient := s.spdkClient
	s.RUnlock()

	return shard.Create(spdkClient, s.portAllocator)
}

func (s *Server) ShardDelete(ctx context.Context, req *spdkrpc.ShardDeleteRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "shard ID is required")
	}

	s.RLock()
	shard := s.shardMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	// Only full deletion drops the cached shard. On detach (cleanupRequired
	// false) the shard stays in the map in Stopped state so a later ShardCreate
	// re-attaches to it instead of hitting AlreadyExists or rebuilding from disk.
	defer func() {
		// Guard on shard != nil so an idempotent delete of an already-absent
		// shard does not bump the generation and trigger a wasted verify cycle.
		if err == nil && req.CleanupRequired && shard != nil {
			s.Lock()
			delete(s.shardMap, req.Name)
			s.shardMapGen++
			s.Unlock()
		}
	}()

	if shard == nil {
		return &emptypb.Empty{}, nil
	}

	if err := shard.Delete(spdkClient, req.CleanupRequired, s.portAllocator); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) ShardGet(ctx context.Context, req *spdkrpc.ShardGetRequest) (ret *spdkrpc.Shard, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "shard ID is required")
	}

	s.RLock()
	shard := s.shardMap[req.Name]
	s.RUnlock()

	if shard == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find shard %v", req.Name)
	}

	return shard.Get(), nil
}

func (s *Server) ShardList(ctx context.Context, req *emptypb.Empty) (*spdkrpc.ShardListResponse, error) {
	shardMap := map[string]*Shard{}
	res := map[string]*spdkrpc.Shard{}

	s.RLock()
	for k, v := range s.shardMap {
		shardMap[k] = v
	}
	s.RUnlock()

	for shardName, shard := range shardMap {
		res[shardName] = shard.Get()
	}

	return &spdkrpc.ShardListResponse{Shards: res}, nil
}

func (s *Server) ShardExpand(ctx context.Context, req *spdkrpc.ShardExpandRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "shard ID is required")
	}
	if req.Size == 0 {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "new size is required")
	}

	s.RLock()
	shard := s.shardMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if shard == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find shard %v", req.Name)
	}

	if err := shard.Expand(spdkClient, req.Size); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// ShardWatch returns a stream of shard updates
func (s *Server) ShardWatch(req *emptypb.Empty, srv spdkrpc.SPDKService_ShardWatchServer) error {
	responseCh, err := s.Subscribe(srv.Context(), types.InstanceTypeShard)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			logrus.WithError(err).Error("SPDK service shard watch errored out")
		} else {
			logrus.Info("SPDK service shard watch ended successfully")
		}
	}()
	logrus.Info("Started new SPDK service shard update watch")

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("spdk gRPC server: stopped shard watch due to the context done")
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

func (s *Server) getOrCreateShard(req *spdkrpc.ShardCreateRequest) (*Shard, error) {
	s.Lock()
	defer s.Unlock()

	// Round req.SizeBytes up to MiB first so the cached record, the reuse
	// check below, and NewShard all see the same value. Create rounds up,
	// like NewReplica. Expand rejects unaligned sizes instead, because all
	// k+m shards and the engine must agree on the exact new size.
	if rounded := util.RoundUp(req.SizeBytes, helpertypes.MiB); rounded != req.SizeBytes {
		logrus.Infof("ShardCreate rounded SizeBytes from %v to %v for shard %s-%d", req.SizeBytes, rounded, req.VolumeName, req.SlotIndex)
		req.SizeBytes = rounded
	}

	// shardMap is keyed by the external shard name (<volumeName>-<slotIndex>),
	// matching what clients send via Name. GetShardLvolName returns the
	// SPDK-internal prefixed form used for the lvol/NQN, not for map keys.
	name := GetShardName(req.VolumeName, req.SlotIndex)
	if shard, ok := s.shardMap[name]; ok {
		shard.RLock()
		cachedLvsName, cachedLvsUUID, cachedSize := shard.LvsName, shard.LvsUUID, shard.SizeBytes
		shard.RUnlock()
		if (req.LvsUuid != "" && req.LvsUuid != cachedLvsUUID) ||
			(req.LvsName != "" && req.LvsName != cachedLvsName) {
			return nil, grpcstatus.Errorf(grpccodes.InvalidArgument,
				"shard %s create request lvstore (name=%s uuid=%s) does not match cached lvstore (name=%s uuid=%s)",
				name, req.LvsName, req.LvsUuid, cachedLvsName, cachedLvsUUID)
		}
		// Reject size mismatch on reuse, matching getOrCreateShardGroup. A
		// request with a different size would diverge the in-memory record
		// from what's actually on disk. Expansion goes through Shard.Expand
		// (which updates SizeBytes in place); a controller resync should call
		// ShardGet, not re-issue ShardCreate with a different size.
		if req.SizeBytes != 0 && req.SizeBytes != cachedSize {
			return nil, grpcstatus.Errorf(grpccodes.InvalidArgument,
				"shard %s create request size %d does not match cached size %d",
				name, req.SizeBytes, cachedSize)
		}
		return shard, nil
	}

	if req.LvsName == "" && req.LvsUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "lvs_name or lvs_uuid is required for shard creation")
	}

	exists, err := s.isLvsExist(req.LvsUuid, req.LvsName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to check lvstore %v(%v) existence for shard %v creation", req.LvsName, req.LvsUuid, name)
	}
	if !exists {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "lvstore %v(%v) does not exist for shard %v creation", req.LvsName, req.LvsUuid, name)
	}

	return NewShard(req.VolumeName, req.SlotIndex, req.LvsName, req.LvsUuid, req.SizeBytes, s.updateChs[types.InstanceTypeShard]), nil
}
