package spdk

import (
	"context"
	"net"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	commonnet "github.com/longhorn/go-common-libs/net"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// EngineFrontendSuspend suspends an engine frontend. The engine frontend can be resumed later. The engine frontend should be in normal state before suspension.
func (s *Server) EngineFrontendSuspend(ctx context.Context, req *spdkrpc.EngineFrontendSuspendRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name is required")
	}

	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v for suspension", req.Name)
	}

	err = ef.Suspend(spdkClient)
	if err != nil {
		return nil, toEngineFrontendLifecycleGRPCError(err, "failed to suspend engine frontend %v", req.Name)
	}

	return &emptypb.Empty{}, nil
}

// EngineFrontendResume resumes an engine frontend. The engine frontend should have been suspended before resumption.
func (s *Server) EngineFrontendResume(ctx context.Context, req *spdkrpc.EngineFrontendResumeRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name is required")
	}

	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v for resumption", req.Name)
	}

	err = ef.Resume(spdkClient)
	if err != nil {
		return nil, toEngineFrontendLifecycleGRPCError(err, "failed to resume engine frontend %v", req.Name)
	}

	return &emptypb.Empty{}, nil
}

// EngineFrontendReplicaAdd initiates a replica-add (rebuild) for the Engine
// that this EngineFrontend is connected to. It is the entry point called by
// the longhorn-manager control plane.
//
// # Design
//
// EngineFrontend (EF) and Engine may reside on different nodes, so this
// handler is a thin proxy:
//
//  1. Validate EF preconditions (not creating, switching over, expanding,
//     restoring, and in Running state).
//  2. Resolve the local pod IP so the Engine can call back to this EF for
//     suspend/resume during the finish step.
//  3. Create a gRPC client to the (potentially remote) Engine node.
//  4. Delegate to Server.EngineReplicaAdd on the Engine node, passing the
//     EF name and address in the request fields.
//
// # Engine-side flow (Server.EngineReplicaAdd → Engine.ReplicaAdd)
//
// Server.EngineReplicaAdd reads the EF fields from the request, then
// calls buildGRPCReplicaAddFrontendSuspendResumeWrapper to create a replicaAddFrontendSuspendResumeWrapper
// — a callback that will call back to this EF for suspend/resume during both
// the snapshot-creation step and the finish step. It then delegates to
// Engine.ReplicaAdd(frontendSuspendResumeWrapper).
//
// Engine.ReplicaAdd runs the synchronous part under the Engine lock:
//
//  0. Snapshot + setup (sync, under frontendSuspendResumeWrapper): suspend frontend via
//     frontendSuspendResumeWrapper → create rebuild snapshot, connect to src/dst replica
//     SPDK services, add dst replica head bdev to RAID, mark dst as ModeWO
//     → resume frontend.
//
// On return (with lock released), a deferred goroutine runs the remaining
// phases:
//
//  1. Shallow copy (async): iterate over snapshots and copy data from the
//     source replica to the destination replica via SPDK shallow copy.
//  2. Finish (async): orchestrates the finish step with the frontendSuspendResumeWrapper:
//     a. If shallow copy failed, mark dst replica as ModeERR, call the
//     real replicaAddFinish for SPDK resource cleanup (detach external
//     snapshot controller, stop expose).
//     b. If shallow copy succeeded, call frontendSuspendResumeWrapper(finish):
//     - frontendSuspendResumeWrapper (buildGRPCReplicaAddFrontendSuspendResumeWrapper) calls back to
//     EF via gRPC: Suspend → finish() → Resume
//     - finish() (replicaAddFinish) detaches the external snapshot
//     NVMe controller on the dst replica, stops the src replica from
//     exposing, and promotes the dst replica from ModeWO to ModeRW.
//     c. If finish fails and was never called (e.g. suspend failure in
//     frontendSuspendResumeWrapper), a cleanup call to the real replicaAddFinish runs
//     inside frontendSuspendResumeWrapper for SPDK resource cleanup.
//
// This call returns as soon as the synchronous part succeeds; the
// remaining phases run in the background goroutine. The caller can monitor
// progress by polling the Engine's ReplicaModeMap — the rebuilding replica
// appears as ModeWO until finished (ModeRW) or failed (ModeERR).
//
// # Error handling
//
// EF is stateless with respect to replica-add. Engine owns all state.
// If the Engine's async goroutine fails at any phase, it sets the replica
// to ModeERR and cleans up SPDK resources — without notifying EF.
//
// The frontendSuspendResumeWrapper (buildGRPCReplicaAddFrontendSuspendResumeWrapper) handles EF-unreachable
// scenarios gracefully:
//   - EF node down / pod deleted (GetServiceClient fails) → proceed with
//     the operation without suspension (no active I/O = suspension unnecessary).
//   - Suspend fails → proceed without suspension (data integrity is not
//     compromised; not proceeding would block the rebuild).
//   - Resume fails → log error but do not override the operation result;
//     longhorn-manager will detect the stuck-suspended EF and recover.
func (s *Server) EngineFrontendReplicaAdd(ctx context.Context, req *spdkrpc.EngineFrontendReplicaAddRequest) (ret *emptypb.Empty, err error) {
	if req.ReplicaName == "" || req.ReplicaAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and address are required")
	}

	s.RLock()
	ef, ok := s.engineFrontendMap[req.EngineFrontendName]
	if !ok {
		s.RUnlock()
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v for replica %s add", req.EngineFrontendName, req.ReplicaName)
	}
	s.RUnlock()

	// Validate EF state and capture engine connection info under lock.
	ef.RLock()
	if ef.isCreating {
		ef.RUnlock()
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "engine frontend %s is still creating", ef.Name)
	}
	if ef.isSwitchingOver {
		ef.RUnlock()
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "engine frontend %s is switching over target", ef.Name)
	}
	if ef.isExpanding {
		ef.RUnlock()
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "engine frontend %s expansion is in progress", ef.Name)
	}
	if ef.IsRestoring {
		ef.RUnlock()
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "engine frontend %s restore is in progress", ef.Name)
	}
	if ef.State != types.InstanceStateRunning {
		ef.RUnlock()
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "invalid state %v for engine frontend %s replica %s add", ef.State, ef.Name, req.ReplicaName)
	}
	engineIP := ef.EngineIP
	engineName := ef.EngineName
	ef.RUnlock()

	// Resolve the local node IP so Engine can call back to this EF
	// for suspend/resume during the finish step.
	localIP, err := commonnet.GetIPForPod()
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get local IP for engine frontend %s: %v", ef.Name, err)
	}
	efAddress := net.JoinHostPort(localIP, strconv.Itoa(types.SPDKServicePort))

	// Create a gRPC client to the (potentially remote) Engine node.
	engineAddress := net.JoinHostPort(engineIP, strconv.Itoa(types.SPDKServicePort))
	engineClient, err := GetServiceClient(engineAddress)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client for engine %s at %s: %v", engineName, engineAddress, err)
	}
	defer func() {
		if errClose := engineClient.Close(); errClose != nil {
			logrus.WithError(errClose).Warnf("Failed to close engine SPDK client for %s", engineName)
		}
	}()

	// Delegate to EngineReplicaAdd on the Engine node, passing EF info for callback.
	if err := engineClient.EngineReplicaAdd(engineName, req.ReplicaName, req.ReplicaAddress, req.FastSync, ef.Name, efAddress); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to add replica %s on engine %s: %v", req.ReplicaName, engineName, err)
	}

	return &emptypb.Empty{}, nil
}

// EngineFrontendCreate creates a new engine frontend.
func (s *Server) EngineFrontendCreate(ctx context.Context, req *spdkrpc.EngineFrontendCreateRequest) (ret *spdkrpc.EngineFrontend, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name is required")
	}
	if req.VolumeName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "volume name is required")
	}
	if req.EngineName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name is required")
	}
	if req.SpecSize == 0 {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "spec size is required")
	}

	if !types.IsFrontendSupported(req.Frontend) {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "frontend %v is not supported", req.Frontend)
	}

	s.Lock()
	_, ok := s.engineFrontendMap[req.Name]
	if ok {
		s.Unlock()
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "engine frontend %v already exists", req.Name)
	}
	if existing := s.engineFrontendByVolumeName(req.VolumeName); existing != nil {
		s.Unlock()
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "engine frontend %v already exists for volume %v", existing.Name, req.VolumeName)
	}

	ef := NewEngineFrontend(req.Name, req.EngineName, req.VolumeName, req.Frontend, req.SpecSize,
		req.UblkQueueDepth, req.UblkNumberOfQueue, s.updateChs[types.InstanceTypeEngineFrontend])
	ef.metadataDir = s.metadataDir

	spdkClient := s.spdkClient
	s.Unlock()

	ret, createErr := ef.Create(spdkClient, req.TargetAddress)

	// Distinguish hard errors (validation / precondition) from runtime
	// failures (e.g. NVMe initiator can't connect).  Hard errors are
	// returned before Create mutates state, so the frontend must NOT be
	// registered.  Runtime failures leave the frontend in Error state;
	// we register it so callers can inspect and clean it up via Delete.
	if createErr != nil &&
		(errors.Is(createErr, ErrEngineFrontendCreateInvalidArgument) ||
			errors.Is(createErr, ErrEngineFrontendCreatePrecondition)) {
		return nil, toEngineFrontendCreateGRPCError(createErr, "failed to create engine frontend %v", req.Name)
	}

	s.Lock()
	// Re-check after Create() to guard against a concurrent create that
	// raced through the same window.
	duplicateName := false
	duplicateVolume := false
	var winner *EngineFrontend
	if existing, exists := s.engineFrontendMap[req.Name]; exists {
		duplicateName = true
		winner = existing
	} else if existing := s.engineFrontendByVolumeName(req.VolumeName); existing != nil {
		duplicateVolume = true
		winner = existing
	}
	if duplicateName || duplicateVolume {
		s.Unlock()
		// The race loser holds a fully-created frontend with real SPDK
		// resources (bdevs, NVMe controllers, etc.). Clean them up so
		// they don't leak.
		// Only clear metadataDir when the loser shares the same
		// volumeName as the winner — they use the same persistence
		// directory, so the loser's Delete() must not remove it.
		// When volumeNames differ, each has its own directory and the
		// loser should clean up its own record.
		if winner != nil && ef.VolumeName == winner.VolumeName {
			ef.metadataDir = ""
		}
		if deleteErr := ef.Delete(spdkClient); deleteErr != nil {
			logrus.WithError(deleteErr).Warnf("Failed to clean up race-loser engine frontend %v", req.Name)
		}
		// The loser's Create() may have overwritten the winner's
		// persistence record (both share the same volumeName key).
		// Re-persist the winner to restore correct on-disk state.
		// Hold the winner's read lock to prevent concurrent mutations
		// (e.g. Delete, switchover) from racing with the field reads
		// inside saveEngineFrontendRecord.
		if winner != nil && winner.metadataDir != "" && ef.VolumeName == winner.VolumeName {
			winner.RLock()
			if err := saveEngineFrontendRecord(winner.metadataDir, winner); err != nil {
				logrus.WithError(err).Warnf("Failed to re-persist winner engine frontend %v record after race", winner.Name)
			}
			winner.RUnlock()
		}
		if duplicateVolume {
			return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "engine frontend already exists for volume %v", req.VolumeName)
		}
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "engine frontend %v already exists", req.Name)
	}
	s.engineFrontendMap[req.Name] = ef
	s.Unlock()

	// Runtime failure: the frontend is registered in Error state so it
	// can be inspected and cleaned up via Delete.
	if createErr != nil {
		return ef.Get(), nil
	}

	return ret, nil
}

// EngineFrontendDelete deletes an engine frontend.
func (s *Server) EngineFrontendDelete(ctx context.Context, req *spdkrpc.EngineFrontendDeleteRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	defer func() {
		if err != nil {
			return
		}

		s.Lock()
		delete(s.engineFrontendMap, req.Name)
		s.Unlock()
	}()

	if ef == nil {
		return &emptypb.Empty{}, nil
	}

	if err := ef.Delete(spdkClient); err != nil {
		return nil, toEngineFrontendLifecycleGRPCError(err, "failed to delete engine frontend %v", req.Name)
	}

	return &emptypb.Empty{}, nil
}

// EngineFrontendGet returns a specific engine frontend
func (s *Server) EngineFrontendGet(ctx context.Context, req *spdkrpc.EngineFrontendGetRequest) (ret *spdkrpc.EngineFrontend, err error) {
	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v", req.Name)
	}

	return ef.Get(), nil
}

// EngineFrontendList lists all engine frontends.
func (s *Server) EngineFrontendList(ctx context.Context, req *emptypb.Empty) (*spdkrpc.EngineFrontendListResponse, error) {
	engineFrontendMap := map[string]*EngineFrontend{}
	res := map[string]*spdkrpc.EngineFrontend{}

	s.RLock()
	for k, v := range s.engineFrontendMap {
		engineFrontendMap[k] = v
	}
	s.RUnlock()

	for engineFrontendName, ef := range engineFrontendMap {
		res[engineFrontendName] = ef.Get()
	}

	return &spdkrpc.EngineFrontendListResponse{EngineFrontends: res}, nil
}

// EngineFrontendWatch watches engine frontends.
func (s *Server) EngineFrontendWatch(req *emptypb.Empty, srv spdkrpc.SPDKService_EngineFrontendWatchServer) error {
	responseCh, err := s.Subscribe(types.InstanceTypeEngineFrontend)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			logrus.WithError(err).Error("SPDK service engine frontend watch errored out")
		} else {
			logrus.Info("SPDK service engine frontend watch ended successfully")
		}
	}()
	logrus.Info("Started new SPDK service engine frontend update watch")

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("spdk gRPC server: stopped engine target watch due to the context done")
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

func (s *Server) EngineFrontendExpand(ctx context.Context, req *spdkrpc.EngineFrontendExpandRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name is required")
	}

	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v", req.Name)
	}

	if types.IsUblkFrontend(ef.Frontend) {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "cannot expand ublk frontend engine %v", ef.Name)
	}

	err = ef.Expand(ctx, spdkClient, req.Size)
	if err != nil {
		return nil, toExpansionGRPCError(err, "failed to expand engine frontend %v", req.Name)
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineFrontendSnapshotCreate(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *spdkrpc.SnapshotResponse, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name is required")
	}

	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v for snapshot creation", req.Name)
	}

	snapshotName, err := ef.SnapshotCreate(req.SnapshotName)
	return &spdkrpc.SnapshotResponse{SnapshotName: snapshotName}, err
}

func (s *Server) EngineFrontendSnapshotDelete(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name and snapshot name are required")
	}

	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v for snapshot deletion", req.Name)
	}

	if err := ef.SnapshotDelete(req.SnapshotName); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineFrontendSnapshotRevert(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name and snapshot name are required")
	}

	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v for snapshot revert", req.Name)
	}

	if err := ef.SnapshotRevert(req.SnapshotName); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineFrontendSnapshotPurge(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name is required")
	}

	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v for snapshot purge", req.Name)
	}

	if err := ef.SnapshotPurge(); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}
