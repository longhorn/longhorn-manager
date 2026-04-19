package spdk

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
)

// EngineCreate creates an engine
func (s *Server) EngineCreate(ctx context.Context, req *spdkrpc.EngineCreateRequest) (ret *spdkrpc.Engine, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name is required")
	}
	if req.VolumeName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine volume name is required")
	}
	if req.SpecSize == 0 {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine spec size is required")
	}

	s.Lock()

	e, ok := s.engineMap[req.Name]
	if ok {
		s.Unlock()
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "engine %v already exists", req.Name)
	}

	if e == nil {
		s.engineMap[req.Name] = NewEngine(req.Name, req.VolumeName, req.Frontend, req.SpecSize, s.updateChs[types.InstanceTypeEngine])
		e = s.engineMap[req.Name]
	}

	spdkClient := s.spdkClient
	s.Unlock()

	return e.Create(spdkClient, req.ReplicaAddressMap, req.PortCount, s.portAllocator, req.SalvageRequested)
}

// EngineDelete deletes an engine
func (s *Server) EngineDelete(ctx context.Context, req *spdkrpc.EngineDeleteRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	defer func() {
		if err == nil {
			s.Lock()
			delete(s.engineMap, req.Name)
			s.Unlock()
		}
	}()

	if e != nil {
		if err := e.Delete(spdkClient, s.portAllocator); err != nil {
			return nil, err
		}
	}

	return &emptypb.Empty{}, nil
}

// EngineGet returns a specific engine
func (s *Server) EngineExpand(ctx context.Context, req *spdkrpc.EngineExpandRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name is required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for expansion", req.Name)
	}

	if types.IsUblkFrontend(e.Frontend) {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "cannot expand ublk frontend engine %v", req.Name)
	}

	err = e.Expand(spdkClient, req.Size)
	if err != nil {
		return nil, toExpansionGRPCError(err, "failed to expand engine %v", req.Name)
	}

	return &emptypb.Empty{}, nil
}

// EngineExpandPrecheck checks if expansion is required for an engine. The engine spec size should be updated before precheck.
func (s *Server) EngineExpandPrecheck(ctx context.Context, req *spdkrpc.EngineExpandPrecheckRequest) (*spdkrpc.EngineExpandPrecheckResponse, error) {
	if req.Name == "" {
		return &spdkrpc.EngineExpandPrecheckResponse{
			ExpansionRequired: false,
		}, grpcstatus.Error(grpccodes.InvalidArgument, "engine name is required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return &spdkrpc.EngineExpandPrecheckResponse{}, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for expansion", req.Name)
	}

	requireExpansion, err := e.ExpandPrecheck(spdkClient, req.Size)
	if err != nil {
		return &spdkrpc.EngineExpandPrecheckResponse{
			ExpansionRequired: false,
		}, toExpansionGRPCError(err, "failed to precheck expand engine %v", req.Name)
	}

	return &spdkrpc.EngineExpandPrecheckResponse{
		ExpansionRequired: requireExpansion,
	}, nil
}

// EngineFrontendSwitchOver switches over the frontend of an engine to a new target address. The engine frontend should be in normal state before switch over.
func (s *Server) EngineFrontendSwitchOver(ctx context.Context, req *spdkrpc.EngineFrontendSwitchOverRequest) (ret *emptypb.Empty, err error) {
	if req == nil {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "request is required")
	}
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend or engine name is required")
	}
	if req.TargetAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "target address is required")
	}
	if targetIP, targetPort, splitErr := splitHostPort(req.TargetAddress); splitErr != nil || targetIP == "" || targetPort == 0 {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "invalid target address %q", req.TargetAddress)
	}

	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	if ef == nil {
		// Backward compatible lookup: allow name to be engine name if there is exactly one frontend.
		for _, frontend := range s.engineFrontendMap {
			if frontend.EngineName != req.Name {
				continue
			}
			if ef != nil {
				s.RUnlock()
				return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "multiple engine frontends found for engine %s", req.Name)
			}
			ef = frontend
		}
	}
	spdkClient := s.spdkClient
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend or engine %v for target switchover", req.Name)
	}

	if err := ef.SwitchOverTarget(spdkClient, req.EngineName, req.TargetAddress, req.SwitchoverPhase); err != nil {
		return nil, toSwitchOverGRPCError(err, "failed to switch over target for %s", req.Name)
	}

	return &emptypb.Empty{}, nil
}

// EngineDeleteTarget deletes the target for an engine.
// TODO: The API is currently not implemented and will be removed in the future as target management will be handled by engine frontends instead of the engine itself.
func (s *Server) EngineDeleteTarget(ctx context.Context, req *spdkrpc.EngineDeleteTargetRequest) (ret *emptypb.Empty, err error) {
	return &emptypb.Empty{}, grpcstatus.Error(grpccodes.Unimplemented, "EngineDeleteTarget is not implemented yet and will be removed in the future")
}

// EngineSetTargetListenerANAState updates the ANA state of an engine's exported NVMe/TCP listener.
func (s *Server) EngineSetTargetListenerANAState(ctx context.Context, req *spdkrpc.EngineSetTargetListenerANAStateRequest) (ret *emptypb.Empty, err error) {
	if req == nil {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "request is required")
	}
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name is required")
	}
	if req.AnaState == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "ANA state is required")
	}

	anaState := NvmeTCPANAState(req.AnaState)
	if _, mapErr := toSPDKListenerANAState(anaState); mapErr != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "invalid ANA state %q", req.AnaState)
	}

	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for ANA state update", req.Name)
	}

	if err := e.SetTargetListenerANAState(spdkClient, anaState); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to set ANA state %s for engine %s: %v", anaState, req.Name, err)
	}

	return &emptypb.Empty{}, nil
}

// EngineGet returns a specific engine
func (s *Server) EngineGet(ctx context.Context, req *spdkrpc.EngineGetRequest) (ret *spdkrpc.Engine, err error) {
	s.RLock()
	e := s.engineMap[req.Name]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v", req.Name)
	}

	return e.Get(), nil
}

// EngineList returns all engines
func (s *Server) EngineList(ctx context.Context, req *emptypb.Empty) (*spdkrpc.EngineListResponse, error) {
	engineMap := map[string]*Engine{}
	res := map[string]*spdkrpc.Engine{}

	s.RLock()
	for k, v := range s.engineMap {
		engineMap[k] = v
	}
	s.RUnlock()

	for engineName, e := range engineMap {
		res[engineName] = e.Get()
	}

	return &spdkrpc.EngineListResponse{Engines: res}, nil
}

// EngineWatch returns a stream of engine updates
func (s *Server) EngineWatch(req *emptypb.Empty, srv spdkrpc.SPDKService_EngineWatchServer) error {
	responseCh, err := s.Subscribe(types.InstanceTypeEngine)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			logrus.WithError(err).Error("SPDK service engine watch errored out")
		} else {
			logrus.Info("SPDK service engine watch ended successfully")
		}
	}()
	logrus.Info("Started new SPDK service engine update watch")

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("spdk gRPC server: stopped engine watch due to the context done")
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

// EngineReplicaAdd handles the full replica-add lifecycle.
//
// When EngineFrontendName and EngineFrontendAddress are both provided, Engine
// calls back to the EngineFrontend for suspend/resume around the snapshot and
// finish steps. When both are omitted, ReplicaAdd runs without a frontendSuspendResumeWrapper,
// preserving the older direct-engine API behavior.
func (s *Server) EngineReplicaAdd(ctx context.Context, req *spdkrpc.EngineReplicaAddRequest) (ret *emptypb.Empty, err error) {
	if req.ReplicaName == "" || req.ReplicaAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and address are required")
	}

	efName := req.EngineFrontendName
	efAddress := req.EngineFrontendAddress
	if (efName == "") != (efAddress == "") {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name and address must be provided together")
	}

	s.RLock()
	e := s.engineMap[req.EngineName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for replica %s add", req.EngineName, req.ReplicaName)
	}

	var frontendSuspendResumeWrapper replicaAddFrontendSuspendResumeWrapper
	if efName != "" {
		log := logrus.WithFields(logrus.Fields{
			"engineName":     req.EngineName,
			"replicaName":    req.ReplicaName,
			"engineFrontend": efName,
		})
		frontendSuspendResumeWrapper = buildGRPCReplicaAddFrontendSuspendResumeWrapper(efName, efAddress, log)
	}

	if err := e.ReplicaAdd(spdkClient, req.ReplicaName, req.ReplicaAddress, req.FastSync, frontendSuspendResumeWrapper); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to add replica %s to engine %s: %v", req.ReplicaName, req.EngineName, err)
	}
	return &emptypb.Empty{}, nil
}

// EngineReplicaList returns all replicas for an engine
func (s *Server) EngineReplicaList(ctx context.Context, req *spdkrpc.EngineReplicaListRequest) (ret *spdkrpc.EngineReplicaListResponse, err error) {
	s.RLock()
	e := s.engineMap[req.EngineName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for replica list", req.EngineName)
	}

	replicas, err := e.ReplicaList(spdkClient)
	if err != nil {
		return nil, err
	}

	ret = &spdkrpc.EngineReplicaListResponse{
		Replicas: map[string]*spdkrpc.Replica{},
	}

	for _, r := range replicas {
		ret.Replicas[r.Name] = api.ReplicaToProtoReplica(r)
	}

	return ret, nil
}

func (s *Server) EngineReplicaDelete(ctx context.Context, req *spdkrpc.EngineReplicaDeleteRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	e := s.engineMap[req.EngineName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for replica %s with address %s delete", req.EngineName, req.ReplicaName, req.ReplicaAddress)
	}

	if err := e.ReplicaDelete(spdkClient, req.ReplicaName, req.ReplicaAddress); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineSnapshotCreate(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *spdkrpc.SnapshotResponse, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name are required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot creation", req.Name)
	}

	snapshotName, err := e.SnapshotCreate(spdkClient, req.SnapshotName)
	return &spdkrpc.SnapshotResponse{SnapshotName: snapshotName}, err
}

func (s *Server) EngineSnapshotDelete(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name and snapshot name are required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot deletion", req.Name)
	}

	if err := e.SnapshotDelete(spdkClient, req.SnapshotName); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineSnapshotRevert(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name and snapshot name are required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot revert", req.Name)
	}

	if err := e.SnapshotRevert(spdkClient, req.SnapshotName); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineSnapshotPurge(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name is required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot purge", req.Name)
	}

	if err := e.SnapshotPurge(spdkClient); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineSnapshotHash(ctx context.Context, req *spdkrpc.SnapshotHashRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name and snapshot name are required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot hash", req.Name)
	}

	if err := e.SnapshotHash(spdkClient, req.SnapshotName, req.Rehash); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineSnapshotHashStatus(ctx context.Context, req *spdkrpc.SnapshotHashStatusRequest) (ret *spdkrpc.EngineSnapshotHashStatusResponse, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name and snapshot name are required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot hash status", req.Name)
	}

	return e.SnapshotHashStatus(req.SnapshotName)
}

func (s *Server) EngineSnapshotClone(ctx context.Context, req *spdkrpc.EngineSnapshotCloneRequest) (ret *emptypb.Empty, err error) {
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: req.Name},
		util.Param{Name: "snapshotName", Value: req.SnapshotName},
		util.Param{Name: "srcEngineName", Value: req.SrcEngineName},
		util.Param{Name: "srcEngineAddress", Value: req.SrcEngineAddress},
	); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "%v", err)
	}

	s.RLock()
	e := s.engineMap[req.Name]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot clone", req.Name)
	}

	if err := e.SnapshotClone(req.SnapshotName, req.SrcEngineName, req.SrcEngineAddress, req.CloneMode); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) EngineBackupCreate(ctx context.Context, req *spdkrpc.BackupCreateRequest) (ret *spdkrpc.BackupCreateResponse, err error) {
	s.RLock()
	e := s.engineMap[req.EngineName]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for backup creation", req.EngineName)
	}

	recv, err := e.BackupCreate(req.BackupName, req.VolumeName, req.EngineName, req.SnapshotName, req.BackingImageName, req.BackingImageChecksum,
		req.Labels, req.BackupTarget, req.Credential, req.ConcurrentLimit, req.CompressionMethod, req.StorageClassName, e.SpecSize)
	if err != nil {
		return nil, err
	}
	return &spdkrpc.BackupCreateResponse{
		Backup:         recv.BackupName,
		IsIncremental:  recv.IsIncremental,
		ReplicaAddress: recv.ReplicaAddress,
	}, nil
}

func (s *Server) EngineBackupStatus(ctx context.Context, req *spdkrpc.BackupStatusRequest) (*spdkrpc.BackupStatusResponse, error) {
	s.RLock()
	e := s.engineMap[req.EngineName]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for backup creation", req.EngineName)
	}

	return e.BackupStatus(req.Backup, req.ReplicaAddress)
}

func (s *Server) EngineBackupRestore(ctx context.Context, req *spdkrpc.EngineBackupRestoreRequest) (ret *spdkrpc.EngineBackupRestoreResponse, err error) {
	logrus.WithFields(logrus.Fields{
		"backup":       req.BackupUrl,
		"engine":       req.EngineName,
		"snapshotName": req.SnapshotName,
		"concurrent":   req.ConcurrentLimit,
	}).Info("Restoring backup")

	s.RLock()
	e := s.engineMap[req.EngineName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for restoring backup", req.EngineName)
	}

	return e.BackupRestore(spdkClient, req.BackupUrl, req.EngineName, req.SnapshotName, req.Credential, req.ConcurrentLimit)
}

func (s *Server) EngineRestoreStatus(ctx context.Context, req *spdkrpc.RestoreStatusRequest) (*spdkrpc.RestoreStatusResponse, error) {
	s.RLock()
	e := s.engineMap[req.EngineName]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for backup creation", req.EngineName)
	}

	resp, err := e.RestoreStatus()
	if err != nil {
		err = errors.Wrapf(err, "failed to get restore status for engine %v", req.EngineName)
		return nil, grpcstatus.Errorf(grpccodes.Internal, "%v", err)
	}
	return resp, nil
}
