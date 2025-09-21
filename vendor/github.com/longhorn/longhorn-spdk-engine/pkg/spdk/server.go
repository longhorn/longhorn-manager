package spdk

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	butil "github.com/longhorn/backupstore/util"
	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util/broadcaster"
)

const (
	MonitorInterval = 3 * time.Second
)

type Server struct {
	spdkrpc.UnimplementedSPDKServiceServer
	sync.RWMutex

	ctx context.Context

	spdkClient    *spdkclient.Client
	portAllocator *commonbitmap.Bitmap

	diskMap    map[string]*Disk
	replicaMap map[string]*Replica
	engineMap  map[string]*Engine

	backupMap map[string]*Backup

	// We store BackingImage in each lvstore
	backingImageMap map[string]*BackingImage

	broadcasters map[types.InstanceType]*broadcaster.Broadcaster
	broadcastChs map[types.InstanceType]chan interface{}
	updateChs    map[types.InstanceType]chan interface{}

	currentBdevIostat *spdktypes.BdevIostatResponse
	bdevMetricMap     map[string]*spdkrpc.Metrics
}

func NewServer(ctx context.Context, portStart, portEnd int32) (*Server, error) {
	cli, err := spdkclient.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	bitmap, err := commonbitmap.NewBitmap(portStart, portEnd)
	if err != nil {
		return nil, err
	}

	if _, err = cli.BdevNvmeSetOptions(
		replicaCtrlrLossTimeoutSec,
		replicaReconnectDelaySec,
		replicaFastIOFailTimeoutSec,
		replicaTransportAckTimeout,
		replicaKeepAliveTimeoutMs); err != nil {
		return nil, errors.Wrap(err, "failed to set NVMe options")
	}

	broadcasters := map[types.InstanceType]*broadcaster.Broadcaster{}
	broadcastChs := map[types.InstanceType]chan interface{}{}
	updateChs := map[types.InstanceType]chan interface{}{}
	for _, t := range []types.InstanceType{types.InstanceTypeReplica, types.InstanceTypeEngine, types.InstanceTypeBackingImage} {
		broadcasters[t] = &broadcaster.Broadcaster{}
		broadcastChs[t] = make(chan interface{})
		updateChs[t] = make(chan interface{})
	}

	s := &Server{
		ctx: ctx,

		spdkClient:    cli,
		portAllocator: bitmap,

		diskMap: map[string]*Disk{},

		replicaMap: map[string]*Replica{},
		engineMap:  map[string]*Engine{},

		backupMap: map[string]*Backup{},

		backingImageMap: map[string]*BackingImage{},

		broadcasters: broadcasters,
		broadcastChs: broadcastChs,
		updateChs:    updateChs,
	}

	if _, err := s.broadcasters[types.InstanceTypeReplica].Subscribe(ctx, s.replicaBroadcastConnector); err != nil {
		return nil, err
	}
	if _, err := s.broadcasters[types.InstanceTypeEngine].Subscribe(ctx, s.engineBroadcastConnector); err != nil {
		return nil, err
	}
	if _, err := s.broadcasters[types.InstanceTypeBackingImage].Subscribe(ctx, s.backingImageBroadcastConnector); err != nil {
		return nil, err
	}

	// TODO: There is no need to maintain the replica map in cache when we can use one SPDK JSON API call to fetch the Lvol tree/chain info
	go s.monitoring()
	go s.broadcasting()

	return s, nil
}

func (s *Server) monitoring() {
	ticker := time.NewTicker(MonitorInterval)
	defer ticker.Stop()

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("spdk gRPC server: stopped monitoring replicas due to the context done")
			done = true
		case <-ticker.C:
			err := s.verify()
			if err == nil {
				break
			}

			logrus.WithError(err).Errorf("spdk gRPC server: failed to verify and update replica cache, will retry later")

			if jsonrpc.IsJSONRPCRespErrorBrokenPipe(err) || jsonrpc.IsJSONRPCRespErrorInvalidCharacter(err) {
				err = s.tryEnsureSPDKTgtConnectionHealthy()
				if err != nil {
					logrus.WithError(err).Error("spdk gRPC server: failed to ensure spdk_tgt connection healthy")
				}
			}
		}
		if done {
			break
		}
	}
}

func (s *Server) tryEnsureSPDKTgtConnectionHealthy() error {
	running, err := util.IsSPDKTargetProcessRunning()
	if err != nil {
		return errors.Wrap(err, "failed to check spdk_tgt is running")
	}
	if !running {
		return errors.New("spdk_tgt is not running")
	}

	logrus.Info("spdk gRPC server: reconnecting to spdk_tgt")
	return s.clientReconnect()
}

func (s *Server) clientReconnect() error {
	s.Lock()
	defer s.Unlock()

	oldClient := s.spdkClient

	client, err := spdkclient.NewClient(s.ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create new SPDK client")
	}
	s.spdkClient = client

	// Try the best effort to close the old client after a new client is created
	err = oldClient.Close()
	if err != nil {
		logrus.WithError(err).Warn("Failed to close old SPDK client")
	}
	return nil
}

func (s *Server) verify() (err error) {
	replicaMap := map[string]*Replica{}
	replicaMapForSync := map[string]*Replica{}
	engineMapForSync := map[string]*Engine{}
	backingImageMap := map[string]*BackingImage{}
	backingImageMapForSync := map[string]*BackingImage{}

	s.Lock()
	for k, v := range s.replicaMap {
		replicaMap[k] = v
		replicaMapForSync[k] = v
	}
	for k, v := range s.engineMap {
		engineMapForSync[k] = v
	}
	for k, v := range s.backingImageMap {
		backingImageMap[k] = v
		backingImageMapForSync[k] = v
	}

	spdkClient := s.spdkClient

	defer func() {
		if err == nil {
			return
		}
		if jsonrpc.IsJSONRPCRespErrorBrokenPipe(err) {
			logrus.WithError(err).Warn("spdk gRPC server: marking all non-stopped and non-error replicas and engines as error")
			for _, r := range replicaMapForSync {
				r.SetErrorState()
			}
			for _, e := range engineMapForSync {
				e.SetErrorState()
			}
		}
	}()

	// Detect if the lvol bdev is an uncached replica or backing image.
	// But cannot detect if a RAID bdev is an engine since:
	//   1. we don't know the frontend
	//   2. RAID bdevs are not persist objects in SPDK. After spdk_tgt start/restart, there is no RAID bdev hence there is no need to do detection.
	// TODO: May need to cache Disks as well.
	bdevList, err := spdkClient.BdevGetBdevs("", 0)
	if err != nil {
		s.Unlock()
		return err
	}
	bdevLvolMap := map[string]*spdktypes.BdevInfo{}
	for idx := range bdevList {
		bdev := &bdevList[idx]
		if spdktypes.GetBdevType(bdev) != spdktypes.BdevTypeLvol {
			continue
		}
		if len(bdev.Aliases) != 1 {
			continue
		}
		bdevLvolMap[spdktypes.GetLvolNameFromAlias(bdev.Aliases[0])] = bdev
	}
	lvsList, err := spdkClient.BdevLvolGetLvstore("", "")
	if err != nil {
		s.Unlock()
		return err
	}
	lvsUUIDNameMap := map[string]string{}
	for _, lvs := range lvsList {
		lvsUUIDNameMap[lvs.UUID] = lvs.Name
	}
	// Backing image lvol name will be "bi-${biName}-disk-${lvsUUID}"
	// Backing image temp lvol name will be "bi-${biName}-disk-${lvsUUID}-temp-head"
	for lvolName, bdevLvol := range bdevLvolMap {
		if bdevLvol.DriverSpecific.Lvol.Snapshot && !types.IsBackingImageSnapLvolName(lvolName) {
			continue
		}
		if types.IsBackingImageTempHead(lvolName) {
			if s.backingImageMap[types.GetBackingImageSnapLvolNameFromTempHeadLvolName(lvolName)] == nil {
				lvsUUID := bdevLvol.DriverSpecific.Lvol.LvolStoreUUID
				logrus.Infof("Found one backing image temp head lvol %v while there is no backing image record in the server", lvolName)
				if err := cleanupOrphanBackingImageTempHead(spdkClient, lvsUUIDNameMap[lvsUUID], lvolName); err != nil {
					logrus.WithError(err).Warnf("Failed to clean up orphan backing image temp head")
				}
			}
			continue
		}
		if replicaMap[lvolName] != nil {
			continue
		}
		if s.backingImageMap[lvolName] != nil {
			continue
		}
		if IsRebuildingLvol(lvolName) {
			if replicaMap[GetReplicaNameFromRebuildingLvolName(lvolName)] != nil {
				continue
			}
		}
		if IsCloningLvol(lvolName) {
			if replicaMap[GetReplicaNameFromCloningLvolName(lvolName)] != nil {
				continue
			}
		}
		if types.IsBackingImageSnapLvolName(lvolName) {
			lvsUUID := bdevLvol.DriverSpecific.Lvol.LvolStoreUUID
			backingImageName, _, err := ExtractBackingImageAndDiskUUID(lvolName)
			if err != nil {
				logrus.WithError(err).Warnf("failed to extract backing image name and disk UUID from lvol name %v", lvolName)
				continue
			}
			size := bdevLvol.NumBlocks * uint64(bdevLvol.BlockSize)
			alias := bdevLvol.Aliases[0]
			expectedChecksum, err := GetSnapXattr(spdkClient, alias, types.LonghornBackingImageSnapshotAttrChecksum)
			if err != nil {
				logrus.WithError(err).Warnf("failed to retrieve checksum attribute for backing image snapshot %v", alias)
				continue
			}
			backingImageUUID, err := GetSnapXattr(spdkClient, alias, types.LonghornBackingImageSnapshotAttrUUID)
			if err != nil {
				logrus.WithError(err).Warnf("failed to retrieve backing image UUID attribute for snapshot %v", alias)
				continue
			}
			backingImage := NewBackingImage(s.ctx, backingImageName, backingImageUUID, lvsUUID, size, expectedChecksum, s.updateChs[types.InstanceTypeBackingImage])
			backingImage.Alias = alias
			// For uncahced backing image, we set the state to pending first, so we can distinguish it from the cached but starting backing image
			backingImage.State = types.BackingImageStatePending
			backingImageMapForSync[lvolName] = backingImage
			backingImageMap[lvolName] = backingImage
		} else if IsProbablyReplicaName(lvolName) {
			lvsUUID := bdevLvol.DriverSpecific.Lvol.LvolStoreUUID
			specSize := bdevLvol.NumBlocks * uint64(bdevLvol.BlockSize)
			actualSize := bdevLvol.DriverSpecific.Lvol.NumAllocatedClusters * uint64(defaultClusterSize)
			replicaMap[lvolName] = NewReplica(s.ctx, lvolName, lvsUUIDNameMap[lvsUUID], lvsUUID, specSize, actualSize, s.updateChs[types.InstanceTypeReplica])
			replicaMapForSync[lvolName] = replicaMap[lvolName]
			logrus.Infof("Detected one possible existing replica %s(%s) with disk %s(%s), spec size %d, actual size %d", bdevLvol.Aliases[0], bdevLvol.UUID, lvsUUIDNameMap[lvsUUID], lvsUUID, specSize, actualSize)
		}
	}
	for replicaName, r := range replicaMap {
		// Try the best to avoid eliminating broken replicas or rebuilding replicas
		if bdevLvolMap[r.Name] == nil {
			if r.IsRebuilding() {
				continue
			}
			noReplicaLvol := true
			for lvolName := range bdevLvolMap {
				if IsReplicaLvol(r.Name, lvolName) {
					noReplicaLvol = false
					break
				}
			}
			if noReplicaLvol {
				delete(replicaMap, replicaName)
				delete(replicaMapForSync, replicaName)
				continue
			}
		}
	}
	if len(s.replicaMap) != len(replicaMap) {
		logrus.Infof("spdk gRPC server: replica map updated, map count is changed from %d to %d", len(s.replicaMap), len(replicaMap))
	}
	s.replicaMap = replicaMap
	s.backingImageMap = backingImageMap
	s.UpdateEngineMetrics()
	s.Unlock()

	for _, r := range replicaMapForSync {
		err = r.Sync(spdkClient)
		if err != nil && jsonrpc.IsJSONRPCRespErrorBrokenPipe(err) {
			return err
		}
	}

	for _, e := range engineMapForSync {
		err = e.ValidateAndUpdate(spdkClient)
		if err != nil && jsonrpc.IsJSONRPCRespErrorBrokenPipe(err) {
			return err
		}
	}

	for _, bi := range backingImageMapForSync {
		err = bi.ValidateAndUpdate(spdkClient)
		if err != nil {
			if jsonrpc.IsJSONRPCRespErrorBrokenPipe(err) {
				return err
			}
			continue
		}
	}

	// TODO: send update signals if there is a Replica/Replica change

	return nil
}

func (s *Server) broadcasting() {
	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("spdk gRPC server: stopped broadcasting instances due to the context done")
			done = true
		case <-s.updateChs[types.InstanceTypeReplica]:
			s.broadcastChs[types.InstanceTypeReplica] <- nil
		case <-s.updateChs[types.InstanceTypeEngine]:
			s.broadcastChs[types.InstanceTypeEngine] <- nil
		case <-s.updateChs[types.InstanceTypeBackingImage]:
			s.broadcastChs[types.InstanceTypeBackingImage] <- nil
		}
		if done {
			break
		}
	}
}

func (s *Server) Subscribe(instanceType types.InstanceType) (<-chan interface{}, error) {
	switch instanceType {
	case types.InstanceTypeEngine:
		return s.broadcasters[types.InstanceTypeEngine].Subscribe(context.TODO(), s.engineBroadcastConnector)
	case types.InstanceTypeReplica:
		return s.broadcasters[types.InstanceTypeReplica].Subscribe(context.TODO(), s.replicaBroadcastConnector)
	case types.InstanceTypeBackingImage:
		return s.broadcasters[types.InstanceTypeBackingImage].Subscribe(context.TODO(), s.backingImageBroadcastConnector)
	}
	return nil, fmt.Errorf("invalid instance type %v for subscription", instanceType)
}

func (s *Server) replicaBroadcastConnector() (chan interface{}, error) {
	return s.broadcastChs[types.InstanceTypeReplica], nil
}

func (s *Server) engineBroadcastConnector() (chan interface{}, error) {
	return s.broadcastChs[types.InstanceTypeEngine], nil
}

func (s *Server) backingImageBroadcastConnector() (chan interface{}, error) {
	return s.broadcastChs[types.InstanceTypeBackingImage], nil
}

func (s *Server) checkLvsReadiness(lvsUUID, lvsName string) (bool, error) {
	var err error
	var lvsList []spdktypes.LvstoreInfo

	if lvsUUID != "" {
		lvsList, err = s.spdkClient.BdevLvolGetLvstore("", lvsUUID)
	} else if lvsName != "" {
		lvsList, err = s.spdkClient.BdevLvolGetLvstore(lvsName, "")
	}
	if err != nil {
		return false, err
	}

	if len(lvsList) == 0 {
		return false, fmt.Errorf("found zero lvstore with name %v and UUID %v", lvsName, lvsUUID)
	}

	return true, nil
}

func (s *Server) newReplica(req *spdkrpc.ReplicaCreateRequest) (*Replica, error) {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.replicaMap[req.Name]; !ok {
		ready, err := s.checkLvsReadiness(req.LvsUuid, req.LvsName)
		if err != nil || !ready {
			return nil, err
		}
		return NewReplica(s.ctx, req.Name, req.LvsName, req.LvsUuid, req.SpecSize, 0, s.updateChs[types.InstanceTypeReplica]), nil
	}

	return s.replicaMap[req.Name], nil
}

func (s *Server) ReplicaCreate(ctx context.Context, req *spdkrpc.ReplicaCreateRequest) (ret *spdkrpc.Replica, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.LvsName == "" && req.LvsUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "lvs name or lvs UUID are required")
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

func (s *Server) getBackingImage(backingImageName, lvsUUID string) (backingImage *BackingImage, err error) {
	backingImageSnapLvolName := GetBackingImageSnapLvolName(backingImageName, lvsUUID)

	s.RLock()
	backingImage = s.backingImageMap[backingImageSnapLvolName]
	s.RUnlock()

	if backingImage == nil {
		return nil, grpcstatus.Error(grpccodes.NotFound, "failed to find the backing image in the spdk server")
	}

	return backingImage, nil
}

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

func (s *Server) ReplicaGet(ctx context.Context, req *spdkrpc.ReplicaGetRequest) (ret *spdkrpc.Replica, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %v", req.Name)
	}

	return r.Get(), nil
}

func (s *Server) ReplicaExpand(ctx context.Context, req *spdkrpc.ReplicaExpandRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %v", req.Name)
	}

	if err := r.Expand(s.spdkClient, req.Size); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

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

func (s *Server) ReplicaSnapshotCreate(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *spdkrpc.Replica, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and snapshot name are required")
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

func (s *Server) ReplicaSnapshotDelete(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and snapshot name are required")
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

	if err = r.RebuildingDstShallowCopyStart(spdkClient, req.SnapshotName); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

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

func (s *Server) EngineCreate(ctx context.Context, req *spdkrpc.EngineCreateRequest) (ret *spdkrpc.Engine, err error) {
	if req.Name == "" || req.VolumeName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name and volume name are required")
	}
	if req.SpecSize == 0 {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine spec size is required")
	}
	if req.Frontend != types.FrontendSPDKTCPBlockdev && req.Frontend != types.FrontendSPDKTCPNvmf && req.Frontend != types.FrontendEmpty && req.Frontend != types.FrontendUBLK {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend is required")
	}

	s.Lock()
	e, ok := s.engineMap[req.Name]
	if ok {
		// Check if the engine already exists.
		// If the engine exists and the initiator address is the same as the target address, return AlreadyExists error.
		if localTargetExists(e) && req.InitiatorAddress == req.TargetAddress {
			s.Unlock()
			return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "engine %v already exists", req.Name)
		}
	}

	if e == nil {
		s.engineMap[req.Name] = NewEngine(req.Name, req.VolumeName, req.Frontend, req.SpecSize, s.updateChs[types.InstanceTypeEngine])
		e = s.engineMap[req.Name]
	}

	spdkClient := s.spdkClient
	s.Unlock()

	return e.Create(spdkClient, req.ReplicaAddressMap, req.PortCount, s.portAllocator, req.InitiatorAddress, req.TargetAddress, req.SalvageRequested)
}

func localTargetExists(e *Engine) bool {
	return e.NvmeTcpFrontend != nil && e.NvmeTcpFrontend.Port != 0 && e.NvmeTcpFrontend.TargetPort != 0
}

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

func (s *Server) EngineSuspend(ctx context.Context, req *spdkrpc.EngineSuspendRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name is required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for suspension", req.Name)
	}

	err = e.Suspend(s.spdkClient)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to suspend engine %v", req.Name).Error())
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineResume(ctx context.Context, req *spdkrpc.EngineResumeRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name is required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for resumption", req.Name)
	}

	err = e.Resume(s.spdkClient)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to resume engine %v", req.Name).Error())
	}

	return &emptypb.Empty{}, nil
}
func (s *Server) EngineExpand(ctx context.Context, req *spdkrpc.EngineExpandRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name is required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for expansion", req.Name)
	}

	if types.IsUblkFrontend(e.Frontend) {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "cannot expand ublk frontend engine %v", req.Name)
	}

	err = e.Expand(s.spdkClient, req.Size, s.portAllocator)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to expand engine %v", req.Name).Error())
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineSwitchOverTarget(ctx context.Context, req *spdkrpc.EngineSwitchOverTargetRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" || req.TargetAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name and target address are required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for target switchover", req.Name)
	}

	err = e.SwitchOverTarget(s.spdkClient, req.TargetAddress)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to switch over target for engine %v", req.Name).Error())
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineDeleteTarget(ctx context.Context, req *spdkrpc.EngineDeleteTargetRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name is required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %s for target deletion", req.Name)
	}

	defer func() {
		if err == nil {
			s.Lock()
			// Only delete the engine if both initiator (e.Port) and target (e.TargetPort) are not exists.
			if e.NvmeTcpFrontend.Port == 0 && e.NvmeTcpFrontend.TargetPort == 0 {
				e.log.Info("Deleting engine %s", req.Name)
				delete(s.engineMap, req.Name)
			}
			s.Unlock()
		}
	}()

	err = e.DeleteTarget(s.spdkClient, s.portAllocator)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to delete target for engine %v", req.Name).Error())
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineGet(ctx context.Context, req *spdkrpc.EngineGetRequest) (ret *spdkrpc.Engine, err error) {
	s.RLock()
	e := s.engineMap[req.Name]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v", req.Name)
	}

	return e.Get(), nil
}

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

func (s *Server) EngineReplicaAdd(ctx context.Context, req *spdkrpc.EngineReplicaAddRequest) (ret *emptypb.Empty, err error) {
	if req.ReplicaName == "" || req.ReplicaAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and address are required")
	}
	s.RLock()
	e := s.engineMap[req.EngineName]
	spdkClient := s.spdkClient
	s.RUnlock()

	log := logrus.WithFields(logrus.Fields{
		"engine":      req.EngineName,
		"replicaName": req.ReplicaName,
	})
	log.Info("Starting replica add")

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for replica %s with address %s add", req.EngineName, req.ReplicaName, req.ReplicaAddress)
	}

	return &emptypb.Empty{}, e.ReplicaAdd(spdkClient, req.ReplicaName, req.ReplicaAddress)
}

func (s *Server) EngineReplicaList(ctx context.Context, req *spdkrpc.EngineReplicaListRequest) (ret *spdkrpc.EngineReplicaListResponse, err error) {
	s.RLock()
	e := s.engineMap[req.EngineName]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for replica list", req.EngineName)
	}

	replicas, err := e.ReplicaList(s.spdkClient)
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

func (s *Server) EngineBackupStatus(ctx context.Context, req *spdkrpc.BackupStatusRequest) (*spdkrpc.BackupStatusResponse, error) {
	s.RLock()
	e := s.engineMap[req.EngineName]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for backup creation", req.EngineName)
	}

	return e.BackupStatus(req.Backup, req.ReplicaAddress)
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

func (s *Server) EngineBackupRestore(ctx context.Context, req *spdkrpc.EngineBackupRestoreRequest) (ret *spdkrpc.EngineBackupRestoreResponse, err error) {
	logrus.WithFields(logrus.Fields{
		"backup":       req.BackupUrl,
		"engine":       req.EngineName,
		"snapshotName": req.SnapshotName,
		"concurrent":   req.ConcurrentLimit,
	}).Info("Restoring backup")

	s.RLock()
	e := s.engineMap[req.EngineName]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for restoring backup", req.EngineName)
	}

	return e.BackupRestore(s.spdkClient, req.BackupUrl, req.EngineName, req.SnapshotName, req.Credential, req.ConcurrentLimit)
}

func (s *Server) ReplicaBackupRestore(ctx context.Context, req *spdkrpc.ReplicaBackupRestoreRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	replica := s.replicaMap[req.ReplicaName]
	defer s.RUnlock()

	if replica == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %v for restoring backup %v", req.ReplicaName, req.BackupUrl)
	}

	err = replica.BackupRestore(s.spdkClient, req.BackupUrl, req.SnapshotName, req.Credential, req.ConcurrentLimit)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
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

func (s *Server) ReplicaRestoreStatus(ctx context.Context, req *spdkrpc.ReplicaRestoreStatusRequest) (ret *spdkrpc.ReplicaRestoreStatusResponse, err error) {
	s.RLock()
	defer s.RUnlock()

	replica, ok := s.replicaMap[req.ReplicaName]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %v", req.ReplicaName)
	}

	if replica.restore == nil {
		return &spdkrpc.ReplicaRestoreStatusResponse{
			ReplicaName:    replica.Name,
			ReplicaAddress: net.JoinHostPort(replica.restore.ip, strconv.Itoa(int(replica.restore.port))),
			IsRestoring:    false,
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

func (s *Server) DiskCreate(ctx context.Context, req *spdkrpc.DiskCreateRequest) (*spdkrpc.Disk, error) {
	s.Lock()
	spdkClient := s.spdkClient

	disk, exists := s.diskMap[req.DiskName]
	if exists {
		if disk.State == DiskStateReady {
			s.Unlock()
			return disk.DiskGet(spdkClient, req.DiskName, req.DiskPath, req.DiskDriver)
		}
		s.Unlock()

		return &spdkrpc.Disk{
			State: string(disk.State),
		}, nil
	}

	disk = NewDisk(req.DiskName, req.DiskUuid, req.DiskPath, req.DiskDriver, req.BlockSize)
	s.diskMap[req.DiskName] = disk
	s.Unlock()

	go func(d *Disk, req *spdkrpc.DiskCreateRequest) {
		if err := d.DiskCreate(spdkClient, req.DiskName, req.DiskUuid, req.DiskPath, req.DiskDriver, req.BlockSize); err != nil {
			logrus.WithError(err).Errorf("Failed to create disk %s(%s) path %s", req.DiskName, req.DiskUuid, req.DiskPath)
			return
		}

		logrus.Infof("Disk %v is created, start scanning", req.DiskName)

		timer := time.NewTimer(3 * time.Minute)
		defer timer.Stop()
		ticker := time.NewTicker(MonitorInterval)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				logrus.Infof("SPDK gRPC server: context done before scanning disk %s(%s) path %s",
					req.DiskName, req.DiskUuid, req.DiskPath)
				return
			case <-timer.C:
				logrus.Infof("SPDK gRPC server: timeout (3m) scanning disk %s(%s) path %s",
					req.DiskName, req.DiskUuid, req.DiskPath)
				return
			case <-ticker.C:
				if err := s.verify(); err == nil {
					logrus.Infof("SPDK gRPC server: successfully scanned disk %s(%s) path %s",
						req.DiskName, req.DiskUuid, req.DiskPath)
					return
				}
			}
		}
	}(disk, req)

	return &spdkrpc.Disk{
		State: string(disk.State),
	}, nil
}

func (s *Server) DiskDelete(ctx context.Context, req *spdkrpc.DiskDeleteRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	disk := s.diskMap[req.DiskName]
	spdkClient := s.spdkClient
	s.RUnlock()

	defer func() {
		if err == nil {
			s.Lock()
			delete(s.diskMap, req.DiskName)
			s.Unlock()
		}
	}()

	if disk != nil {
		return disk.DiskDelete(spdkClient, req.DiskName, req.DiskUuid, req.DiskPath, req.DiskDriver)
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) DiskGet(ctx context.Context, req *spdkrpc.DiskGetRequest) (ret *spdkrpc.Disk, err error) {
	s.RLock()
	disk := s.diskMap[req.DiskName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if disk == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find disk %v", req.DiskName)
	}

	return disk.DiskGet(spdkClient, req.DiskName, req.DiskPath, req.DiskDriver)
}

func (s *Server) LogSetLevel(ctx context.Context, req *spdkrpc.LogSetLevelRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	spdkClient := s.spdkClient
	s.RUnlock()

	err = svcLogSetLevel(spdkClient, req.Level)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) LogSetFlags(ctx context.Context, req *spdkrpc.LogSetFlagsRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	spdkClient := s.spdkClient
	s.RUnlock()

	err = svcLogSetFlags(spdkClient, req.Flags)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) LogGetLevel(ctx context.Context, req *emptypb.Empty) (ret *spdkrpc.LogGetLevelResponse, err error) {
	s.RLock()
	spdkClient := s.spdkClient
	s.RUnlock()

	level, err := svcLogGetLevel(spdkClient)
	if err != nil {
		return nil, err
	}

	return &spdkrpc.LogGetLevelResponse{
		Level: level,
	}, nil
}

func (s *Server) LogGetFlags(ctx context.Context, req *emptypb.Empty) (ret *spdkrpc.LogGetFlagsResponse, err error) {
	s.RLock()
	spdkClient := s.spdkClient
	s.RUnlock()

	flags, err := svcLogGetFlags(spdkClient)
	if err != nil {
		return nil, err
	}

	return &spdkrpc.LogGetFlagsResponse{
		Flags: flags,
	}, nil
}

func (s *Server) VersionDetailGet(context.Context, *emptypb.Empty) (*spdkrpc.VersionDetailGetReply, error) {
	// TODO: Implement this
	return &spdkrpc.VersionDetailGetReply{
		Version: &spdkrpc.VersionOutput{},
	}, nil
}

func (s *Server) newBackingImage(req *spdkrpc.BackingImageCreateRequest) (*BackingImage, error) {
	s.Lock()
	defer s.Unlock()

	// The backing image key is in this form "bi-%s-disk-%s" to distinguish different disks.
	backingImageSnapLvolName := GetBackingImageSnapLvolName(req.Name, req.LvsUuid)
	if _, ok := s.backingImageMap[backingImageSnapLvolName]; !ok {
		ready, err := s.checkLvsReadiness(req.LvsUuid, "")
		if err != nil || !ready {
			return nil, err
		}
		s.backingImageMap[backingImageSnapLvolName] = NewBackingImage(s.ctx, req.Name, req.BackingImageUuid, req.LvsUuid, req.Size, req.Checksum, s.updateChs[types.InstanceTypeBackingImage])
	}

	return s.backingImageMap[backingImageSnapLvolName], nil
}

func (s *Server) BackingImageCreate(ctx context.Context, req *spdkrpc.BackingImageCreateRequest) (ret *spdkrpc.BackingImage, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "backing image name is required")
	}
	if req.BackingImageUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "backing image UUID is required")
	}
	if req.Size == uint64(0) {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "backing image size is required")
	}
	if req.LvsUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "lvs UUID is required")
	}
	if req.Checksum == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "checksum is required")
	}

	// Don't recreate the backing image
	backingImageSnapLvolName := GetBackingImageSnapLvolName(req.Name, req.LvsUuid)
	if bi, ok := s.backingImageMap[backingImageSnapLvolName]; ok {
		if bi.BackingImageUUID == req.BackingImageUuid {
			return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "backing image %v already exists", req.Name)
		}

		logrus.Infof("Found backing image exists with different backing image UUID %v, deleting it", bi.BackingImageUUID)

		if err := bi.Delete(s.spdkClient, s.portAllocator); err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to delete backing image %v in lvs %v with different UUID", req.Name, req.LvsUuid).Error())
		}

		s.Lock()
		delete(s.backingImageMap, backingImageSnapLvolName)
		s.Unlock()
	}

	bi, err := s.newBackingImage(req)
	if err != nil {
		return nil, err
	}

	s.RLock()
	spdkClient := s.spdkClient
	s.RUnlock()

	return bi.Create(spdkClient, s.portAllocator, req.FromAddress, req.SrcLvsUuid)
}

func (s *Server) BackingImageDelete(ctx context.Context, req *spdkrpc.BackingImageDeleteRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "backing image name is required")
	}
	if req.LvsUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "lvs UUID is required")
	}

	s.RLock()
	bi := s.backingImageMap[GetBackingImageSnapLvolName(req.Name, req.LvsUuid)]
	spdkClient := s.spdkClient
	s.RUnlock()

	defer func() {
		if err == nil {
			s.Lock()
			delete(s.backingImageMap, GetBackingImageSnapLvolName(req.Name, req.LvsUuid))
			s.Unlock()
		}
	}()

	if bi != nil {
		if err := bi.Delete(spdkClient, s.portAllocator); err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to delete backing image %v in lvs %v", req.Name, req.LvsUuid).Error())
		}
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) BackingImageGet(ctx context.Context, req *spdkrpc.BackingImageGetRequest) (ret *spdkrpc.BackingImage, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "backing image name is required")
	}
	if req.LvsUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "lvs UUID is required")
	}

	backingImageSnapLvolName := GetBackingImageSnapLvolName(req.Name, req.LvsUuid)

	s.RLock()
	bi := s.backingImageMap[backingImageSnapLvolName]
	s.RUnlock()

	if bi == nil {
		lvsName, err := GetLvsNameByUUID(s.spdkClient, req.LvsUuid)
		if err != nil {
			return nil, grpcstatus.Errorf(grpccodes.NotFound, "failed to get the lvs name with lvs uuid %v", req.LvsUuid)
		}

		if lvsName != "" {
			backingImageSnapLvolAlias := spdktypes.GetLvolAlias(lvsName, backingImageSnapLvolName)
			bdevLvolList, err := s.spdkClient.BdevLvolGet(backingImageSnapLvolAlias, 0)
			if err != nil {
				return nil, grpcstatus.Errorf(grpccodes.NotFound, "got error %v when getting lvol %v in the lvs %v", err, req.Name, req.LvsUuid)
			}
			if len(bdevLvolList) != 1 {
				return nil, grpcstatus.Errorf(grpccodes.NotFound, "zero or multiple lvols with alias %s found when finding backing image %v in lvs %v", backingImageSnapLvolAlias, req.Name, req.LvsUuid)
			}
			// If we can get the lvol, verify() will reconstruct the backing image record in the server, should inform the caller
			return nil, grpcstatus.Errorf(grpccodes.NotFound, "backing image %v lvol found in the lvs %v but failed to find the record in the server", req.Name, req.LvsUuid)
		}
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find backing image %v in lvs %v", req.Name, req.LvsUuid)
	}

	return bi.Get(), nil
}

func (s *Server) BackingImageList(ctx context.Context, req *emptypb.Empty) (ret *spdkrpc.BackingImageListResponse, err error) {
	backingImageMap := map[string]*BackingImage{}
	res := map[string]*spdkrpc.BackingImage{}

	s.RLock()
	for k, v := range s.backingImageMap {
		backingImageMap[k] = v
	}
	s.RUnlock()

	// backingImageName is in the form of "bi-%s-disk-%s"
	for backingImageName, bi := range backingImageMap {
		res[backingImageName] = bi.Get()
	}

	return &spdkrpc.BackingImageListResponse{BackingImages: res}, nil
}

func (s *Server) BackingImageWatch(req *emptypb.Empty, srv spdkrpc.SPDKService_BackingImageWatchServer) error {
	responseCh, err := s.Subscribe(types.InstanceTypeBackingImage)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			logrus.WithError(err).Error("SPDK service backing image watch errored out")
		} else {
			logrus.Info("SPDK service backing image watch ended successfully")
		}
	}()
	logrus.Info("Started new SPDK service backing image update watch")

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("spdk gRPC server: stopped backing image watch due to the context done")
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

func (s *Server) BackingImageExpose(ctx context.Context, req *spdkrpc.BackingImageGetRequest) (ret *spdkrpc.BackingImageExposeResponse, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "backing image name is required")
	}
	if req.LvsUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "lvs UUID is required")
	}
	s.RLock()
	bi := s.backingImageMap[GetBackingImageSnapLvolName(req.Name, req.LvsUuid)]
	spdkClient := s.spdkClient
	s.RUnlock()

	if bi == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find backing image %v in lvs %v", req.Name, req.LvsUuid)
	}

	exposedSnapshotLvolAddress, err := bi.BackingImageExpose(spdkClient, s.portAllocator)
	if err != nil {
		return nil, err
	}
	return &spdkrpc.BackingImageExposeResponse{ExposedSnapshotLvolAddress: exposedSnapshotLvolAddress}, nil

}

func (s *Server) BackingImageUnexpose(ctx context.Context, req *spdkrpc.BackingImageGetRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "backing image name is required")
	}
	if req.LvsUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "lvs UUID is required")
	}
	s.RLock()
	bi := s.backingImageMap[GetBackingImageSnapLvolName(req.Name, req.LvsUuid)]
	spdkClient := s.spdkClient
	s.RUnlock()

	if bi == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find backing image %v in lvs %v", req.Name, req.LvsUuid)
	}

	err = bi.BackingImageUnexpose(spdkClient, s.portAllocator)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to unexpose backing image %v in lvs %v", req.Name, req.LvsUuid).Error())
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) UpdateEngineMetrics() {
	previousBdevIostat := s.currentBdevIostat
	bdevIostat, err := s.spdkClient.BdevGetIostat("", false)
	if err != nil {
		logrus.WithError(err).Error("failed to get bdev iostat")
		return
	}
	s.currentBdevIostat = bdevIostat
	// If this is the first execution, there is no previous data, so exit
	if previousBdevIostat == nil {
		return
	}
	// Calculate the elapsed time in ticks
	tickElapsed := s.currentBdevIostat.Ticks - previousBdevIostat.Ticks
	if tickElapsed == 0 {
		return
	}
	// Convert ticks to seconds
	elapsedSeconds := float64(tickElapsed) / float64(s.currentBdevIostat.TickRate)

	// Convert previous Bdev data into a map for quick lookup
	prevBdevMap := make(map[string]spdktypes.BdevStats)
	for _, prevBdev := range previousBdevIostat.Bdevs {
		prevBdevMap[prevBdev.Name] = prevBdev
	}

	// Initialize a new BdevMetricMap
	s.bdevMetricMap = make(map[string]*spdkrpc.Metrics)
	for _, bdev := range s.currentBdevIostat.Bdevs {
		prevBdev, exists := prevBdevMap[bdev.Name]
		if !exists {
			continue
		}

		// Calculate differences in operations and data
		readOpsDiff := bdev.NumReadOps - prevBdev.NumReadOps
		writeOpsDiff := bdev.NumWriteOps - prevBdev.NumWriteOps
		bytesReadDiff := bdev.BytesRead - prevBdev.BytesRead
		bytesWrittenDiff := bdev.BytesWritten - prevBdev.BytesWritten
		readLatencyDiff := bdev.ReadLatencyTicks - prevBdev.ReadLatencyTicks
		writeLatencyDiff := bdev.WriteLatencyTicks - prevBdev.WriteLatencyTicks

		// Convert latency from ticks to nanoseconds
		readLatency := calculateLatencyInNs(readLatencyDiff, readOpsDiff, s.currentBdevIostat.TickRate)
		writeLatency := calculateLatencyInNs(writeLatencyDiff, writeOpsDiff, s.currentBdevIostat.TickRate)

		s.bdevMetricMap[bdev.Name] = &spdkrpc.Metrics{
			ReadIOPS:        uint64(float64(readOpsDiff) / elapsedSeconds),
			WriteIOPS:       uint64(float64(writeOpsDiff) / elapsedSeconds),
			ReadThroughput:  uint64(float64(bytesReadDiff) / elapsedSeconds),
			WriteThroughput: uint64(float64(bytesWrittenDiff) / elapsedSeconds),
			ReadLatency:     readLatency,
			WriteLatency:    writeLatency,
		}
	}
}

// Converts latency from ticks to nanoseconds
func calculateLatencyInNs(latencyTicks, opsDiff, tickRate uint64) uint64 {
	if opsDiff == 0 {
		return 0 // Prevent division by zero
	}
	return uint64((float64(latencyTicks) / float64(opsDiff)) * (1e9 / float64(tickRate)))
}

func (s *Server) MetricsGet(ctx context.Context, req *spdkrpc.MetricsRequest) (ret *spdkrpc.Metrics, err error) {
	s.RLock()
	defer s.RUnlock()
	m, ok := s.bdevMetricMap[req.Name]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine metrics: %s", req.Name)
	}
	return m, nil
}
