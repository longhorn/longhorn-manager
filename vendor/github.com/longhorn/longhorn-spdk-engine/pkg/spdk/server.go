package spdk

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

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

	diskCreateLock sync.Mutex
	hotplugActive  atomic.Bool // use atomic.Bool to avoid data races across goroutines.

	ctx context.Context

	spdkClient    *spdkclient.Client
	portAllocator *commonbitmap.Bitmap

	diskMap           map[string]*Disk
	replicaMap        map[string]*Replica
	engineMap         map[string]*Engine
	engineFrontendMap map[string]*EngineFrontend

	backupMap map[string]*Backup

	// We store BackingImage in each lvstore
	backingImageMap map[string]*BackingImage

	broadcasters map[types.InstanceType]*broadcaster.Broadcaster
	broadcastChs map[types.InstanceType]chan interface{}
	updateChs    map[types.InstanceType]chan interface{}

	currentBdevIostat *spdktypes.BdevIostatResponse
	bdevMetricMap     map[string]*spdkrpc.Metrics

	// metadataDir is the base path for persisting engine frontend records
	// (e.g. /var/lib/longhorn). If empty, persistence is disabled.
	metadataDir string
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
	for _, t := range []types.InstanceType{types.InstanceTypeReplica, types.InstanceTypeEngine, types.InstanceTypeEngineFrontend, types.InstanceTypeBackingImage} {
		broadcasters[t] = &broadcaster.Broadcaster{}
		broadcastChs[t] = make(chan interface{})
		updateChs[t] = make(chan interface{})
	}

	s := &Server{
		ctx: ctx,

		hotplugActive: atomic.Bool{},

		spdkClient:    cli,
		portAllocator: bitmap,

		diskMap: map[string]*Disk{},

		replicaMap:        map[string]*Replica{},
		engineMap:         map[string]*Engine{},
		engineFrontendMap: map[string]*EngineFrontend{},

		backupMap: map[string]*Backup{},

		backingImageMap: map[string]*BackingImage{},

		broadcasters: broadcasters,
		broadcastChs: broadcastChs,
		updateChs:    updateChs,

		metadataDir: types.MetadataDir,
	}
	s.hotplugActive.Store(true)

	if _, err := s.broadcasters[types.InstanceTypeReplica].Subscribe(ctx, s.replicaBroadcastConnector); err != nil {
		return nil, err
	}
	if _, err := s.broadcasters[types.InstanceTypeEngine].Subscribe(ctx, s.engineBroadcastConnector); err != nil {
		return nil, err
	}
	if _, err := s.broadcasters[types.InstanceTypeEngineFrontend].Subscribe(ctx, s.engineFrontendBroadcastConnector); err != nil {
		return nil, err
	}
	if _, err := s.broadcasters[types.InstanceTypeBackingImage].Subscribe(ctx, s.backingImageBroadcastConnector); err != nil {
		return nil, err
	}

	// Start broadcasting before recovery so that UpdateCh sends inside
	// RecoverFromHost do not block on the unbuffered channel.
	go s.broadcasting()

	s.recoverEngineFrontends()

	// TODO: There is no need to maintain the replica map in cache when we can use one SPDK JSON API call to fetch the Lvol tree/chain info
	go s.monitoring()

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
	defer func() {
		s.Unlock()
	}()

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

type verifyState struct {
	replicaMap            map[string]*Replica
	replicaMapForSync     map[string]*Replica
	engineMapForSync      map[string]*Engine
	engineFrontendForSync map[string]*EngineFrontend
	backingImageMap       map[string]*BackingImage
	backingImageForSync   map[string]*BackingImage
	spdkClient            *spdkclient.Client
}

func (s *Server) verify() (err error) {
	s.Lock()
	locked := true
	defer func() {
		if locked {
			s.Unlock()
		}
	}()

	state := s.newVerifyState()

	defer func() {
		s.handleVerifyError(err, state)
	}()

	s.trySelfHealHotplug()

	if err = s.rebuildCachedLvolObjects(state); err != nil {
		return err
	}

	if len(s.replicaMap) != len(state.replicaMap) {
		logrus.Infof("spdk gRPC server: replica map updated, map count is changed from %d to %d", len(s.replicaMap), len(state.replicaMap))
	}

	s.replicaMap = state.replicaMap
	s.backingImageMap = state.backingImageMap
	s.UpdateEngineMetrics()

	s.Unlock()
	locked = false

	return s.syncVerifiedObjects(state)
}

func (s *Server) newVerifyState() *verifyState {
	state := &verifyState{
		replicaMap:            map[string]*Replica{},
		replicaMapForSync:     map[string]*Replica{},
		engineMapForSync:      map[string]*Engine{},
		engineFrontendForSync: map[string]*EngineFrontend{},
		backingImageMap:       map[string]*BackingImage{},
		backingImageForSync:   map[string]*BackingImage{},
		spdkClient:            s.spdkClient,
	}

	for k, v := range s.replicaMap {
		state.replicaMap[k] = v
		state.replicaMapForSync[k] = v
	}
	for k, v := range s.engineMap {
		state.engineMapForSync[k] = v
	}
	for k, v := range s.engineFrontendMap {
		state.engineFrontendForSync[k] = v
	}
	for k, v := range s.backingImageMap {
		state.backingImageMap[k] = v
		state.backingImageForSync[k] = v
	}

	return state
}

func (s *Server) handleVerifyError(err error, state *verifyState) {
	if err == nil {
		return
	}
	if jsonrpc.IsJSONRPCRespErrorBrokenPipe(err) {
		logrus.WithError(err).Warn("spdk gRPC server: marking all non-stopped and non-error replicas and engines as error")
		for _, r := range state.replicaMapForSync {
			r.SetErrorState()
		}
		for _, e := range state.engineMapForSync {
			e.SetErrorState()
		}
		for _, ef := range state.engineFrontendForSync {
			ef.SetErrorState()
		}
	}
}

func (s *Server) trySelfHealHotplug() {
	// Self-heal: re-enable hotplug only if no disks are being created and the last enablement failed.
	isDiskCreating := false
	for _, disk := range s.diskMap {
		if disk.GetState() == DiskStateCreating {
			isDiskCreating = true
			break
		}
	}
	if !isDiskCreating && !s.hotplugActive.Load() {
		if success := setNvmeHotPlug(s.spdkClient, true); success {
			s.hotplugActive.Store(true)
		}
	}
}

func buildBdevLvolMap(bdevList []spdktypes.BdevInfo) map[string]*spdktypes.BdevInfo {
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
	return bdevLvolMap
}

func buildLvsUUIDNameMap(lvsList []spdktypes.LvstoreInfo) map[string]string {
	lvsUUIDNameMap := map[string]string{}
	for _, lvs := range lvsList {
		lvsUUIDNameMap[lvs.UUID] = lvs.Name
	}
	return lvsUUIDNameMap
}

func (s *Server) rebuildCachedLvolObjects(state *verifyState) error {
	bdevList, err := state.spdkClient.BdevGetBdevs("", 0)
	if err != nil {
		return err
	}
	bdevLvolMap := buildBdevLvolMap(bdevList)

	lvsList, err := state.spdkClient.BdevLvolGetLvstore("", "")
	if err != nil {
		return err
	}
	lvsUUIDNameMap := buildLvsUUIDNameMap(lvsList)

	// Detect if the lvol bdev is an uncached replica or backing image.
	for lvolName, bdevLvol := range bdevLvolMap {
		if bdevLvol.DriverSpecific.Lvol.Snapshot && !types.IsBackingImageSnapLvolName(lvolName) {
			continue
		}
		if types.IsBackingImageTempHead(lvolName) {
			if state.backingImageMap[types.GetBackingImageSnapLvolNameFromTempHeadLvolName(lvolName)] == nil {
				lvsUUID := bdevLvol.DriverSpecific.Lvol.LvolStoreUUID
				logrus.Infof("Found one backing image temp head lvol %v while there is no backing image record in the server", lvolName)
				if err := cleanupOrphanBackingImageTempHead(state.spdkClient, lvsUUIDNameMap[lvsUUID], lvolName); err != nil {
					logrus.WithError(err).Warnf("Failed to clean up orphan backing image temp head")
				}
			}
			continue
		}
		if state.replicaMap[lvolName] != nil {
			continue
		}
		if state.backingImageMap[lvolName] != nil {
			continue
		}
		if IsRebuildingLvol(lvolName) {
			if state.replicaMap[GetReplicaNameFromRebuildingLvolName(lvolName)] != nil {
				continue
			}
		}
		if IsCloningLvol(lvolName) {
			if state.replicaMap[GetReplicaNameFromCloningLvolName(lvolName)] != nil {
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
			expectedChecksum, err := GetSnapXattr(state.spdkClient, alias, types.LonghornBackingImageSnapshotAttrChecksum)
			if err != nil {
				logrus.WithError(err).Warnf("failed to retrieve checksum attribute for backing image snapshot %v", alias)
				continue
			}
			backingImageUUID, err := GetSnapXattr(state.spdkClient, alias, types.LonghornBackingImageSnapshotAttrUUID)
			if err != nil {
				logrus.WithError(err).Warnf("failed to retrieve backing image UUID attribute for snapshot %v", alias)
				continue
			}
			backingImage := NewBackingImage(s.ctx, backingImageName, backingImageUUID, lvsUUID, size, expectedChecksum, s.updateChs[types.InstanceTypeBackingImage])
			backingImage.Alias = alias
			backingImage.State = types.BackingImageStatePending
			state.backingImageForSync[lvolName] = backingImage
			state.backingImageMap[lvolName] = backingImage
		} else if IsProbablyReplicaName(lvolName) {
			lvsUUID := bdevLvol.DriverSpecific.Lvol.LvolStoreUUID
			specSize := bdevLvol.NumBlocks * uint64(bdevLvol.BlockSize)
			actualSize := bdevLvol.DriverSpecific.Lvol.NumAllocatedClusters * uint64(defaultClusterSize)
			state.replicaMap[lvolName] = NewReplica(s.ctx, lvolName, lvsUUIDNameMap[lvsUUID], lvsUUID, specSize, true, s.updateChs[types.InstanceTypeReplica])
			state.replicaMapForSync[lvolName] = state.replicaMap[lvolName]
			logrus.Infof("Detected one possible existing replica %s(%s) with disk %s(%s), spec size %d, actual size %d", bdevLvol.Aliases[0], bdevLvol.UUID, lvsUUIDNameMap[lvsUUID], lvsUUID, specSize, actualSize)
		}
	}

	// Remove replicas from the cache if their lvol bdevs are gone.
	for replicaName, r := range state.replicaMap {
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
				delete(state.replicaMap, replicaName)
				delete(state.replicaMapForSync, replicaName)
			}
		}
	}

	return nil
}

func (s *Server) syncVerifiedObjects(state *verifyState) error {
	for _, r := range state.replicaMapForSync {
		if err := r.Sync(state.spdkClient); err != nil && jsonrpc.IsJSONRPCRespErrorBrokenPipe(err) {
			return err
		}
	}

	for _, e := range state.engineMapForSync {
		if err := e.ValidateAndUpdate(state.spdkClient); err != nil && jsonrpc.IsJSONRPCRespErrorBrokenPipe(err) {
			return err
		}
	}

	for _, ef := range state.engineFrontendForSync {
		if err := ef.ValidateAndUpdate(state.spdkClient); err != nil && jsonrpc.IsJSONRPCRespErrorBrokenPipe(err) {
			return err
		}
	}

	for _, bi := range state.backingImageForSync {
		if err := bi.ValidateAndUpdate(state.spdkClient); err != nil {
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
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("spdk gRPC server: stopped broadcasting instances due to the context done")
			// Keep draining updateChs so that senders on unbuffered channels
			// do not block forever after broadcasting stops forwarding.
			// Other goroutines will eventually observe ctx.Done() and stop sending.
			for {
				select {
				case <-s.updateChs[types.InstanceTypeReplica]:
				case <-s.updateChs[types.InstanceTypeEngine]:
				case <-s.updateChs[types.InstanceTypeEngineFrontend]:
				case <-s.updateChs[types.InstanceTypeBackingImage]:
				}
			}
		case <-s.updateChs[types.InstanceTypeReplica]:
			s.broadcastChs[types.InstanceTypeReplica] <- nil
		case <-s.updateChs[types.InstanceTypeEngine]:
			s.broadcastChs[types.InstanceTypeEngine] <- nil
		case <-s.updateChs[types.InstanceTypeEngineFrontend]:
			s.broadcastChs[types.InstanceTypeEngineFrontend] <- nil
		case <-s.updateChs[types.InstanceTypeBackingImage]:
			s.broadcastChs[types.InstanceTypeBackingImage] <- nil
		}
	}
}

func (s *Server) Subscribe(instanceType types.InstanceType) (<-chan interface{}, error) {
	switch instanceType {
	case types.InstanceTypeEngine:
		return s.broadcasters[types.InstanceTypeEngine].Subscribe(context.TODO(), s.engineBroadcastConnector)
	case types.InstanceTypeEngineFrontend:
		return s.broadcasters[types.InstanceTypeEngineFrontend].Subscribe(context.TODO(), s.engineFrontendBroadcastConnector)
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

func (s *Server) engineFrontendBroadcastConnector() (chan interface{}, error) {
	return s.broadcastChs[types.InstanceTypeEngineFrontend], nil
}

func (s *Server) backingImageBroadcastConnector() (chan interface{}, error) {
	return s.broadcastChs[types.InstanceTypeBackingImage], nil
}

func (s *Server) isLvsExist(lvsUUID, lvsName string) (bool, error) {
	if lvsUUID == "" && lvsName == "" {
		return false, fmt.Errorf("either lvstore UUID or name must be provided")
	}

	name := ""
	uuid := ""

	if lvsUUID != "" {
		uuid = lvsUUID
	} else {
		name = lvsName
	}

	lvsList, err := s.spdkClient.BdevLvolGetLvstore(name, uuid)
	if err != nil {
		return false, err
	}

	if len(lvsList) == 0 {
		return false, fmt.Errorf("found zero lvstore with name %q and UUID %q", lvsName, lvsUUID)
	}

	return true, nil
}

func (s *Server) newReplica(req *spdkrpc.ReplicaCreateRequest) (*Replica, error) {
	s.Lock()
	defer func() {
		s.Unlock()
	}()

	r, ok := s.replicaMap[req.Name]
	if ok {
		r.Lock()
		if req.SpecSize != 0 {
			r.SpecSize = req.SpecSize
		}
		if req.LvsName != "" {
			r.LvsName = req.LvsName
		}
		if req.LvsUuid != "" {
			r.LvsUUID = req.LvsUuid
		}
		r.Unlock()
		return r, nil
	}

	exists, err := s.isLvsExist(req.LvsUuid, req.LvsName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to check lvstore %v(%v) existence for replica %v creation", req.LvsName, req.LvsUuid, req.Name)
	}
	if !exists {
		return nil, fmt.Errorf("lvstore %v(%v) does not exist for replica %v creation", req.LvsName, req.LvsUuid, req.Name)
	}
	return NewReplica(s.ctx, req.Name, req.LvsName, req.LvsUuid, req.SpecSize, true, s.updateChs[types.InstanceTypeReplica]), nil
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

func toSwitchOverGRPCError(err error, format string, args ...interface{}) error {
	code := grpccodes.Internal

	// Preserve downstream gRPC code when available.
	if statusErr, ok := grpcstatus.FromError(errors.UnwrapAll(err)); ok {
		code = statusErr.Code()
	} else {
		switch {
		case errors.Is(err, ErrSwitchOverTargetInvalidInput):
			code = grpccodes.InvalidArgument
		case errors.Is(err, ErrSwitchOverTargetPrecondition):
			code = grpccodes.FailedPrecondition
		case errors.Is(err, ErrSwitchOverTargetEngineNotFound):
			code = grpccodes.NotFound
		case errors.Is(err, context.DeadlineExceeded):
			code = grpccodes.DeadlineExceeded
		case errors.Is(err, context.Canceled):
			code = grpccodes.Canceled
		}
	}

	return grpcstatus.Error(code, errors.Wrapf(err, format, args...).Error())
}

// buildGRPCReplicaAddFrontendSuspendResumeWrapper builds a replicaAddFrontendSuspendResumeWrapper that
// calls back to the EngineFrontend on a (potentially remote) node via gRPC
// for suspend/resume around the work step.
//
// If the EngineFrontend is unreachable (node down, pod deleted, etc.), the
// wrapper proceeds with work() without suspension. This is safe because an
// unreachable frontend means there is no active I/O to quiesce, and not
// running the work would leak SPDK resources (detach controller, stop expose).
func buildGRPCReplicaAddFrontendSuspendResumeWrapper(efName, efAddress string, log *logrus.Entry) replicaAddFrontendSuspendResumeWrapper {
	return func(work func() error) error {
		efClient, err := GetServiceClient(efAddress)
		if err != nil {
			// Cannot connect to the EF node at all — proceed without suspension.
			log.WithError(err).Warnf("Engine frontend %s at %s is unreachable, proceeding without suspension", efName, efAddress)
			return work()
		}
		defer func() {
			if errClose := efClient.Close(); errClose != nil {
				log.WithError(errClose).Warnf("Failed to close engine frontend SPDK client for %s", efName)
			}
		}()

		// Suspend the frontend before running the work.
		// If suspend fails for any reason (EF deleted, node down, unimplemented
		// frontend type), proceed without suspension rather than aborting. The
		// data has already been copied; not running the work is worse than a
		// brief I/O disruption.
		suspended := false
		if err := efClient.EngineFrontendSuspend(efName); err != nil {
			log.WithError(err).Warnf("Failed to suspend engine frontend %s before replica add work, proceeding without suspension", efName)
		} else {
			suspended = true
		}

		workErr := work()

		// Resume the frontend after the work.
		// If resume fails (EF disappeared during work, or internal error),
		// log a warning but do not override workErr — the replica-add result
		// is determined by work(), not by resume. longhorn-manager will
		// detect the stuck-suspended EF and handle recovery.
		if suspended {
			if resumeErr := efClient.EngineFrontendResume(efName); resumeErr != nil {
				log.WithError(resumeErr).Errorf("Failed to resume engine frontend %s after replica add work (work succeeded: %v)", efName, workErr == nil)
			}
		}

		return workErr
	}
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
		exists, err := s.isLvsExist(req.LvsUuid, "")
		if err != nil || !exists {
			return nil, err
		}
		s.backingImageMap[backingImageSnapLvolName] = NewBackingImage(s.ctx, req.Name, req.BackingImageUuid, req.LvsUuid, req.Size, req.Checksum, s.updateChs[types.InstanceTypeBackingImage])
	}

	return s.backingImageMap[backingImageSnapLvolName], nil
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

func setNvmeHotPlug(spdkClient *spdkclient.Client, enable bool) (success bool) {
	// default value: 100000 microseconds = 0.1 seconds
	_, hotPlugErr := spdkClient.BdevNvmeSetHotplug(enable, 100000)
	if hotPlugErr != nil {
		logrus.WithError(hotPlugErr).Warnf("Failed to set nvme hotplug to %v", enable)
		return false
	}
	return true
}

// engineFrontendByVolumeName returns the first engine frontend that matches
// the given volume name, or nil if none exists. Caller must hold s.RLock or
// s.Lock.
func (s *Server) engineFrontendByVolumeName(volumeName string) *EngineFrontend {
	for _, ef := range s.engineFrontendMap {
		if ef.VolumeName == volumeName {
			return ef
		}
	}
	return nil
}

func toEngineFrontendCreateGRPCError(err error, format string, args ...any) error {
	code := grpccodes.Internal

	// Check sentinel errors first — they are the most specific indicators
	// of what went wrong and should take priority over any embedded gRPC
	// status that might exist deeper in the error chain.
	switch {
	case errors.Is(err, ErrEngineFrontendCreateInvalidArgument):
		code = grpccodes.InvalidArgument
	case errors.Is(err, ErrEngineFrontendCreatePrecondition):
		code = grpccodes.FailedPrecondition
	case errors.Is(err, context.DeadlineExceeded):
		code = grpccodes.DeadlineExceeded
	case errors.Is(err, context.Canceled):
		code = grpccodes.Canceled
	default:
		// Fall back to any embedded gRPC status.
		if statusErr, ok := grpcstatus.FromError(errors.UnwrapAll(err)); ok {
			code = statusErr.Code()
		}
	}

	return grpcstatus.Error(code, errors.Wrapf(err, format, args...).Error())
}

func toEngineFrontendLifecycleGRPCError(err error, format string, args ...any) error {
	code := grpccodes.Internal

	switch {
	case errors.Is(err, ErrEngineFrontendLifecyclePrecondition), errors.Is(err, ErrSwitchOverTargetPrecondition):
		code = grpccodes.FailedPrecondition
	case errors.Is(err, ErrEngineFrontendLifecycleUnimplemented):
		code = grpccodes.Unimplemented
	case errors.Is(err, context.DeadlineExceeded):
		code = grpccodes.DeadlineExceeded
	case errors.Is(err, context.Canceled):
		code = grpccodes.Canceled
	default:
		if statusErr, ok := grpcstatus.FromError(errors.UnwrapAll(err)); ok {
			code = statusErr.Code()
		}
	}

	return grpcstatus.Error(code, errors.Wrapf(err, format, args...).Error())
}

func toExpansionGRPCError(err error, format string, args ...interface{}) error {
	code := grpccodes.Internal

	// Preserve downstream gRPC code when available.
	if statusErr, ok := grpcstatus.FromError(errors.UnwrapAll(err)); ok {
		code = statusErr.Code()
	} else {
		switch {
		case errors.Is(err, ErrExpansionInProgress), errors.Is(err, ErrRestoringInProgress):
			code = grpccodes.FailedPrecondition
		case errors.Is(err, ErrExpansionInvalidSize):
			code = grpccodes.InvalidArgument
		case errors.Is(err, context.DeadlineExceeded):
			code = grpccodes.DeadlineExceeded
		case errors.Is(err, context.Canceled):
			code = grpccodes.Canceled
		}
	}

	return grpcstatus.Error(code, errors.Wrapf(err, format, args...).Error())
}

// GetEngineStruct returns the internal Engine struct.
// This is for testing purposes only to allow access to internal fields and methods not exposed via RPC.
func (s *Server) GetEngineStruct(name string) *Engine {
	s.RLock()
	defer s.RUnlock()
	return s.engineMap[name]
}

// GetReplicaStruct returns the internal Replica struct.
// This is for testing purposes only to allow tests to inspect or manipulate internal replica state.
func (s *Server) GetReplicaStruct(name string) *Replica {
	s.RLock()
	defer s.RUnlock()
	return s.replicaMap[name]
}

// recoverEngineFrontends loads persisted engine frontend records from disk
// and attempts to recover them by detecting existing NVMe initiators on the host.
// This is called during server startup to restore state after instance-manager restart.
func (s *Server) recoverEngineFrontends() {
	if s.metadataDir == "" {
		return
	}

	records, err := loadEngineFrontendRecords(s.metadataDir)
	if err != nil {
		logrus.WithError(err).Error("Failed to load engine frontend records for recovery")
		return
	}

	if len(records) == 0 {
		return
	}

	logrus.Infof("Recovering %d engine frontend(s) from persisted records", len(records))

	s.Lock()
	spdkClient := s.spdkClient
	for _, record := range records {
		if _, exists := s.engineFrontendMap[record.Name]; exists {
			logrus.Infof("Engine frontend %s already exists in map, skipping recovery", record.Name)
			continue
		}

		ef := NewEngineFrontend(record.Name, record.EngineName, record.VolumeName,
			record.Frontend, record.SpecSize, 0, 0, s.updateChs[types.InstanceTypeEngineFrontend])
		ef.metadataDir = s.metadataDir
		if ef.NvmeTcpFrontend != nil {
			if record.TargetIP != "" {
				ef.NvmeTcpFrontend.TargetIP = record.TargetIP
				ef.EngineIP = record.TargetIP
			}
			if record.TargetPort != 0 {
				ef.NvmeTcpFrontend.TargetPort = record.TargetPort
			}
		}

		s.engineFrontendMap[record.Name] = ef

		logrus.Infof("Recovered engine frontend %s for volume %s from persisted record", record.Name, record.VolumeName)
	}
	s.Unlock()

	// Attempt to recover each frontend's initiator state from the host.
	// This is done outside the server lock to avoid holding it during potentially
	// slow NVMe device discovery operations.
	for _, record := range records {
		s.RLock()
		ef := s.engineFrontendMap[record.Name]
		s.RUnlock()

		if ef == nil {
			continue
		}

		if err := ef.RecoverFromHost(spdkClient); err != nil {
			if errors.Is(err, ErrRecoverDeviceNotFound) {
				logrus.Warnf("Removing engine frontend %s from map: device not found on host", record.Name)
			} else {
				logrus.WithError(err).Warnf("Removing engine frontend %s from map: recovery failed", record.Name)
			}

			// Properly shut down the frontend instance (close stopCh,
			// clean up any partially-recovered initiator, remove the
			// persisted record) before removing it from the map.
			// This follows the same pattern as the race-loser cleanup
			// in EngineFrontendCreate.
			if deleteErr := ef.Delete(spdkClient); deleteErr != nil {
				logrus.WithError(deleteErr).Warnf("Failed to clean up engine frontend %s during recovery removal", record.Name)
			}

			s.Lock()
			delete(s.engineFrontendMap, record.Name)
			s.Unlock()
		}
	}
}
