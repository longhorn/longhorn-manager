package spdk

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// BackingImageGet will return the backing image information in the server.
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

	s.RLock()
	bi := s.backingImageMap[backingImageSnapLvolName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if bi != nil {
		if bi.BackingImageUUID == req.BackingImageUuid {
			return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "backing image %v already exists", req.Name)
		}

		logrus.Infof("Found backing image exists with different backing image UUID %v, deleting it", bi.BackingImageUUID)

		if err := bi.Delete(spdkClient, s.portAllocator); err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to delete backing image %v in lvs %v with different UUID", req.Name, req.LvsUuid).Error())
		}

		s.Lock()
		delete(s.backingImageMap, backingImageSnapLvolName)
		s.Unlock()
	}

	newBI, err := s.newBackingImage(req)
	if err != nil {
		return nil, err
	}

	s.RLock()
	spdkClient = s.spdkClient
	s.RUnlock()

	return newBI.Create(spdkClient, s.portAllocator, req.FromAddress, req.SrcLvsUuid)
}

// BackingImageDelete will delete the backing image.
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

// BackingImageGet will return the backing image information in the server.
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
	spdkClient := s.spdkClient
	s.RUnlock()

	if bi == nil {
		lvsName, err := GetLvsNameByUUID(spdkClient, req.LvsUuid)
		if err != nil {
			return nil, grpcstatus.Errorf(grpccodes.NotFound, "failed to get the lvs name with lvs uuid %v", req.LvsUuid)
		}

		if lvsName != "" {
			backingImageSnapLvolAlias := spdktypes.GetLvolAlias(lvsName, backingImageSnapLvolName)
			bdevLvolList, err := spdkClient.BdevLvolGet(backingImageSnapLvolAlias, 0)
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

// BackingImageList will return the backing image list in the server.
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

// BackingImageWatch will watch the backing image update.
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

// BackingImageExpose will expose the backing image as a new lvol.
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

// BackingImageUnexpose will unexpose the backing image.
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
