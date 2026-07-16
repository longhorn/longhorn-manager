package spdk

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/types/pkg/generated/spdkrpc"
)

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
