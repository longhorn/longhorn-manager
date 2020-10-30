package client

import (
	"strconv"

	"github.com/longhorn/longhorn-share-manager/pkg/rpc"
	"github.com/longhorn/longhorn-share-manager/pkg/types"
)

func ShareMapToRPC(obj map[string]*types.Share) *rpc.ShareListResponse {

	shares := map[string]*rpc.ShareResponse{}
	for vol, share := range obj {
		shares[vol] = ShareToRPC(share)
	}

	rsp := &rpc.ShareListResponse{
		Shares: shares,
	}
	return rsp
}

func ShareToRPC(obj *types.Share) *rpc.ShareResponse {
	rsp := &rpc.ShareResponse{
		Volume:   obj.Volume,
		ExportId: strconv.FormatUint(uint64(obj.ExportID), 10),
		State:    string(obj.State),
		Error:    obj.Error,
	}

	if obj.State == "" {
		rsp.State = string(types.ShareStateUnknown)
	}

	if obj.Error != "" {
		rsp.State = string(types.ShareStateError)
	}

	return rsp
}

func RPCToShare(obj *rpc.ShareResponse) *types.Share {
	id, _ := strconv.ParseUint(string(obj.ExportId), 10, 16)
	s := types.Share{
		Volume:   obj.Volume,
		ExportID: uint16(id),
		State:    types.ShareState(obj.State),
		Error:    obj.Error,
	}

	if obj.State == "" {
		s.State = types.ShareStateUnknown
	}

	if obj.Error != "" {
		s.State = types.ShareStateError
	}

	return &s
}

func RPCToShareList(obj *rpc.ShareListResponse) map[string]*types.Share {
	ret := map[string]*types.Share{}
	for name, p := range obj.Shares {
		ret[name] = RPCToShare(p)
	}
	return ret
}

type ShareStream struct {
	service *ServiceConnection
	stream  rpc.ShareManagerService_ShareWatchClient
}

func NewShareStream(service *ServiceConnection, stream rpc.ShareManagerService_ShareWatchClient) *ShareStream {
	return &ShareStream{service, stream}
}

func (s *ShareStream) Close() error {
	if s.service == nil {
		return nil
	}
	return s.service.Close()
}

func (s *ShareStream) Recv() (*types.Share, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		return nil, err
	}
	return RPCToShare(resp), nil
}

type LogStream struct {
	service *ServiceConnection
	stream  rpc.ShareManagerService_LogWatchClient
}

func NewLogStream(service *ServiceConnection, stream rpc.ShareManagerService_LogWatchClient) *LogStream {
	return &LogStream{service, stream}
}

func (s *LogStream) Close() error {
	if s.service == nil {
		return nil
	}
	return s.service.Close()
}

func (s *LogStream) Recv() (string, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		return "", err
	}
	return resp.Line, nil
}
