package client

import (
	"fmt"

	"github.com/longhorn/longhorn-share-manager/pkg/meta"
	"github.com/longhorn/longhorn-share-manager/pkg/rpc"
	"github.com/longhorn/longhorn-share-manager/pkg/types"
)

type ShareManagerClient struct {
	address string
}

func NewShareManagerClient(address string) *ShareManagerClient {
	return &ShareManagerClient{
		address: address,
	}
}

func (cli *ShareManagerClient) ShareCreate(volume string) (*types.Share, error) {
	if volume == "" {
		return nil, fmt.Errorf("failed to create share: missing required parameter volume")
	}

	service, err := NewServiceConnectionWithTimeout(cli.address, types.GRPCServiceTimeout)
	if err != nil {
		return nil, err
	}
	defer service.Close()

	rsp, err := service.Client.ShareCreate(service.Context, &rpc.ShareCreateRequest{Volume: volume})
	if err != nil {
		return nil, fmt.Errorf("failed to create share %v: %v", volume, err)
	}
	return RPCToShare(rsp), nil
}

func (cli *ShareManagerClient) ShareDelete(volume string) (*types.Share, error) {
	if volume == "" {
		return nil, fmt.Errorf("failed to delete share: missing required parameter volume")
	}

	service, err := NewServiceConnectionWithTimeout(cli.address, types.GRPCServiceTimeout)
	if err != nil {
		return nil, err
	}
	defer service.Close()

	rsp, err := service.Client.ShareDelete(service.Context, &rpc.ShareDeleteRequest{Volume: volume})
	if err != nil {
		return nil, fmt.Errorf("failed to delete share %v: %v", volume, err)
	}
	return RPCToShare(rsp), nil
}

func (cli *ShareManagerClient) ShareGet(volume string) (*types.Share, error) {
	if volume == "" {
		return nil, fmt.Errorf("failed to get share: missing required parameter volume")
	}

	service, err := NewServiceConnectionWithTimeout(cli.address, types.GRPCServiceTimeout)
	if err != nil {
		return nil, err
	}
	defer service.Close()

	rsp, err := service.Client.ShareGet(service.Context, &rpc.ShareGetRequest{Volume: volume})
	if err != nil {
		return nil, fmt.Errorf("failed to get share for volume %v: %v", volume, err)
	}
	return RPCToShare(rsp), nil
}

func (cli *ShareManagerClient) ShareList() (map[string]*types.Share, error) {
	service, err := NewServiceConnectionWithTimeout(cli.address, types.GRPCServiceTimeout)
	if err != nil {
		return nil, err
	}
	defer service.Close()

	rsp, err := service.Client.ShareList(service.Context, &rpc.ShareListRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list shares: %v", err)
	}
	return RPCToShareList(rsp), nil
}

func (cli *ShareManagerClient) ShareWatch() (*ShareStream, error) {
	service, err := NewServiceConnection(cli.address)
	if err != nil {
		return nil, err
	}

	// Don't cleanup the Client here, we don't know when the user will be done with the Stream.
	// Pass it to the wrapper and allow the user to take care of it.
	stream, err := service.Client.ShareWatch(service.Context, &rpc.ShareWatchRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get share stream: %v", err)
	}

	return NewShareStream(service, stream), nil
}

func (cli *ShareManagerClient) LogWatch() (*LogStream, error) {
	service, err := NewServiceConnection(cli.address)
	if err != nil {
		return nil, err
	}

	// Don't cleanup the Client here, we don't know when the user will be done with the Stream.
	// Pass it to the wrapper and allow the user to take care of it.
	stream, err := service.Client.LogWatch(service.Context, &rpc.LogWatchRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get log stream: %v", err)
	}
	return NewLogStream(service, stream), nil
}

func (cli *ShareManagerClient) VersionGet() (*meta.VersionOutput, error) {
	service, err := NewServiceConnectionWithTimeout(cli.address, types.GRPCServiceTimeout)
	if err != nil {
		return nil, err
	}
	defer service.Close()

	resp, err := service.Client.VersionGet(service.Context, &rpc.VersionRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %v", err)
	}
	return &meta.VersionOutput{
		Version:   resp.Version,
		GitCommit: resp.GitCommit,
		BuildDate: resp.BuildDate,

		APIVersion:    int(resp.ApiVersion),
		APIMinVersion: int(resp.ApiMinVersion),
	}, nil
}
