package client

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-share-manager/pkg/rpc"
)

type ServiceConnection struct {
	Connection *grpc.ClientConn
	Client     rpc.ShareManagerServiceClient
	Context    context.Context
	Cancel     context.CancelFunc
}

func NewServiceConnection(address string) (*ServiceConnection, error) {
	return NewServiceConnectionWithTimeout(address, 0)
}

func NewServiceConnectionWithTimeout(address string, timeout time.Duration) (*ServiceConnection, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to share manager service on %v: %v", address, err)
	}

	client := rpc.NewShareManagerServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}

	return &ServiceConnection{
		Connection: conn,
		Client:     client,
		Context:    ctx,
		Cancel:     cancel,
	}, nil
}

func (srv *ServiceConnection) Close() error {
	if srv.Cancel != nil {
		srv.Cancel()
	}
	if srv.Connection != nil {
		if err := srv.Connection.Close(); err != nil {
			return err
		}
	}
	return nil
}
