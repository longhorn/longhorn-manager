package client

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	rpc "github.com/longhorn/longhorn-share-manager/pkg/rpc"
	"github.com/longhorn/longhorn-share-manager/pkg/types"
)

type ShareManagerClient struct {
	address string
	conn    *grpc.ClientConn
	client  rpc.ShareManagerServiceClient
}

func NewShareManagerClient(address string) (*ShareManagerClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect share manager service to %v", address)
	}

	return &ShareManagerClient{
		address: address,
		conn:    conn,
		client:  rpc.NewShareManagerServiceClient(conn),
	}, nil
}

func (c *ShareManagerClient) Close() error {
	if c.conn == nil {
		return nil
	}

	return c.conn.Close()
}

func (c *ShareManagerClient) FilesystemTrim(encryptedDevice bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	_, err := c.client.FilesystemTrim(ctx, &rpc.FilesystemTrimRequest{EncryptedDevice: encryptedDevice})
	return err
}

func (c *ShareManagerClient) FilesystemMount() error {
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	_, err := c.client.FilesystemMount(ctx, &empty.Empty{})
	return err
}

func (c *ShareManagerClient) FilesystemMountStatus() (*rpc.FilesystemMountStatusResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	return c.client.FilesystemMountStatus(ctx, &empty.Empty{})
}
