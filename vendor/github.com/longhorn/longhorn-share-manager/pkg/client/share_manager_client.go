package client

import (
	"context"

	"github.com/cockroachdb/errors"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	rpc "github.com/longhorn/types/pkg/generated/smrpc"

	"github.com/longhorn/longhorn-share-manager/pkg/types"
)

type ShareManagerClient struct {
	address string
	conn    *grpc.ClientConn
	client  rpc.ShareManagerServiceClient
	health  healthpb.HealthClient
}

func NewShareManagerClient(address string) (*ShareManagerClient, error) {
	// Disable gRPC service config discovery to prevent DNS flooding in Kubernetes
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithNoProxy(),
		grpc.WithDisableServiceConfig(),
	)

	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect share manager service to %v", address)
	}

	return &ShareManagerClient{
		address: address,
		conn:    conn,
		client:  rpc.NewShareManagerServiceClient(conn),
		health:  healthpb.NewHealthClient(conn),
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

func (c *ShareManagerClient) FilesystemResize() error {
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	_, err := c.client.FilesystemResize(ctx, &emptypb.Empty{})
	return err
}

func (c *ShareManagerClient) Unmount() error {
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	_, err := c.client.Unmount(ctx, &emptypb.Empty{})
	return err
}

func (c *ShareManagerClient) Mount() error {
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	_, err := c.client.Mount(ctx, &emptypb.Empty{})
	return err
}

func (c *ShareManagerClient) IsServing() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := c.health.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return false, err
	}

	return resp.GetStatus() == healthpb.HealthCheckResponse_SERVING, nil
}
