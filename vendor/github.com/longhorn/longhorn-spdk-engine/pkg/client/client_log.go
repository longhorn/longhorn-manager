package client

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/types/pkg/generated/spdkrpc"
)

// LogSetLevel sets the server log level.
func (c *SPDKClient) LogSetLevel(level string) error {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.LogSetLevel(ctx, &spdkrpc.LogSetLevelRequest{
		Level: level,
	})
	return err
}

// LogSetFlags enables the specified server log flags.
func (c *SPDKClient) LogSetFlags(flags string) error {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.LogSetFlags(ctx, &spdkrpc.LogSetFlagsRequest{
		Flags: flags,
	})
	return err
}

// LogGetLevel returns the current server log level.
func (c *SPDKClient) LogGetLevel() (string, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.LogGetLevel(ctx, &emptypb.Empty{})
	if err != nil {
		return "", err
	}
	return resp.Level, nil
}

// LogGetFlags returns the currently enabled server log flags.
func (c *SPDKClient) LogGetFlags() (string, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.LogGetFlags(ctx, &emptypb.Empty{})
	if err != nil {
		return "", err
	}
	return resp.Flags, nil
}

// MetricsGet returns metrics for the specified object.
func (c *SPDKClient) MetricsGet(name string) (*spdkrpc.Metrics, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get engine metrics: missing required parameter")
	}
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()
	resp, err := client.MetricsGet(ctx, &spdkrpc.MetricsRequest{
		Name: name,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get engine %v metrics", name)
	}
	return resp, nil
}
