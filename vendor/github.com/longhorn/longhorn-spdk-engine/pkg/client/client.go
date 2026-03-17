package client

import (
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/longhorn/types/pkg/generated/spdkrpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Close closes the underlying gRPC connection for the SPDK service context.
func (c *SPDKServiceContext) Close() error {
	if c.cc != nil {
		if err := c.cc.Close(); err != nil {
			return err
		}
		c.cc = nil
	}
	return nil
}

func (c *SPDKClient) getSPDKServiceClient() spdkrpc.SPDKServiceClient {
	return c.service
}

// NewSPDKClient creates an SPDK gRPC client connected to the given service URL.
func NewSPDKClient(serviceURL string) (*SPDKClient, error) {
	getSPDKServiceContext := func(serviceUrl string) (SPDKServiceContext, error) {
		// Disable gRPC service config discovery to prevent DNS flooding in Kubernetes
		connection, err := grpc.NewClient(
			serviceUrl,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithNoProxy(),
			grpc.WithDisableServiceConfig(),
		)
		if err != nil {
			return SPDKServiceContext{}, errors.Wrapf(err, "cannot connect to SPDKService %v", serviceUrl)
		}

		return SPDKServiceContext{
			cc:      connection,
			service: spdkrpc.NewSPDKServiceClient(connection),
		}, nil
	}

	serviceContext, err := getSPDKServiceContext(serviceURL)
	if err != nil {
		return nil, err
	}

	return &SPDKClient{
		serviceURL:         serviceURL,
		SPDKServiceContext: serviceContext,
	}, nil
}
