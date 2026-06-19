package client

import (
	"crypto/tls"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/longhorn/types/pkg/generated/spdkrpc"
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

// NewSPDKClient creates an SPDK gRPC client connected to the given service URL without TLS.
func NewSPDKClient(serviceURL string) (*SPDKClient, error) {
	return NewSPDKClientWithTLSConfig(serviceURL, nil)
}

// NewSPDKClientWithTLSConfig creates an SPDK gRPC client using a pre-built TLS configuration.
// If tlsConfig is nil, the connection is established without TLS.
func NewSPDKClientWithTLSConfig(serviceURL string, tlsConfig *tls.Config) (*SPDKClient, error) {
	var transportCredentials grpc.DialOption
	if tlsConfig == nil {
		// Disable gRPC service config discovery to prevent DNS flooding in Kubernetes
		transportCredentials = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		transportCredentials = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	connection, err := grpc.NewClient(
		serviceURL,
		transportCredentials,
		grpc.WithNoProxy(),
		grpc.WithDisableServiceConfig(),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot connect to SPDKService %v", serviceURL)
	}

	return &SPDKClient{
		serviceURL: serviceURL,
		SPDKServiceContext: SPDKServiceContext{
			cc:      connection,
			service: spdkrpc.NewSPDKServiceClient(connection),
		},
	}, nil
}
