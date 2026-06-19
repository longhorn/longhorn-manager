package engineapi

import (
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/types"
)

// imClientProber is satisfied by all IM gRPC client types
// (*ProcessManagerClient, *InstanceServiceClient, *DiskServiceClient, *ProxyClient).
type imClientProber interface {
	CheckConnection() error
	Close() error
}

// newIMClient tries TLS first and falls back to a plain client on any TLS
// failure. This preserves backward-compatible behaviour for clusters that are
// in transition: TLS is attempted but not required.
// TODO: remove the plain fallback once all clusters are fully mTLS-configured.
func newIMClient[C imClientProber](
	buildTLS func() (C, error),
	buildPlain func() (C, error),
	extraProbe func(C) error,
	logger logrus.FieldLogger,
	imName, imIP, serviceName string,
) (C, error) {
	var zero C

	tlsClient, tlsErr := buildTLS()
	if tlsErr == nil {
		if connErr := tlsClient.CheckConnection(); connErr != nil {
			_ = tlsClient.Close()
			tlsErr = connErr
		} else if extraProbe != nil {
			if probeErr := extraProbe(tlsClient); probeErr != nil {
				_ = tlsClient.Close()
				tlsErr = probeErr
			}
		}
	}
	if tlsErr == nil {
		return tlsClient, nil
	}

	logger.WithError(tlsErr).Tracef("Falling back to non-TLS %v client for %v IP %v",
		serviceName, imName, imIP)
	plain, plainErr := buildPlain()
	if plainErr != nil {
		return zero, errors.Wrapf(plainErr,
			"failed to initialize %v client for %v IP %v", serviceName, imName, imIP)
	}
	if connErr := plain.CheckConnection(); connErr != nil {
		_ = plain.Close()
		return zero, errors.Wrapf(connErr,
			"failed to check %v client connection for %v IP %v", serviceName, imName, imIP)
	}
	if extraProbe != nil {
		if probeErr := extraProbe(plain); probeErr != nil {
			_ = plain.Close()
			return zero, errors.Wrapf(probeErr,
				"failed to probe %v client for %v IP %v", serviceName, imName, imIP)
		}
	}
	return plain, nil
}

// imTLSFiles returns the standard certificate paths and peer name for all IM gRPC
// client connections.
func imTLSFiles() (caFile, certFile, keyFile, peerName string) {
	return filepath.Join(types.TLSDirectoryInContainer, types.TLSCAFile),
		filepath.Join(types.TLSDirectoryInContainer, types.TLSCertFile),
		filepath.Join(types.TLSDirectoryInContainer, types.TLSKeyFile),
		types.TLSPeerName
}
