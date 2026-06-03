package engineapi

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type fakeIMClient struct {
	checkErr error
	closed   bool
}

func (f *fakeIMClient) CheckConnection() error { return f.checkErr }
func (f *fakeIMClient) Close() error           { f.closed = true; return nil }

func TestNewIMClientWithTLSFallback_FallsBackWhenTLSCheckConnectionFails(t *testing.T) {
	tlsClient := &fakeIMClient{checkErr: errors.New("tls: handshake failure")}
	plainClient := &fakeIMClient{}

	got, err := newIMClient(
		func() (*fakeIMClient, error) { return tlsClient, nil },
		func() (*fakeIMClient, error) { return plainClient, nil },
		nil,
		logrus.New(), "test-im", "10.0.0.1", "test-service",
	)

	require.NoError(t, err)
	require.Equal(t, plainClient, got, "fallback must return the plain client")
	require.True(t, tlsClient.closed, "TLS client must be closed before falling back")
}

func TestNewIMClientWithTLSFallback_ReturnsTLSClientOnSuccess(t *testing.T) {
	tlsClient := &fakeIMClient{}
	plainClient := &fakeIMClient{}

	got, err := newIMClient(
		func() (*fakeIMClient, error) { return tlsClient, nil },
		func() (*fakeIMClient, error) { return plainClient, nil },
		nil,
		logrus.New(), "test-im", "10.0.0.1", "test-service",
	)

	require.NoError(t, err)
	require.Equal(t, tlsClient, got, "must return TLS client when TLS succeeds")
	require.False(t, tlsClient.closed, "TLS client must not be closed on success")
}

func TestNewIMClientWithTLSFallback_ExtraProbeCalledOnFallback(t *testing.T) {
	tlsClient := &fakeIMClient{checkErr: errors.New("tls: connection refused")}
	plainClient := &fakeIMClient{}
	var probeCalledWith *fakeIMClient

	got, err := newIMClient(
		func() (*fakeIMClient, error) { return tlsClient, nil },
		func() (*fakeIMClient, error) { return plainClient, nil },
		func(c *fakeIMClient) error { probeCalledWith = c; return nil },
		logrus.New(), "test-im", "10.0.0.1", "test-service",
	)

	require.NoError(t, err)
	require.Equal(t, plainClient, got)
	require.Equal(t, plainClient, probeCalledWith, "extraProbe must be called on the plain client")
}

func TestNewIMClientWithTLSFallback_FallsBackWhenExtraProbeFails(t *testing.T) {
	tlsClient := &fakeIMClient{}
	plainClient := &fakeIMClient{}

	got, err := newIMClient(
		func() (*fakeIMClient, error) { return tlsClient, nil },
		func() (*fakeIMClient, error) { return plainClient, nil },
		func(c *fakeIMClient) error {
			if c == tlsClient {
				return errors.New("version mismatch")
			}
			return nil
		},
		logrus.New(), "test-im", "10.0.0.1", "test-service",
	)

	require.NoError(t, err)
	require.Equal(t, plainClient, got, "must fall back to plain when extraProbe fails on TLS client")
	require.True(t, tlsClient.closed, "TLS client must be closed when extraProbe fails")
}

func TestNewIMClientWithTLSFallback_FallsBackWhenBuildTLSFails(t *testing.T) {
	plainClient := &fakeIMClient{}

	got, err := newIMClient(
		func() (*fakeIMClient, error) { return nil, errors.New("cert file not found") },
		func() (*fakeIMClient, error) { return plainClient, nil },
		nil,
		logrus.New(), "test-im", "10.0.0.1", "test-service",
	)

	require.NoError(t, err)
	require.Equal(t, plainClient, got, "must fall back to plain when buildTLS fails")
}

func TestNewIMClientWithTLSFallback_ReturnsErrorWhenExtraProbeFailsOnPlain(t *testing.T) {
	tlsClient := &fakeIMClient{checkErr: errors.New("tls: failed")}
	plainClient := &fakeIMClient{}

	_, err := newIMClient(
		func() (*fakeIMClient, error) { return tlsClient, nil },
		func() (*fakeIMClient, error) { return plainClient, nil },
		func(c *fakeIMClient) error { return errors.New("plain: version too old") },
		logrus.New(), "test-im", "10.0.0.1", "test-service",
	)

	require.Error(t, err)
	require.True(t, plainClient.closed, "plain client must be closed when extraProbe fails on it")
}

func TestNewIMClientWithTLSFallback_ReturnsErrorWhenBuildPlainFails(t *testing.T) {
	tlsClient := &fakeIMClient{checkErr: errors.New("tls: failed")}

	_, err := newIMClient(
		func() (*fakeIMClient, error) { return tlsClient, nil },
		func() (*fakeIMClient, error) { return nil, errors.New("plain: dial failed") },
		nil,
		logrus.New(), "test-im", "10.0.0.1", "test-service",
	)

	require.Error(t, err)
}
