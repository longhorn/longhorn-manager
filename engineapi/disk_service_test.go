package engineapi

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	imclient "github.com/longhorn/longhorn-instance-manager/pkg/client"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type minimalHealthServer struct {
	healthpb.UnimplementedHealthServer
}

func (s *minimalHealthServer) Check(_ context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func TestNewDiskServiceClient_InstanceManagerNotRunning(t *testing.T) {
	im := &longhorn.InstanceManager{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "instance-manager-1",
			Namespace: "longhorn-system",
		},
		Status: longhorn.InstanceManagerStatus{
			CurrentState: longhorn.InstanceManagerStateError,
			IP:           "10.0.0.1",
		},
	}

	logger := logrus.New().WithField("test", "TestNewDiskServiceClient_InstanceManagerNotRunning")

	client, err := NewDiskServiceClient(im, logger)

	require.Error(t, err, "should fail when instance manager is not in running state")
	require.Nil(t, client, "client should be nil when instance manager is not running")
	require.Contains(t, err.Error(), "not running state", "error message should indicate instance manager is not in running state")
}

func TestNewDiskServiceClient_MissingIP(t *testing.T) {
	im := &longhorn.InstanceManager{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "instance-manager-1",
			Namespace: "longhorn-system",
		},
		Status: longhorn.InstanceManagerStatus{
			CurrentState: longhorn.InstanceManagerStateRunning,
			IP:           "",
		},
	}

	logger := logrus.New().WithField("test", "TestNewDiskServiceClient_MissingIP")

	client, err := NewDiskServiceClient(im, logger)

	require.Error(t, err, "should fail when instance manager IP is missing")
	require.Nil(t, client, "client should be nil when instance manager IP is missing")
	require.Contains(t, err.Error(), "IP is missing", "error message should indicate IP is missing")
}

func TestDiskServiceTLSClient_FailsCheckConnectionOnPlaintextServer(t *testing.T) {
	// Start a plaintext gRPC server with health service.
	srv := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	healthpb.RegisterHealthServer(srv, &minimalHealthServer{})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	// Build a minimal TLS config (any valid CA pool works -- we just need a TLS client).
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		BasicConstraintsValid: true,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER}))

	clientTLS := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    pool,
		ServerName: "longhorn-backend.longhorn-system",
	}

	ctx, cancel := context.WithCancel(context.Background())
	c, err := imclient.NewDiskServiceClient(ctx, cancel, "tcp://"+lis.Addr().String(), clientTLS)
	require.NoError(t, err) // lazy dial; construction succeeds
	defer func() { _ = c.Close() }()

	// The plaintext server will not complete a TLS handshake, so CheckConnection must fail.
	require.Error(t, c.CheckConnection(),
		"TLS client connecting to a plaintext server must fail at CheckConnection")
}
