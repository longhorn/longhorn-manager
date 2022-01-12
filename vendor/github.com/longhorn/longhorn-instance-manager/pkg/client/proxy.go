package client

import (
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
	"github.com/longhorn/longhorn-instance-manager/pkg/meta"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"

	emeta "github.com/longhorn/longhorn-engine/pkg/meta"
)

var (
	ErrParameter = errors.Errorf("missing required parameter")
)

type ServiceContext struct {
	cc *grpc.ClientConn

	ctx  context.Context
	quit context.CancelFunc

	service rpc.ProxyEngineServiceClient
}

func (c *ProxyClient) Close() error {
	c.quit()
	if err := c.cc.Close(); err != nil {
		return errors.Wrapf(err, "error closing proxy gRPC connection")
	}
	return nil
}

type ProxyClient struct {
	ServiceURL string
	ServiceContext

	Version int
}

func NewProxyClient(ctx context.Context, ctxCancel context.CancelFunc, address string, port int) (*ProxyClient, error) {
	getServiceCtx := func(serviceUrl string) (ServiceContext, error) {
		connection, err := grpc.Dial(serviceUrl, grpc.WithInsecure())
		if err != nil {
			return ServiceContext{}, errors.Wrapf(err, "cannot connect to ProxyService %v", serviceUrl)
		}
		return ServiceContext{
			cc:      connection,
			ctx:     ctx,
			quit:    ctxCancel,
			service: rpc.NewProxyEngineServiceClient(connection),
		}, nil
	}

	serviceURL := util.GetURL(address, port)
	serviceCtx, err := getServiceCtx(serviceURL)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Connected to proxy service on %v", serviceURL)

	return &ProxyClient{
		ServiceURL:     serviceURL,
		ServiceContext: serviceCtx,
		Version:        meta.InstanceManagerProxyAPIVersion,
	}, nil
}

const (
	GRPCServiceTimeout = 3 * time.Minute
)

func (c *ProxyClient) Ping() (err error) {
	_, err = c.service.Ping(c.ctx, &empty.Empty{})
	if err != nil {
		return errors.Wrapf(err, "failed to ping %v proxy server", c.ServiceURL)
	}
	return nil
}

func (c *ProxyClient) ServerVersionGet(serviceAddress string) (version *emeta.VersionOutput, err error) {
	if serviceAddress == "" {
		return version, errors.Wrapf(ErrParameter, "failed to get server version")
	}
	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Getting server version via proxy")

	req := &rpc.ProxyEngineRequest{
		Address: serviceAddress,
	}
	resp, err := c.service.ServerVersionGet(c.ctx, req)
	if err != nil {
		return version, errors.Wrapf(err, "failed to get server version via proxy %v to %v", c.ServiceURL, serviceAddress)
	}

	serverVersion := resp.Version
	version = &emeta.VersionOutput{
		Version:                 serverVersion.Version,
		GitCommit:               serverVersion.GitCommit,
		BuildDate:               serverVersion.BuildDate,
		CLIAPIVersion:           int(serverVersion.CliAPIVersion),
		CLIAPIMinVersion:        int(serverVersion.CliAPIMinVersion),
		ControllerAPIVersion:    int(serverVersion.ControllerAPIVersion),
		ControllerAPIMinVersion: int(serverVersion.ControllerAPIMinVersion),
		DataFormatVersion:       int(serverVersion.DataFormatVersion),
		DataFormatMinVersion:    int(serverVersion.DataFormatMinVersion),
	}
	return version, nil
}

func (c *ProxyClient) ClientVersionGet() (version emeta.VersionOutput) {
	logrus.Debug("Getting client version on proxy")
	return emeta.GetVersion()
}
