package engineapi

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	imclient "github.com/longhorn/longhorn-instance-manager/pkg/client"
	imtypes "github.com/longhorn/longhorn-instance-manager/pkg/types"
	imutil "github.com/longhorn/longhorn-instance-manager/pkg/util"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	ProxyPollInterval = 5 * time.Second
)

type EngineClientProxy interface {
	EngineClient

	IsGRPC() bool
	Start(*longhorn.InstanceManager, logrus.FieldLogger, *datastore.DataStore) error
	Stop(string) error
	Ping() error
}

// TODO: replace with EngineClientProxy interface later
type Client interface {
	IsGRPC() bool
	Start(*longhorn.InstanceManager, logrus.FieldLogger, *datastore.DataStore) error
	Stop(string) error
	Ping() error

	VersionGet(engine *longhorn.Engine, clientOnly bool) (version *EngineVersion, err error)

	VolumeGet(*longhorn.Engine) (volume *Volume, err error)
	VolumeExpand(engine *longhorn.Engine) error

	ReplicaAdd(engine *longhorn.Engine, url string, isRestoreVolume bool) error
	ReplicaRemove(engine *longhorn.Engine, address string) error
	ReplicaList(*longhorn.Engine) (map[string]*Replica, error)
}

type EngineClientProxyHandler struct {
	Clients map[string]Client

	logger logrus.FieldLogger

	ds *datastore.DataStore

	lock *sync.RWMutex
}

func NewEngineClientProxyHandler(logger logrus.FieldLogger, ds *datastore.DataStore) *EngineClientProxyHandler {
	return &EngineClientProxyHandler{
		Clients: make(map[string]Client),
		logger:  logger,
		ds:      ds,
		lock:    &sync.RWMutex{},
	}
}

func getLoggerForProxy(logger logrus.FieldLogger, image string) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"instanceManagerImage": image,
		},
	)
}

func (h *EngineClientProxyHandler) GetCompatibleClient(e *longhorn.Engine, fallBack interface{}) (c Client, err error) {
	if e == nil {
		return nil, errors.Errorf("BUG: failed to get proxy client due to missing engine")
	}

	im, err := h.ds.GetInstanceManager(e.Status.InstanceManagerName)
	if err != nil {
		return nil, err
	}

	log := getLoggerForProxy(h.logger, im.Spec.Image)

	shouldFallBack := false

	if im != nil {
		if err := CheckInstanceManagerProxyCompatibility(im); err != nil {
			log.WithError(err).Warnf("Use fallback client")
			shouldFallBack = true
		}
	}

	if shouldFallBack {
		if fallBack == nil {
			return nil, errors.Errorf("missing fallback client for proxy")
		}

		if obj, ok := fallBack.(*EngineBinary); ok {
			return obj, nil
		}

		return nil, errors.Errorf("BUG: invalid fallback client for proxy: %v", fallBack)
	}

	return h.GetClient(im)
}

func (h *EngineClientProxyHandler) GetClient(im *longhorn.InstanceManager) (c Client, err error) {
	if im == nil {
		return nil, errors.Errorf("BUG: failed to get proxy client due to missing instance manager")
	}

	log := getLoggerForProxy(h.logger, im.Spec.Image)

	h.lock.Lock()
	client, exist := h.Clients[im.Spec.Image]
	h.lock.Unlock()
	if exist {
		err := client.Ping()
		if err == nil {
			return client, nil
		}

		log.WithError(err).Debug("Resetting proxy connection")
		client.Stop(im.Spec.Image)

		h.lock.Lock()
		delete(h.Clients, im.Spec.Image)
		h.lock.Unlock()
	}

	return h.NewClient(log, im)
}

func (h *EngineClientProxyHandler) NewClient(log logrus.FieldLogger, im *longhorn.InstanceManager) (Client, error) {
	h.lock.Lock()
	defer h.lock.Unlock()

	if existClient, exist := h.Clients[im.Spec.Image]; exist {
		// TODO: we do not delete the map to avoid nil memory panic, so need to
		// check the ping to find any concurrent NewClient. Fix this when
		// refactoring to use goroutine for proxy client handling.
		err := existClient.Ping()
		if err == nil {
			log.Debugf("Proxy client already created")
			return existClient, nil
		}
	}

	isInstanceManagerRunning := im.Status.CurrentState == longhorn.InstanceManagerStateRunning
	if !isInstanceManagerRunning {
		return nil, errors.Errorf("cannot create new gRPC client while instance manager in %v state", im.Status.CurrentState)
	}

	hasIP := im.Status.IP != ""
	if !hasIP {
		return nil, errors.Errorf("cannot create new gRPC client while instance manager status IP is empty")
	}

	client := &Proxy{}
	if err := client.Start(im, log, h.ds); err != nil {
		return nil, err
	}

	log.Debug("Adding client to proxy handler")
	h.Clients[im.Spec.Image] = client

	return client, nil
}

type Proxy struct {
	logger logrus.FieldLogger

	ds *datastore.DataStore

	InstanceManagerName         string
	InstanceManagerControllerID string

	// used to notify the controller that proxy has stopped
	voluntaryStopCh chan struct{}
	stopCh          chan struct{}

	grpcClient *imclient.ProxyClient

	lock *sync.RWMutex
}

func (p *Proxy) IsGRPC() bool {
	return p.grpcClient != nil
}

func (p *Proxy) Start(im *longhorn.InstanceManager, logger logrus.FieldLogger, ds *datastore.DataStore) error {
	p.logger = logger
	p.ds = ds
	p.InstanceManagerName = im.Name
	p.InstanceManagerControllerID = im.Status.OwnerID
	p.stopCh = make(chan struct{})
	p.voluntaryStopCh = make(chan struct{})
	p.lock = &sync.RWMutex{}

	p.logger.Debug("Connecting to proxy server")
	ctx, cancel := context.WithTimeout(context.Background(), imtypes.GRPCServiceTimeout)
	client, err := imclient.NewProxyClient(ctx, cancel, im.Status.IP, InstanceManagerProxyDefaultPort)
	if err != nil {
		return errors.Wrapf(err, "failed to start %v proxy client", im.Spec.Image)
	}

	p.grpcClient = client

	go p.Run()

	go func() {
		<-p.voluntaryStopCh
		if err := p.Stop(im.Spec.Image); err != nil {
			p.logger.WithError(err).Error("Failed to close proxy client")
		}
	}()

	return nil
}

func (p *Proxy) Stop(name string) error {
	if p.grpcClient == nil {
		return nil
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	p.logger.Debugf("Closing %v proxy client", name)
	if err := p.grpcClient.Close(); err != nil {
		return err
	}
	return nil
}

func (p *Proxy) Run() {
	p.logger.Debug("Starting proxy")
	defer func() {
		p.logger.Debug("Stopping proxy")
		close(p.voluntaryStopCh)
	}()

	ticker := time.NewTicker(ProxyPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if needStop := p.sync(); needStop {
				return
			}
		case <-p.stopCh:
			return
		}
	}
}

func (p *Proxy) sync() bool {
	im, err := p.ds.GetInstanceManagerRO(p.InstanceManagerName)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			p.logger.Info("Stopping proxy because the instance manager no longer exists")
			return true
		}

		utilruntime.HandleError(errors.Wrapf(err, "fail to get instance manager %v for proxy sync", p.InstanceManagerName))
		return false
	}

	hasNewOwnerID := im.Status.OwnerID != p.InstanceManagerControllerID
	if hasNewOwnerID {
		p.logger.Infof("Stopping proxy because the instance manager changed ownership from %v to %v", p.InstanceManagerControllerID, im.Status.OwnerID)
		return true
	}

	return false
}

func (p *Proxy) DirectToURL(e *longhorn.Engine) string {
	if e == nil {
		p.logger.Debug("BUG: cannot get proxy direct URL with nil engine object")
		return ""
	}

	return imutil.GetURL(e.Status.IP, e.Status.Port)
}

func (p *Proxy) Ping() (err error) {
	return p.grpcClient.Ping()
}

func (p *Proxy) VersionGet(e *longhorn.Engine, clientOnly bool) (version *EngineVersion, err error) {
	recvClientVersion := p.grpcClient.ClientVersionGet()
	clientVersion := (*longhorn.EngineVersionDetails)(&recvClientVersion)

	if clientOnly {
		return &EngineVersion{
			ClientVersion: clientVersion,
		}, nil
	}

	recvServerVersion, err := p.grpcClient.ServerVersionGet(p.DirectToURL(e))
	if err != nil {
		return nil, err
	}

	return &EngineVersion{
		ClientVersion: clientVersion,
		ServerVersion: (*longhorn.EngineVersionDetails)(recvServerVersion),
	}, nil
}
