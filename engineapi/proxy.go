package engineapi

import (
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/connectivity"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"

	imclient "github.com/longhorn/longhorn-instance-manager/pkg/client"
	imtypes "github.com/longhorn/longhorn-instance-manager/pkg/types"
	imutil "github.com/longhorn/longhorn-instance-manager/pkg/util"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/util"
)

const (
	MaxProxyClientConcurrentInUse = 1000
	DefaultProxyClientPerImage    = 3
	MaxProxyClientPerImage        = 3 * DefaultProxyClientPerImage
	ProxyPollInterval             = 5 * time.Second
	ProxyClientMaxRetry           = 10
	ProxyGCMaxRetry               = 3
)

type EngineClientProxyHandler struct {
	proxyPools map[string]*engineClientProxyPool

	logger logrus.FieldLogger
	ds     *datastore.DataStore

	stopCh chan struct{}

	lock *sync.RWMutex

	resyncPeriod time.Duration
}

type engineClientProxyPool struct {
	clients map[string]EngineClientProxy

	instanceManagerName string

	lock *sync.RWMutex
}

func NewEngineClientProxyHandler(logger logrus.FieldLogger, ds *datastore.DataStore, resyncPeriod time.Duration, stopCh chan struct{}) *EngineClientProxyHandler {
	return &EngineClientProxyHandler{
		proxyPools:   make(map[string]*engineClientProxyPool),
		logger:       logger,
		ds:           ds,
		stopCh:       stopCh,
		lock:         &sync.RWMutex{},
		resyncPeriod: resyncPeriod,
	}
}

func getLoggerForProxyHandler(logger logrus.FieldLogger, im *longhorn.InstanceManager) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"instanceManager": im.Name,
			"image":           im.Spec.Image,
		},
	)
}

func (h *EngineClientProxyHandler) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	h.logger.Info("Starting Longhorn engine client proxy handler")
	defer h.logger.Info("Shutting down Longhorn engine client proxy handler")

	// Set the loop time the same as the gRPC client keepalive ping so there
	// are enough time for the periodic ping and also able to recover some of
	// the TransientFailure connections.
	go wait.Until(h.resync, time.Second*10, stopCh)

	<-stopCh
}

func (h *EngineClientProxyHandler) resync() {
	handledPools := h.getHandledEngineClientProxyPools()
	for instanceManagerImage, pool := range handledPools {
		im, err := h.ds.GetInstanceManagerRO(pool.instanceManagerName)
		if err != nil {
			if !datastore.ErrorIsNotFound(err) {
				h.logger.WithError(err).Warnf("Failed to get %v instance manager for engine client proxy", pool.instanceManagerName)
				continue
			}

			h.logger.Warnf("%v instance manager no longer exist for engine client proxy", pool.instanceManagerName)
			for _, client := range pool.clients {
				client.getEngineClientProxy().setNextGarbageCollect()
			}
			continue
		}

		for _, client := range pool.clients {
			engineClientProxy := client.getEngineClientProxy()
			if !engineClientProxy.shouldResync() {
				continue
			}

			log := engineClientProxy.getLogger()

			// Try to stop and delete clients in garbage collect. We will
			// force delete after some retry if the client cannot be stopped
			// or the calls does not return by the caller.
			h.garbageCollectEngineClientProxy(instanceManagerImage, client)

			// Replace client in bad connection state with new client. Mark
			// the bad client for garbage collect so it will not be called for
			// any new requests.
			if engineClientProxy.shouldGarbageCollect() {
				engineClientProxy.setNextGarbageCollect()

				if err := h.NewEngineClientProxy(im); err != nil {
					log.WithError(err).Warn("Failed to replace with new engine client proxy")
				}

				engineClientProxy.setNextResync(h.resyncPeriod)

				// gRPC is expecting the TransientFailure state to recover, but
				// this is not guaranteed. So we replace one connection at a
				// time leaving some time for other TransientFailure to recover.
				break
			}

			// Start client connection for new client added to the pool.
			if err := engineClientProxy.startNewClient(im, h.logger, h.ds, h.stopCh); err != nil {
				log.WithError(err).Warn("Failed to start new engine client proxy")
			}

			engineClientProxy.setNextResync(h.resyncPeriod)
		}

		// When the client gets called, the calling count gets increased,
		// and vice vesa decrease the calling count when the caller is done
		// with the request.
		// When exceeding the max concurrent use, we will create an extra
		// client to handle more concurent use.
		exceedMaxConnection := true
		for _, client := range pool.clients {
			engineClientProxy := client.getEngineClientProxy()
			if engineClientProxy.getInUse() >= MaxProxyClientConcurrentInUse {
				continue
			}

			exceedMaxConnection = false
			break
		}
		if len(pool.clients) != 0 && exceedMaxConnection {
			log := getLoggerForProxyHandler(h.logger, im)
			log.Debugf("Exceed engine client proxy max concurrent use limit:%v", MaxProxyClientConcurrentInUse)

			if err := h.NewEngineClientProxy(im); err != nil {
				log.WithError(err).Warn("Failed to add more engine client proxy")
			}
		}
	}
}

func (h *EngineClientProxyHandler) garbageCollectEngineClientProxy(instanceManagerImage string, client EngineClientProxy) {
	engineClientProxy := client.getEngineClientProxy()

	log := engineClientProxy.getLogger()

	nextGC := engineClientProxy.getNextGarbageCollect()
	now := time.Now()
	if nextGC == nil || now.Before(*nextGC) {
		return
	}

	retryFailedMsg := ""
	if engineClientProxy.getInUse() != 0 {
		retryFailedMsg = fmt.Sprintf("Cannot stop engine client proxy with %v still in use", engineClientProxy.getInUse())
		if engineClientProxy.getGarbageCollectRetry() < ProxyGCMaxRetry {
			log.Warnf(retryFailedMsg)
			engineClientProxy.setNextGarbageCollect()
			return
		}
	}

	log.Debug("Stopping engine client proxy")
	if err := engineClientProxy.stop(); err != nil {
		retryFailedMsg = "Failed to stop engine client proxy"
		if engineClientProxy.getGarbageCollectRetry() < ProxyGCMaxRetry {
			log.WithError(err).Warnf(retryFailedMsg)
			engineClientProxy.setNextGarbageCollect()
			return
		}
	}

	if retryFailedMsg != "" {
		log.Warn("Exceed max retry limit, force delete engine client proxy")
	}

	pool, _ := h.getHandledEngineClientProxyPoolByImage(instanceManagerImage)

	pool.lock.Lock()
	defer pool.lock.Unlock()

	delete(pool.clients, engineClientProxy.getClientUUID())
	log.Debug("Deleted engine client proxy")
}

func (h *EngineClientProxyHandler) getNewClientUUID(clients map[string]EngineClientProxy) (string, error) {
	if len(clients) >= MaxProxyClientPerImage {
		return "", errors.Errorf("exceed max engine client proxy per image limit: %v", MaxProxyClientPerImage)
	}

	canidateID := util.UUID()
	if _, exist := clients[canidateID]; exist {
		return "", errors.Errorf("proxy(%v) already exist", canidateID)
	}

	return canidateID, nil
}

func (h *EngineClientProxyHandler) getHandledEngineClientProxyPools() map[string]*engineClientProxyPool {
	h.lock.RLock()
	defer h.lock.RUnlock()

	return h.proxyPools
}

func (h *EngineClientProxyHandler) getHandledEngineClientProxyPoolByImage(name string) (*engineClientProxyPool, bool) {
	h.lock.RLock()
	defer h.lock.RUnlock()

	pool, exist := h.proxyPools[name]
	return pool, exist
}

func (h *EngineClientProxyHandler) NewEngineClientProxy(im *longhorn.InstanceManager) error {
	imImage := im.Spec.Image
	log := getLoggerForProxyHandler(h.logger, im)

	h.lock.Lock()
	defer h.lock.Unlock()

	_, poolExist := h.proxyPools[imImage]
	if !poolExist {
		log.Debug("Initializing engine client proxy pool")
		h.proxyPools[imImage] = &engineClientProxyPool{
			clients:             make(map[string]EngineClientProxy),
			instanceManagerName: im.Name,
			lock:                &sync.RWMutex{},
		}
	}

	pool := h.proxyPools[imImage]
	clientUUID, err := h.getNewClientUUID(pool.clients)
	if err != nil {
		return err
	}

	log.Debugf("Initializing engine client proxy(%v) in proxy pool", clientUUID)
	h.proxyPools[imImage].clients[clientUUID] = &Proxy{
		logger: h.logger.WithFields(logrus.Fields{
			"instanceManager": im.Name,
			"clientUUID":      clientUUID,
			"image":           im.Spec.Image,
			"serverIP":        im.Status.IP,
		}),
		ds:                          h.ds,
		clientUUID:                  clientUUID,
		InstanceManagerName:         im.Name,
		InstanceManagerControllerID: im.Status.OwnerID,
		voluntaryStopCh:             make(chan struct{}),
		stopCh:                      h.stopCh,
		lock:                        sync.RWMutex{},
		inUse:                       0,
		inUseLock:                   sync.RWMutex{},
		nextResync:                  time.Now(),
		resyncLock:                  sync.RWMutex{},
		nextGC:                      nil,
		gcRetry:                     0,
		gcLock:                      sync.RWMutex{},
	}
	return nil
}

func (h *EngineClientProxyHandler) GetCompatibleClient(e *longhorn.Engine, fallBack interface{}) (c EngineClientProxy, err error) {
	if e == nil {
		return nil, errors.Errorf("BUG: failed to get engine client proxy due to missing engine")
	}

	im, err := h.ds.GetInstanceManagerRO(e.Status.InstanceManagerName)
	if err != nil {
		return nil, err
	}

	log := getLoggerForProxyHandler(h.logger, im)

	shouldFallBack := false

	if im != nil {
		if err := CheckInstanceManagerProxySupport(im); err != nil {
			log.WithError(err).Trace("Use fallback client")
			shouldFallBack = true
		}
	}

	if shouldFallBack {
		if fallBack == nil {
			return nil, errors.Errorf("missing engine client proxy fallback client")
		}

		if obj, ok := fallBack.(*EngineBinary); ok {
			return obj, nil
		}

		return nil, errors.Errorf("BUG: invalid engine client proxy fallback client: %v", fallBack)
	}

	return h.GetClientByInstanceManager(im)
}

func (h *EngineClientProxyHandler) GetClientByInstanceManager(im *longhorn.InstanceManager) (c EngineClientProxy, err error) {
	if im == nil {
		return nil, errors.Errorf("BUG: failed to get engine client proxy due to missing instance manager")
	}

	_, exist := h.getHandledEngineClientProxyPoolByImage(im.Spec.Image)
	if !exist {
		for i := 0; i < DefaultProxyClientPerImage; i++ {
			if err := h.NewEngineClientProxy(im); err != nil {
				h.logger.WithError(err).Warn("Failed to initialize engine client proxy")
			}
		}
	}

	failCount := 0
	for {
		if failCount > ProxyClientMaxRetry {
			return nil, errors.Errorf("exceed max retry to get %v engine client proxy", im.Name)
		}

		failCount++
		candidate, err := h.getEngineClientProxyCandidate(im)
		if err != nil {
			log := getLoggerForProxyHandler(h.logger, im)
			log.WithError(err).Debugf("Retry(%v) to get engine client proxy", failCount)
		} else {
			candidate.use()
			return candidate, nil
		}

		time.Sleep(PollInterval)
	}
}

func (h *EngineClientProxyHandler) getEngineClientProxyCandidate(im *longhorn.InstanceManager) (EngineClientProxy, error) {
	var candidate EngineClientProxy
	lessUsed := 0
	pool, _ := h.getHandledEngineClientProxyPoolByImage(im.Spec.Image)
	for _, client := range pool.clients {
		candidate = client
		lessUsed = candidate.getEngineClientProxy().getInUse()
		break
	}

	for _, client := range pool.clients {
		engineClientProxy := client.getEngineClientProxy()

		if err := checkClient(client); err != nil {
			continue
		}

		inUse := engineClientProxy.getInUse()
		healthyCandidate := checkClient(candidate) != nil
		if healthyCandidate && inUse >= lessUsed {
			continue
		}

		lessUsed = inUse
		candidate = client
	}
	if err := checkClient(candidate); err != nil {
		return nil, err
	}

	return candidate, nil
}

func checkClient(c EngineClientProxy) error {
	engineClientProxy := c.getEngineClientProxy()
	if !engineClientProxy.isGRPC() {
		return errors.Errorf("waiting for engine client proxy to start")
	}

	clientUUID := engineClientProxy.getClientUUID()
	if engineClientProxy.getNextGarbageCollect() != nil {
		return errors.Errorf("engine client proxy(%v) is in garbage collect", clientUUID)
	}

	conn := engineClientProxy.getConnectionState()
	if conn != connectivity.Ready && conn != connectivity.Idle {
		return errors.Errorf("engine client proxy(%v) is not usable in %v state", clientUUID, conn)
	}

	return nil
}

type Proxy struct {
	logger logrus.FieldLogger

	ds *datastore.DataStore

	clientUUID string

	InstanceManagerName         string
	InstanceManagerControllerID string

	// used to notify the controller that proxy has stopped
	voluntaryStopCh chan struct{}
	stopCh          chan struct{}

	grpcClient *imclient.ProxyClient

	lock sync.RWMutex

	inUse     int
	inUseLock sync.RWMutex

	nextResync time.Time
	resyncLock sync.RWMutex

	nextGC  *time.Time
	gcRetry int
	gcLock  sync.RWMutex
}

type EngineClientProxy interface {
	EngineClient

	getEngineClientProxy() engineClientProxy

	use()
	Done()
}

func (p *Proxy) getEngineClientProxy() engineClientProxy {
	return p
}

// use increments the number of an engine proxy client concurrent use count.
func (p *Proxy) use() {
	p.incrementUse(true)
}

// Done decrements the number of an engine proxy client concurrent use count.
func (p *Proxy) Done() {
	p.incrementUse(false)
}

type engineClientProxy interface {
	getLogger() logrus.FieldLogger

	isGRPC() bool
	getConnectionState() connectivity.State
	getClientUUID() string
	GetPoolID() string

	startNewClient(*longhorn.InstanceManager, logrus.FieldLogger, *datastore.DataStore, chan struct{}) error
	stop() error

	setNextResync(time.Duration)
	shouldResync() bool

	shouldGarbageCollect() bool
	setNextGarbageCollect()
	getNextGarbageCollect() *time.Time
	getGarbageCollectRetry() int

	incrementUse(bool)
	getInUse() int
}

func (p *Proxy) getLogger() logrus.FieldLogger {
	return p.logger
}

func (p *Proxy) isGRPC() bool {
	return p.grpcClient != nil
}

func (p *Proxy) getClientUUID() string {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.clientUUID
}

func (p *Proxy) GetPoolID() string {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.InstanceManagerName
}

func (p *Proxy) getConnectionState() connectivity.State {
	return p.grpcClient.ServiceContext.GetConnectionState()
}

func (p *Proxy) shouldGarbageCollect() bool {
	return p.getNextGarbageCollect() == nil &&
		p.grpcClient != nil &&
		(p.getConnectionState() == connectivity.Shutdown || p.getConnectionState() == connectivity.TransientFailure || p.getConnectionState().String() == "Invalid-State")
}

func (p *Proxy) getNextResync() time.Time {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.nextResync
}

func (p *Proxy) setNextResync(t time.Duration) {
	nextResync := p.getNextResync()
	now := time.Now()
	if now.Before(nextResync) || now.Equal(nextResync) {
		return
	}

	p.resyncLock.Lock()
	defer p.resyncLock.Unlock()

	p.nextResync = now.Add(t)
}

func (p *Proxy) shouldResync() bool {
	nextResync := p.getNextResync()
	now := time.Now()
	return now.After(nextResync) || now.Equal(nextResync)
}

func (p *Proxy) setNextGarbageCollect() {
	nextGC := p.getNextGarbageCollect()
	now := time.Now()
	if nextGC != nil && (now.Before(*nextGC) || now.Equal(*nextGC)) {
		return
	}

	log := p.getLogger()
	log.WithField("connectionState", p.getConnectionState()).Debug("Deleting engine client proxy")

	p.gcLock.Lock()
	defer p.gcLock.Unlock()

	p.gcRetry++

	newGC := time.Now().Add(imtypes.GRPCServiceTimeout)
	p.nextGC = &newGC

	log.Debugf("Next garbage collect for engine client proxy is at %v", p.nextGC)
}

func (p *Proxy) getNextGarbageCollect() *time.Time {
	p.gcLock.RLock()
	defer p.gcLock.RUnlock()

	return p.nextGC
}

func (p *Proxy) getGarbageCollectRetry() int {
	p.gcLock.RLock()
	defer p.gcLock.RUnlock()

	return p.gcRetry
}

func (p *Proxy) incrementUse(increment bool) {
	p.inUseLock.Lock()
	defer p.inUseLock.Unlock()

	if increment {
		p.inUse++
		return
	}

	if p.inUse > 0 {
		p.inUse--
	}
}

func (p *Proxy) getInUse() int {
	p.inUseLock.RLock()
	defer p.inUseLock.RUnlock()

	return p.inUse
}

func (p *Proxy) startNewClient(im *longhorn.InstanceManager, logger logrus.FieldLogger, ds *datastore.DataStore, stopCh chan struct{}) error {
	if p.grpcClient != nil {
		return nil
	}

	log := p.getLogger()
	log.Debug("Creating new engine client proxy")

	nextGC := p.getNextGarbageCollect()
	if nextGC != nil {
		return errors.Errorf("engine client proxy is marked for deletion")
	}

	isInstanceManagerRunning := im.Status.CurrentState == longhorn.InstanceManagerStateRunning
	if !isInstanceManagerRunning {
		return errors.Errorf("instance manager in %v, not running state", im.Status.CurrentState)
	}

	hasIP := im.Status.IP != ""
	if !hasIP {
		return errors.Errorf("missing instance manager status IP")
	}

	log.Debug("Connecting to engine client proxy server")
	ctx, cancel := context.WithCancel(context.Background())
	client, err := imclient.NewProxyClient(ctx, cancel, im.Status.IP, InstanceManagerProxyDefaultPort)
	if err != nil {
		return err
	}

	p.lock.Lock()
	p.grpcClient = client
	p.lock.Unlock()

	go p.run()

	go func() {
		<-p.voluntaryStopCh
		log.Debug("Voluntary stopping engine client proxy")
		if err := p.stop(); err != nil {
			log.WithError(err).Errorf("failed to close engine client proxy")
		}
	}()

	return nil
}

func (p *Proxy) stop() error {
	if !p.isGRPC() {
		return nil
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	if err := p.grpcClient.Close(); err != nil {
		return errors.Wrap(err, "failed to close engine client proxy")
	}
	return nil
}

func (p *Proxy) run() {
	log := p.getLogger()
	log.Info("Starting engine client proxy")

	defer func() {
		close(p.voluntaryStopCh)
	}()

	ticker := time.NewTicker(ProxyPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := p.sync(); err != nil {
				log.WithError(err).Info("Stopping engine client proxy")
				return
			}
		case <-p.stopCh:
			return
		}
	}
}

func (p *Proxy) sync() error {
	im, err := p.ds.GetInstanceManagerRO(p.InstanceManagerName)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return errors.Errorf("instance manager no longer exists")
		}

		utilruntime.HandleError(errors.Wrapf(err, "fail to get instance manager %v for engine client proxy sync", p.InstanceManagerName))
		return nil
	}

	hasNewOwnerID := im.Status.OwnerID != p.InstanceManagerControllerID
	if hasNewOwnerID {
		return errors.Errorf("instance manager changed ownership from %v to %v", p.InstanceManagerControllerID, im.Status.OwnerID)
	}

	return nil
}

func (p *Proxy) DirectToURL(e *longhorn.Engine) string {
	if e == nil {
		p.getLogger().Debug("BUG: cannot get engine client proxy re-direct URL with nil engine object")
		return ""
	}

	return imutil.GetURL(e.Status.StorageIP, e.Status.Port)
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
