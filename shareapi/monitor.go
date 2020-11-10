package shareapi

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	"github.com/longhorn/longhorn-manager/types"
	smclient "github.com/longhorn/longhorn-share-manager/pkg/client"
)

const ShareManagerMonitorTickPeriod = time.Second
const ShareManagerMonitorSyncPeriod = time.Minute

type ShareManagerMonitor struct {
	Manager   string
	namespace string
	IP        string
	logger    logrus.FieldLogger

	client *ShareManagerClient
	shares map[string]*types.Share
	lock   sync.Mutex
	stale  bool

	syncCallback func(key string)

	ctx  context.Context
	quit context.CancelFunc
}

func NewShareManagerMonitor(logger logrus.FieldLogger, sm *longhorn.ShareManager, syncCallback func(key string)) (*ShareManagerMonitor, error) {
	client, err := NewShareManagerClient(sm)
	if err != nil {
		return nil, err
	}

	ctx, quit := context.WithCancel(context.Background())
	monitor := &ShareManagerMonitor{
		Manager:   sm.Name,
		namespace: sm.Namespace,
		IP:        sm.Status.IP,
		logger:    logger,

		client: client,
		shares: map[string]*types.Share{},
		lock:   sync.Mutex{},
		stale:  true,

		syncCallback: syncCallback,

		ctx:  ctx,
		quit: quit,
	}

	shareStream, err := client.ShareWatch()
	if err != nil {
		return nil, err
	}

	go monitor.monitorShareStream(shareStream)
	go monitor.monitorShares()
	return monitor, nil
}

func (m *ShareManagerMonitor) Close() error {
	m.quit()
	return nil
}

func (m *ShareManagerMonitor) syncShares() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	rsp, err := m.client.ShareList()
	if err != nil {
		return err
	}

	// if there are failed shares we want to requeue the share manager so it can try to fix them
	// the goal is to get into a stable running state at which point we don't need to keep queuing,
	// but any failure scenarios require system interaction to get them fixed.
	hasFailedShares := false
	for _, share := range rsp {
		if share.State == types.ShareStateError {
			hasFailedShares = true
			break
		}
	}

	if equal := reflect.DeepEqual(m.shares, rsp); !equal || hasFailedShares {
		m.logger.Infof("Enqueuing share manager, received state is equal: %v has failed shares: %v", equal, hasFailedShares)
		m.shares = rsp
		m.stale = false
		key := m.namespace + "/" + m.Manager
		m.syncCallback(key)
	}

	return nil
}

func (m *ShareManagerMonitor) monitorShares() {
	ticker := time.NewTicker(ShareManagerMonitorTickPeriod)
	defer ticker.Stop()
	timeSinceLastSync := time.Duration(0)
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			timeSinceLastSync += ShareManagerMonitorTickPeriod
			if timeSinceLastSync > ShareManagerMonitorSyncPeriod || m.stale {
				timeSinceLastSync = 0
				if err := m.syncShares(); err != nil {
					m.logger.WithError(err).Warn("Failed to sync shares with server")
					// we set stale to be guaranteed to retry this next tick
					m.stale = true
				}
			}
		}
	}
}

func (m *ShareManagerMonitor) monitorShareStream(shareStream *smclient.ShareStream) {

	// monitor share stream
	// currently we only use the share stream as a trigger
	// to mark our share map as stale
	go func() {
		<-m.ctx.Done()
		_ = shareStream.Close()
	}()

	for {
		if done := m.ctx.Err() != nil; done {
			return
		}

		if _, err := shareStream.Recv(); err != nil {
			// TODO: improve error handling
			continue
		}

		m.stale = true
	}
}

// Share will ensure that there is a share for a volume
// a volume can only be shared once
func (m *ShareManagerMonitor) Share(volume string) (types.Share, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	share, err := m.client.ShareCreate(volume)
	if err != nil {
		return types.Share{
			Volume: volume,
			State:  types.ShareStateUnknown,
		}, err
	}
	m.shares[volume] = share
	return *share, nil
}

func (m *ShareManagerMonitor) Unshare(volume string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	share, err := m.client.ShareDelete(volume)
	if err != nil {
		return err
	}
	m.shares[volume] = share
	return nil
}

func (m *ShareManagerMonitor) GetShare(volume string) (types.Share, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if share, ok := m.shares[volume]; ok {
		return *share, nil
	}

	return types.Share{
		Volume: volume,
		State:  types.ShareStateUnknown,
	}, nil
}

// GetShares returns a copy of the share state
// based on the current monitor information
func (m *ShareManagerMonitor) GetShares() map[string]types.Share {
	m.lock.Lock()
	defer m.lock.Unlock()
	out := map[string]types.Share{}
	for vol, share := range m.shares {
		out[vol] = *share
	}
	return out
}
