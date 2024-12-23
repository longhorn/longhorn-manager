package monitor

import (
	"context"
	"reflect"
	"sync"

	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/longhorn/longhorn-manager/datastore"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type FakeEnvironmentCheckMonitor struct {
	*baseMonitor

	nodeName string

	collectedDataLock sync.RWMutex
	collectedData     *CollectedEnvironmentCheckInfo

	syncCallback func(key string)
}

func NewFakeEnvironmentCheckMonitor(logger logrus.FieldLogger, ds *datastore.DataStore, nodeName string, syncCallback func(key string)) (*FakeEnvironmentCheckMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())

	m := &FakeEnvironmentCheckMonitor{
		baseMonitor: newBaseMonitor(ctx, quit, logger, ds, EnvironmentCheckMonitorSyncPeriod),

		nodeName: nodeName,

		collectedDataLock: sync.RWMutex{},
		collectedData:     &CollectedEnvironmentCheckInfo{},

		syncCallback: syncCallback,
	}

	return m, nil
}

func (m *FakeEnvironmentCheckMonitor) Start() {
	if err := wait.PollUntilContextCancel(m.ctx, m.syncPeriod, true, func(context.Context) (bool, error) {
		if err := m.run(struct{}{}); err != nil {
			m.logger.WithError(err).Error("Stopped monitoring environment check")
		}
		return false, nil
	}); err != nil {
		if errors.Is(err, context.Canceled) {
			m.logger.WithError(err).Warning("Environment check monitor is stopped")
		} else {
			m.logger.WithError(err).Error("Failed to start environment check monitor")
		}
	}
}

func (m *FakeEnvironmentCheckMonitor) Stop() {
	m.quit()
}

func (m *FakeEnvironmentCheckMonitor) RunOnce() error {
	return m.run(struct{}{})
}

func (m *FakeEnvironmentCheckMonitor) UpdateConfiguration(map[string]interface{}) error {
	return nil
}

func (m *FakeEnvironmentCheckMonitor) GetCollectedData() (interface{}, error) {
	m.collectedDataLock.RLock()
	defer m.collectedDataLock.RUnlock()

	data := []longhorn.Condition{}
	if err := copier.CopyWithOption(&data, &m.collectedData.conditions, copier.Option{IgnoreEmpty: true, DeepCopy: true}); err != nil {
		return data, errors.Wrap(err, "failed to copy collected data")
	}

	return data, nil
}

func (m *FakeEnvironmentCheckMonitor) run(value interface{}) error {
	node, err := m.ds.GetNode(m.nodeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get longhorn node %v", m.nodeName)
	}

	collectedData := &CollectedEnvironmentCheckInfo{
		conditions: []longhorn.Condition{},
	}
	if !reflect.DeepEqual(m.collectedData, collectedData) {
		func() {
			m.collectedDataLock.Lock()
			defer m.collectedDataLock.Unlock()
			m.collectedData = collectedData
		}()

		key := node.Namespace + "/" + m.nodeName
		m.syncCallback(key)
	}

	return nil
}
