package monitor

import (
	"context"
	"reflect"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/jinzhu/copier"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/longhorn/longhorn-manager/datastore"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type FakeCPUPolicyCheckMonitor struct {
	*baseMonitor

	nodeName string

	collectedDataLock sync.RWMutex
	collectedData     *CollectedCPUPolicyCheckInfo

	syncCallback func(key string)
}

func NewFakeCPUPolicyCheckMonitor(logger logrus.FieldLogger, ds *datastore.DataStore, nodeName string, syncCallback func(key string)) (*FakeCPUPolicyCheckMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())

	m := &FakeCPUPolicyCheckMonitor{
		baseMonitor: newBaseMonitor(ctx, quit, logger, ds, cpuPolicyCheckMonitorSyncPeriod),

		nodeName: nodeName,

		collectedDataLock: sync.RWMutex{},
		collectedData:     &CollectedCPUPolicyCheckInfo{},

		syncCallback: syncCallback,
	}

	return m, nil
}

func (m *FakeCPUPolicyCheckMonitor) Start() {
	if err := wait.PollUntilContextCancel(m.ctx, m.syncPeriod, true, func(context.Context) (bool, error) {
		if err := m.run(struct{}{}); err != nil {
			m.logger.WithError(err).Error("Stopped monitoring CPU policy check")
		}
		return false, nil
	}); err != nil {
		if errors.Is(err, context.Canceled) {
			m.logger.WithError(err).Warn("CPU policy check monitor is stopped")
		} else {
			m.logger.WithError(err).Error("Failed to start CPU policy check monitor")
		}
	}
}

func (m *FakeCPUPolicyCheckMonitor) Stop() {
	m.quit()
}

func (m *FakeCPUPolicyCheckMonitor) RunOnce() error {
	return m.run(struct{}{})
}

func (m *FakeCPUPolicyCheckMonitor) UpdateConfiguration(map[string]interface{}) error {
	return nil
}

func (m *FakeCPUPolicyCheckMonitor) GetCollectedData() (interface{}, error) {
	m.collectedDataLock.RLock()
	defer m.collectedDataLock.RUnlock()

	data := longhorn.CPUManagerPolicyUnknown
	if err := copier.CopyWithOption(&data, &m.collectedData.CPUPolicy, copier.Option{IgnoreEmpty: true, DeepCopy: true}); err != nil {
		return data, errors.Wrap(err, "failed to copy collected data")
	}

	return data, nil
}

func (m *FakeCPUPolicyCheckMonitor) run(value interface{}) error {
	node, err := m.ds.GetNode(m.nodeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get longhorn node %v", m.nodeName)
	}

	collectedData := &CollectedCPUPolicyCheckInfo{
		CPUPolicy: longhorn.CPUManagerPolicyUnknown,
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
