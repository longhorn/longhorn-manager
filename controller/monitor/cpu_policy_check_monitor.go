package monitor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/jinzhu/copier"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/util/wait"

	lhns "github.com/longhorn/go-common-libs/ns"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	cpuPolicyCheckMonitorSyncPeriod = 60 * time.Second

	// Path to the kubelet CPU manager state file.
	// If running in a pod, this path should match where you mounted the host's /var/lib/kubelet
	DefaultKubeletRootDir      = "/var/lib/kubelet"
	DefaultCpuManagerStateFile = "cpu_manager_state"
)

// CPUManagerState represents the structure of the kubelet's CPU manager state file.
type CPUManagerState struct {
	PolicyName    longhorn.CPUManagerPolicy    `json:"policyName"`
	DefaultCPUSet string                       `json:"defaultCpuSet,omitempty"`
	Entries       map[string]map[string]string `json:"entries,omitempty"`
	Checksum      uint32                       `json:"checksum,omitempty"`
}

type CollectedCPUPolicyCheckInfo struct {
	CPUPolicy longhorn.CPUManagerPolicy
}

type CPUPolicyCheckMonitor struct {
	*baseMonitor

	nodeName string

	collectedDataLock sync.RWMutex
	collectedData     *CollectedCPUPolicyCheckInfo

	syncCallback func(key string)
}

func NewCPUPolicyCheckMonitor(logger logrus.FieldLogger, ds *datastore.DataStore, nodeName string, syncCallback func(key string)) (*CPUPolicyCheckMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())

	m := &CPUPolicyCheckMonitor{
		baseMonitor: newBaseMonitor(ctx, quit, logger, ds, cpuPolicyCheckMonitorSyncPeriod),

		nodeName: nodeName,

		collectedDataLock: sync.RWMutex{},
		collectedData:     &CollectedCPUPolicyCheckInfo{},

		syncCallback: syncCallback,
	}

	go m.Start()

	return m, nil
}

func (m *CPUPolicyCheckMonitor) Start() {
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

func (m *CPUPolicyCheckMonitor) Stop() {
	m.quit()
}

func (m *CPUPolicyCheckMonitor) RunOnce() error {
	return m.run(struct{}{})
}

func (m *CPUPolicyCheckMonitor) UpdateConfiguration(map[string]interface{}) error {
	return nil
}

func (m *CPUPolicyCheckMonitor) GetCollectedData() (interface{}, error) {
	m.collectedDataLock.RLock()
	defer m.collectedDataLock.RUnlock()

	data := longhorn.CPUManagerPolicyUnknown
	if err := copier.CopyWithOption(&data, &m.collectedData.CPUPolicy, copier.Option{IgnoreEmpty: true, DeepCopy: true}); err != nil {
		return data, errors.Wrap(err, "failed to copy collected data")
	}

	return data, nil
}

func (m *CPUPolicyCheckMonitor) run(value interface{}) error {
	node, err := m.ds.GetNodeRO(m.nodeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get longhorn node %v", m.nodeName)
	}

	collectedData := m.cpuPolicyCheck()
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

func (m *CPUPolicyCheckMonitor) cpuPolicyCheck() *CollectedCPUPolicyCheckInfo {
	collectedData := &CollectedCPUPolicyCheckInfo{
		CPUPolicy: longhorn.CPUManagerPolicyUnknown,
	}

	if err := m.syncNodeCPUManagerPolicy(collectedData); err != nil {
		m.logger.WithError(err).Debug("Failed to sync node CPU manager policy")
	}

	return collectedData
}

func (m *CPUPolicyCheckMonitor) syncNodeCPUManagerPolicy(collectedData *CollectedCPUPolicyCheckInfo) error {
	kubeletCPUManagerStateFilePath := getKubeletCPUManagerStateFilePath()
	data, err := lhns.ReadFileContent(kubeletCPUManagerStateFilePath)
	if err != nil {
		m.logger.WithError(err).Debugf("Failed to read CPU state file %s", kubeletCPUManagerStateFilePath)
		collectedData.CPUPolicy = longhorn.CPUManagerPolicyUnknown
		return nil
	}

	// Unmarshal the JSON data
	var state CPUManagerState
	if err := json.Unmarshal([]byte(data), &state); err != nil {
		m.logger.WithError(err).Debugf("Failed to parse CPU state file %s", kubeletCPUManagerStateFilePath)
		collectedData.CPUPolicy = longhorn.CPUManagerPolicyUnknown
		return nil
	}

	collectedData.CPUPolicy = state.PolicyName
	return nil
}

func getKubeletCPUManagerStateFilePath() string {
	kubeletRootDirPath := strings.TrimSpace(os.Getenv(types.EnvKubeletRootDir))
	if kubeletRootDirPath == "" {
		kubeletRootDirPath = DefaultKubeletRootDir
	}
	return filepath.Join(kubeletRootDirPath, DefaultCpuManagerStateFile)
}
