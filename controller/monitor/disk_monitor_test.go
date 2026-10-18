package monitor

import (
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	logrustest "github.com/sirupsen/logrus/hooks/test"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	imapi "github.com/longhorn/longhorn-instance-manager/pkg/api"
	spdkdisk "github.com/longhorn/longhorn-spdk-engine/pkg/spdk"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	testNamespace     = "longhorn-system"
	testNodeName      = "test-node"
	testBlockDiskName = "block-disk"
	testBlockDiskPath = "0000:05:00.0"
	testBlockDiskUUID = "block-disk-uuid"
)

func TestTruncateDiskMessage(t *testing.T) {
	assert := require.New(t)

	short := "unsupported disk driver vfio-pci for disk path"
	assert.Equal(short, truncateDiskMessage(short))
	assert.Equal("", truncateDiskMessage(""))

	long := strings.Repeat("a", diskMessageMaxLength+10)
	truncated := truncateDiskMessage(long)
	assert.Equal(diskMessageMaxLength+len("..."), len(truncated))
	assert.True(strings.HasSuffix(truncated, "..."))

	// A multi-byte rune straddling the cut must not leave invalid UTF-8 behind.
	multiByte := strings.Repeat("a", diskMessageMaxLength-1) + "日本語"
	assert.True(utf8.ValidString(truncateDiskMessage(multiByte)))
}

// newTestDiskMonitor builds a disk monitor whose datastore has no settings, so no
// disk service client is created and only the injected handlers are exercised.
func newTestDiskMonitor(t *testing.T, getDiskConfig GetDiskConfigHandler, generateDiskConfig GenerateDiskConfigHandler) *DiskMonitor {
	kubeClient := kubefake.NewSimpleClientset()
	lhClient := lhfake.NewSimpleClientset() // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset()
	informerFactories := util.NewInformerFactories(testNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStoreForGlobal(testNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	m, err := NewFakeDiskMonitor(logrus.StandardLogger(), ds, testNodeName, func(string) {})
	require.NoError(t, err)

	m.getDiskConfigHandler = getDiskConfig
	m.generateDiskConfigHandler = generateDiskConfig

	return m
}

func newTestBlockDiskNode() *longhorn.Node {
	return &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{Name: testNodeName, Namespace: testNamespace},
		Spec: longhorn.NodeSpec{
			Disks: map[string]longhorn.DiskSpec{
				testBlockDiskName: {
					Type:       longhorn.DiskTypeBlock,
					Path:       testBlockDiskPath,
					DiskDriver: longhorn.DiskDriverAuto,
				},
			},
		},
		Status: longhorn.NodeStatus{
			DiskStatus: map[string]*longhorn.DiskStatus{
				testBlockDiskName: {
					Type:       longhorn.DiskTypeBlock,
					DiskUUID:   testBlockDiskUUID,
					DiskPath:   testBlockDiskPath,
					DiskDriver: longhorn.DiskDriverNvme,
				},
			},
		},
	}
}

// TestCollectDiskDataRetriesFailedBlockDisk covers longhorn/longhorn#13893: the disk
// service keeps a failed disk in the error state and still answers DiskGet, so the
// monitor has to re-issue the creation itself for the disk to ever recover.
func TestCollectDiskDataRetriesFailedBlockDisk(t *testing.T) {
	assert := require.New(t)

	failureMessage := "unsupported disk driver vfio-pci for disk path 0000:05:00.0"
	generateCalls := 0
	generatedUUID, generatedDriver := "", ""

	m := newTestDiskMonitor(t,
		func(diskType longhorn.DiskType, diskName, diskPath string, diskDriver longhorn.DiskDriver, client *DiskServiceClient) (*util.DiskConfig, error) {
			return &util.DiskConfig{
				DiskName: diskName,
				State:    string(spdkdisk.DiskStateError),
				Message:  failureMessage,
			}, nil
		},
		func(diskType longhorn.DiskType, diskName, diskUUID, diskPath, diskDriver string, client *DiskServiceClient, ds *datastore.DataStore) (*util.DiskConfig, error) {
			generateCalls++
			generatedUUID, generatedDriver = diskUUID, diskDriver
			// The disk service only reports the state when a creation is started.
			return &util.DiskConfig{State: string(spdkdisk.DiskStateCreating)}, nil
		},
	)

	diskInfoMap := m.collectDiskData(newTestBlockDiskNode())

	assert.Equal(1, generateCalls)
	// The recorded UUID and driver must be reused, otherwise the retry orphans the
	// lvstore of the previous attempt or re-resolves a driver that is already known.
	assert.Equal(testBlockDiskUUID, generatedUUID)
	assert.Equal(string(longhorn.DiskDriverNvme), generatedDriver)

	diskInfo, ok := diskInfoMap[testBlockDiskName]
	assert.True(ok)
	assert.NotNil(diskInfo.Condition)
	// A retry restarts from the creating state with no message, so the reason of the
	// previous failure has to be carried over; otherwise it is never reported.
	assert.Contains(diskInfo.Condition.Message, failureMessage)
}

// TestCollectDiskDataKeepsReportingFailureAcrossCollections covers the steady state
// of longhorn/longhorn#13893: the retried creation fails fast on the still-bound
// device, so the disk service latches back to the error state and keeps answering
// DiskGet with the reason. Each collection therefore re-surfaces the failure without
// relying on any state carried between collections.
func TestCollectDiskDataKeepsReportingFailureAcrossCollections(t *testing.T) {
	assert := require.New(t)

	failureMessage := "unsupported disk driver vfio-pci for disk path 0000:05:00.0"
	generateCalls := 0

	m := newTestDiskMonitor(t,
		func(diskType longhorn.DiskType, diskName, diskPath string, diskDriver longhorn.DiskDriver, client *DiskServiceClient) (*util.DiskConfig, error) {
			return &util.DiskConfig{
				DiskName: diskName,
				State:    string(spdkdisk.DiskStateError),
				Message:  failureMessage,
			}, nil
		},
		func(diskType longhorn.DiskType, diskName, diskUUID, diskPath, diskDriver string, client *DiskServiceClient, ds *datastore.DataStore) (*util.DiskConfig, error) {
			generateCalls++
			return &util.DiskConfig{State: string(spdkdisk.DiskStateCreating)}, nil
		},
	)

	for i := 0; i < 2; i++ {
		diskInfoMap := m.collectDiskData(newTestBlockDiskNode())
		diskInfo, ok := diskInfoMap[testBlockDiskName]
		assert.True(ok)
		assert.NotNil(diskInfo.Condition)
		assert.Contains(diskInfo.Condition.Message, failureMessage)
	}
	// Every collection re-issues the creation, so the disk can recover as soon as the
	// underlying device is released.
	assert.Equal(2, generateCalls)
}

// TestCollectDiskDataDoesNotRetryReadyBlockDisk guards the retry from re-creating a
// disk that the disk service already reports as ready.
func TestCollectDiskDataDoesNotRetryReadyBlockDisk(t *testing.T) {
	assert := require.New(t)

	generateCalls := 0

	m := newTestDiskMonitor(t,
		func(diskType longhorn.DiskType, diskName, diskPath string, diskDriver longhorn.DiskDriver, client *DiskServiceClient) (*util.DiskConfig, error) {
			return &util.DiskConfig{
				DiskName:   diskName,
				DiskUUID:   testBlockDiskUUID,
				DiskDriver: longhorn.DiskDriverNvme,
				State:      string(spdkdisk.DiskStateReady),
			}, nil
		},
		func(diskType longhorn.DiskType, diskName, diskUUID, diskPath, diskDriver string, client *DiskServiceClient, ds *datastore.DataStore) (*util.DiskConfig, error) {
			generateCalls++
			return &util.DiskConfig{State: string(spdkdisk.DiskStateCreating)}, nil
		},
	)

	diskInfoMap := m.collectDiskData(newTestBlockDiskNode())

	assert.Equal(0, generateCalls)
	diskInfo, ok := diskInfoMap[testBlockDiskName]
	assert.True(ok)
	assert.Nil(diskInfo.Condition)
}

// TestCollectDiskDataToleratesNilDiskStatus makes sure a nil status entry, which
// the monitor may read straight from the CR before it is normalized, does not
// panic the controller.
func TestCollectDiskDataToleratesNilDiskStatus(t *testing.T) {
	assert := require.New(t)

	m := newTestDiskMonitor(t,
		func(diskType longhorn.DiskType, diskName, diskPath string, diskDriver longhorn.DiskDriver, client *DiskServiceClient) (*util.DiskConfig, error) {
			return &util.DiskConfig{
				DiskName:   diskName,
				DiskUUID:   testBlockDiskUUID,
				DiskDriver: longhorn.DiskDriverNvme,
				State:      string(spdkdisk.DiskStateReady),
			}, nil
		},
		func(diskType longhorn.DiskType, diskName, diskUUID, diskPath, diskDriver string, client *DiskServiceClient, ds *datastore.DataStore) (*util.DiskConfig, error) {
			return &util.DiskConfig{State: string(spdkdisk.DiskStateCreating)}, nil
		},
	)

	node := newTestBlockDiskNode()
	node.Status.DiskStatus[testBlockDiskName] = nil

	assert.NotPanics(func() {
		m.collectDiskData(node)
	})
}

func getReadyBlockDiskConfig(diskType longhorn.DiskType, diskName, diskPath string, diskDriver longhorn.DiskDriver, client *DiskServiceClient) (*util.DiskConfig, error) {
	return &util.DiskConfig{
		DiskName:   diskName,
		DiskUUID:   testBlockDiskUUID,
		DiskDriver: longhorn.DiskDriverNvme,
		State:      string(spdkdisk.DiskStateReady),
	}, nil
}

func countWarnLogs(hook *logrustest.Hook, substr string) int {
	count := 0
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, substr) {
			count++
		}
	}
	return count
}

// TestCollectDiskDataReportsHealthDataCollectionFailure covers longhorn/longhorn#12353:
// a failed disk health collection is recorded in the HealthDataCollected condition and
// only the first failure is logged as a warning.
func TestCollectDiskDataReportsHealthDataCollectionFailure(t *testing.T) {
	assert := require.New(t)

	m := newTestDiskMonitor(t, getReadyBlockDiskConfig,
		func(diskType longhorn.DiskType, diskName, diskUUID, diskPath, diskDriver string, client *DiskServiceClient, ds *datastore.DataStore) (*util.DiskConfig, error) {
			return &util.DiskConfig{State: string(spdkdisk.DiskStateCreating)}, nil
		},
	)
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.TraceLevel)
	m.logger = logger

	healthErr := fmt.Errorf("smartctl is not available")
	m.getDiskHealthHandler = func(diskType longhorn.DiskType, diskName, diskPath string, diskDriver longhorn.DiskDriver, lastCollectedAt time.Time, client *DiskServiceClient, logger logrus.FieldLogger) (map[string]longhorn.HealthData, time.Time, error) {
		return nil, time.Now().Truncate(time.Second), healthErr
	}

	// First failure: no condition recorded yet, so a warning is logged.
	node := newTestBlockDiskNode()
	diskInfo := m.collectDiskData(node)[testBlockDiskName]
	assert.NotNil(diskInfo)
	assert.NotNil(diskInfo.HealthDataCondition)
	assert.Equal(longhorn.DiskConditionTypeHealthDataCollected, diskInfo.HealthDataCondition.Type)
	assert.Equal(longhorn.ConditionStatusFalse, diskInfo.HealthDataCondition.Status)
	assert.Equal(longhorn.DiskConditionReasonHealthDataCollectionFailed, diskInfo.HealthDataCondition.Reason)
	assert.Contains(diskInfo.HealthDataCondition.Message, healthErr.Error())
	assert.Equal(1, countWarnLogs(hook, "Failed to get disk health"))

	// Repeated failure: the failure is already recorded in the disk status, so no
	// further warning is logged.
	node.Status.DiskStatus[testBlockDiskName].Conditions = types.SetCondition(nil,
		longhorn.DiskConditionTypeHealthDataCollected, longhorn.ConditionStatusFalse,
		longhorn.DiskConditionReasonHealthDataCollectionFailed, diskInfo.HealthDataCondition.Message)
	diskInfo = m.collectDiskData(node)[testBlockDiskName]
	assert.NotNil(diskInfo.HealthDataCondition)
	assert.Equal(longhorn.ConditionStatusFalse, diskInfo.HealthDataCondition.Status)
	assert.Equal(1, countWarnLogs(hook, "Failed to get disk health"))

	// Recovered collection is reported as collected.
	m.getDiskHealthHandler = func(diskType longhorn.DiskType, diskName, diskPath string, diskDriver longhorn.DiskDriver, lastCollectedAt time.Time, client *DiskServiceClient, logger logrus.FieldLogger) (map[string]longhorn.HealthData, time.Time, error) {
		return map[string]longhorn.HealthData{}, lastCollectedAt.Add(HealthDataUpdateInterval), nil
	}
	diskInfo = m.collectDiskData(node)[testBlockDiskName]
	assert.NotNil(diskInfo.HealthDataCondition)
	assert.Equal(longhorn.ConditionStatusTrue, diskInfo.HealthDataCondition.Status)

	// A failure after a recovery is warned again.
	node.Status.DiskStatus[testBlockDiskName].Conditions = types.SetCondition(node.Status.DiskStatus[testBlockDiskName].Conditions,
		longhorn.DiskConditionTypeHealthDataCollected, longhorn.ConditionStatusTrue, "", diskInfo.HealthDataCondition.Message)
	m.getDiskHealthHandler = func(diskType longhorn.DiskType, diskName, diskPath string, diskDriver longhorn.DiskDriver, lastCollectedAt time.Time, client *DiskServiceClient, logger logrus.FieldLogger) (map[string]longhorn.HealthData, time.Time, error) {
		return nil, time.Now().Truncate(time.Second), healthErr
	}
	diskInfo = m.collectDiskData(node)[testBlockDiskName]
	assert.Equal(longhorn.ConditionStatusFalse, diskInfo.HealthDataCondition.Status)
	assert.Equal(2, countWarnLogs(hook, "Failed to get disk health"))
}

type failingDiskHealthClient struct {
	err error
}

func (c *failingDiskHealthClient) DiskHealthGet(diskType, diskName, diskPath, diskDriver string) (*imapi.DiskHealth, error) {
	return nil, c.err
}

func TestCollectDiskDataSuppressesRepeatedNVMeHealthWarnings(t *testing.T) {
	assert := require.New(t)
	m := newTestDiskMonitor(t, getReadyBlockDiskConfig,
		func(diskType longhorn.DiskType, diskName, diskUUID, diskPath, diskDriver string, client *DiskServiceClient, ds *datastore.DataStore) (*util.DiskConfig, error) {
			return &util.DiskConfig{State: string(spdkdisk.DiskStateCreating)}, nil
		},
	)
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	m.logger = logger
	healthErr := fmt.Errorf("NVMe controller health request failed")
	m.getDiskHealthHandler = func(diskType longhorn.DiskType, diskName, diskPath string, diskDriver longhorn.DiskDriver, lastCollectedAt time.Time, client *DiskServiceClient, logger logrus.FieldLogger) (map[string]longhorn.HealthData, time.Time, error) {
		health, err := getHealthDataFromControllerHealthInfo(diskName, diskPath, &failingDiskHealthClient{err: healthErr}, logger)
		assert.ErrorIs(err, healthErr)
		return health, time.Now().Truncate(time.Second), err
	}

	node := newTestBlockDiskNode()
	for attempt := 0; attempt < 3; attempt++ {
		info := m.collectDiskData(node)[testBlockDiskName]
		assert.NotNil(info)
		assert.NotNil(info.HealthDataCondition)
		assert.Equal(longhorn.ConditionStatusFalse, info.HealthDataCondition.Status)
		assert.Contains(info.HealthDataCondition.Message, healthErr.Error())
		assert.Equal(1, countWarnLogs(hook, ""))
		assert.Equal(1, countWarnLogs(hook, "Failed to get disk health"))
		node.Status.DiskStatus[testBlockDiskName].Conditions = types.SetCondition(nil,
			info.HealthDataCondition.Type, info.HealthDataCondition.Status,
			info.HealthDataCondition.Reason, info.HealthDataCondition.Message)
	}
	helperDebugLogs := 0
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.DebugLevel && entry.Message == "Failed to get controller health info" {
			helperDebugLogs++
		}
	}
	assert.Equal(3, helperDebugLogs)
}

// TestCollectDiskDataSkippedHealthDataCollection makes sure a skipped health data
// collection keeps the recorded HealthDataCollected condition as is.
func TestCollectDiskDataSkippedHealthDataCollection(t *testing.T) {
	assert := require.New(t)

	m := newTestDiskMonitor(t, getReadyBlockDiskConfig,
		func(diskType longhorn.DiskType, diskName, diskUUID, diskPath, diskDriver string, client *DiskServiceClient, ds *datastore.DataStore) (*util.DiskConfig, error) {
			return &util.DiskConfig{State: string(spdkdisk.DiskStateCreating)}, nil
		},
	)

	node := newTestBlockDiskNode()
	diskInfo := m.collectDiskData(node)[testBlockDiskName]
	assert.NotNil(diskInfo)
	assert.Nil(diskInfo.HealthDataCondition)

	failureMessage := "failed to collect health data"
	node.Status.DiskStatus[testBlockDiskName].Conditions = types.SetCondition(nil,
		longhorn.DiskConditionTypeHealthDataCollected, longhorn.ConditionStatusFalse,
		longhorn.DiskConditionReasonHealthDataCollectionFailed, failureMessage)
	diskInfo = m.collectDiskData(node)[testBlockDiskName]
	assert.NotNil(diskInfo.HealthDataCondition)
	assert.Equal(longhorn.ConditionStatusFalse, diskInfo.HealthDataCondition.Status)
	assert.Equal(failureMessage, diskInfo.HealthDataCondition.Message)
}

func TestCollectDiskDataSkippedHealthDataCollectionIgnoresConditionTimestamps(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprintf("failed=%v", failed), func(t *testing.T) {
			assert := require.New(t)
			m := newTestDiskMonitor(t, getReadyBlockDiskConfig,
				func(diskType longhorn.DiskType, diskName, diskUUID, diskPath, diskDriver string, client *DiskServiceClient, ds *datastore.DataStore) (*util.DiskConfig, error) {
					return &util.DiskConfig{State: string(spdkdisk.DiskStateCreating)}, nil
				},
			)
			collectedAt := time.Now().Truncate(time.Second)
			m.getDiskHealthHandler = func(diskType longhorn.DiskType, diskName, diskPath string, diskDriver longhorn.DiskDriver, lastCollectedAt time.Time, client *DiskServiceClient, logger logrus.FieldLogger) (map[string]longhorn.HealthData, time.Time, error) {
				if failed {
					return nil, collectedAt, fmt.Errorf("health collection failed")
				}
				return map[string]longhorn.HealthData{}, collectedAt, nil
			}

			node := newTestBlockDiskNode()
			collectedData := m.collectDiskData(node)
			info := collectedData[testBlockDiskName]
			assert.NotNil(info)
			assert.NotNil(info.HealthDataCondition)

			// Simulate the controller persisting health data and condition timestamps.
			diskStatus := node.Status.DiskStatus[testBlockDiskName]
			diskStatus.HealthData = info.HealthData
			diskStatus.HealthDataLastCollectedAt = metav1.NewTime(info.HealthDataLastCollectedAt)
			condition := info.HealthDataCondition
			diskStatus.Conditions = types.SetCondition(nil, condition.Type, condition.Status, condition.Reason, condition.Message)
			diskStatus.Conditions[0].LastProbeTime = collectedAt.Format(time.RFC3339)
			persistedCondition := diskStatus.Conditions[0]
			assert.NotEmpty(persistedCondition.LastTransitionTime)
			originalNode := node.DeepCopy()

			m.getDiskHealthHandler = func(diskType longhorn.DiskType, diskName, diskPath string, diskDriver longhorn.DiskDriver, lastCollectedAt time.Time, client *DiskServiceClient, logger logrus.FieldLogger) (map[string]longhorn.HealthData, time.Time, error) {
				return nil, lastCollectedAt, nil
			}
			for poll := 0; poll < 2; poll++ {
				skippedData := m.collectDiskData(node)
				assert.Equal(collectedData, skippedData, "skipped polls must not trigger a node reconciliation")
				assert.Equal(originalNode, node, "persisted condition timestamps must remain unchanged")
			}
		})
	}
}
