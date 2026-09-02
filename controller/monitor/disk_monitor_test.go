package monitor

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	spdkdisk "github.com/longhorn/longhorn-spdk-engine/pkg/spdk"

	"github.com/longhorn/longhorn-manager/datastore"
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
	ds := datastore.NewDataStore(testNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

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
