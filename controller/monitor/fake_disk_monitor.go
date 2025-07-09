package monitor

import (
	"context"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"

	lhtypes "github.com/longhorn/go-common-libs/types"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	TestDiskID1 = "fsid"

	TestOrphanedReplicaDirectoryName = "test-volume-r-000000000"
)

func NewFakeDiskMonitor(logger logrus.FieldLogger, ds *datastore.DataStore, nodeName string, syncCallback func(key string)) (*DiskMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())

	m := &DiskMonitor{
		baseMonitor: newBaseMonitor(ctx, quit, logger, ds, DiskMonitorSyncPeriod),

		nodeName:        nodeName,
		checkVolumeMeta: false,

		collectedDataLock: sync.RWMutex{},
		collectedData:     make(map[string]*CollectedDiskInfo, 0),

		syncCallback: syncCallback,

		getDiskStatHandler:          fakeGetDiskStat,
		getDiskConfigHandler:        fakeGetDiskConfig,
		generateDiskConfigHandler:   fakeGenerateDiskConfig,
		getReplicaDataStoresHandler: fakeGetReplicaDataStores,
		getDiskMetricsHandler:       fakeGetDiskMetrics,
	}

	return m, nil
}

func fakeGetReplicaDataStores(diskType longhorn.DiskType, node *longhorn.Node, diskName, diskUUID, diskPath, diskDriver string, client *DiskServiceClient) (map[string]string, error) {
	return map[string]string{
		TestOrphanedReplicaDirectoryName: "",
	}, nil
}

func fakeGetDiskStat(diskType longhorn.DiskType, name, directory string, diskDriver longhorn.DiskDriver, client *DiskServiceClient) (*lhtypes.DiskStat, error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return &lhtypes.DiskStat{
			DiskID:      "fsid",
			Name:        name,
			Path:        directory,
			Type:        "ext4",
			Driver:      "",
			FreeBlocks:  0,
			TotalBlocks: 0,
			BlockSize:   0,

			StorageMaximum:   0,
			StorageAvailable: 0,
		}, nil
	case longhorn.DiskTypeBlock:
		return &lhtypes.DiskStat{
			DiskID:      "block",
			Name:        name,
			Path:        directory,
			Type:        "ext4",
			Driver:      "",
			FreeBlocks:  0,
			TotalBlocks: 0,
			BlockSize:   0,

			StorageMaximum:   0,
			StorageAvailable: 0,
		}, nil
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func fakeGetDiskMetrics(diskType longhorn.DiskType, name, directory string, diskDriver longhorn.DiskDriver, client *DiskServiceClient) (*engineapi.Metrics, error) {
	// Return nil metrics for fake implementation - consistent with filesystem disk behavior where metrics are not supported
	return nil, nil
}

func fakeGetDiskConfig(diskType longhorn.DiskType, name, path string, diskDriver longhorn.DiskDriver, client *DiskServiceClient) (*util.DiskConfig, error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return &util.DiskConfig{
			DiskName: name,
			DiskUUID: TestDiskID1,
		}, nil
	case longhorn.DiskTypeBlock:
		return &util.DiskConfig{
			DiskName: name,
			DiskUUID: TestDiskID1,
		}, nil
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func fakeGenerateDiskConfig(diskType longhorn.DiskType, name, uuid, path, diskDriver string, client *DiskServiceClient, ds *datastore.DataStore) (*util.DiskConfig, error) {
	return &util.DiskConfig{
		DiskName: name,
		DiskUUID: TestDiskID1,
	}, nil
}
