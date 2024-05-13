package monitor

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/util/wait"

	lhtypes "github.com/longhorn/go-common-libs/types"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	NodeMonitorSyncPeriod = 30 * time.Second

	volumeMetaData = "volume.meta"
)

type DiskServiceClient struct {
	c   *engineapi.DiskService
	err error
}

type NodeMonitor struct {
	*baseMonitor

	nodeName        string
	checkVolumeMeta bool

	collectedDataLock sync.RWMutex
	collectedData     map[string]*CollectedDiskInfo

	syncCallback func(key string)

	getDiskStatHandler             GetDiskStatHandler
	getDiskConfigHandler           GetDiskConfigHandler
	generateDiskConfigHandler      GenerateDiskConfigHandler
	getReplicaInstanceNamesHandler GetReplicaInstanceNamesHandler
}

type CollectedDiskInfo struct {
	Path                          string
	NodeOrDiskEvicted             bool
	DiskStat                      *lhtypes.DiskStat
	DiskName                      string
	DiskUUID                      string
	DiskDriver                    longhorn.DiskDriver
	Condition                     *longhorn.Condition
	OrphanedReplicaDirectoryNames map[string]string
	InstanceManagerName           string
}

type GetDiskStatHandler func(longhorn.DiskType, string, string, longhorn.DiskDriver, *DiskServiceClient) (*lhtypes.DiskStat, error)
type GetDiskConfigHandler func(longhorn.DiskType, string, string, longhorn.DiskDriver, *DiskServiceClient) (*util.DiskConfig, error)
type GenerateDiskConfigHandler func(longhorn.DiskType, string, string, string, string, *DiskServiceClient) (*util.DiskConfig, error)
type GetReplicaInstanceNamesHandler func(longhorn.DiskType, *longhorn.Node, string, string, string, string, *DiskServiceClient) (map[string]string, error)

func NewDiskMonitor(logger logrus.FieldLogger, ds *datastore.DataStore, nodeName string, syncCallback func(key string)) (*NodeMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())

	m := &NodeMonitor{
		baseMonitor: newBaseMonitor(ctx, quit, logger, ds, NodeMonitorSyncPeriod),

		nodeName:        nodeName,
		checkVolumeMeta: true,

		collectedDataLock: sync.RWMutex{},
		collectedData:     make(map[string]*CollectedDiskInfo, 0),

		syncCallback: syncCallback,

		getDiskStatHandler:             getDiskStat,
		getDiskConfigHandler:           getDiskConfig,
		generateDiskConfigHandler:      generateDiskConfig,
		getReplicaInstanceNamesHandler: getReplicaInstanceNames,
	}

	go m.Start()

	return m, nil
}

func (m *NodeMonitor) Start() {
	if err := wait.PollUntilContextCancel(m.ctx, m.syncPeriod, false, func(context.Context) (bool, error) {
		if err := m.run(struct{}{}); err != nil {
			m.logger.WithError(err).Error("Stopped monitoring disks")
		}
		return false, nil
	}); err != nil {
		m.logger.WithError(err).Error("Failed to start node monitor")
	}
}

func (m *NodeMonitor) Close() {
	m.quit()
}

func (m *NodeMonitor) RunOnce() error {
	return m.run(struct{}{})
}

func (m *NodeMonitor) UpdateConfiguration(map[string]interface{}) error {
	return nil
}

func (m *NodeMonitor) GetCollectedData() (interface{}, error) {
	m.collectedDataLock.RLock()
	defer m.collectedDataLock.RUnlock()

	data := make(map[string]*CollectedDiskInfo, 0)
	if err := copier.CopyWithOption(&data, &m.collectedData, copier.Option{IgnoreEmpty: true, DeepCopy: true}); err != nil {
		return data, errors.Wrap(err, "failed to copy node monitor collected data")
	}

	return data, nil
}

func (m *NodeMonitor) run(value interface{}) error {
	node, err := m.ds.GetNode(m.nodeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get longhorn node %v", m.nodeName)
	}

	collectedData := m.collectDiskData(node)
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

func (m *NodeMonitor) getRunningInstanceManagerRO(dataEngine longhorn.DataEngineType) (*longhorn.InstanceManager, error) {
	switch dataEngine {
	case longhorn.DataEngineTypeV1:
		return m.ds.GetDefaultInstanceManagerByNodeRO(m.nodeName, dataEngine)
	case longhorn.DataEngineTypeV2:
		im, err := m.ds.GetDefaultInstanceManagerByNodeRO(m.nodeName, dataEngine)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get default instance manager for node %v", m.nodeName)
		}
		if im.Status.CurrentState == longhorn.InstanceManagerStateRunning {
			return im, nil
		}
		ims, err := m.ds.ListInstanceManagersByNodeRO(m.nodeName, longhorn.InstanceManagerTypeAllInOne, dataEngine)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to list instance managers for node %v", m.nodeName)
		}
		for _, im := range ims {
			if im.Status.CurrentState == longhorn.InstanceManagerStateRunning {
				return im, nil
			}
		}
		return nil, fmt.Errorf("failed to find running instance manager for node %v", m.nodeName)
	}
	return nil, fmt.Errorf("unknown data engine %v", dataEngine)
}

func (m *NodeMonitor) newDiskServiceClients() map[longhorn.DataEngineType]*DiskServiceClient {
	clients := map[longhorn.DataEngineType]*DiskServiceClient{}

	dataEngines := m.ds.GetDataEngines()

	for dataEngine := range dataEngines {
		// so we can skip it for now.
		if types.IsDataEngineV1(dataEngine) {
			continue
		}

		var client *engineapi.DiskService

		im, err := m.getRunningInstanceManagerRO(dataEngine)
		if err == nil {
			client, err = engineapi.NewDiskServiceClient(im, m.logger)
		}

		clients[dataEngine] = &DiskServiceClient{
			c:   client,
			err: err,
		}
	}

	return clients
}

func (m *NodeMonitor) closeDiskServiceClients(clients map[longhorn.DataEngineType]*DiskServiceClient) {
	for _, client := range clients {
		if client.c != nil {
			client.c.Close()
			client.c = nil
		}
	}
}

// Collect disk data and generate disk UUID blindly.
func (m *NodeMonitor) collectDiskData(node *longhorn.Node) map[string]*CollectedDiskInfo {
	diskInfoMap := make(map[string]*CollectedDiskInfo, 0)

	diskServiceClients := m.newDiskServiceClients()
	defer func() {
		m.closeDiskServiceClients(diskServiceClients)
	}()

	for diskName, disk := range node.Spec.Disks {
		dataEngine := util.GetDataEngineForDiskType(disk.Type)
		diskServiceClient := diskServiceClients[dataEngine]
		orphanedReplicaInstanceNames := map[string]string{}
		nodeOrDiskEvicted := isNodeOrDiskEvicted(node, disk)

		diskDriver := longhorn.DiskDriverNone
		if node.Status.DiskStatus != nil {
			if diskStatus, ok := node.Status.DiskStatus[diskName]; ok {
				diskDriver = diskStatus.DiskDriver
			}
		}

		instanceManagerName := ""
		errMsg := ""
		errReason := ""

		if diskServiceClient == nil {
			if types.IsDataEngineV2(dataEngine) {
				errMsg = fmt.Sprintf("Disk %v (%v) on node %v is not ready: data engine is disabled", diskName, disk.Path, node.Name)
				errReason = string(longhorn.DiskConditionReasonDiskServiceUnreachable)
			} else {
				// TODO: disk service is currently not used by filesystem-type disk for v1 data engine.
				if im, err := m.ds.GetDefaultInstanceManagerByNodeRO(m.nodeName, dataEngine); err != nil {
					errMsg = fmt.Sprintf("Disk %v (%v) on node %v is not ready: %v", diskName, disk.Path, node.Name, err.Error())
					errReason = string(longhorn.DiskConditionReasonDiskServiceUnreachable)
				} else {
					instanceManagerName = im.Name
				}
			}
		} else if diskServiceClient.err != nil {
			errMsg = fmt.Sprintf("Disk %v (%v) on node %v is not ready: %v", diskName, disk.Path, node.Name, diskServiceClient.err.Error())
			errReason = string(longhorn.DiskConditionReasonDiskServiceUnreachable)
		} else {
			instanceManagerName = diskServiceClient.c.GetInstanceManagerName()
		}

		diskInfoMap[diskName] = NewDiskInfo(diskName, "", disk.Path, diskDriver, nodeOrDiskEvicted, nil,
			orphanedReplicaInstanceNames, instanceManagerName, errReason, errMsg)

		diskConfig, err := m.getDiskConfigHandler(disk.Type, diskName, disk.Path, diskDriver, diskServiceClient)
		if err != nil {
			if !types.ErrorIsNotFound(err) {
				diskInfoMap[diskName] = NewDiskInfo(diskName, "", disk.Path, diskDriver, nodeOrDiskEvicted, nil,
					orphanedReplicaInstanceNames, instanceManagerName, string(longhorn.DiskConditionReasonNoDiskInfo),
					fmt.Sprintf("Disk %v(%v) on node %v is not ready: failed to get disk config: error: %v",
						diskName, disk.Path, node.Name, err))
				continue
			}

			diskUUID := ""
			diskDriver := disk.DiskDriver
			if node.Status.DiskStatus != nil {
				if diskStatus, ok := node.Status.DiskStatus[diskName]; ok {
					diskUUID = diskStatus.DiskUUID
					if diskStatus.DiskDriver != "" {
						diskDriver = diskStatus.DiskDriver
					}
				}
			}

			// Filesystem-type disk
			//   Blindly check or generate disk config.
			//   The handling of all disks containing the same fsid will be done in NodeController.
			// Block-type disk
			//   Create a bdev lvstore
			if diskConfig, err = m.generateDiskConfigHandler(disk.Type, diskName, diskUUID, disk.Path, string(diskDriver), diskServiceClient); err != nil {
				diskInfoMap[diskName] = NewDiskInfo(diskName, diskUUID, disk.Path, diskDriver, nodeOrDiskEvicted, nil,
					orphanedReplicaInstanceNames, instanceManagerName, string(longhorn.DiskConditionReasonNoDiskInfo),
					fmt.Sprintf("Disk %v(%v) on node %v is not ready: failed to generate disk config: error: %v",
						diskName, disk.Path, node.Name, err))
				continue
			}
		}

		stat, err := m.getDiskStatHandler(disk.Type, diskName, disk.Path, diskDriver, diskServiceClient)
		if err != nil {
			diskInfoMap[diskName] = NewDiskInfo(diskName, "", disk.Path, diskDriver, nodeOrDiskEvicted, nil,
				orphanedReplicaInstanceNames, instanceManagerName, string(longhorn.DiskConditionReasonNoDiskInfo),
				fmt.Sprintf("Disk %v(%v) on node %v is not ready: Get disk information error: %v",
					diskName, node.Spec.Disks[diskName].Path, node.Name, err))
			continue
		}

		replicaInstanceNames, err := m.getReplicaInstanceNamesHandler(disk.Type, node, diskName, diskConfig.DiskUUID, disk.Path, string(disk.DiskDriver), diskServiceClient)
		if err != nil {
			m.logger.WithError(err).Warnf("Failed to get replica instance names for disk %v(%v) on node %v", diskName, disk.Path, node.Name)
			continue
		}

		orphanedReplicaInstanceNames, err = m.getOrphanedReplicaInstanceNames(disk.Type, diskConfig.DiskUUID, disk.Path, replicaInstanceNames)
		if err != nil {
			m.logger.WithError(err).Warnf("Failed to get orphaned replica instance names for disk %v(%v) on node %v", diskName, disk.Path, node.Name)
			continue
		}

		diskInfoMap[diskName] = NewDiskInfo(diskConfig.DiskName, diskConfig.DiskUUID, disk.Path, diskConfig.DiskDriver, nodeOrDiskEvicted, stat,
			orphanedReplicaInstanceNames, instanceManagerName, string(longhorn.DiskConditionReasonNoDiskInfo), "")
	}

	return diskInfoMap
}

func isNodeOrDiskEvicted(node *longhorn.Node, disk longhorn.DiskSpec) bool {
	return node.Spec.EvictionRequested || disk.EvictionRequested
}

func getReplicaInstanceNames(diskType longhorn.DiskType, node *longhorn.Node, diskName, diskUUID, diskPath, diskDriver string, client *DiskServiceClient) (map[string]string, error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return getReplicaDirectoryNames(node, diskName, diskUUID, diskPath)
	case longhorn.DiskTypeBlock:
		return getSpdkReplicaInstanceNames(client, string(diskType), diskName, diskDriver)
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func getReplicaDirectoryNames(node *longhorn.Node, diskName, diskUUID, diskPath string) (map[string]string, error) {
	if !canCollectDiskData(node, diskName, diskUUID, diskPath) {
		return map[string]string{}, nil
	}

	possibleReplicaDirectoryNames, err := util.GetPossibleReplicaDirectoryNames(diskPath)
	if err != nil {
		logrus.WithError(err).Errorf("Failed to get possible replica directories in disk %v on node %v", diskPath, node.Name)
		return map[string]string{}, nil
	}

	return possibleReplicaDirectoryNames, nil
}

func canCollectDiskData(node *longhorn.Node, diskName, diskUUID, diskPath string) bool {
	return !node.Spec.EvictionRequested &&
		!node.Spec.Disks[diskName].EvictionRequested &&
		node.Spec.Disks[diskName].Path == diskPath &&
		node.Status.DiskStatus != nil &&
		node.Status.DiskStatus[diskName] != nil &&
		node.Status.DiskStatus[diskName].DiskUUID == diskUUID &&
		types.GetCondition(node.Status.DiskStatus[diskName].Conditions, longhorn.DiskConditionTypeReady).Status == longhorn.ConditionStatusTrue
}

func NewDiskInfo(diskName, diskUUID, diskPath string, diskDriver longhorn.DiskDriver, nodeOrDiskEvicted bool, stat *lhtypes.DiskStat,
	orphanedReplicaDirectoryNames map[string]string, instanceManagerName string, errorReason, errorMessage string) *CollectedDiskInfo {
	diskInfo := &CollectedDiskInfo{
		DiskName:                      diskName,
		DiskUUID:                      diskUUID,
		Path:                          diskPath,
		NodeOrDiskEvicted:             nodeOrDiskEvicted,
		DiskDriver:                    diskDriver,
		DiskStat:                      stat,
		OrphanedReplicaDirectoryNames: orphanedReplicaDirectoryNames,
		InstanceManagerName:           instanceManagerName,
	}

	if errorMessage != "" {
		diskInfo.Condition = &longhorn.Condition{
			Type:    longhorn.DiskConditionTypeError,
			Status:  longhorn.ConditionStatusFalse,
			Reason:  errorReason,
			Message: errorMessage,
		}
	}

	return diskInfo
}

func (m *NodeMonitor) getOrphanedReplicaInstanceNames(diskType longhorn.DiskType, diskUUID, diskPath string, replicaDirectoryNames map[string]string) (map[string]string, error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return m.getOrphanedReplicaDirectoryNames(diskUUID, diskPath, replicaDirectoryNames)
	case longhorn.DiskTypeBlock:
		return m.getOrphanedReplicaLvolNames(replicaDirectoryNames)
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func (m *NodeMonitor) getOrphanedReplicaLvolNames(replicaDirectoryNames map[string]string) (map[string]string, error) {
	if len(replicaDirectoryNames) == 0 {
		return map[string]string{}, nil
	}

	for name := range replicaDirectoryNames {
		_, err := m.ds.GetReplica(name)
		if err == nil || !datastore.ErrorIsNotFound(err) {
			delete(replicaDirectoryNames, name)
		}
	}

	return replicaDirectoryNames, nil
}

func (m *NodeMonitor) getOrphanedReplicaDirectoryNames(diskUUID, diskPath string, replicaDirectoryNames map[string]string) (map[string]string, error) {
	if len(replicaDirectoryNames) == 0 {
		return map[string]string{}, nil
	}

	// Find out the orphaned directories by checking with replica CRs
	replicas, err := m.ds.ListReplicasByDiskUUID(diskUUID)
	if err != nil {
		m.logger.WithError(err).Errorf("Failed to list replicas for disk UUID %v", diskUUID)
		return map[string]string{}, nil
	}

	for _, replica := range replicas {
		if replica.Spec.DiskPath == diskPath {
			delete(replicaDirectoryNames, replica.Spec.DataDirectoryName)
		}
	}

	if m.checkVolumeMeta {
		for name := range replicaDirectoryNames {
			if err := isVolumeMetaFileExist(diskPath, name); err != nil {
				delete(replicaDirectoryNames, name)
			}
		}
	}

	return replicaDirectoryNames, nil
}

func isVolumeMetaFileExist(diskPath, replicaDirectoryName string) error {
	path := filepath.Join(diskPath, "replicas", replicaDirectoryName, volumeMetaData)
	_, err := util.GetVolumeMeta(path)
	return err
}

func GetDiskNamesFromDiskMap(diskInfoMap map[string]*CollectedDiskInfo) []string {
	disks := []string{}
	for diskName := range diskInfoMap {
		disks = append(disks, diskName)
	}
	return disks
}
