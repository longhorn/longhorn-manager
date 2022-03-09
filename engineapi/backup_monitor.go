package engineapi

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/util/wait"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	BackupMonitorSyncPeriod = 2 * time.Second
)

type BackupMonitor struct {
	logger logrus.FieldLogger

	namespace      string
	backupName     string
	snapshotName   string
	replicaAddress string
	engineClient   EngineClient

	backupStatus     longhorn.BackupStatus
	backupStatusLock sync.RWMutex

	syncCallback func(key string)

	ctx  context.Context
	quit context.CancelFunc
}

func NewBackupMonitor(logger logrus.FieldLogger,
	backup *longhorn.Backup, volume *longhorn.Volume, backupTargetClient *BackupTargetClient,
	biChecksum string, engineClient EngineClient, syncCallback func(key string)) (*BackupMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())
	m := &BackupMonitor{
		logger: logger,

		namespace:    backup.Namespace,
		backupName:   backup.Name,
		snapshotName: backup.Spec.SnapshotName,
		engineClient: engineClient,

		backupStatusLock: sync.RWMutex{},

		syncCallback: syncCallback,

		ctx:  ctx,
		quit: quit,
	}

	// Call engine API snapshot backup
	if backup.Status.State == longhorn.BackupStateNew {
		_, replicaAddress, err := engineClient.SnapshotBackup(backup.Name, backup.Spec.SnapshotName,
			backupTargetClient.URL, volume.Spec.BackingImage, biChecksum,
			backup.Spec.Labels, backupTargetClient.Credential)
		if err != nil {
			if !strings.Contains(err.Error(), "DeadlineExceeded") {
				m.logger.WithError(err).Warn("Cannot take snapshot backup")
				return nil, err
			}

			// [Workaround]
			// Special handling the RPC call return code DeadlineExceeded, mark it as Pending state.
			// The snapshot backup initialization _probably_ succeeded in the replica sync agent server.
			// Use the backup monitor routine to monitor the backup status stays in Error state or change
			// to InProgress/Completed state with the maximum retry count mechanism.
			// [TODO]
			// Since API engineclient.SnapshotBackup is not idempotent, this controller cannot blindly
			// retry the call when error DeadlineExceeded is triggered.
			// Instead, it has to mark the backup as a kind of special state Pending, then relies on the
			// backup monitor routine periodically checking if the backup creation actually started.
			// After making the API call idempotent and being able to deprecate the old version
			// engine image, we can remove this part.
			// https://github.com/longhorn/longhorn/issues/3545
			m.logger.WithError(err).Warnf("Snapshot backup timeout")
			backup.Status.State = longhorn.BackupStatePending
		}

		m.backupStatus = backup.Status
		m.replicaAddress = replicaAddress
	}

	// Create a goroutine to monitor the replica backup state/progress
	go m.monitorBackups()

	return m, nil
}

func (m *BackupMonitor) monitorBackups() {
	wait.PollUntil(BackupMonitorSyncPeriod, func() (done bool, err error) {
		if err := m.syncBackups(); err != nil {
			m.logger.Errorf("Stop monitoring because of %v", err)
			m.Close()
			return false, err
		}
		return false, nil
	}, m.ctx.Done())
}

func (m *BackupMonitor) syncBackups() error {
	currentBackupStatus := longhorn.BackupStatus{}

	m.backupStatusLock.RLock()
	m.backupStatus.DeepCopyInto(&currentBackupStatus)
	m.backupStatusLock.RUnlock()

	var err error
	defer func() {
		if err != nil && currentBackupStatus.Error == "" {
			currentBackupStatus.Error = err.Error()
			currentBackupStatus.State = longhorn.BackupStateError
		}

		// new information, request a resync for this backup
		m.backupStatusLock.Lock()
		defer m.backupStatusLock.Unlock()
		if !reflect.DeepEqual(m.backupStatus, currentBackupStatus) {
			m.backupStatus = currentBackupStatus
			key := m.namespace + "/" + m.backupName
			m.syncCallback(key)
		}
	}()

	engineBackupStatus, err := m.engineClient.SnapshotBackupStatus(m.backupName, m.replicaAddress)
	if err != nil {
		return err
	}
	if engineBackupStatus == nil {
		err = fmt.Errorf("cannot find backup %s status in longhorn engine", m.backupName)
		return err
	}
	if engineBackupStatus.SnapshotName != m.snapshotName {
		err = fmt.Errorf("cannot find matched snapshot %s/%s of backup %s status in longhorn engine", engineBackupStatus.SnapshotName, m.snapshotName, m.backupName)
		return err
	}

	currentBackupStatus.Progress = engineBackupStatus.Progress
	currentBackupStatus.URL = engineBackupStatus.BackupURL
	currentBackupStatus.Error = engineBackupStatus.Error
	currentBackupStatus.SnapshotName = engineBackupStatus.SnapshotName
	currentBackupStatus.State = ConvertEngineBackupState(engineBackupStatus.State)
	currentBackupStatus.ReplicaAddress = engineBackupStatus.ReplicaAddress
	return nil
}

func (m *BackupMonitor) GetBackupStatus() longhorn.BackupStatus {
	m.backupStatusLock.RLock()
	defer m.backupStatusLock.RUnlock()
	return m.backupStatus
}

func (m *BackupMonitor) Close() {
	m.quit()
}
