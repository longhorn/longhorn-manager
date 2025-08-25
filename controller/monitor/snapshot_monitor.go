package monitor

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/avast/retry-go"
	"github.com/go-co-op/gocron"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"

	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	monitorName = "snapshot monitor"

	prefixChecksumDetermineFailure = "failed to determine the checksum of snapshot %v "

	snapshotHashMaxRetries = 10

	snapshotCheckWorkerMax     = 1
	snapshotCheckProcessPeriod = 60 * time.Second

	snapshotHashSyncStatusPeriod   = 5 // seconds
	snapshotHashSyncStatusAttempts = (24 * 60 * 60 / snapshotHashSyncStatusPeriod)
)

type SnapshotChangeEvent struct {
	VolumeName   string
	SnapshotName string
}

type snapshotCheckTask struct {
	volumeName   string
	snapshotName string
	changeEvent  bool
}

type SnapshotMonitorStatus struct {
	LastSnapshotPeriodicCheckedAt metav1.Time
}

type SnapshotMonitor struct {
	sync.RWMutex
	*baseMonitor

	nodeName      string
	eventRecorder record.EventRecorder

	checkSchedulers map[longhorn.DataEngineType]*gocron.Scheduler

	snapshotChangeEventQueue workqueue.TypedInterface[any]
	snapshotCheckTaskQueue   workqueue.TypedRateLimitingInterface[any]

	inProgressSnapshotCheckTasks     map[string]struct{}
	inProgressSnapshotCheckTasksLock sync.RWMutex

	existingDataIntegrityCronJobs map[longhorn.DataEngineType]string
	scheduledJobs                 map[longhorn.DataEngineType]*gocron.Job

	syncCallback func(key string)

	proxyConnCounter util.Counter

	SnapshotMonitorStatus
}

func NewSnapshotMonitor(logger logrus.FieldLogger, ds *datastore.DataStore, nodeName string, eventRecorder record.EventRecorder,
	snapshotChangeEventQueue workqueue.TypedInterface[any], syncCallback func(key string)) (*SnapshotMonitor, error) {

	ctx, quit := context.WithCancel(context.Background())

	m := &SnapshotMonitor{
		baseMonitor: newBaseMonitor(ctx, quit, logger, ds, 0),

		nodeName:      nodeName,
		eventRecorder: eventRecorder,

		checkSchedulers: make(map[longhorn.DataEngineType]*gocron.Scheduler),

		snapshotChangeEventQueue: snapshotChangeEventQueue,

		snapshotCheckTaskQueue: workqueue.NewTypedRateLimitingQueue[any](workqueue.NewTypedMaxOfRateLimiter[any](
			workqueue.NewTypedItemExponentialFailureRateLimiter[any](1*time.Second, 1000*time.Second),
			&workqueue.TypedBucketRateLimiter[any]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
		)),

		inProgressSnapshotCheckTasks: map[string]struct{}{},

		existingDataIntegrityCronJobs: make(map[longhorn.DataEngineType]string),
		scheduledJobs:                 make(map[longhorn.DataEngineType]*gocron.Job),

		syncCallback:     syncCallback,
		proxyConnCounter: util.NewAtomicCounter(),
	}
	// Periodically check snapshot disk files
	for _, dataEngine := range []longhorn.DataEngineType{
		longhorn.DataEngineTypeV1,
		longhorn.DataEngineTypeV2,
	} {
		m.checkSchedulers[dataEngine] = gocron.NewScheduler(time.Local)
		m.checkSchedulers[dataEngine].SingletonModeAll()
	}

	go m.Start()

	return m, nil
}

func (m *SnapshotMonitor) Start() {
	for i := 0; i < snapshotCheckWorkerMax; i++ {
		go m.snapshotCheckWorker(i)
	}

	go m.processSnapshotChangeEvent()
}

func (m *SnapshotMonitor) processNextEvent() bool {
	key, quit := m.snapshotChangeEventQueue.Get()
	if quit {
		return false
	}
	defer m.snapshotChangeEventQueue.Done(key)

	event := key.(SnapshotChangeEvent)

	m.snapshotCheckTaskQueue.Add(snapshotCheckTask{
		volumeName:   event.VolumeName,
		snapshotName: event.SnapshotName,
		changeEvent:  true,
	})

	return true
}

func (m *SnapshotMonitor) processSnapshotChangeEvent() {
	for m.processNextEvent() {
	}
}

func (m *SnapshotMonitor) checkSnapshots(dataEngine longhorn.DataEngineType) {
	m.logger.WithField("monitor", monitorName).Info("Starting checking snapshots")
	defer m.logger.WithField("monitor", monitorName).Infof("Finished checking snapshots")

	engines, err := m.ds.ListEnginesByNodeRO(m.nodeName)
	if err != nil {
		m.logger.WithField("monitor", monitorName).WithError(err).Errorf("failed to list engines on node %v", m.nodeName)
		return
	}

	defer func() {
		m.Lock()
		defer m.Unlock()
		m.LastSnapshotPeriodicCheckedAt = metav1.Time{Time: time.Now().UTC()}
	}()

	for _, engine := range engines {
		if engine.Spec.DataEngine == dataEngine {
			m.logger.WithField("monitor", monitorName).Infof("Populating engine %v snapshots for engine type %v", engine.Name, dataEngine)
			m.populateEngineSnapshots(engine)
		}
	}
}

func (m *SnapshotMonitor) populateEngineSnapshots(engine *longhorn.Engine) {
	snapshots := engine.Status.Snapshots
	for _, snapshot := range snapshots {
		// Skip volume-head because it is not a real snapshot.
		// A system-generated snapshot is also ignored, because the prune operations the snapshots are out of
		// sync during replica rebuilding. More investigation is in https://github.com/longhorn/longhorn/issues/4513
		if snapshot.Name == etypes.VolumeHeadName || !snapshot.UserCreated {
			continue
		}

		m.snapshotCheckTaskQueue.Add(snapshotCheckTask{
			volumeName:   engine.Spec.VolumeName,
			snapshotName: snapshot.Name,
			changeEvent:  false,
		})
	}
}

func (m *SnapshotMonitor) processNextWorkItem(id int) bool {
	key, quit := m.snapshotCheckTaskQueue.Get()
	if quit {
		return false
	}
	defer m.snapshotCheckTaskQueue.Done(key)

	task := key.(snapshotCheckTask)

	dataIntegrity, err := m.ds.GetVolumeSnapshotDataIntegrity(task.volumeName)
	if err != nil {
		return true
	}

	if dataIntegrity == longhorn.SnapshotDataIntegrityDisabled {
		return true
	}

	err = m.run(task)
	m.handleErr(err, key)

	return true
}

func (m *SnapshotMonitor) handleErr(err error, key interface{}) {
	if err == nil {
		m.snapshotCheckTaskQueue.Forget(key)
		return
	}

	if !strings.Contains(err.Error(), etypes.CannotRequestHashingSnapshotPrefix) {
		m.snapshotCheckTaskQueue.Forget(key)
		return
	}

	if m.snapshotCheckTaskQueue.NumRequeues(key) < snapshotHashMaxRetries {
		m.logger.WithError(err).Warnf("Error syncing snapshot check task %v", key)
		m.snapshotCheckTaskQueue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)

	m.logger.WithError(err).Warnf("Dropping hashing request of snapshot %v", key)
	m.snapshotCheckTaskQueue.Forget(key)
}

func (m *SnapshotMonitor) snapshotCheckWorker(id int) {
	for m.processNextWorkItem(id) {
	}
}

func (m *SnapshotMonitor) Stop() {
	m.logger.WithField("monitor", monitorName).Info("Closing snapshot monitor")

	m.snapshotCheckTaskQueue.ShutDown()
	for _, dataEngine := range []longhorn.DataEngineType{
		longhorn.DataEngineTypeV1,
		longhorn.DataEngineTypeV2,
	} {
		m.checkSchedulers[dataEngine].Stop()
	}
	m.quit()
}

func (m *SnapshotMonitor) RunOnce() error {
	return fmt.Errorf("RunOnce is not implemented")
}

func (m *SnapshotMonitor) UpdateConfiguration(map[string]interface{}) error {
	dataIntegrityCronJobs := make(map[longhorn.DataEngineType]string)

	for _, dataEngine := range []longhorn.DataEngineType{
		longhorn.DataEngineTypeV1,
		longhorn.DataEngineTypeV2,
	} {
		dataIntegrityCronJob, err := m.ds.GetSettingValueExistedByDataEngine(types.SettingNameSnapshotDataIntegrityCronJob, dataEngine)
		if err != nil {
			return errors.Wrapf(err, "failed to get %v setting for %s", types.SettingNameSnapshotDataIntegrityCronJob, dataEngine)
		}
		dataIntegrityCronJobs[dataEngine] = dataIntegrityCronJob

	}

	m.RLock()
	var modified = false
	for _, dataEngine := range []longhorn.DataEngineType{
		longhorn.DataEngineTypeV1,
		longhorn.DataEngineTypeV2,
	} {
		if dataIntegrityCronJobs[dataEngine] != m.existingDataIntegrityCronJobs[dataEngine] || m.checkSchedulers[dataEngine].Len() == 0 {
			modified = true
		}
	}

	if !modified {
		m.RUnlock()
		return nil
	}
	m.RUnlock()

	for _, dataEngine := range []longhorn.DataEngineType{
		longhorn.DataEngineTypeV1,
		longhorn.DataEngineTypeV2,
	} {
		if m.checkSchedulers[dataEngine].Len() > 0 {
			m.checkSchedulers[dataEngine].Remove(m.checkSnapshots)
		}
		job, err := m.checkSchedulers[dataEngine].Cron(dataIntegrityCronJobs[dataEngine]).Do(m.checkSnapshots, dataEngine)

		if err != nil {
			return errors.Wrap(err, "failed to schedule snapshot check job")
		}
		m.scheduledJobs[dataEngine] = job

		m.Lock()
		previousDataIntegrityCronJob := m.existingDataIntegrityCronJobs[dataEngine]
		m.existingDataIntegrityCronJobs[dataEngine] = dataIntegrityCronJobs[dataEngine]
		m.Unlock()

		m.checkSchedulers[dataEngine].StartAsync()

		m.logger.WithField("monitor", monitorName).Infof("Cron is changed from %v to %v for all volumes with %s. Next snapshot check job will be executed at %v",
			previousDataIntegrityCronJob, m.existingDataIntegrityCronJobs[dataEngine], dataEngine, job.NextRun())

	}

	return nil
}

func (m *SnapshotMonitor) GetCollectedData() (interface{}, error) {
	m.RLock()
	defer m.RUnlock()
	return m.SnapshotMonitorStatus, nil
}

func (m *SnapshotMonitor) shouldAddToInProgressSnapshotCheckTasks(snapshotName string) bool {
	m.inProgressSnapshotCheckTasksLock.Lock()
	defer m.inProgressSnapshotCheckTasksLock.Unlock()

	_, ok := m.inProgressSnapshotCheckTasks[snapshotName]
	if ok {
		m.logger.WithField("monitor", monitorName).Infof("Checking snapshot %s", snapshotName)
		return false
	}
	m.inProgressSnapshotCheckTasks[snapshotName] = struct{}{}

	return true
}

func (m *SnapshotMonitor) deleteFromInProgressSnapshotCheckTasks(snapshotName string) {
	m.inProgressSnapshotCheckTasksLock.Lock()
	defer m.inProgressSnapshotCheckTasksLock.Unlock()

	delete(m.inProgressSnapshotCheckTasks, snapshotName)
}

func getNumberOfHealthyReplicas(e *longhorn.Engine) int {
	numOfHealthyReplicas := 0
	for _, mode := range e.Status.ReplicaModeMap {
		if mode == longhorn.ReplicaModeRW {
			numOfHealthyReplicas++
		}
	}
	return numOfHealthyReplicas
}

func (m *SnapshotMonitor) run(arg interface{}) error {
	task, ok := arg.(snapshotCheckTask)
	if !ok {
		return fmt.Errorf("failed to assert value: %v", arg)
	}

	if !m.shouldAddToInProgressSnapshotCheckTasks(task.snapshotName) {
		return nil
	}
	defer m.deleteFromInProgressSnapshotCheckTasks(task.snapshotName)

	engine, err := m.ds.GetVolumeCurrentEngine(task.volumeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get engine for volume %v", task.volumeName)
	}

	// Skip snapshot hashing if the volume has less than 2 healthy replicas
	numOfHealthyReplicas := getNumberOfHealthyReplicas(engine)
	if numOfHealthyReplicas < 2 {
		m.logger.WithField("monitor", monitorName).Debugf("Skipping snapshot calculation for volume %v since it has less than 2 healthy replicas", task.volumeName)
		return nil
	}

	if err := m.canRequestSnapshotHash(engine); err != nil {
		return errors.Wrapf(err, etypes.CannotRequestHashingSnapshotPrefix)
	}

	engineCliClient, err := engineapi.GetEngineBinaryClient(m.ds, engine.Spec.VolumeName, m.nodeName)
	if err != nil {
		return err
	}

	engineClientProxy, err := engineapi.GetCompatibleClient(engine, engineCliClient, m.ds, m.logger, m.proxyConnCounter)
	if err != nil {
		return err
	}
	defer engineClientProxy.Close()

	err = m.requestSnapshotHashing(engine, engineClientProxy, task.snapshotName, task.changeEvent)
	if err != nil {
		return err
	}

	return m.waitAndHandleSnapshotHashing(engine, engineClientProxy, task.snapshotName)
}

func (m *SnapshotMonitor) canRequestSnapshotHash(engine *longhorn.Engine) error {
	if err := m.checkVolumeIsNotPurging(engine); err != nil {
		return err
	}

	if len(engine.Status.RebuildStatus) > 0 {
		return fmt.Errorf("cannot hash snapshot during rebuilding")
	}

	if err := m.checkVolumeIsNotRestoring(engine); err != nil {
		return err
	}

	if err := m.checkVolumeNotInMigration(engine.Spec.VolumeName); err != nil {
		return err
	}

	return nil
}

func (m *SnapshotMonitor) requestSnapshotHashing(engine *longhorn.Engine, engineClientProxy engineapi.EngineClientProxy,
	snapshotName string, changeEvent bool) error {
	// One snapshot CR might be updated many times in a short period.
	// The checksum calculation is expected to run once if it is triggered by snapshot update event.
	// So, if refresh is false and the checksum is existing, don't need to calculate it again if the ctime is not changed.
	// The periodc snapshot check mechanism will do the regular checks.
	// In other words, the full hash will be issued by the cron job only, no matter if the field is enabled or fast-check.
	rehash := false
	if !changeEvent {
		dataIntegrity, err := m.ds.GetVolumeSnapshotDataIntegrity(engine.Spec.VolumeName)
		if err != nil {
			return err
		}
		if dataIntegrity == longhorn.SnapshotDataIntegrityEnabled {
			rehash = true
		}
	}
	return engineClientProxy.SnapshotHash(engine, snapshotName, rehash)
}

func (m *SnapshotMonitor) waitAndHandleSnapshotHashing(engine *longhorn.Engine, engineClientProxy engineapi.EngineClientProxy,
	snapshotName string) error {
	opts := []retry.Option{
		retry.Context(m.ctx),
		retry.Attempts(snapshotHashSyncStatusAttempts),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
		retry.Delay(snapshotHashSyncStatusPeriod * time.Second),
		retry.RetryIf(func(err error) bool {
			if err == nil {
				return false
			}
			return strings.Contains(err.Error(), string(engineapi.ProcessStateInProgress))
		}),
	}

	// retry does periodically fetching and syncing the snapshot hashing status.
	if err := retry.Do(func() (err error) {
		return m.syncHashStatusFromEngineReplicas(engine, engineClientProxy, snapshotName)
	}, opts...); err != nil {
		return errors.Wrapf(err, "failed to sync hash status for snapshot %v since %v", snapshotName, err)
	}

	return nil
}

func (m *SnapshotMonitor) checkVolumeNotInMigration(volumeName string) error {
	v, err := m.ds.GetVolume(volumeName)
	if err != nil {
		return err
	}
	if v.Spec.MigrationNodeID != "" {
		return fmt.Errorf("cannot hash snapshot during migration")
	}
	return nil
}

func (m *SnapshotMonitor) checkVolumeIsNotPurging(engine *longhorn.Engine) error {
	for _, status := range engine.Status.PurgeStatus {
		if status.IsPurging {
			return fmt.Errorf("cannot hash snapshot during purging")
		}
	}
	return nil
}

func (m *SnapshotMonitor) checkVolumeIsNotRestoring(engine *longhorn.Engine) error {
	for _, status := range engine.Status.RestoreStatus {
		if status.IsRestoring {
			return fmt.Errorf("cannot hash snapshot during restoring")
		}
	}
	return nil
}

func (m *SnapshotMonitor) syncHashStatusFromEngineReplicas(engine *longhorn.Engine, engineClientProxy engineapi.EngineClientProxy,
	snapshotName string) error {
	hashStatus, err := engineClientProxy.SnapshotHashStatus(engine, snapshotName)
	if err != nil {
		return errors.Wrapf(err, "failed to get hash status for snapshot %v", snapshotName)
	}

	for _, status := range hashStatus {
		if status.State == string(longhorn.SnapshotHashStatusError) {
			return fmt.Errorf("failed to hash snapshot %v since %v", snapshotName, status.Error)
		}

		if status.State == string(engineapi.ProcessStateInProgress) {
			return errors.New(string(engineapi.ProcessStateInProgress))
		}
	}

	snapshot, err := m.ds.GetSnapshot(snapshotName)
	if err != nil {
		return err
	}
	existingSnapshot := snapshot.DeepCopy()

	checksum, err := determineChecksumFromHashStatus(m.logger, snapshotName, snapshot.Status.Checksum, hashStatus)
	if err != nil {
		m.eventRecorder.Eventf(engine, corev1.EventTypeWarning, constant.EventReasonFailedSnapshotDataIntegrityCheck,
			"Failed to check the data integrity of snapshot %v for volume %v", snapshotName, engine.Spec.VolumeName)
		return errors.Wrapf(err, "failed to determine checksum for snapshot %v", snapshotName)
	}

	snapshot.Status.Checksum = checksum

	if !reflect.DeepEqual(existingSnapshot.Status, snapshot.Status) {
		if _, err := m.ds.UpdateSnapshotStatus(snapshot); err != nil {
			return errors.Wrapf(err, "failed to update status for snapshot %v", snapshotName)
		}
	}

	m.kickOutCorruptedReplicas(engine, engineClientProxy, checksum, hashStatus)

	return nil
}

func (m *SnapshotMonitor) kickOutCorruptedReplicas(engine *longhorn.Engine, engineClientProxy engineapi.EngineClientProxy,
	checksum string, hashStatus map[string]*longhorn.HashStatus) {
	for address, status := range hashStatus {
		if status.Checksum == checksum {
			continue
		}

		m.eventRecorder.Eventf(engine, corev1.EventTypeWarning, constant.EventReasonFaulted, "Detected corrupted replica %v", address)

		if err := engineClientProxy.ReplicaModeUpdate(engine, address, string(etypes.ERR)); err != nil {
			m.logger.WithField("monitor", monitorName).Errorf("failed to update replica %v mode to ERR", address)
		}
	}
}

func determineChecksumFromHashStatus(log logrus.FieldLogger, snapshotName, existingChecksum string, hashStatus map[string]*longhorn.HashStatus) (string, error) {
	checksum := ""
	defer func() {
		if existingChecksum != "" && checksum != "" && existingChecksum != checksum {
			log.WithField("monitor", monitorName).Infof("snapshot %v checksum is changed", snapshotName)
		}
	}()

	checksums := map[string][]string{}

	for address, status := range hashStatus {
		// The checksum from a silently corrupted snapshot disk file cannot vote.
		if status.SilentlyCorrupted {
			continue
		}
		checksums[status.Checksum] = append(checksums[status.Checksum], address)
	}

	if len(checksums) == 0 {
		return "", fmt.Errorf(prefixChecksumDetermineFailure+"since snapshot disk files are silently corrupted", snapshotName)
	}

	// The checksums from replicas might be different from previous values because of purge, trim, corruption and etc.
	// So, the vote mechanism is always executed to get the latest checksum and then update the status.checksum.
	// If the checksum cannot be determined by the ones from replicas, the existingChecksum (snapshot.status.checksum) will
	// help to determine the final checksum.
	found, checksum, maxVotes := determineChecksum(checksums)
	if found {
		return checksum, nil
	}

	if existingChecksum == "" {
		return "", fmt.Errorf(prefixChecksumDetermineFailure+"since there is no existing checksum", snapshotName)
	}

	replicas, ok := checksums[existingChecksum]
	if !ok {
		return "", fmt.Errorf(prefixChecksumDetermineFailure+"since the existing checksum is not found in the checksums from replicas", snapshotName)
	}

	if len(replicas) == maxVotes {
		return existingChecksum, nil
	}

	return "", fmt.Errorf(prefixChecksumDetermineFailure+"from the existing checksum and the checksums from replicas", snapshotName)
}

func determineChecksum(checksums map[string][]string) (bool, string, int) {
	checksum := ""
	found := false
	maxVotes := 0
	for c, replicas := range checksums {
		if len(replicas) > maxVotes {
			checksum = c
			maxVotes = len(replicas)
			found = true
		} else if len(replicas) == maxVotes {
			found = false
		}
	}

	return found, checksum, maxVotes
}
