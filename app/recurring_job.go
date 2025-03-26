package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"golang.org/x/sync/errgroup"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

const (
	HTTPClientTimout = 1 * time.Minute

	SnapshotPurgeStatusInterval = 5 * time.Second
	// SnapshotPurgeStatusTimeout is set to 24 hours because we don't know the appropriate value.
	SnapshotPurgeStatusTimeout = 24 * time.Hour

	WaitInterval              = 5 * time.Second
	DetachingWaitInterval     = 10 * time.Second
	VolumeAttachTimeout       = 300 // 5 minutes
	BackupProcessStartTimeout = 90  // 1.5 minutes
	SnapshotReadyTimeout      = 390 // 6.5 minutes
)

type Job struct {
	logger         logrus.FieldLogger
	lhClient       lhclientset.Interface
	namespace      string
	volumeName     string
	snapshotName   string
	retain         int
	task           longhorn.RecurringJobType
	labels         map[string]string
	parameters     map[string]string
	executionCount int

	eventRecorder record.EventRecorder

	api *longhornclient.RancherClient
}

func RecurringJobCmd() cli.Command {
	return cli.Command{
		Name: "recurring-job",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  FlagManagerURL,
				Usage: "Longhorn manager API URL",
			},
		},
		Action: func(c *cli.Context) {
			if err := recurringJob(c); err != nil {
				logrus.WithError(err).Fatal("Failed to do a recurring job")
			}
		},
	}
}

func recurringJob(c *cli.Context) (err error) {
	logger := logrus.StandardLogger()

	var managerURL = c.String(FlagManagerURL)
	if managerURL == "" {
		return fmt.Errorf("require %v", FlagManagerURL)
	}

	if c.NArg() != 1 {
		return errors.New("job name is required")
	}
	jobName := c.Args()[0]

	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return fmt.Errorf("failed detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}
	lhClient, err := getLonghornClientset()
	if err != nil {
		return errors.Wrap(err, "failed to get clientset")
	}
	recurringJob, err := lhClient.LonghornV1beta2().RecurringJobs(namespace).Get(context.TODO(), jobName, metav1.GetOptions{})
	if err != nil {
		logrus.WithError(err).Errorf("Failed to get recurring job %v.", jobName)
		return nil
	}

	var jobGroups []string = recurringJob.Spec.Groups
	var jobRetain int = recurringJob.Spec.Retain
	var jobConcurrent int = recurringJob.Spec.Concurrency
	jobTask := recurringJob.Spec.Task

	recurringJob.Status.ExecutionCount += 1
	if _, err = lhClient.LonghornV1beta2().RecurringJobs(namespace).UpdateStatus(context.TODO(), recurringJob, metav1.UpdateOptions{}); err != nil {
		return errors.Wrap(err, "failed to update job execution count")
	}

	jobExecutionCount := recurringJob.Status.ExecutionCount

	jobLabelMap := map[string]string{}
	if recurringJob.Spec.Labels != nil {
		jobLabelMap = recurringJob.Spec.Labels
	}
	jobLabelMap[types.RecurringJobLabel] = recurringJob.Name
	labelJSON, err := json.Marshal(jobLabelMap)
	if err != nil {
		return errors.Wrap(err, "failed to get JSON encoding for labels")
	}

	jobParameterMap := map[string]string{}
	if recurringJob.Spec.Parameters != nil {
		jobParameterMap = recurringJob.Spec.Parameters
	}

	allowDetachedSetting := types.SettingNameAllowRecurringJobWhileVolumeDetached
	allowDetached, err := getSettingAsBoolean(allowDetachedSetting, namespace, lhClient)
	if err != nil {
		return errors.Wrapf(err, "failed to get %v setting", allowDetachedSetting)
	}
	logger.Infof("Setting %v is %v", allowDetachedSetting, allowDetached)

	volumes, err := getVolumesBySelector(types.LonghornLabelRecurringJob, jobName, namespace, lhClient)
	if err != nil {
		return err
	}
	filteredVolumes := []string{}
	filterVolumesForJob(allowDetached, volumes, &filteredVolumes)
	for _, jobGroup := range jobGroups {
		volumes, err := getVolumesBySelector(types.LonghornLabelRecurringJobGroup, jobGroup, namespace, lhClient)
		if err != nil {
			return err
		}
		filterVolumesForJob(allowDetached, volumes, &filteredVolumes)
	}
	logger.Infof("Found %v volumes with recurring job %v", len(filteredVolumes), jobName)

	concurrentLimiter := make(chan struct{}, jobConcurrent)
	ewg := &errgroup.Group{}
	defer func() {
		if wgError := ewg.Wait(); wgError != nil {
			err = wgError
		}
	}()
	for _, volumeName := range filteredVolumes {
		startJobVolumeName := volumeName
		ewg.Go(func() error {
			return startVolumeJob(startJobVolumeName, logger, concurrentLimiter, managerURL, jobName, jobTask, jobRetain, jobConcurrent, jobGroups, jobLabelMap, labelJSON, jobParameterMap, jobExecutionCount)
		})
	}

	return err
}

func startVolumeJob(
	volumeName string, logger *logrus.Logger, concurrentLimiter chan struct{}, managerURL string,
	jobName string, jobTask longhorn.RecurringJobType, jobRetain int, jobConcurrent int, jobGroups []string, jobLabelMap map[string]string, labelJSON []byte, jobParameterMap map[string]string, jobExecutionCount int) error {

	concurrentLimiter <- struct{}{}
	defer func() {
		<-concurrentLimiter
	}()

	log := logger.WithFields(logrus.Fields{
		"job":            jobName,
		"volume":         volumeName,
		"task":           jobTask,
		"retain":         jobRetain,
		"concurrent":     jobConcurrent,
		"groups":         strings.Join(jobGroups, ","),
		"labels":         string(labelJSON),
		"parameters":     jobParameterMap,
		"executionCount": jobExecutionCount,
	})
	log.Info("Creating job")

	snapshotName := sliceStringSafely(types.GetCronJobNameForRecurringJob(jobName), 0, 8) + "-" + util.UUID()
	job, err := newJob(
		logger,
		managerURL,
		volumeName,
		snapshotName,
		jobLabelMap,
		jobParameterMap,
		jobRetain,
		jobTask,
		jobExecutionCount)
	if err != nil {
		log.WithError(err).Error("Failed to create new job for volume")
		return err
	}
	err = job.run()
	if err != nil {
		log.WithError(err).Errorf("Failed to run job for volume")
		return err
	}

	log.Info("Created job")
	return nil
}

func sliceStringSafely(s string, begin, end int) string {
	if begin < 0 {
		begin = 0
	}
	if end > len(s) {
		end = len(s)
	}
	return s[begin:end]
}

func newJob(logger logrus.FieldLogger, managerURL, volumeName, snapshotName string, labels map[string]string, parameters map[string]string, retain int, task longhorn.RecurringJobType, executionCount int) (*Job, error) {
	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return nil, fmt.Errorf("failed detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get client config")
	}
	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get clientset")
	}

	clientOpts := &longhornclient.ClientOpts{
		Url:     managerURL,
		Timeout: HTTPClientTimout,
	}
	apiClient, err := longhornclient.NewRancherClient(clientOpts)
	if err != nil {
		return nil, errors.Wrap(err, "could not create longhorn-manager api client")
	}

	logger = logger.WithFields(logrus.Fields{
		"namespace":      namespace,
		"volumeName":     volumeName,
		"snapshotName":   snapshotName,
		"labels":         labels,
		"retain":         retain,
		"task":           task,
		"executionCount": executionCount,
	})

	scheme := runtime.NewScheme()
	if err := longhorn.SchemeBuilder.AddToScheme(scheme); err != nil {
		return nil, errors.Wrap(err, "failed to create scheme")
	}

	eventBroadcaster, err := createEventBroadcaster(config)
	if err != nil {
		return nil, err
	}

	return &Job{
		logger:         logger,
		lhClient:       lhClient,
		namespace:      namespace,
		volumeName:     volumeName,
		snapshotName:   snapshotName,
		labels:         labels,
		parameters:     parameters,
		executionCount: executionCount,
		retain:         retain,
		task:           task,
		api:            apiClient,

		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-recurring-job"}),
	}, nil
}

func (job *Job) run() (err error) {
	job.logger.Info("job starts running")

	volumeAPI := job.api.Volume
	volumeName := job.volumeName
	volume, err := volumeAPI.ById(volumeName)
	if err != nil {
		return errors.Wrapf(err, "could not get volume %v", volumeName)
	}

	if len(volume.Controllers) > 1 {
		return fmt.Errorf("cannot run job for volume %v that is using %v engines", volume.Name, len(volume.Controllers))
	}

	if volume.State != string(longhorn.VolumeStateAttached) && volume.State != string(longhorn.VolumeStateDetached) {
		return fmt.Errorf("volume %v is in an invalid state for recurring job: %v. Volume must be in state Attached or Detached", volumeName, volume.State)
	}

	// only recurring job types `snapshot` and `backup` need to check if old snapshots can be deleted or not before creating
	switch job.task {
	case longhorn.RecurringJobTypeSnapshot, longhorn.RecurringJobTypeBackup:
		if err := job.doSnapshotCleanup(false); err != nil {
			return err
		}
	}

	switch job.task {
	case longhorn.RecurringJobTypeFilesystemTrim:
		job.logger.Infof("Running recurring filesystem trim for volume %v", volumeName)
		return job.doRecurringFilesystemTrim(volume)

	case longhorn.RecurringJobTypeBackup, longhorn.RecurringJobTypeBackupForceCreate:
		job.logger.Infof("Running recurring backup for volume %v", volumeName)
		return job.doRecurringBackup()

	default:
		job.logger.Infof("Running recurring snapshot for volume %v", volumeName)
		return job.doRecurringSnapshot()
	}
}

func (job *Job) doRecurringSnapshot() (err error) {
	defer func() {
		err = errors.Wrap(err, "failed recurring snapshot")
		if err == nil {
			job.logger.Info("Finished recurring snapshot")
		}
	}()

	switch job.task {
	case longhorn.RecurringJobTypeSnapshot, longhorn.RecurringJobTypeSnapshotForceCreate:
		if err = job.doSnapshot(); err != nil {
			return err
		}
		fallthrough
	case longhorn.RecurringJobTypeSnapshotCleanup, longhorn.RecurringJobTypeSnapshotDelete:
		return job.doSnapshotCleanup(false)
	default:
		return errors.Errorf("Unsupported task: %v", job.task)
	}
}

func (job *Job) doSnapshot() (err error) {
	volumeAPI := job.api.Volume
	volumeName := job.volumeName
	volume, err := volumeAPI.ById(volumeName)
	if err != nil {
		return errors.Wrapf(err, "could not get volume %v", volumeName)
	}

	// get all snapshot CR belong to this recurring job
	snapshotCRList, err := job.api.Volume.ActionSnapshotCRList(volume)
	if err != nil {
		return err
	}

	snapshotCRs := filterSnapshotCRsWithLabel(snapshotCRList.Data, types.RecurringJobLabel, job.labels[types.RecurringJobLabel])

	// Check if the latest snapshot of this recurring job is pending creation
	// If yes, update the snapshot name to be that snapshot
	// If no, create a new one with new snapshot job name
	var latestSnapshotCR longhornclient.SnapshotCR
	var latestSnapshotCRCreationTime time.Time
	for _, snapshotCR := range snapshotCRs {
		if !snapshotCR.CreateSnapshot {
			// This snapshot was created by old snapshot API (AKA old recurring job).
			// So we skip considering it
			continue
		}
		t, err := time.Parse(time.RFC3339, snapshotCR.CrCreationTime)
		if err != nil {
			logrus.Errorf("Failed to parse datetime %v for snapshot %v",
				snapshotCR.CrCreationTime, snapshotCR.Name)
			continue
		}
		if t.After(latestSnapshotCRCreationTime) {
			latestSnapshotCRCreationTime = t
			latestSnapshotCR = snapshotCR
		}
	}

	alreadyCreatedBefore := latestSnapshotCR.CreationTime != ""
	if latestSnapshotCR.Name != "" && !alreadyCreatedBefore {
		job.snapshotName = latestSnapshotCR.Name
	} else {
		_, err = job.api.Volume.ActionSnapshotCRCreate(volume, &longhornclient.SnapshotCRInput{
			Labels: job.labels,
			Name:   job.snapshotName,
		})
		if err != nil {
			return err
		}
	}

	if err := job.waitForSnaphotReady(volume, SnapshotReadyTimeout); err != nil {
		return err
	}

	job.logger.Infof("Complete creating the snapshot %v", job.snapshotName)

	return nil
}

func (job *Job) waitForSnaphotReady(volume *longhornclient.Volume, timeout int) error {
	for i := 0; i < timeout; i++ {
		existSnapshotCR, err := job.api.Volume.ActionSnapshotCRGet(volume, &longhornclient.SnapshotCRInput{
			Name: job.snapshotName,
		})
		if err != nil {
			return fmt.Errorf("error while waiting for snapshot %v to be ready: %v", job.snapshotName, err)
		}
		if existSnapshotCR.ReadyToUse {
			return nil
		}
		time.Sleep(WaitInterval)
	}
	return fmt.Errorf("timeouted waiting for the the snapshot %v of volume %v to be ready", job.snapshotName, volume.Name)
}

func (job *Job) doSnapshotCleanup(backupDone bool) (err error) {
	defer func() {
		if err != nil {
			errMessage := errors.Wrapf(err, "failed to clean up old snapshots of volume %v", job.volumeName).Error()
			if err := job.eventCreate(corev1.EventTypeWarning, constant.EventReasonFailedDeleting, errMessage); err != nil {
				job.logger.WithError(err).Warn("failed to create an event log")
			}
		}
	}()

	volumeName := job.volumeName
	volume, err := job.api.Volume.ById(volumeName)
	if err != nil {
		return errors.Wrapf(err, "could not get volume %v", volumeName)
	}

	collection, err := job.api.Volume.ActionSnapshotCRList(volume)
	if err != nil {
		return err
	}

	cleanupSnapshotNames := job.listSnapshotNamesToCleanup(collection.Data, backupDone)
	for _, snapshotName := range cleanupSnapshotNames {
		if _, err := job.api.Volume.ActionSnapshotCRDelete(volume, &longhornclient.SnapshotCRInput{
			Name: snapshotName,
		}); err != nil {
			return err
		}
		job.logger.Infof("Cleaned up snapshot CR %v for %v", snapshotName, volumeName)
	}

	if job.task == longhorn.RecurringJobTypeSnapshotCleanup {
		if err := job.purgeSnapshots(volume, job.api.Volume); err != nil {
			return err
		}
	}

	return nil
}

func (job *Job) eventCreate(eventType, eventReason, message string) error {
	jobName, found := job.labels[types.RecurringJobLabel]
	if !found || jobName == "" {
		return fmt.Errorf("missing RecurringJob label")
	}
	recurringJob, err := job.lhClient.LonghornV1beta2().RecurringJobs(job.namespace).Get(context.TODO(), jobName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	job.eventRecorder.Eventf(recurringJob, eventType, eventReason, message)

	return nil
}

func (job *Job) purgeSnapshots(volume *longhornclient.Volume, volumeAPI longhornclient.VolumeOperations) error {
	// Trigger snapshot purge of the volume
	if _, err := volumeAPI.ActionSnapshotPurge(volume); err != nil {
		return err
	}

	startTime := time.Now()
	ticker := time.NewTicker(SnapshotPurgeStatusInterval)
	defer ticker.Stop()

	for range ticker.C {
		// Retrieve the latest volume state
		volume, err := volumeAPI.ById(volume.Name)
		if err != nil {
			return err
		}

		// Check if snapshot purge is completed
		done := true
		errorList := map[string]string{}
		for _, status := range volume.PurgeStatus {
			if status.IsPurging {
				done = false
				break
			}
			if status.Error != "" {
				errorList[status.Replica] = status.Error
			}
		}

		// Log encountered errors when snapshot purge completes
		if done {
			if len(errorList) != 0 {
				for replica, errMsg := range errorList {
					job.logger.Warnf("Error purging snapshots on replica %v: %v", replica, errMsg)
				}
				job.logger.Warn("Encountered one or more errors while purging snapshots")
			}
			job.logger.WithField("volume", volume.Name).Info("Purged snapshots")
			return nil
		}

		// Return error when snapshot exceeds timeout
		if time.Since(startTime) > SnapshotPurgeStatusTimeout {
			return fmt.Errorf("timed out waiting for snapshot purge to complete")
		}
	}
	// This should never be reached, return this error just in case.
	return fmt.Errorf("unexpected error: stopped waiting for snapshot purge without completing or timing out")
}

type NameWithTimestamp struct {
	Name      string
	Timestamp time.Time
}

func (job *Job) listSnapshotNamesToCleanup(snapshotCRs []longhornclient.SnapshotCR, backupDone bool) []string {
	switch job.task {
	case longhorn.RecurringJobTypeSnapshotDelete:
		return job.filterExpiredSnapshots(snapshotCRs)
	case longhorn.RecurringJobTypeSnapshotCleanup:
		return []string{}
	default:
		return job.filterExpiredSnapshotsOfCurrentRecurringJob(snapshotCRs, backupDone)
	}
}

func (job *Job) filterExpiredSnapshotsOfCurrentRecurringJob(snapshotCRs []longhornclient.SnapshotCR, backupDone bool) []string {
	jobLabel, found := job.labels[types.RecurringJobLabel]
	if !found {
		return []string{}
	}

	// Only consider deleting the snapshots that were created by our current job
	snapshotCRs = filterSnapshotCRsWithLabel(snapshotCRs, types.RecurringJobLabel, jobLabel)

	allowBackupSnapshotDeleted, err := job.GetSettingAsBool(types.SettingNameAutoCleanupRecurringJobBackupSnapshot)
	if err != nil {
		job.logger.WithError(err).Warnf("Failed to get the setting %v", types.SettingNameAutoCleanupRecurringJobBackupSnapshot)
		return []string{}
	}

	// For recurring snapshot job and AutoCleanupRecurringJobBackupSnapshot is disabled, keeps the number of the snapshots as job.retain.
	if job.task == longhorn.RecurringJobTypeSnapshot || job.task == longhorn.RecurringJobTypeSnapshotForceCreate || !allowBackupSnapshotDeleted {
		return filterExpiredItems(snapshotCRsToNameWithTimestamps(snapshotCRs), job.retain)
	}

	// For the recurring backup job, only keep the snapshot of the last backup and the current snapshot when AutoCleanupRecurringJobBackupSnapshot is enabled.
	retainingSnapshotCRs := map[string]struct{}{job.snapshotName: {}}
	if !backupDone {
		lastBackup, err := job.getLastBackup()
		if err == nil && lastBackup != nil {
			retainingSnapshotCRs[lastBackup.SnapshotName] = struct{}{}
		}
	}
	return snapshotCRsToNames(filterSnapshotCRsNotInTargets(snapshotCRs, retainingSnapshotCRs))
}

func (job *Job) filterExpiredSnapshots(snapshotCRs []longhornclient.SnapshotCR) []string {
	return filterExpiredItems(snapshotCRsToNameWithTimestamps(snapshotCRs), job.retain)
}

func (job *Job) doRecurringBackup() (err error) {
	defer func() {
		if err == nil {
			job.logger.Info("Finished recurring backup")
		}
	}()

	defer func() {
		err = errors.Wrapf(err, "failed to complete backupAndCleanup for %v", job.volumeName)
	}()

	volume, err := job.api.Volume.ById(job.volumeName)
	if err != nil {
		return errors.Wrapf(err, "could not get volume %v", job.volumeName)
	}

	if err := job.doSnapshot(); err != nil {
		return err
	}

	backupMode := longhorn.BackupModeIncremental
	if intervalStr, exists := job.parameters[types.RecurringJobBackupParameterFullBackupInterval]; exists {
		interval, err := strconv.Atoi(intervalStr)
		if err != nil {
			return errors.Wrapf(err, "interval %v is not number", intervalStr)
		}

		if interval != 0 {
			if job.executionCount%interval == 0 {
				backupMode = longhorn.BackupModeFull
			}
		}
	}

	if _, err := job.api.Volume.ActionSnapshotBackup(volume, &longhornclient.SnapshotInput{
		Labels:     job.labels,
		Name:       job.snapshotName,
		BackupMode: string(backupMode),
	}); err != nil {
		return err
	}

	if err := job.waitForBackupProcessStart(BackupProcessStartTimeout); err != nil {
		return err
	}

	// Wait for backup creation complete
	for {
		volume, err := job.api.Volume.ById(job.volumeName)
		if err != nil {
			return err
		}

		var info *longhornclient.BackupStatus
		for _, status := range volume.BackupStatus {
			if status.Snapshot == job.snapshotName {
				info = &status
				break
			}
		}

		if info == nil {
			return fmt.Errorf("cannot find the status of the backup for snapshot %v. It might because the engine has restarted", job.snapshotName)
		}

		complete := false

		switch info.State {
		case string(longhorn.BackupStateCompleted):
			complete = true
			job.logger.Infof("Completed creating backup %v", info.Id)
		case string(longhorn.BackupStateNew), string(longhorn.BackupStatePending), string(longhorn.BackupStateInProgress):
			job.logger.Infof("Creating backup %v, current progress %v", info.Id, info.Progress)
		case string(longhorn.BackupStateError), string(longhorn.BackupStateUnknown):
			return fmt.Errorf("failed to create backup %v: %v", info.Id, info.Error)
		default:
			return fmt.Errorf("invalid state %v for backup %v", info.State, info.Id)
		}

		if complete {
			break
		}
		time.Sleep(WaitInterval)
	}

	defer func() {
		if err != nil {
			job.logger.WithError(err).Warnf("Created backup successfully but errored on cleanup for %v", job.volumeName)
			err = nil
		}
	}()

	backupVolume, err := job.api.BackupVolume.ById(job.volumeName)
	if err != nil {
		return err
	}
	backups, err := job.api.BackupVolume.ActionBackupList(backupVolume)
	if err != nil {
		return err
	}
	cleanupBackups := job.listBackupsForCleanup(backups.Data)
	for _, backup := range cleanupBackups {
		if _, err := job.api.BackupVolume.ActionBackupDelete(backupVolume, &longhornclient.BackupInput{
			Name: backup,
		}); err != nil {
			return fmt.Errorf("cleaned up backup %v failed for %v: %v", backup, job.volumeName, err)
		}
		job.logger.Infof("Cleaned up backup %v for %v", backup, job.volumeName)
	}

	if err := job.doSnapshotCleanup(true); err != nil {
		return err
	}
	return nil
}

func (job *Job) doRecurringFilesystemTrim(volume *longhornclient.Volume) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to complete filesystem-trim for %v", volume.Name)
		if err == nil {
			job.logger.Info("Finished recurring filesystem trim")
		}
	}()
	volumeAPI := job.api.Volume
	volume, err = volumeAPI.ActionTrimFilesystem(volume)
	if err != nil {
		return err
	}
	return job.purgeSnapshots(volume, volumeAPI)
}

// waitForBackupProcessStart timeout in second
// Return nil if the backup progress has started; error if error or timeout
func (job *Job) waitForBackupProcessStart(timeout int) error {
	volumeAPI := job.api.Volume
	volumeName := job.volumeName
	snapshot := job.snapshotName

	for i := 0; i < timeout; i++ {
		volume, err := volumeAPI.ById(volumeName)
		if err != nil {
			return err
		}

		for _, status := range volume.BackupStatus {
			if status.Snapshot == snapshot {
				return nil
			}
		}
		time.Sleep(WaitInterval)
	}
	return fmt.Errorf("timeout waiting for the backup of the snapshot %v of volume %v to start", snapshot, volumeName)
}

// getLastBackup return the last backup of the volume
// return nil, nil if volume doesn't have any backup
func (job *Job) getLastBackup() (*longhornclient.Backup, error) {
	var err error
	defer func() {
		err = errors.Wrapf(err, "failed to get last backup for %v", job.volumeName)
	}()

	volume, err := job.api.Volume.ById(job.volumeName)
	if err != nil {
		return nil, err
	}
	if volume.LastBackup == "" {
		return nil, nil
	}
	backupVolume, err := job.api.BackupVolume.ById(job.volumeName)
	if err != nil {
		return nil, err
	}
	return job.api.BackupVolume.ActionBackupGet(backupVolume, &longhornclient.BackupInput{
		Name: volume.LastBackup,
	})
}

func (job *Job) listBackupsForCleanup(backups []longhornclient.Backup) []string {
	sts := []NameWithTimestamp{}

	// only remove backups that where created by our current job
	jobLabel, found := job.labels[types.RecurringJobLabel]
	if !found {
		return []string{}
	}
	for _, backup := range backups {
		backupLabel, found := backup.Labels[types.RecurringJobLabel]
		if found && jobLabel == backupLabel {
			t, err := time.Parse(time.RFC3339, backup.Created)
			if err != nil {
				job.logger.Errorf("Failed to parse datetime %v for backup %v",
					backup.Created, backup)
				continue
			}
			sts = append(sts, NameWithTimestamp{
				Name:      backup.Name,
				Timestamp: t,
			})
		}
	}
	return filterExpiredItems(sts, job.retain)
}

func (job *Job) GetVolume(name string) (*longhorn.Volume, error) {
	return job.lhClient.LonghornV1beta2().Volumes(job.namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (job *Job) GetEngineImage(name string) (*longhorn.EngineImage, error) {
	return job.lhClient.LonghornV1beta2().EngineImages(job.namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (job *Job) UpdateVolumeStatus(v *longhorn.Volume) (*longhorn.Volume, error) {
	return job.lhClient.LonghornV1beta2().Volumes(job.namespace).UpdateStatus(context.TODO(), v, metav1.UpdateOptions{})
}

// GetSettingAsBool returns boolean of the setting value searching by name.
func (job *Job) GetSettingAsBool(name types.SettingName) (bool, error) {
	obj, err := job.lhClient.LonghornV1beta2().Settings(job.namespace).Get(context.TODO(), string(name), metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	value, err := strconv.ParseBool(obj.Value)
	if err != nil {
		return false, err
	}

	return value, nil
}

func filterSnapshotCRs(snapshotCRs []longhornclient.SnapshotCR, predicate func(snapshot longhornclient.SnapshotCR) bool) []longhornclient.SnapshotCR {
	filtered := []longhornclient.SnapshotCR{}
	for _, snapshotCR := range snapshotCRs {
		if predicate(snapshotCR) {
			filtered = append(filtered, snapshotCR)
		}
	}
	return filtered
}

// filterSnapshotCRsWithLabel return snapshotCRs that have LabelKey and LabelValue
func filterSnapshotCRsWithLabel(snapshotCRs []longhornclient.SnapshotCR, labelKey, labelValue string) []longhornclient.SnapshotCR {
	return filterSnapshotCRs(snapshotCRs, func(snapshotCR longhornclient.SnapshotCR) bool {
		snapshotLabelValue, found := snapshotCR.Labels[labelKey]
		return found && labelValue == snapshotLabelValue
	})
}

// filterSnapshotCRsNotInTargets returns snapshots that are not in the Targets
func filterSnapshotCRsNotInTargets(snapshotCRs []longhornclient.SnapshotCR, targets map[string]struct{}) []longhornclient.SnapshotCR {
	return filterSnapshotCRs(snapshotCRs, func(snapshotCR longhornclient.SnapshotCR) bool {
		if _, ok := targets[snapshotCR.Name]; !ok {
			return true
		}
		return false
	})
}

// filterExpiredItems returns a list of names from the input sts excluding the latest retainCount names
func filterExpiredItems(nts []NameWithTimestamp, retainCount int) []string {
	sort.Slice(nts, func(i, j int) bool {
		return nts[i].Timestamp.Before(nts[j].Timestamp)
	})

	ret := []string{}
	for i := 0; i < len(nts)-retainCount; i++ {
		ret = append(ret, nts[i].Name)
	}
	return ret
}

func snapshotCRsToNameWithTimestamps(snapshotCRs []longhornclient.SnapshotCR) []NameWithTimestamp {
	result := []NameWithTimestamp{}
	for _, snapshotCR := range snapshotCRs {
		t, err := time.Parse(time.RFC3339, snapshotCR.CrCreationTime)
		if err != nil {
			logrus.Errorf("Failed to parse datetime %v for snapshot CR %v",
				snapshotCR.CrCreationTime, snapshotCR.Name)
			continue
		}
		result = append(result, NameWithTimestamp{
			Name:      snapshotCR.Name,
			Timestamp: t,
		})
	}
	return result
}

func snapshotCRsToNames(snapshotCRs []longhornclient.SnapshotCR) []string {
	result := []string{}
	for _, snapshotCR := range snapshotCRs {
		result = append(result, snapshotCR.Name)
	}
	return result
}

func filterVolumesForJob(allowDetached bool, volumes []longhorn.Volume, filterNames *[]string) {
	logger := logrus.StandardLogger()
	for _, volume := range volumes {
		// skip duplicates
		if util.Contains(*filterNames, volume.Name) {
			continue
		}

		if volume.Status.RestoreRequired {
			logger.Infof("Bypassed to create job for %v volume during restoring from the backup", volume.Name)
			continue
		}

		if volume.Status.Robustness != longhorn.VolumeRobustnessFaulted &&
			(volume.Status.State == longhorn.VolumeStateAttached || allowDetached) {
			*filterNames = append(*filterNames, volume.Name)
			continue
		}
		logger.Warnf("Cannot create job for %v volume in state %v", volume.Name, volume.Status.State)
	}
}

func getVolumesBySelector(recurringJobType, recurringJobName, namespace string, client *lhclientset.Clientset) ([]longhorn.Volume, error) {
	logger := logrus.StandardLogger()

	label := fmt.Sprintf("%s=%s",
		types.GetRecurringJobLabelKey(recurringJobType, recurringJobName), types.LonghornLabelValueEnabled)
	logger.Infof("Got volumes from label %v", label)

	volumes, err := client.LonghornV1beta2().Volumes(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: label,
	})
	if err != nil {
		return nil, err
	}
	return volumes.Items, nil
}

func getSettingAsBoolean(name types.SettingName, namespace string, client *lhclientset.Clientset) (bool, error) {
	obj, err := client.LonghornV1beta2().Settings(namespace).Get(context.TODO(), string(name), metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	value, err := strconv.ParseBool(obj.Value)
	if err != nil {
		return false, err
	}
	return value, nil
}

func getLonghornClientset() (*lhclientset.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get client config")
	}
	return lhclientset.NewForConfig(config)
}
