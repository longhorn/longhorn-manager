package recurringjob

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func StartVolumeJobs(job *Job, recurringJob *longhorn.RecurringJob) error {
	allowDetachedSetting := types.SettingNameAllowRecurringJobWhileVolumeDetached
	allowDetached, err := getSettingAsBoolean(allowDetachedSetting, job.namespace, job.lhClient)
	if err != nil {
		return errors.Wrapf(err, "failed to get %v setting", allowDetachedSetting)
	}
	job.logger.Infof("Setting %v is %v", allowDetachedSetting, allowDetached)

	volumes, err := getVolumesBySelector(types.LonghornLabelRecurringJob, job.name, job.namespace, job.lhClient)
	if err != nil {
		return err
	}

	filteredVolumes := []string{}
	filterVolumesForJob(allowDetached, volumes, &filteredVolumes)

	jobGroups := recurringJob.Spec.Groups
	for _, jobGroup := range jobGroups {
		volumes, err := getVolumesBySelector(types.LonghornLabelRecurringJobGroup, jobGroup, job.namespace, job.lhClient)
		if err != nil {
			return err
		}
		filterVolumesForJob(allowDetached, volumes, &filteredVolumes)
	}

	job.logger.Infof("Found %v volumes with recurring job %v", len(filteredVolumes), job.name)

	concurrentLimiter := make(chan struct{}, recurringJob.Spec.Concurrency)
	ewg := &errgroup.Group{}
	defer func() {
		if wgError := ewg.Wait(); wgError != nil {
			err = wgError
		}
	}()
	for _, volumeName := range filteredVolumes {
		startJobVolumeName := volumeName
		ewg.Go(func() error {
			return startVolumeJob(job, recurringJob, startJobVolumeName, concurrentLimiter, jobGroups)
		})
	}

	return err
}

func startVolumeJob(job *Job, recurringJob *longhorn.RecurringJob,
	volumeName string, concurrentLimiter chan struct{}, jobGroups []string) error {

	concurrentLimiter <- struct{}{}
	defer func() {
		<-concurrentLimiter
	}()

	volumeJob, err := newVolumeJob(job, recurringJob, volumeName, jobGroups)
	if err != nil {
		job.logger.WithError(err).Errorf("Failed to initialize job for volume %v", volumeName)
		return err
	}

	volumeJob.logger.Info("Creating volume job")

	err = volumeJob.run()
	if err != nil {
		volumeJob.logger.WithError(err).Error("Failed to run volume job")
		return err
	}

	volumeJob.logger.Info("Created volume job")
	return nil
}

func newVolumeJob(job *Job, recurringJob *longhorn.RecurringJob, volumeName string, groups []string) (*VolumeJob, error) {
	specLabels := map[string]string{}
	if recurringJob.Spec.Labels != nil {
		specLabels = make(map[string]string, len(recurringJob.Spec.Labels))
		for k, v := range recurringJob.Spec.Labels {
			specLabels[k] = v
		}
	}
	specLabels[types.RecurringJobLabel] = recurringJob.Name

	specLabelsJSON, err := json.Marshal(specLabels)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get JSON encoding for labels")
	}

	snapshotName := sliceStringSafely(types.GetCronJobNameForRecurringJob(job.name), 0, 8) + "-" + util.UUID()

	logger := job.logger.WithFields(logrus.Fields{
		// job-specific fields
		"job":            job.name,
		"task":           job.task,
		"retain":         job.retain,
		"parameters":     job.parameters,
		"executionCount": job.executionCount,
		// volume-specific fields
		"concurrent":   recurringJob.Spec.Concurrency,
		"groups":       strings.Join(groups, ","),
		"volumeName":   volumeName,
		"snapshotName": snapshotName,
		"specLabels":   string(specLabelsJSON),
	})

	newJob := &VolumeJob{
		Job:          job,
		logger:       logger,
		concurrent:   recurringJob.Spec.Concurrency,
		groups:       groups,
		volumeName:   volumeName,
		snapshotName: snapshotName,
		specLabels:   specLabels,
	}
	return newJob, nil
}

func (job *VolumeJob) run() (err error) {
	job.logger.Info("Starting volume job")

	volumeAPI := job.api.Volume
	volumeName := job.volumeName
	volume, err := volumeAPI.ById(volumeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get volume %v", volumeName)
	}
	if volume == nil {
		job.logger.Infof("Volume %v not found, skipping", volumeName)
		return nil
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

func (job *VolumeJob) doRecurringSnapshot() (err error) {
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

func (job *VolumeJob) doSnapshot() (err error) {
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

	snapshotCRs := filterSnapshotCRsWithLabel(snapshotCRList.Data, types.RecurringJobLabel, job.specLabels[types.RecurringJobLabel])

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
			Labels: job.specLabels,
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

func (job *VolumeJob) waitForSnaphotReady(volume *longhornclient.Volume, timeout int) error {
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

func (job *VolumeJob) doSnapshotCleanup(backupDone bool) (err error) {
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

func (job *VolumeJob) eventCreate(eventType, eventReason, message string) error {
	jobName, found := job.specLabels[types.RecurringJobLabel]
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

func (job *VolumeJob) purgeSnapshots(volume *longhornclient.Volume, volumeAPI longhornclient.VolumeOperations) error {
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

func (job *VolumeJob) listSnapshotNamesToCleanup(snapshotCRs []longhornclient.SnapshotCR, backupDone bool) []string {
	switch job.task {
	case longhorn.RecurringJobTypeSnapshotDelete:
		return job.filterExpiredSnapshots(snapshotCRs)
	case longhorn.RecurringJobTypeSnapshotCleanup:
		return []string{}
	default:
		return job.filterExpiredSnapshotsOfCurrentRecurringJob(snapshotCRs, backupDone)
	}
}

func (job *VolumeJob) filterExpiredSnapshotsOfCurrentRecurringJob(snapshotCRs []longhornclient.SnapshotCR, backupDone bool) []string {
	jobLabel, found := job.specLabels[types.RecurringJobLabel]
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

func (job *VolumeJob) filterExpiredSnapshots(snapshotCRs []longhornclient.SnapshotCR) []string {
	return filterExpiredItems(snapshotCRsToNameWithTimestamps(snapshotCRs), job.retain)
}

func (job *VolumeJob) doRecurringBackup() (err error) {
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
	if intervalStr, exists := job.parameters[types.RecurringJobParameterFullBackupInterval]; exists {
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
		Labels:     job.specLabels,
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

	backupVolume, err := job.getBackupVolume(volume.BackupTargetName)
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

func (job *VolumeJob) getBackupVolume(backupTargetName string) (*longhornclient.BackupVolume, error) {
	list, err := job.api.BackupVolume.List(&longhornclient.ListOpts{})
	if err != nil {
		return nil, err
	}

	backupVolumeName := ""
	for _, bv := range list.Data {
		if bv.BackupTargetName == backupTargetName && bv.VolumeName == job.volumeName {
			backupVolumeName = bv.Name
			break
		}
	}

	return job.api.BackupVolume.ById(backupVolumeName)
}

func (job *VolumeJob) doRecurringFilesystemTrim(volume *longhornclient.Volume) (err error) {
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
func (job *VolumeJob) waitForBackupProcessStart(timeout int) error {
	volumeAPI := job.api.Volume
	volumeName := job.volumeName
	snapshot := job.snapshotName

	for i := 0; i < timeout; i++ {
		time.Sleep(WaitInterval)

		volume, err := volumeAPI.ById(volumeName)
		if err != nil {
			return err
		}

		if volume.BackupStatus == nil {
			continue
		}

		for _, status := range volume.BackupStatus {
			if status.Snapshot == snapshot {
				return nil
			}
		}
	}
	return fmt.Errorf("timeout waiting for the backup of the snapshot %v of volume %v to start", snapshot, volumeName)
}

// getLastBackup return the last backup of the volume
// return nil, nil if volume doesn't have any backup
func (job *VolumeJob) getLastBackup() (*longhornclient.Backup, error) {
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
	backupVolume, err := job.getBackupVolume(volume.BackupTargetName)
	if err != nil {
		return nil, err
	}
	return job.api.BackupVolume.ActionBackupGet(backupVolume, &longhornclient.BackupInput{
		Name: volume.LastBackup,
	})
}

func (job *VolumeJob) listBackupsForCleanup(backups []longhornclient.Backup) []string {
	sts := []NameWithTimestamp{}

	// only remove backups that where created by our current job
	jobLabel, found := job.specLabels[types.RecurringJobLabel]
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
