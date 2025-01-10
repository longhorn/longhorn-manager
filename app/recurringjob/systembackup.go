package recurringjob

import (
	"time"

	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lhutils "github.com/longhorn/go-common-libs/utils"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func StartSystemBackupJob(job *Job, recurringJob *longhorn.RecurringJob) error {
	systemBackupJob, err := newSystemBackupJob(job)
	if err != nil {
		job.logger.WithError(err).Errorf("Failed to initialize system backup job")
		return err
	}

	defer systemBackupJob.cleanup()

	err = systemBackupJob.run()
	if err != nil {
		systemBackupJob.logger.WithError(err).Error("Failed to run system backup job")
		return err
	}

	systemBackupJob.logger.Info("Created system backup job")

	return nil
}

func newSystemBackupJob(job *Job) (*SystemBackupJob, error) {
	volumeBackupPolicy := job.parameters[types.RecurringJobParameterVolumeBackupPolicy]

	systemBackupName := sliceStringSafely(types.GetCronJobNameForRecurringJob(job.name), 0, 8) + "-" + util.UUID()

	logger := job.logger.WithFields(logrus.Fields{
		// job-specific fields
		"job":            job.name,
		"task":           job.task,
		"retain":         job.retain,
		"parameters":     job.parameters,
		"executionCount": job.executionCount,
		// system-backup-specific fields
		"systemBackupName":   systemBackupName,
		"volumeBackupPolicy": volumeBackupPolicy,
	})

	newJob := &SystemBackupJob{
		Job:                job,
		logger:             logger,
		systemBackupName:   systemBackupName,
		volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicy(volumeBackupPolicy),
	}
	return newJob, nil
}

func (job *SystemBackupJob) run() (err error) {
	job.logger.Info("Starting system backup job")
	defer func() {
		if err != nil {
			job.logger.WithError(err).Error("Failed to run system backup job")
		} else {
			job.logger.Info("Finished running system backup job")
		}
	}()

	newSystemBackup := &longhorn.SystemBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.systemBackupName,
			Namespace: job.namespace,
			Labels: map[string]string{
				types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJob, string(longhorn.RecurringJobTypeSystemBackup)): job.name,
			},
		},
		Spec: longhorn.SystemBackupSpec{
			VolumeBackupPolicy: job.volumeBackupPolicy,
		},
	}

	_, err = job.CreateSystemBackup(newSystemBackup)
	if err != nil {
		return err
	}

	finalStates := []longhorn.SystemBackupState{
		longhorn.SystemBackupStateReady,
		longhorn.SystemBackupStateError,
	}
	return job.waitForSystemBackupToStates(finalStates)
}

func (job *SystemBackupJob) cleanup() {
	job.logger.Info("Cleaning up expired system backups")
	defer job.logger.Info("Finished cleaning up expired system backups")

	systemBackupList, err := job.ListSystemBackup()
	if err != nil {
		job.logger.WithError(err).Warn("Failed to list system backups")
		return
	}

	expiredSystemBackups := filterExpiredItems(systemBackupsToNameWithTimestamps(systemBackupList), job.retain)
	for _, systemBackupName := range expiredSystemBackups {
		job.logger.Infof("Deleting system backup %v", systemBackupName)
		err = job.DeleteSystemBackup(systemBackupName)
		if err != nil {
			job.logger.WithError(err).Warnf("Failed to delete system backup %v", systemBackupName)
		}
	}
}

func (job *SystemBackupJob) waitForSystemBackupToStates(expectedStates []longhorn.SystemBackupState) (err error) {
	job.logger.Info("Waiting for system backup to complete")
	defer func() {
		if err != nil {
			job.logger.WithError(err).Errorf("Failed to wait for system backup to reach state %v", expectedStates)
		} else {
			job.logger.Infof("System backup reached state %v", expectedStates)
		}
	}()

	for {
		systemBackup, err := job.GetSystemBackup(job.systemBackupName)
		if err != nil {
			return err
		}

		if lhutils.Contains(expectedStates, systemBackup.Status.State) {
			return nil
		}

		job.logger.Infof("Waiting for system backup to reach state %v, current state is %v", expectedStates, systemBackup.Status.State)
		time.Sleep(WaitInterval)
	}
}
