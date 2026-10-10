package recurringjob

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/util/wait"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	defaultDeadlineSeconds       = 300
	snapshotGroupWaitGracePeriod = 30 * time.Second
)

func StartSnapshotGroupJob(job *Job, recurringJob *longhorn.RecurringJob) error {
	// concurrency guard: skip if this job's previous group is still running
	sgExists, err := job.ListSnapshotGroup()
	if err != nil {
		job.eventRecorder.Event(recurringJob, corev1.EventTypeWarning,
			constant.EventReasonFailedSnapshotGroup, fmt.Sprintf("Failed to list SnapshotGroups recurring job: %v", err))
		return nil
	}
	if name, isProgressing := anyInProgress(sgExists.Items); isProgressing {
		job.eventRecorder.Event(recurringJob, corev1.EventTypeNormal,
			constant.EventReasonSkippedSnapshotGroup,
			fmt.Sprintf("previous group %s is still InProgress; skipping this run", name))
		return nil
	}

	snapshotGroupJob, err := newSnapshotGroupJob(job, recurringJob)
	if err != nil {
		job.logger.WithError(err).Errorf("Failed to initialize snapshotGroup job")
		job.eventRecorder.Event(recurringJob, corev1.EventTypeWarning,
			constant.EventReasonFailedSnapshotGroup, err.Error())
		return err
	}

	sg, err := snapshotGroupJob.run()

	switch {
	case err != nil:
		snapshotGroupJob.logger.WithError(err).Error("Failed to run snapshotGroup job")
		job.eventRecorder.Event(recurringJob, corev1.EventTypeWarning,
			constant.EventReasonFailedSnapshotGroup, err.Error())
	case sg.Status.Phase == longhorn.SnapshotGroupPhaseReady:
		snapshotGroupJob.logger.Infof("Finished running snapshotGroup job: group %v is Ready", sg.Name)
		job.eventRecorder.Event(recurringJob, corev1.EventTypeNormal,
			constant.EventReasonCompletedSnapshotGroup,
			fmt.Sprintf("SnapshotGroup %s is ready", sg.Name))
	default:
		snapshotGroupJob.logger.Warnf("Finished running snapshotGroup job: group %v ended in phase %v", sg.Name, sg.Status.Phase)
		job.eventRecorder.Event(recurringJob, corev1.EventTypeWarning,
			constant.EventReasonFailedSnapshotGroup,
			fmt.Sprintf("SnapshotGroup %s failed", sg.Name))
	}

	return nil
}

func newSnapshotGroupJob(job *Job, recurringJob *longhorn.RecurringJob) (*SnapshotGroupJob, error) {
	selector, err := buildVolumeSelector(recurringJob.Spec.Labels)
	if err != nil {
		return nil, err
	}

	snapshotGroupName := sliceStringSafely(types.GetCronJobNameForRecurringJob(job.name), 0, 8) + "-" + util.UUID()

	logger := job.logger.WithFields(logrus.Fields{
		// job-specific fields
		"job":            job.name,
		"task":           job.task,
		"retainCount":    job.retainCount,
		"parameters":     job.parameters,
		"executionCount": job.executionCount,
		// snapshotGroup-specific fields
		"snapshotGroup":  snapshotGroupName,
		"volumeSelector": selector,
	})

	newJob := &SnapshotGroupJob{
		Job:             job,
		logger:          logger,
		snapshotGroup:   snapshotGroupName,
		volumeSelector:  selector,
		deadlineSeconds: defaultDeadlineSeconds,
	}

	return newJob, nil
}

func (job *SnapshotGroupJob) run() (*longhorn.SnapshotGroup, error) {
	job.logger.Info("Starting SnapshotGroup job")

	newSnapshotGroup := &longhorn.SnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.snapshotGroup,
			Namespace: job.namespace,
			Labels: map[string]string{
				types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJob, string(longhorn.RecurringJobTypeSnapshotGroup)): job.name,
			},
		},
		Spec: longhorn.SnapshotGroupSpec{
			VolumeSelector:  job.volumeSelector,
			DeadlineSeconds: job.deadlineSeconds,
		},
	}

	_, err := job.CreateSnapshotGroup(newSnapshotGroup)
	if err != nil {
		job.logger.WithError(err).Error("Failed to run snapshotGroup job")
		return nil, fmt.Errorf("failed to create SnapshotGroup: %v", err)
	}
	job.logger.Infof("Created SnapshotGroup %v, waiting for it to finish", job.snapshotGroup)

	return job.waitForTerminalPhase()
}

func (job *SnapshotGroupJob) waitForTerminalPhase() (*longhorn.SnapshotGroup, error) {
	// the controller fails the group after deadlineSeconds
	// the extra margin is a backstop in case the controller itself is not running
	timeout := time.Duration(job.deadlineSeconds)*time.Second + snapshotGroupWaitGracePeriod
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var final *longhorn.SnapshotGroup
	err := wait.PollUntilContextCancel(ctx, WaitInterval, true, func(ctx context.Context) (bool, error) {
		sg, err := job.GetSnapshotGroup(job.snapshotGroup)
		if err != nil {
			return false, fmt.Errorf("failed to get SnapshotGroup: %v", err)
		}
		final = sg

		switch sg.Status.Phase {
		case longhorn.SnapshotGroupPhaseReady, longhorn.SnapshotGroupPhaseFailed:
			return true, nil
		}
		job.logger.Debugf("SnapshotGroup %v is in phase %q, waiting", sg.Name, sg.Status.Phase)
		return false, nil
	})
	if err != nil {
		return final, fmt.Errorf("failed waiting for SnapshotGroup %v to finish: %v", job.snapshotGroup, err)
	}
	return final, nil
}

func anyInProgress(snapshotGroups []longhorn.SnapshotGroup) (string, bool) {
	for _, snapshotGroup := range snapshotGroups {
		if snapshotGroup.Status.Phase == longhorn.SnapshotGroupPhaseInProgress {
			return snapshotGroup.Name, true
		}
	}
	return "", false
}

func buildVolumeSelector(labels map[string]string) (*metav1.LabelSelector, error) {
	if len(labels) == 0 {
		return nil, fmt.Errorf("snapshot-group requires at least one label to select volumes")
	}
	return &metav1.LabelSelector{MatchLabels: labels}, nil
}
