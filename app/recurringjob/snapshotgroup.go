package recurringjob

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/util/wait"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

// StartSnapshotGroupJob runs one scheduled snapshot-group recurring job: it skips
// the run if a previous group is still active, otherwise creates a SnapshotGroup
// of the volumes assigned to the job and waits for it to finish.
func StartSnapshotGroupJob(job *Job, recurringJob *longhorn.RecurringJob) error {
	sgExists, err := job.ListSnapshotGroup()
	if err != nil {
		job.eventRecorder.Event(recurringJob, corev1.EventTypeWarning,
			constant.EventReasonFailedSnapshotGroup, fmt.Sprintf("Failed to list SnapshotGroups for recurring job: %v", err))
		return fmt.Errorf("failed to list SnapshotGroups for recurring job %s: %w", recurringJob.Name, err)
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

	recordFailure := func(err error) {
		snapshotGroupJob.logger.WithError(err).Error("snapshotGroup job failed")
		job.eventRecorder.Event(recurringJob, corev1.EventTypeWarning,
			constant.EventReasonFailedSnapshotGroup, err.Error())
	}
	recordCompleted := func(msg string) {
		snapshotGroupJob.logger.Info(msg)
		job.eventRecorder.Event(recurringJob, corev1.EventTypeNormal,
			constant.EventReasonCompletedSnapshotGroup, msg)
	}

	switch {
	case err != nil:
		recordFailure(err)
		return err

	case sg.Status.Phase == longhorn.SnapshotGroupPhaseFailed:
		err = fmt.Errorf("SnapshotGroup %s ended in phase %v: %s", sg.Name, sg.Status.Phase, sg.Status.Error)
		recordFailure(err)
		return err

	case sg.Status.Phase == longhorn.SnapshotGroupPhaseReady:
		recordCompleted(fmt.Sprintf("SnapshotGroup %s is ready", sg.Name))

	default:
		err = fmt.Errorf("SnapshotGroup %s ended in unexpected phase %q", sg.Name, sg.Status.Phase)
		recordFailure(err)
		return err
	}

	return nil
}

// newSnapshotGroupJob resolves the volumes assigned to the recurring job and builds the job.
func newSnapshotGroupJob(job *Job, recurringJob *longhorn.RecurringJob) (*SnapshotGroupJob, error) {
	allowDetached, err := getSettingAsBoolean(types.SettingNameAllowRecurringJobWhileVolumeDetached, job.namespace, job.lhClient)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get %v setting", types.SettingNameAllowRecurringJobWhileVolumeDetached)
	}

	volumes, foundIneligibleVols, err := resolveAssignedVolumes(job, recurringJob, allowDetached)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve volumes assigned to recurring job %v: %v", job.name, err)
	}
	// A snapshot group is meant to be a consistent set. If any assigned volume is left out by
	// filterVolumesForJob (restoring, faulted, or detached while that is not allowed), skip the
	// run instead of creating a smaller group that would still report Ready.
	if foundIneligibleVols != 0 {
		return nil, fmt.Errorf("skipping snapshotGroup for job %v: %d volumes are either faulted, restoring from backup or detached is disabled", job.name, foundIneligibleVols)
	}
	if len(volumes) == 0 {
		return nil, fmt.Errorf("no volumes are assigned to recurring job %v", job.name)
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
		"snapshotGroup": snapshotGroupName,
		"volumes":       volumes,
	})

	newJob := &SnapshotGroupJob{
		Job:             job,
		logger:          logger,
		snapshotGroup:   snapshotGroupName,
		volumes:         volumes,
		labels:          recurringJob.Spec.Labels,
		deadlineSeconds: defaultDeadlineSeconds,
	}

	return newJob, nil
}

// run creates the SnapshotGroup with the resolved volumes as explicit members,
// tags it with the owning job's label, and waits for it to reach a terminal phase.
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
			Volumes:         job.volumes,
			Labels:          job.labels,
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

// waitForTerminalPhase polls the group until it is Ready or Failed. The timeout
// (deadline plus grace period) is only a backstop: the controller itself fails the
// group once its deadline passes.
func (job *SnapshotGroupJob) waitForTerminalPhase() (*longhorn.SnapshotGroup, error) {
	// the controller fails the group after deadlineSeconds
	// the extra margin is a backstop in case the controller itself is not running
	timeout := time.Duration(job.deadlineSeconds)*time.Second + snapshotGroupWaitGracePeriod
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var final *longhorn.SnapshotGroup
	var lastGetErr error
	err := wait.PollUntilContextCancel(ctx, WaitInterval, true, func(ctx context.Context) (bool, error) {
		sg, err := job.GetSnapshotGroup(ctx, job.snapshotGroup)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// retrying cannot help: the group was deleted
				return false, fmt.Errorf("failed to get SnapshotGroup: %w", err)
			}
			lastGetErr = err
			job.logger.WithError(err).Warn("Transient error getting SnapshotGroup, will retry")
			return false, nil
		}
		lastGetErr = nil
		final = sg

		switch sg.Status.Phase {
		case longhorn.SnapshotGroupPhaseReady, longhorn.SnapshotGroupPhaseFailed:
			return true, nil
		}
		job.logger.Debugf("SnapshotGroup %v is in phase %q, waiting", sg.Name, sg.Status.Phase)
		return false, nil
	})
	if err != nil {
		if lastGetErr != nil {
			err = fmt.Errorf("%w (last GET error: %v)", err, lastGetErr)
		}
		return final, fmt.Errorf("failed waiting for SnapshotGroup %v to finish: %v", job.snapshotGroup, err)
	}
	return final, nil
}

// anyInProgress reports a group that is created but not yet finished: an empty phase
// (not yet reconciled by the controller) or InProgress. Only Ready and Failed are terminal.
func anyInProgress(snapshotGroups []longhorn.SnapshotGroup) (string, bool) {
	for _, snapshotGroup := range snapshotGroups {
		if snapshotGroup.Status.Phase == longhorn.SnapshotGroupPhaseInProgress || snapshotGroup.Status.Phase == "" {
			return snapshotGroup.Name, true
		}
	}
	return "", false
}
