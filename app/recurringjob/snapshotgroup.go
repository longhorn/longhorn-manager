package recurringjob

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/wait"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	defaultDeadlineSeconds       = 300
	snapshotGroupWaitGracePeriod = 30 * time.Second
)

var errSnapshotLimitReached = errors.New("member volumes have reached their snapshot limits")

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
		// Retention normally runs after a group is created. A volume at its limit may be
		// full of this job's own expired groups, so run it here too; otherwise every later
		// run is rejected the same way. It deletes only groups beyond retainCount, e.g. with
		// retainCount 2 and SnapshotMaxCount 3:
		//   - 2 groups of this job + 1 other snapshot: nothing is expired, so nothing is
		//     deleted; the volume stays full until the user frees a snapshot.
		//   - 3 groups of this job (one kept earlier for an active backup that has since
		//     finished): the oldest is deleted, and the next run has room once it is purged.
		if errors.Is(err, errSnapshotLimitReached) {
			retentionJob := &SnapshotGroupJob{Job: job, logger: logrus.NewEntry(job.logger).WithField("job", job.name)}
			retentionJob.cleanup()
		}
		return err
	}

	sg, err := snapshotGroupJob.run()
	defer snapshotGroupJob.cleanup()

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

	volumes, foundIneligibleVols, err := resolveSnapshotGroupVolumes(job, recurringJob, allowDetached)
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

	// Under count-based retention, a member volume must be able to hold retainCount
	// snapshots plus the new one. If retainCount >= its SnapshotMaxCount, every run's
	// SnapshotGroup fails on that volume and is only cleaned up, so no group ever
	// succeeds. Reject the job up front instead.
	countBasedRetention := job.retentionPolicy == longhorn.RecurringJobRetentionPolicyCountBased
	volumeNames := make([]string, 0, len(volumes))
	var invalidVolumes []string
	for _, volume := range volumes {
		volumeNames = append(volumeNames, volume.Name)
		if countBasedRetention && job.retainCount >= volume.Spec.SnapshotMaxCount {
			invalidVolumes = append(invalidVolumes, fmt.Sprintf("%v(snapshotMaxCount=%d)", volume.Name, volume.Spec.SnapshotMaxCount))
		}
	}

	if len(invalidVolumes) > 0 {
		err := fmt.Errorf("invalid retainCount: retainCount %d of recurring job %v must be less than SnapshotMaxCount of every member volume: %v",
			job.retainCount, job.name, strings.Join(invalidVolumes, ", "))
		return nil, err
	}

	if err := checkSnapshotLimits(job, volumes, volumeNames); err != nil {
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
		"snapshotGroup": snapshotGroupName,
		"volumes":       volumes,
	})

	newJob := &SnapshotGroupJob{
		Job:             job,
		logger:          logger,
		snapshotGroup:   snapshotGroupName,
		volumes:         volumeNames,
		labels:          recurringJob.Spec.Labels,
		deadlineSeconds: defaultDeadlineSeconds,
	}

	return newJob, nil
}

// resolveSnapshotGroupVolumes returns the volumes assigned to the recurring job, either
// directly through the job label or through one of its groups, that are eligible to
// join a snapshot group, sorted by name. It also returns how many assigned volumes
// were ineligible, so the caller can skip the run rather than create a partial group.
//
// The selectors mirror the ones in startVolumeJobs; keep the two in sync.
func resolveSnapshotGroupVolumes(job *Job, recurringJob *longhorn.RecurringJob, allowDetached bool) ([]*longhorn.Volume, int, error) {
	var assignedVols []*longhorn.Volume
	collectAssigned := func(vols []longhorn.Volume) {
		for i := range vols {
			assignedVols = append(assignedVols, &vols[i])
		}
	}

	volumes, err := getVolumesBySelector(types.LonghornLabelRecurringJob, job.name, job.namespace, job.lhClient)
	if err != nil {
		return nil, 0, err
	}
	collectAssigned(volumes)

	for _, jobGroup := range recurringJob.Spec.Groups {
		volumes, err := getVolumesBySelector(types.LonghornLabelRecurringJobGroup, jobGroup, job.namespace, job.lhClient)
		if err != nil {
			return nil, 0, err
		}
		collectAssigned(volumes)
	}

	// A volume can match both the job label and a group label. Sort by name (stable, so
	// the first-listed copy is kept) and drop adjacent duplicates. Filtering below keeps
	// the order, so the result is already sorted.
	slices.SortStableFunc(assignedVols, func(a, b *longhorn.Volume) int { return strings.Compare(a.Name, b.Name) })
	assignedVols = slices.CompactFunc(assignedVols, func(a, b *longhorn.Volume) bool { return a.Name == b.Name })

	var filteredVols []*longhorn.Volume
	for _, volume := range assignedVols {
		if isVolumeEligibleForJob(allowDetached, volume) {
			filteredVols = append(filteredVols, volume)
		}
	}
	return filteredVols, len(assignedVols) - len(filteredVols), nil
}

// checkSnapshotLimits returns an error naming the member volumes that have already reached
// SnapshotMaxCount or SnapshotMaxSize, because the engine would refuse their member snapshot
// and fail the whole group.
func checkSnapshotLimits(job *Job, volumes []*longhorn.Volume, volumeNames []string) error {
	requirement, err := labels.NewRequirement(types.LonghornLabelVolume, selection.In, volumeNames)
	if err != nil {
		return errors.Wrapf(err, "failed to build the engine selector for recurring job %v", job.name)
	}
	engineList, err := job.lhClient.LonghornV1beta2().Engines(job.namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: labels.NewSelector().Add(*requirement).String(),
	})
	if err != nil {
		return errors.Wrapf(err, "failed to list engines of member volumes for recurring job %v", job.name)
	}

	type snapshotUsage struct {
		count int
		size  int64
	}
	usageByVolume := make(map[string]snapshotUsage, len(volumes))
	for _, engine := range engineList.Items {
		var usage snapshotUsage
		for _, snapshot := range engine.Status.Snapshots {
			if snapshot == nil || snapshot.Name == etypes.VolumeHeadName {
				continue
			}
			usage.count++
			if snapshot.Removed || snapshot.Size == "" {
				continue
			}
			size, err := strconv.ParseInt(snapshot.Size, 10, 64)
			if err != nil {
				return errors.Wrapf(err, "failed to parse size %q of snapshot %v of volume %v", snapshot.Size, snapshot.Name, engine.Spec.VolumeName)
			}
			usage.size += size
		}
		// A volume has two engines during a live migration; keep the larger usage.
		current := usageByVolume[engine.Spec.VolumeName]
		usageByVolume[engine.Spec.VolumeName] = snapshotUsage{count: max(current.count, usage.count), size: max(current.size, usage.size)}
	}

	var limitReached []string
	for _, volume := range volumes {
		usage := usageByVolume[volume.Name]
		switch {
		case volume.Spec.SnapshotMaxCount > 0 && usage.count >= volume.Spec.SnapshotMaxCount:
			limitReached = append(limitReached, fmt.Sprintf("%v (%d/%d snapshots)", volume.Name, usage.count, volume.Spec.SnapshotMaxCount))
		case volume.Spec.SnapshotMaxSize > 0 && usage.size >= volume.Spec.SnapshotMaxSize:
			limitReached = append(limitReached, fmt.Sprintf("%v (%d/%d bytes of snapshots)", volume.Name, usage.size, volume.Spec.SnapshotMaxSize))
		}
	}
	if len(limitReached) > 0 {
		return fmt.Errorf("cannot create snapshot group for recurring job %v: %w: %v; delete snapshots or raise the limits",
			job.name, errSnapshotLimitReached, strings.Join(limitReached, ", "))
	}
	return nil
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

// cleanup removes expired SnapshotGroups based on the configured retention policy.
// Only Ready and Failed SnapshotGroups are considered for retention;
// Groups containing snapshots used by active backups are skipped.
func (job *SnapshotGroupJob) cleanup() {
	job.logger.Info("Cleaning up expired snapshot groups")
	defer job.logger.Info("Finished cleaning up expired snapshot groups")

	snapshotGroupList, err := job.ListSnapshotGroup()
	if err != nil {
		job.logger.WithError(err).Warn("Failed to list snapshot groups")
		return
	}

	// Ready and Failed only; InProgress and Degraded are never counted or deleted.
	eligible := make([]longhorn.SnapshotGroup, 0, len(snapshotGroupList.Items))
	for _, snapshotGroup := range snapshotGroupList.Items {
		if snapshotGroup.DeletionTimestamp == nil && isSnapshotGroupEligibleForRetention(snapshotGroup.Status.Phase) {
			eligible = append(eligible, snapshotGroup)
		}
	}

	toDelete := filterExpiredSnapshotGroupItems(eligible, job.retainCount, job.retainAge, job.retentionPolicy, time.Now())

	if len(toDelete) == 0 {
		return
	}

	backupList, err := job.lhClient.LonghornV1beta2().Backups(job.namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		job.logger.WithError(err).Warn("Failed to list backups, skipping snapshot group cleanup")
		return
	}
	snapshotBackupMap := activeSnapshotBackups(backupList.Items)

	for _, snapshotGroup := range toDelete {
		name := snapshotGroup.Name
		if snapshot, backup, anyActive := firstActiveBackupMember(snapshotGroup, snapshotBackupMap); anyActive {
			job.logger.Debugf("Deferred deleting snapshot group %v: member snapshot %v is used by active backup %v",
				name, snapshot, backup)
			continue
		}

		job.logger.Infof("Deleting snapshot group %v", name)
		err = job.DeleteSnapshotGroup(name)
		if err != nil && !apierrors.IsNotFound(err) {
			job.logger.WithError(err).Warnf("Failed to delete snapshot group %v", name)
		}
	}
}

// isSnapshotGroupEligibleForRetention reports whether a SnapshotGroup should be
// considered by retention cleanup. Only the completed terminal states Ready and
// Failed participate.
func isSnapshotGroupEligibleForRetention(phase longhorn.SnapshotGroupPhase) bool {
	return phase == longhorn.SnapshotGroupPhaseReady || phase == longhorn.SnapshotGroupPhaseFailed
}

// filterExpiredSnapshotGroupItems returns the snapshot groups that
// retention says to delete. groups must be limited to one job's Ready/Failed
// groups.
//   - Count-based: keeps at most retainCount groups. Failed groups are deleted
//     before Ready ones, oldest first within each phase, so a Ready group is
//     deleted only once no Failed group is left. A negative retainCount deletes nothing.
//   - Age-based: deletes every group older than retainAge, regardless of phase.
//     A retainAge <= 0 deletes nothing.
//   - Any other policy deletes nothing.
func filterExpiredSnapshotGroupItems(groups []longhorn.SnapshotGroup, retainCount int, retainAge time.Duration,
	policy longhorn.RecurringJobRetentionPolicy, now time.Time) []longhorn.SnapshotGroup {
	var filtered []longhorn.SnapshotGroup

	switch policy {
	case longhorn.RecurringJobRetentionPolicyCountBased:
		excess := len(groups) - retainCount
		if retainCount < 0 || excess <= 0 {
			return filtered
		}

		sort.Slice(groups, func(i, j int) bool {
			iFailed := groups[i].Status.Phase == longhorn.SnapshotGroupPhaseFailed
			jFailed := groups[j].Status.Phase == longhorn.SnapshotGroupPhaseFailed
			if iFailed != jFailed {
				return iFailed // Failed first
			}
			if !groups[i].CreationTimestamp.Equal(&groups[j].CreationTimestamp) {
				return groups[i].CreationTimestamp.Before(&groups[j].CreationTimestamp)
			}
			return groups[i].Name < groups[j].Name
		})

		filtered = append(filtered, groups[:excess]...)

	case longhorn.RecurringJobRetentionPolicyAgeBased:
		if retainAge <= 0 {
			return filtered
		}
		for _, group := range groups {
			if now.Sub(group.CreationTimestamp.Time) > retainAge {
				filtered = append(filtered, group)
			}
		}

	default:
		// Unknown policy: delete nothing.
	}

	return filtered
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
