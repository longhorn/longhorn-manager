package app

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	typedv1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	"github.com/longhorn/longhorn-manager/constant"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
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
)

type Job struct {
	logger       logrus.FieldLogger
	lhClient     lhclientset.Interface
	namespace    string
	volumeName   string
	snapshotName string
	retain       int
	task         longhorn.RecurringJobType
	labels       map[string]string

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
				logrus.WithError(err).Fatal("Failed to snapshot")
			}
		},
	}
}

func recurringJob(c *cli.Context) error {
	logger := logrus.StandardLogger()
	var err error

	var managerURL string = c.String(FlagManagerURL)
	if managerURL == "" {
		return fmt.Errorf("require %v", FlagManagerURL)
	}

	if c.NArg() != 1 {
		return errors.New("job name is required")
	}
	jobName := c.Args()[0]

	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return fmt.Errorf("cannot detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}
	lhClient, err := getLonghornClientset()
	if err != nil {
		return errors.Wrap(err, "unable to get clientset")
	}
	recurringJob, err := lhClient.LonghornV1beta2().RecurringJobs(namespace).Get(context.TODO(), jobName, metav1.GetOptions{})
	if err != nil {
		logrus.WithError(err).Errorf("Failed to get recurring job %v.", jobName)
		return nil
	}

	var jobGroups []string = recurringJob.Spec.Groups
	var jobRetain int = recurringJob.Spec.Retain
	var jobConcurrent int = recurringJob.Spec.Concurrency

	jobLabelMap := map[string]string{}
	if recurringJob.Spec.Labels != nil {
		jobLabelMap = recurringJob.Spec.Labels
	}
	jobLabelMap[types.RecurringJobLabel] = recurringJob.Name
	labelJSON, err := json.Marshal(jobLabelMap)
	if err != nil {
		return errors.Wrap(err, "failed to get JSON encoding for labels")
	}

	allowDetachedSetting := types.SettingNameAllowRecurringJobWhileVolumeDetached
	allowDetached, err := getSettingAsBoolean(allowDetachedSetting, namespace, lhClient)
	if err != nil {
		return errors.Wrapf(err, "unable to get %v setting", allowDetachedSetting)
	}
	logger.Debugf("Setting %v is %v", allowDetachedSetting, allowDetached)

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
	var wg sync.WaitGroup
	defer wg.Wait()
	for _, volumeName := range filteredVolumes {
		wg.Add(1)
		go func(volumeName string) {
			concurrentLimiter <- struct{}{}
			defer func() {
				<-concurrentLimiter
				wg.Done()
			}()

			log := logger.WithFields(logrus.Fields{
				"job":        jobName,
				"volume":     volumeName,
				"task":       recurringJob.Spec.Task,
				"retain":     jobRetain,
				"concurrent": jobConcurrent,
				"groups":     strings.Join(jobGroups, ","),
				"labels":     string(labelJSON),
			})
			log.Info("Creating job")

			snapshotName := sliceStringSafely(types.GetCronJobNameForRecurringJob(jobName), 0, 8) + "-" + util.UUID()
			job, err := NewJob(
				logger,
				managerURL,
				volumeName,
				snapshotName,
				jobLabelMap,
				jobRetain,
				recurringJob.Spec.Task)
			if err != nil {
				log.WithError(err).Error("Failed to create new job for volume")
				return
			}
			err = job.run()
			if err != nil {
				log.WithError(err).Errorf("Failed to run job for volume")
				return
			}

			log.Info("Created job")
		}(volumeName)
	}

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

func NewJob(logger logrus.FieldLogger, managerURL, volumeName, snapshotName string, labels map[string]string, retain int, task longhorn.RecurringJobType) (*Job, error) {
	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return nil, fmt.Errorf("cannot detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get client config")
	}
	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get clientset")
	}

	clientOpts := &longhornclient.ClientOpts{
		Url:     managerURL,
		Timeout: HTTPClientTimout,
	}
	apiClient, err := longhornclient.NewRancherClient(clientOpts)
	if err != nil {
		return nil, errors.Wrapf(err, "could not create longhorn-manager api client")
	}

	logger = logger.WithFields(logrus.Fields{
		"namespace":    namespace,
		"volumeName":   volumeName,
		"snapshotName": snapshotName,
		"labels":       labels,
		"retain":       retain,
		"task":         task,
	})

	scheme := runtime.NewScheme()
	if err := longhorn.SchemeBuilder.AddToScheme(scheme); err != nil {
		return nil, errors.Wrap(err, "unable to create scheme")
	}

	eventBroadcaster, err := createEventBroadcaster(config)
	if err != nil {
		return nil, err
	}

	return &Job{
		logger:       logger,
		lhClient:     lhClient,
		namespace:    namespace,
		volumeName:   volumeName,
		snapshotName: snapshotName,
		labels:       labels,
		retain:       retain,
		task:         task,
		api:          apiClient,

		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-recurring-job"}),
	}, nil
}

func createEventBroadcaster(config *rest.Config) (record.EventBroadcaster, error) {
	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get k8s client")
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&typedv1core.EventSinkImpl{Interface: typedv1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	return eventBroadcaster, nil
}

// handleVolumeDetachment decides whether the current recurring job should detach the volume.
// It should detach the volume when:
// 1. The volume is attached by this recurring job
// 2. The volume state is VolumeStateAttached
// NOTE:
//
//	The volume could remain attached when the recurring job pod gets force
//	terminated and unable to complete detachment within the grace period. Thus
//	there is a workaround in the recurring job controller to handle the
//	detachment again (detachVolumeAutoAttachedByRecurringJob).
func (job *Job) handleVolumeDetachment() {
	volumeAPI := job.api.Volume
	volumeName := job.volumeName
	jobName := job.labels[types.RecurringJobLabel]
	for {
		volume, err := volumeAPI.ById(volumeName)
		if err == nil {
			if volume.LastAttachedBy != jobName {
				return
			}
			// !volume.DisableFrontend condition makes sure that volume is detached by this recurring job,
			// not by the auto-reattachment feature.
			if volume.State == string(longhorn.VolumeStateDetached) && !volume.DisableFrontend {
				job.logger.Infof("Volume %v is detached", volumeName)
				return
			}
			if volume.State == string(longhorn.VolumeStateAttached) {
				job.logger.Infof("Attempting to detach volume %v from all nodes", volumeName)
				if _, err := volumeAPI.ActionDetach(volume, &longhornclient.DetachInput{HostId: ""}); err != nil {
					job.logger.WithError(err).Info("Volume detach request failed")
				}
			}

		} else {
			job.logger.WithError(err).Infof("Could not get volume %v", volumeName)
		}
		time.Sleep(DetachingWaitInterval)
	}
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

	defer job.handleVolumeDetachment()

	if volume.State != string(longhorn.VolumeStateAttached) && volume.State != string(longhorn.VolumeStateDetached) {
		return fmt.Errorf("volume %v is in an invalid state for recurring job: %v. Volume must be in state Attached or Detached", volumeName, volume.State)
	}

	if volume.State == string(longhorn.VolumeStateDetached) {
		// Find a random ready node to attach the volume
		// For load balancing purpose, the node need to be random
		nodeToAttach, err := job.findARandomReadyNode(volume)
		if err != nil {
			return errors.Wrapf(err, "cannot do auto attaching for volume %v", volumeName)
		}

		jobName := job.labels[types.RecurringJobLabel]
		// Automatically attach the volume
		// Disable the volume's frontend make sure that pod cannot use the volume during the recurring job.
		// This is necessary so that we can safely detach the volume when finishing the job.
		job.logger.Infof("Automatically attach volume %v to node %v", volumeName, nodeToAttach)
		if _, err = volumeAPI.ActionAttach(volume, &longhornclient.AttachInput{
			DisableFrontend: true,
			HostId:          nodeToAttach,
			AttachedBy:      jobName,
		}); err != nil {
			return err
		}

		volume, err = job.waitForVolumeState(string(longhorn.VolumeStateAttached), VolumeAttachTimeout)
		if err != nil {
			return err
		}
		job.logger.Infof("Volume %v is in state %v", volumeName, volume.State)
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
		err = errors.Wrapf(err, "failed recurring snapshot")
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

	if _, err := volumeAPI.ActionSnapshotCreate(volume, &longhornclient.SnapshotInput{
		Labels: job.labels,
		Name:   job.snapshotName,
	}); err != nil {
		return err
	}

	job.logger.Infof("Created the snapshot %v", job.snapshotName)

	return nil
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

	volumeAPI := job.api.Volume
	volumeName := job.volumeName
	volume, err := volumeAPI.ById(volumeName)
	if err != nil {
		return errors.Wrapf(err, "could not get volume %v", volumeName)
	}

	collection, err := volumeAPI.ActionSnapshotList(volume)
	if err != nil {
		return err
	}

	deleteSnapshotNames := job.listSnapshotNamesToCleanup(collection.Data, backupDone)
	if err := job.deleteSnapshots(deleteSnapshotNames, volume, volumeAPI); err != nil {
		return err
	}

	shouldPurgeSnapshots := len(deleteSnapshotNames) > 0 || job.task == longhorn.RecurringJobTypeSnapshotCleanup || job.task == longhorn.RecurringJobTypeSnapshotDelete
	if shouldPurgeSnapshots {
		if err := job.purgeSnapshots(volume, volumeAPI); err != nil {
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

func (job *Job) deleteSnapshots(names []string, volume *longhornclient.Volume, volumeAPI longhornclient.VolumeOperations) error {
	for _, name := range names {
		_, err := volumeAPI.ActionSnapshotDelete(volume, &longhornclient.SnapshotInput{Name: name})
		if err != nil {
			return err
		}
		job.logger.WithField("volume", volume.Name).Debugf("Deleted snapshot %v", name)
	}
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
			job.logger.WithField("volume", volume.Name).Debug("Purged snapshots")
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

func (job *Job) listSnapshotNamesToCleanup(snapshots []longhornclient.Snapshot, backupDone bool) []string {
	switch job.task {
	case longhorn.RecurringJobTypeSnapshotDelete:
		return job.filterExpiredSnapshots(snapshots)
	case longhorn.RecurringJobTypeSnapshotCleanup:
		return []string{}
	default:
		return job.filterExpiredSnapshotsOfCurrentRecurringJob(snapshots, backupDone)
	}
}

func (job *Job) filterExpiredSnapshotsOfCurrentRecurringJob(snapshots []longhornclient.Snapshot, backupDone bool) []string {
	jobLabel, found := job.labels[types.RecurringJobLabel]
	if !found {
		return []string{}
	}

	// Only consider deleting the snapshots that were created by our current job
	snapshots = filterSnapshotsWithLabel(snapshots, types.RecurringJobLabel, jobLabel)

	if job.task == longhorn.RecurringJobTypeSnapshot || job.task == longhorn.RecurringJobTypeSnapshotForceCreate {
		return filterExpiredItems(snapshotsToNameWithTimestamps(snapshots), job.retain)
	}

	// For the recurring backup job, only keep the snapshot of the last backup and the current snapshot
	retainingSnapshots := map[string]struct{}{job.snapshotName: {}}
	if !backupDone {
		lastBackup, err := job.getLastBackup()
		if err == nil && lastBackup != nil {
			retainingSnapshots[lastBackup.SnapshotName] = struct{}{}
		}
	}
	return snapshotsToNames(filterSnapshotsNotInTargets(snapshots, retainingSnapshots))
}

func (job *Job) filterExpiredSnapshots(snapshots []longhornclient.Snapshot) []string {
	return filterExpiredItems(snapshotsToNameWithTimestamps(snapshots), job.retain)
}

func (job *Job) doRecurringBackup() (err error) {
	defer func() {
		if err == nil {
			job.logger.Info("Finished recurring backup")
		}
	}()
	backupAPI := job.api.BackupVolume
	volumeAPI := job.api.Volume
	snapshot := job.snapshotName
	volumeName := job.volumeName

	defer func() {
		err = errors.Wrapf(err, "failed to complete backupAndCleanup for %v", volumeName)
	}()

	volume, err := volumeAPI.ById(volumeName)
	if err != nil {
		return errors.Wrapf(err, "could not get volume %v", volumeName)
	}

	if err := job.doSnapshot(); err != nil {
		return err
	}

	if _, err := volumeAPI.ActionSnapshotBackup(volume, &longhornclient.SnapshotInput{
		Labels: job.labels,
		Name:   snapshot,
	}); err != nil {
		return err
	}

	if err := job.waitForBackupProcessStart(BackupProcessStartTimeout); err != nil {
		return err
	}

	// Wait for backup creation complete
	for {
		volume, err := volumeAPI.ById(volumeName)
		if err != nil {
			return err
		}

		hasRunningController := false // controller is the engine process
		for _, controller := range volume.Controllers {
			if controller.Running {
				hasRunningController = true
				break
			}
		}
		if !hasRunningController {
			return fmt.Errorf("the volume %v is detached on the middle of the backup process", volumeName)
		}

		var info *longhornclient.BackupStatus
		for _, status := range volume.BackupStatus {
			if status.Snapshot == snapshot {
				info = &status
				break
			}
		}

		if info == nil {
			return fmt.Errorf("cannot find the status of the backup for snapshot %v. It might because the engine has restarted", snapshot)
		}

		complete := false

		switch info.State {
		case string(longhorn.BackupStateCompleted):
			complete = true
			job.logger.Debugf("Complete creating backup %v", info.Id)
		case string(longhorn.BackupStateNew), string(longhorn.BackupStateInProgress):
			job.logger.Debugf("Creating backup %v, current progress %v", info.Id, info.Progress)
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
			job.logger.WithError(err).Warnf("Created backup successfully but errored on cleanup for %v", volumeName)
			err = nil
		}
	}()

	backupVolume, err := backupAPI.ById(volumeName)
	if err != nil {
		return err
	}
	backups, err := backupAPI.ActionBackupList(backupVolume)
	if err != nil {
		return err
	}
	cleanupBackups := job.listBackupsForCleanup(backups.Data)
	for _, backup := range cleanupBackups {
		if _, err := backupAPI.ActionBackupDelete(backupVolume, &longhornclient.BackupInput{
			Name: backup,
		}); err != nil {
			return fmt.Errorf("cleaned up backup %v failed for %v: %v", backup, volumeName, err)
		}
		job.logger.Debugf("Cleaned up backup %v for %v", backup, volumeName)
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

// waitForVolumeState timeout in second
func (job *Job) waitForVolumeState(state string, timeout int) (*longhornclient.Volume, error) {
	volumeAPI := job.api.Volume
	volumeName := job.volumeName

	for i := 0; i < timeout; i++ {
		volume, err := volumeAPI.ById(volumeName)
		if err == nil {
			if volume.State == state {
				return volume, nil
			}
		}
		time.Sleep(1 * time.Second)
	}

	return nil, fmt.Errorf("timeout waiting for volume %v to be in state %v", volumeName, state)
}

// findARandomReadyNode return a random ready node to attach the volume
// return error if there is no ready node
func (job *Job) findARandomReadyNode(v *longhornclient.Volume) (string, error) {
	nodeCollection, err := job.api.Node.List(longhornclient.NewListOpts())
	if err != nil {
		return "", err
	}

	engineImage, err := job.GetEngineImage(types.GetEngineImageChecksumName(v.CurrentImage))
	if err != nil {
		return "", err
	}
	if engineImage.Status.State != longhorn.EngineImageStateDeployed && engineImage.Status.State != longhorn.EngineImageStateDeploying {
		return "", fmt.Errorf("error: the volume's engine image %v is in state: %v", engineImage.Name, engineImage.Status.State)
	}

	var readyNodeList []string
	for _, node := range nodeCollection.Data {
		var readyConditionInterface map[string]interface{}
		for i := range node.Conditions {
			con := node.Conditions[i].(map[string]interface{})
			if con["type"] == longhorn.NodeConditionTypeReady {
				readyConditionInterface = con
			}
		}

		if readyConditionInterface != nil {
			// convert the interface to json
			jsonString, err := json.Marshal(readyConditionInterface)
			if err != nil {
				return "", err
			}

			readyCondition := longhorn.Condition{}

			if err := json.Unmarshal(jsonString, &readyCondition); err != nil {
				return "", err
			}

			if readyCondition.Status == longhorn.ConditionStatusTrue && engineImage.Status.NodeDeploymentMap[node.Name] {
				readyNodeList = append(readyNodeList, node.Name)
			}
		}
	}

	if len(readyNodeList) == 0 {
		return "", fmt.Errorf("cannot find a ready node")
	}
	return readyNodeList[rand.Intn(len(readyNodeList))], nil
}

func filterSnapshots(snapshots []longhornclient.Snapshot, predicate func(snapshot longhornclient.Snapshot) bool) []longhornclient.Snapshot {
	filtered := []longhornclient.Snapshot{}
	for _, snapshot := range snapshots {
		if predicate(snapshot) {
			filtered = append(filtered, snapshot)
		}
	}
	return filtered
}

// filterSnapshotsWithLabel return snapshots that have LabelKey and LabelValue
func filterSnapshotsWithLabel(snapshots []longhornclient.Snapshot, labelKey, labelValue string) []longhornclient.Snapshot {
	return filterSnapshots(snapshots, func(snapshot longhornclient.Snapshot) bool {
		snapshotLabelValue, found := snapshot.Labels[labelKey]
		return found && labelValue == snapshotLabelValue
	})
}

// filterSnapshotsNotInTargets returns snapshots that are not in the Targets
func filterSnapshotsNotInTargets(snapshots []longhornclient.Snapshot, targets map[string]struct{}) []longhornclient.Snapshot {
	return filterSnapshots(snapshots, func(snapshot longhornclient.Snapshot) bool {
		if _, ok := targets[snapshot.Name]; !ok {
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

func snapshotsToNameWithTimestamps(snapshots []longhornclient.Snapshot) []NameWithTimestamp {
	result := []NameWithTimestamp{}
	for _, snapshot := range snapshots {
		if snapshot.Name == etypes.VolumeHeadName {
			continue
		}

		t, err := time.Parse(time.RFC3339, snapshot.Created)
		if err != nil {
			logrus.Errorf("Failed to parse datetime %v for snapshot %v",
				snapshot.Created, snapshot.Name)
			continue
		}
		result = append(result, NameWithTimestamp{
			Name:      snapshot.Name,
			Timestamp: t,
		})
	}
	return result
}

func snapshotsToNames(snapshots []longhornclient.Snapshot) []string {
	result := []string{}
	for _, snapshot := range snapshots {
		result = append(result, snapshot.Name)
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
			logger.Debugf("Bypassed to create job for %v volume during restoring from the backup", volume.Name)
			continue
		}
		if volume.Status.Robustness != longhorn.VolumeRobustnessFaulted &&
			(volume.Status.State == longhorn.VolumeStateAttached || allowDetached) {
			*filterNames = append(*filterNames, volume.Name)
			continue
		}
		logger.Debugf("Cannot create job for %v volume in state %v", volume.Name, volume.Status.State)
	}
}

func getVolumesBySelector(recurringJobType, recurringJobName, namespace string, client *lhclientset.Clientset) ([]longhorn.Volume, error) {
	logger := logrus.StandardLogger()

	label := fmt.Sprintf("%s=%s",
		types.GetRecurringJobLabelKey(recurringJobType, recurringJobName), types.LonghornLabelValueEnabled)
	logger.Debugf("Get volumes from label %v", label)

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
		return nil, errors.Wrap(err, "unable to get client config")
	}
	return lhclientset.NewForConfig(config)
}
