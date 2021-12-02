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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	"github.com/longhorn/longhorn-manager/engineapi"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

const (
	FlagSnapshotName = "snapshot-name"
	FlagGroups       = "groups"
	FlagLabels       = "labels"
	FlagRetain       = "retain"
	FlagConcurrent   = "concurrent"
	FlagBackup       = "backup"

	HTTPClientTimout = 1 * time.Minute

	SnapshotPurgeStatusInterval = 5 * time.Second

	WaitInterval              = 5 * time.Second
	DetachingWaitInterval     = 10 * time.Second
	VolumeAttachTimeout       = 300 // 5 minutes
	BackupProcessStartTimeout = 90  // 1.5 minutes

	jobTypeSnapshot = string("snapshot")
	jobTypeBackup   = string("backup")
)

type Job struct {
	logger       logrus.FieldLogger
	lhClient     lhclientset.Interface
	namespace    string
	volumeName   string
	snapshotName string
	retain       int
	jobType      string
	labels       map[string]string

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
				logrus.Fatalf("Error taking snapshot: %v", err)
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
	var jobTask string = string(recurringJob.Spec.Task)
	var jobRetain int = recurringJob.Spec.Retain
	var jobConcurrent int = recurringJob.Spec.Concurrency

	jobLabelMap := map[string]string{}
	if recurringJob.Spec.Labels != nil {
		jobLabelMap = recurringJob.Spec.Labels
	}
	jobLabelMap[types.RecurringJobLabel] = recurringJob.Name
	labelJSON, err := json.Marshal(jobLabelMap)
	if err != nil {
		return errors.Wrap(err, "Failed to get JSON encoding for labels")
	}

	var doBackup bool = false
	if jobTask == string(longhorn.RecurringJobTypeBackup) {
		doBackup = true
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

	var wg sync.WaitGroup
	concurrentLimiter := make(chan struct{}, jobConcurrent)
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
				"task":       jobTask,
				"retain":     jobRetain,
				"concurrent": jobConcurrent,
				"groups":     strings.Join(jobGroups, ","),
				"labels":     string(labelJSON),
			})
			log.Info("Creating job")

			snapshotName := types.GetCronJobNameForRecurringJob(jobName) + "-" + util.RandomID()
			job, err := NewJob(
				logger,
				managerURL,
				volumeName,
				snapshotName,
				jobLabelMap,
				jobRetain,
				doBackup)
			if err != nil {
				log.WithError(err).Error("failed to create new job for volume")
				return
			}
			err = job.run()
			if err != nil {
				log.WithError(err).Errorf("failed to run job for volume")
				return
			}

			log.Info("Created job")
		}(volumeName)
	}

	return nil
}

func NewJob(logger logrus.FieldLogger, managerURL, volumeName, snapshotName string, labels map[string]string, retain int, backup bool) (*Job, error) {
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

	// must at least retain 1 of course
	if retain == 0 {
		retain = 1
	}

	jobType := jobTypeSnapshot
	if backup {
		jobType = jobTypeBackup
	}

	logger = logger.WithFields(logrus.Fields{
		"namespace":    namespace,
		"volumeName":   volumeName,
		"snapshotName": snapshotName,
		"labels":       labels,
		"retain":       retain,
		"jobType":      jobType,
	})

	return &Job{
		logger:       logger,
		lhClient:     lhClient,
		namespace:    namespace,
		volumeName:   volumeName,
		snapshotName: snapshotName,
		labels:       labels,
		retain:       retain,
		jobType:      jobType,
		api:          apiClient,
	}, nil
}

// handleVolumeDetachment decides whether the current recurring job should detach the volume.
// It should detach the volume when:
// 1. The volume is attached by this recurring job
// 2. The volume state is VolumeStateAttached
func (job *Job) handleVolumeDetachment() {
	volumeAPI := job.api.Volume
	volumeName := job.volumeName
	jobName, _ := job.labels[types.RecurringJobLabel]
	if jobName == "" {
		job.logger.Warn("Missing RecurringJob label")
		return
	}

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
	jobName, _ := job.labels[types.RecurringJobLabel]
	if jobName == "" {
		return fmt.Errorf("missing RecurringJob label")
	}
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

		// Automatically attach the volume
		// Disable the volume's frontend make sure that pod cannot use the volume during the recurring job.
		// This is necessary so that we can safely detach the volume when finishing the job.
		job.logger.Infof("Automatically attach volume %v to node %v", volumeName, nodeToAttach)
		if volume, err = volumeAPI.ActionAttach(volume, &longhornclient.AttachInput{
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

	if job.jobType == jobTypeBackup {
		job.logger.Infof("Running recurring backup for volume %v", volumeName)
		return job.doRecurringBackup()
	}
	job.logger.Infof("Running recurring snapshot for volume %v", volumeName)
	return job.doRecurringSnapshot()
}

func (job *Job) doRecurringSnapshot() (err error) {
	defer func() {
		if err == nil {
			job.logger.Info("Finish running recurring snapshot")
		}
	}()

	err = job.doSnapshot()
	if err != nil {
		return err
	}

	return job.doSnapshotCleanup(false)
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

	cleanupSnapshotNames := job.listSnapshotNamesForCleanup(collection.Data, backupDone)
	for _, snapshot := range cleanupSnapshotNames {
		if _, err := volumeAPI.ActionSnapshotDelete(volume, &longhornclient.SnapshotInput{
			Name: snapshot,
		}); err != nil {
			return err
		}
		job.logger.Debugf("Cleaned up snapshot %v for %v", snapshot, volumeName)
	}
	if len(cleanupSnapshotNames) > 0 {
		if _, err := volumeAPI.ActionSnapshotPurge(volume); err != nil {
			return err
		}
		for {
			done := true
			errorList := map[string]string{}
			volume, err := volumeAPI.ById(volumeName)
			if err != nil {
				return err
			}

			for _, status := range volume.PurgeStatus {
				if status.IsPurging {
					done = false
					break
				}
				if status.Error != "" {
					errorList[status.Replica] = status.Error
				}
			}
			if done {
				if len(errorList) != 0 {
					for replica, errMsg := range errorList {
						job.logger.Warnf("error purging snapshots on replica %v: %v", replica, errMsg)
					}
					job.logger.Warnf("encountered one or more errors while purging snapshots")
				}
				return nil
			}
			time.Sleep(SnapshotPurgeStatusInterval)
		}
	}
	return nil
}

type NameWithTimestamp struct {
	Name      string
	Timestamp time.Time
}

// getLastSnapshot return the last snapshot of the volume exclude the volume-head
// return nil, nil if volume doesn't have any snapshot other than the volume-head
func (job *Job) getLastSnapshot(volume *longhornclient.Volume) (*longhornclient.Snapshot, error) {
	volumeHead, err := job.api.Volume.ActionSnapshotGet(volume, &longhornclient.SnapshotInput{
		Name: engineapi.VolumeHeadName,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "could not get volume-head for volume %v", job.volumeName)
	}

	if volumeHead.Parent == "" {
		return nil, nil
	}

	lastSnapshot, err := job.api.Volume.ActionSnapshotGet(volume, &longhornclient.SnapshotInput{
		Name: volumeHead.Parent,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "could not get parent of volume-head for volume %v", job.volumeName)
	}

	return lastSnapshot, nil
}

// getVolumeHeadSize return the size of volume-head snapshot
func (job *Job) getVolumeHeadSize(volume *longhornclient.Volume) (int64, error) {
	volumeHead, err := job.api.Volume.ActionSnapshotGet(volume, &longhornclient.SnapshotInput{
		Name: engineapi.VolumeHeadName,
	})
	if err != nil {
		return 0, errors.Wrapf(err, "could not get volume-head for volume %v", job.volumeName)
	}
	volumeHeadSize, err := strconv.ParseInt(volumeHead.Size, 10, 64)
	if err != nil {
		return 0, err
	}

	return volumeHeadSize, nil
}

func (job *Job) listSnapshotNamesForCleanup(snapshots []longhornclient.Snapshot, backupDone bool) []string {
	jobLabel, found := job.labels[types.RecurringJobLabel]
	if !found {
		return []string{}
	}

	// Only consider deleting the snapshots that were created by our current job
	snapshots = filterSnapshotsWithLabel(snapshots, types.RecurringJobLabel, jobLabel)

	if job.jobType == jobTypeSnapshot {
		return filterExpiredItems(snapshotsToNameWithTimestamps(snapshots), job.retain)
	}

	// For the recurring backup job, only keep the snapshot of the last backup and the current snapshot
	retainingSnapshots := map[string]struct{}{job.snapshotName: struct{}{}}
	if !backupDone {
		lastBackup, err := job.getLastBackup()
		if err == nil && lastBackup != nil {
			retainingSnapshots[lastBackup.SnapshotName] = struct{}{}
		}
	}
	return snapshotsToNames(filterSnapshotsNotInTargets(snapshots, retainingSnapshots))
}

func (job *Job) doRecurringBackup() (err error) {
	defer func() {
		if err == nil {
			job.logger.Info("Finish running recurring backup")
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

	if err := job.doSnapshotCleanup(false); err != nil {
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
			job.logger.Warnf("created backup successfully but errored on cleanup for %v: %v", volumeName, err)
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
				job.logger.Errorf("Fail to parse datetime %v for backup %v",
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

	engineImage, err := job.api.EngineImage.ById(types.GetEngineImageChecksumName(v.CurrentImage))
	if err != nil {
		return "", err
	}
	if engineImage.State != string(longhorn.EngineImageStateDeployed) && engineImage.State != string(longhorn.EngineImageStateDeploying) {
		return "", fmt.Errorf("error: the volume's engine image %v is in state: %v", engineImage.Name, engineImage.State)
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

			if readyCondition.Status == longhorn.ConditionStatusTrue && engineImage.NodeDeploymentMap[node.Name] {
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
		t, err := time.Parse(time.RFC3339, snapshot.Created)
		if err != nil {
			logrus.Errorf("Fail to parse datetime %v for snapshot %v",
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
