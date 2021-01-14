package app

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

const (
	FlagSnapshotName = "snapshot-name"
	FlagLabels       = "labels"
	FlagRetain       = "retain"
	FlagBackup       = "backup"

	SnapshotPurgeStatusInterval = 5 * time.Second

	WaitInterval              = 5 * time.Second
	DetachingWaitInterval     = 10 * time.Second
	VolumeAttachTimeout       = 300 // 5 minutes
	BackupProcessStartTimeout = 90  // 1.5 minutes

	jobTypeSnapshot = string("snapshot")
	jobTypeBackup   = string("backup")
)

func SnapshotCmd() cli.Command {
	return cli.Command{
		Name: "snapshot",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  FlagManagerURL,
				Usage: "Longhorn manager API URL",
			},
			cli.StringFlag{
				Name:  FlagSnapshotName,
				Usage: "the base of snapshot name",
			},
			cli.StringSliceFlag{
				Name:  FlagLabels,
				Usage: "specify labels, in the format of `--label key1=value1 --label key2=value2`",
			},
			cli.IntFlag{
				Name:  FlagRetain,
				Usage: "retain number of snapshots with the same label",
			},
			cli.BoolFlag{
				Name:  FlagBackup,
				Usage: "run the job with backup creation and cleanup",
			},
		},
		Action: func(c *cli.Context) {
			if err := snapshot(c); err != nil {
				logrus.Fatalf("Error taking snapshot: %v", err)
			}
		},
	}
}

func snapshot(c *cli.Context) error {
	var err error

	managerURL := c.String(FlagManagerURL)
	if managerURL == "" {
		return fmt.Errorf("require %v", FlagManagerURL)
	}

	if c.NArg() == 0 {
		return errors.New("volume name is required")
	}
	volume := c.Args()[0]
	retain := c.Int(FlagRetain)

	baseName := c.String(FlagSnapshotName)
	if baseName == "" {
		return fmt.Errorf("Missing required parameter --" + FlagSnapshotName)
	}
	// it's designed to call with same parameter for multiple times
	snapshotName := baseName + "-" + util.RandomID()

	labelMap := map[string]string{}
	labels := c.StringSlice(FlagLabels)
	if labels != nil {
		labelMap, err = util.ParseLabels(labels)
		if err != nil {
			return errors.Wrap(err, "cannot parse labels")
		}
	}

	backup := c.Bool(FlagBackup)
	job, err := NewJob(managerURL, volume, snapshotName, labelMap, retain, backup)
	if err != nil {
		return err
	}

	return job.run()
}

type Job struct {
	lhClient     lhclientset.Interface
	namespace    string
	volumeName   string
	snapshotName string
	retain       int
	jobType      string
	labels       map[string]string

	api *longhornclient.RancherClient
}

func NewJob(managerURL, volumeName, snapshotName string, labels map[string]string, retain int, backup bool) (*Job, error) {
	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return nil, fmt.Errorf("Cannot detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get client config")
	}
	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get clientset")
	}

	clientOpts := &longhornclient.ClientOpts{Url: managerURL}
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

	return &Job{
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
		logrus.Warn("Missing RecurringJob label")
		return
	}

	logrus.Infof("Handling volume detachment for volume %v", volumeName)
	for {
		volume, err := volumeAPI.ById(volumeName)
		if err == nil {
			if volume.LastAttachedBy != jobName {
				return
			}
			// !volume.DisableFrontend condition makes sure that volume is detached by this recurring job,
			// not by the auto-reattachment feature.
			if volume.State == string(types.VolumeStateDetached) && !volume.DisableFrontend {
				logrus.Infof("Volume %v is detached", volumeName)
				return
			}
			if volume.State == string(types.VolumeStateAttached) {
				logrus.Infof("Attempting to detach volume %v from all nodes", volumeName)
				if _, err := volumeAPI.ActionDetach(volume, &longhornclient.DetachInput{HostId: ""}); err != nil {
					logrus.Infof("%v ", err)
				}
			}

		} else {
			logrus.Infof("%v ", err)
		}
		time.Sleep(DetachingWaitInterval)
	}
}

func (job *Job) run() (err error) {
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

	if volume.MigrationNodeID != "" {
		return fmt.Errorf("cannot run job for volume %v during migration", volume.Name)
	}

	defer job.handleVolumeDetachment()

	if volume.State != string(types.VolumeStateAttached) && volume.State != string(types.VolumeStateDetached) {
		return fmt.Errorf("volume %v is in an invalid state for recurring job: %v. Volume must be in state Attached or Detached", volumeName, volume.State)
	}

	if volume.State == string(types.VolumeStateDetached) {
		// Find a random ready node to attach the volume
		// For load balancing purpose, the node need to be random
		nodeToAttach, err := job.findARandomReadyNode(volume)
		if err != nil {
			return errors.Wrapf(err, "cannot do auto attaching for volume %v", volumeName)
		}

		// Automatically attach the volume
		// Disable the volume's frontend make sure that pod cannot use the volume during the recurring job.
		// This is necessary so that we can safely detach the volume when finishing the job.
		logrus.Infof("Automatically attach volume %v to node %v", volumeName, nodeToAttach)
		if volume, err = volumeAPI.ActionAttach(volume, &longhornclient.AttachInput{
			DisableFrontend: true,
			HostId:          nodeToAttach,
			AttachedBy:      jobName,
		}); err != nil {
			return err
		}

		volume, err = job.waitForVolumeState(string(types.VolumeStateAttached), VolumeAttachTimeout)
		if err != nil {
			return err
		}
		logrus.Infof("Volume %v is in state %v", volumeName, volume.State)
	}

	if job.jobType == jobTypeBackup {
		logrus.Infof("Running recurring backup for volume %v", volumeName)
		return job.backupAndCleanup()
	}
	logrus.Infof("Running recurring snapshot for volume %v", volumeName)
	return job.snapshotAndCleanup()
}

func (job *Job) snapshotAndCleanup() (err error) {
	volumeAPI := job.api.Volume
	volumeName := job.volumeName

	volume, err := volumeAPI.ById(volumeName)
	if err != nil {
		return errors.Wrapf(err, "could not get volume %v", volumeName)
	}

	shouldDo, err := job.shouldDoRecurringSnapshot(volume)
	if err != nil {
		return err
	}
	if !shouldDo {
		logrus.Infof("Skipping taking snapshot because it is not triggered by recurring backup job AND the volume %v doesn't have new data in volume-head", volumeName)
		return nil
	}

	if _, err := volumeAPI.ActionSnapshotCreate(volume, &longhornclient.SnapshotInput{
		Labels: job.labels,
		Name:   job.snapshotName,
	}); err != nil {
		return err
	}

	defer func() {
		if err != nil {
			logrus.Warnf("created snapshot successfully but errored on cleanup for %v: %v", volumeName, err)
			err = nil
		}
	}()

	collection, err := volumeAPI.ActionSnapshotList(volume)
	if err != nil {
		return err
	}
	cleanupSnapshotNames := job.listSnapshotNamesForCleanup(collection.Data)
	for _, snapshot := range cleanupSnapshotNames {
		if _, err := volumeAPI.ActionSnapshotDelete(volume, &longhornclient.SnapshotInput{
			Name: snapshot,
		}); err != nil {
			return err
		}
		logrus.Debugf("Cleaned up snapshot %v for %v", snapshot, volumeName)
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
						logrus.Warnf("error purging snapshots on replica %v: %v", replica, errMsg)
					}
					logrus.Warnf("encountered one or more errors while purging snapshots")
				}
				return nil
			}
			time.Sleep(SnapshotPurgeStatusInterval)
		}
	}
	return nil
}

// shouldDoRecurringSnapshot return whether we should take a new snapshot in the current running of the job
func (job *Job) shouldDoRecurringSnapshot(volume *longhornclient.Volume) (bool, error) {
	volumeHeadSize, err := job.getVolumeHeadSize(volume)
	if err != nil {
		return false, err
	}

	// If volume-head has new data, we need to take snapshot, we need to take a new snapshot
	if volumeHeadSize > 0 {
		return true, nil
	}

	// The snapshot job is triggered by a recurring backup, we need to take a new snapshot
	if job.jobType == jobTypeBackup {
		return true, nil
	}

	return false, nil
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

func (job *Job) listSnapshotNamesForCleanup(snapshots []longhornclient.Snapshot) []string {
	sts := []*NameWithTimestamp{}

	// only remove snapshots that where created by our current job
	jobLabel, found := job.labels[types.RecurringJobLabel]
	if !found {
		return []string{}
	}
	for _, snapshot := range snapshots {
		snapshotLabel, found := snapshot.Labels[types.RecurringJobLabel]
		if found && jobLabel == snapshotLabel {
			t, err := time.Parse(time.RFC3339, snapshot.Created)
			if err != nil {
				logrus.Errorf("Fail to parse datetime %v for snapshot %v",
					snapshot.Created, snapshot.Name)
				continue
			}
			sts = append(sts, &NameWithTimestamp{
				Name:      snapshot.Name,
				Timestamp: t,
			})
		}
	}
	return job.getCleanupList(sts)
}

func (job *Job) getCleanupList(sts []*NameWithTimestamp) []string {
	sort.Slice(sts, func(i, j int) bool {
		if sts[i].Timestamp.Before(sts[j].Timestamp) {
			return true
		}
		return false
	})

	ret := []string{}
	for i := 0; i < len(sts)-job.retain; i++ {
		ret = append(ret, sts[i].Name)
	}
	return ret
}

func (job *Job) backupAndCleanup() (err error) {
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

	shouldDo, err := job.shouldDoRecurringBackup(volume)
	if err != nil {
		return err
	}
	if !shouldDo {
		logrus.Infof("Skipping taking backup because volume %v is either empty or doesn't have new data since the last backup", volumeName)
		return nil
	}

	if err := job.snapshotAndCleanup(); err != nil {
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
		case engineapi.BackupStateComplete:
			complete = true
			logrus.Debugf("Complete creating backup %v", info.BackupURL)
		case engineapi.BackupStateInProgress:
			logrus.Debugf("Creating backup %v, current progress %v", info.BackupURL, info.Progress)
		case engineapi.BackupStateError:
			return fmt.Errorf("failed to create backup %v: %v", info.BackupURL, info.Error)
		default:
			return fmt.Errorf("invalid state %v for backup %v", info.State, info.BackupURL)
		}

		if complete {
			break
		}
		time.Sleep(WaitInterval)
	}

	defer func() {
		if err != nil {
			logrus.Warnf("created backup successfully but errored on cleanup for %v: %v", volumeName, err)
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
			return fmt.Errorf("Cleaned up backup %v failed for %v: %v", backup, volumeName, err)
		}
		logrus.Debugf("Cleaned up backup %v for %v", backup, volumeName)
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

// shouldDoRecurringBackup return whether the recurring backup should take place
// We should do recurring backup if there is new data since the last backup
func (job *Job) shouldDoRecurringBackup(volume *longhornclient.Volume) (bool, error) {
	volumeHeadSize, err := job.getVolumeHeadSize(volume)
	if err != nil {
		return false, err
	}

	// If volume-head has new data, we need to do backup
	if volumeHeadSize > 0 {
		return true, nil
	}

	lastBackup, err := job.getLastBackup(volume)
	if err != nil {
		return false, err
	}

	lastSnapshot, err := job.getLastSnapshot(volume)
	if err != nil {
		return false, err
	}

	if lastSnapshot == nil {
		return false, nil
	}

	if lastBackup != nil && lastBackup.SnapshotName == lastSnapshot.Name {
		return false, nil
	}

	return true, nil
}

// getLastBackup return the last backup of the volume
// return nil, nil if volume doesn't have any backup
func (job *Job) getLastBackup(volume *longhornclient.Volume) (*longhornclient.Backup, error) {
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
	sts := []*NameWithTimestamp{}

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
				logrus.Errorf("Fail to parse datetime %v for backup %v",
					backup.Created, backup)
				continue
			}
			sts = append(sts, &NameWithTimestamp{
				Name:      backup.Name,
				Timestamp: t,
			})
		}
	}
	return job.getCleanupList(sts)
}

func (job *Job) GetVolume(name string) (*longhorn.Volume, error) {
	return job.lhClient.LonghornV1beta1().Volumes(job.namespace).Get(name, metav1.GetOptions{})
}

func (job *Job) UpdateVolumeStatus(v *longhorn.Volume) (*longhorn.Volume, error) {
	return job.lhClient.LonghornV1beta1().Volumes(job.namespace).UpdateStatus(v)
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
	if engineImage.State != types.EngineImageStateDeployed && engineImage.State != types.EngineImageStateDeploying {
		return "", fmt.Errorf("error: the volume's engine image %v is in state: %v", engineImage.Name, engineImage.State)
	}

	var readyNodeList []string
	for _, node := range nodeCollection.Data {
		if readyConditionInterface, ok := node.Conditions[types.NodeConditionTypeReady]; ok {
			// convert the interface to json
			jsonString, err := json.Marshal(readyConditionInterface)
			if err != nil {
				return "", err
			}

			readyCondition := types.Condition{}

			if err := json.Unmarshal(jsonString, &readyCondition); err != nil {
				return "", err
			}

			if readyCondition.Status == types.ConditionStatusTrue && engineImage.NodeDeploymentMap[node.Name] {
				readyNodeList = append(readyNodeList, node.Name)
			}
		}
	}

	if len(readyNodeList) == 0 {
		return "", fmt.Errorf("cannot find a ready node")
	}
	return readyNodeList[rand.Intn(len(readyNodeList))], nil
}
