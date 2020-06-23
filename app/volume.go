package app

import (
	"fmt"
	"os"
	"sort"
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

	WaitInterval = 5 * time.Second
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
	job, err := NewJob(managerURL, volume, snapshotName, labelMap, retain)
	if err != nil {
		return err
	}
	if backup {
		return job.backupAndCleanup()
	}
	return job.snapshotAndCleanup()
}

type Job struct {
	lhClient     lhclientset.Interface
	namespace    string
	volume       *longhornclient.Volume
	snapshotName string
	retain       int
	labels       map[string]string

	api *longhornclient.RancherClient
}

func NewJob(managerURL, volumeName, snapshotName string, labels map[string]string, retain int) (*Job, error) {
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
	volume, err := apiClient.Volume.ById(volumeName)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get volume %v", volumeName)
	}
	// must at least retain 1 of course
	if retain == 0 {
		retain = 1
	}
	return &Job{
		lhClient:     lhClient,
		namespace:    namespace,
		volume:       volume,
		snapshotName: snapshotName,
		labels:       labels,
		retain:       retain,
		api:          apiClient,
	}, nil
}

func (job *Job) snapshotAndCleanup() (err error) {
	volumeAPI := job.api.Volume
	volume := job.volume
	volumeName := volume.Name
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

type NameWithTimestamp struct {
	Name      string
	Timestamp time.Time
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
	volume := job.volume
	volumeName := volume.Name
	defer func() {
		err = errors.Wrapf(err, "failed to complete backupAndCleanup for %v", volumeName)
	}()

	if err := job.snapshotAndCleanup(); err != nil {
		return err
	}

	if _, err := volumeAPI.ActionSnapshotBackup(volume, &longhornclient.SnapshotInput{
		Labels: job.labels,
		Name:   snapshot,
	}); err != nil {
		return err
	}

	// Wait for backup creation complete
	for {
		volume, err := volumeAPI.ById(volumeName)
		if err != nil {
			return err
		}
		var info *longhornclient.BackupStatus
		for _, status := range volume.BackupStatus {
			if status.Snapshot == snapshot {
				info = &status
				break
			}
		}

		complete := false
		if info != nil {
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
