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

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

	lhclientset "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

const (
	FlagSnapshotName = "snapshot-name"
	FlagLabels       = "labels"
	FlagRetain       = "retain"
	FlagBackupTarget = "backuptarget"
)

func SnapshotCmd() cli.Command {
	return cli.Command{
		Name: "snapshot",
		Flags: []cli.Flag{
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
			cli.StringFlag{
				Name:  FlagBackupTarget,
				Usage: "backup to destination if supplied, would be url like s3://bucket@region/path/ or vfs:///path/",
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

	backupTarget := c.String(FlagBackupTarget)
	job, err := NewJob(volume, snapshotName, backupTarget, labelMap, retain)
	if err != nil {
		return err
	}
	if backupTarget != "" {
		return job.backupAndCleanup()
	}
	return job.snapshotAndCleanup()
}

type Job struct {
	namespace    string
	volumeName   string
	snapshotName string
	backupTarget string
	retain       int
	labels       map[string]string

	engine      engineapi.EngineClient
	engineImage string
}

func NewJob(volumeName, snapshotName, backupTarget string, labels map[string]string, retain int) (*Job, error) {
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

	v, err := lhClient.LonghornV1alpha1().Volumes(namespace).Get(volumeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if v.Spec.MigrationNodeID != "" {
		return nil, fmt.Errorf("cannot take snapshot for volume %v during migration", v.Name)
	}
	eList, err := lhClient.LonghornV1alpha1().Engines(namespace).List(metav1.ListOptions{
		LabelSelector: datastore.LonghornVolumeKey + "=" + volumeName,
	})
	if err != nil {
		return nil, err
	}
	if len(eList.Items) != 1 {
		return nil, fmt.Errorf("cannot find suitable engine: %+v", eList)
	}
	e := eList.Items[0]
	if e.Status.IP == "" {
		return nil, fmt.Errorf("engine %v is not running, no IP available", e.Name)
	}

	engines := engineapi.EngineCollection{}
	engineImage := e.Status.CurrentImage
	engineClient, err := engines.NewEngineClient(&engineapi.EngineClientRequest{
		VolumeName:  v.Name,
		EngineImage: engineImage,
		IP:          e.Status.IP,
	})
	if err != nil {
		return nil, err
	}
	// must at least retain 1 of course
	if retain == 0 {
		retain = 1
	}
	return &Job{
		namespace:    namespace,
		volumeName:   volumeName,
		snapshotName: snapshotName,
		backupTarget: backupTarget,
		labels:       labels,
		retain:       retain,
		engine:       engineClient,
		engineImage:  engineImage,
	}, nil
}

func (job *Job) snapshotAndCleanup() error {
	engine := job.engine
	if _, err := engine.SnapshotCreate(job.snapshotName, job.labels); err != nil {
		return err
	}
	snapshots, err := job.engine.SnapshotList()
	if err != nil {
		return err
	}
	cleanupSnapshotNames := job.listSnapshotNamesForCleanup(snapshots, job.retain)
	for _, snapshot := range cleanupSnapshotNames {
		if err := job.engine.SnapshotDelete(snapshot); err != nil {
			return err
		}
		logrus.Debugf("Cleaned up snapshot %v for %v", snapshot, job.volumeName)
	}
	if len(cleanupSnapshotNames) > 0 {
		if err := engine.SnapshotPurge(); err != nil {
			return err
		}
	}
	return nil
}

type NameWithTimestamp struct {
	Name      string
	Timestamp time.Time
}

func (job *Job) listSnapshotNamesForCleanup(snapshots map[string]*engineapi.Snapshot, retain int) []string {
	sts := []*NameWithTimestamp{}

	// if no label specified, don't action. We don't want to remove all
	// unlabeled snapshots
	if len(job.labels) == 0 {
		return []string{}
	}
	for _, snapshot := range snapshots {
		matched := true
		for k, v := range job.labels {
			value, ok := snapshot.Labels[k]
			if !ok {
				matched = false
				break
			}
			if v != value {
				matched = false
				break
			}
		}
		if matched {
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

func (job *Job) backupAndCleanup() error {
	if err := job.snapshotAndCleanup(); err != nil {
		return err
	}
	// CronJob template has covered the credential already, so we don't need to get the credential secret.
	if err := job.engine.SnapshotBackup(job.snapshotName, job.backupTarget, job.labels, nil); err != nil {
		return err
	}
	target := engineapi.NewBackupTarget(job.backupTarget, job.engineImage, nil)
	backups, err := target.List(job.volumeName)
	if err != nil {
		return err
	}
	cleanupBackupURLs := job.listBackupURLsForCleanup(backups)
	for _, url := range cleanupBackupURLs {
		if err := target.DeleteBackup(url); err != nil {
			return fmt.Errorf("Cleaned up backup %v failed for %v: %v", url, job.volumeName, err)
		}
		logrus.Debugf("Cleaned up backup %v for %v", url, job.volumeName)
	}
	return nil
}

func (job *Job) listBackupURLsForCleanup(backups []*engineapi.Backup) []string {
	sts := []*NameWithTimestamp{}

	// if no label specified, don't action. We don't want to remove all
	// unlabeled backups
	if len(job.labels) == 0 {
		return []string{}
	}
	for _, backup := range backups {
		matched := true
		for k, v := range job.labels {
			value, ok := backup.Labels[k]
			if !ok {
				matched = false
				break
			}
			if v != value {
				matched = false
				break
			}
		}
		if matched {
			t, err := time.Parse(time.RFC3339, backup.Created)
			if err != nil {
				logrus.Errorf("Fail to parse datetime %v for backup %v",
					backup.Created, backup.URL)
				continue
			}
			sts = append(sts, &NameWithTimestamp{
				Name:      backup.URL,
				Timestamp: t,
			})
		}
	}
	return job.getCleanupList(sts)
}
