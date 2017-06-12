package manager

import (
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/robfig/cron"

	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

const (
	LabelRecurringJob = "RecurringJob"
)

func (v *ManagedVolume) updateRecurringJobs() error {
	if !reflect.DeepEqual(v.RecurringJobs, v.recurringJobScheduled) {
		if err := v.updateRecurringJobScheduled(v.RecurringJobs); err != nil {
			return err
		}
	}
	return nil
}

func (v *ManagedVolume) updateRecurringJobScheduled(jobs []types.RecurringJob) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to update recurring jobs: %+v", jobs)
	}()

	if jobs == nil {
		return fmt.Errorf("BUG: update nil recurring job")
	}

	newCron := cron.New()
	for _, job := range jobs {
		cronJob := v.newCronJob(&job)
		if err := newCron.AddJob(job.Cron, cronJob); err != nil {
			return err
		}
	}
	v.recurringJobScheduled = jobs
	if v.recurringCron != nil {
		v.recurringCron.Stop()
	}
	v.recurringCron = newCron
	v.recurringCron.Start()
	return nil
}

func (v *ManagedVolume) newCronJob(job *types.RecurringJob) *CronJob {
	cronJob := &CronJob{
		RecurringJob: *job,
		volumeName:   v.Name,
		m:            v.m,
	}
	return cronJob
}

func (job *CronJob) Run() {
	var err error

	defer func() {
		if err != nil {
			logrus.Errorf("Fail to run cronjob %+v: %v", job, err)
		}
	}()
	v, err := job.m.getManagedVolume(job.volumeName, false)
	if err != nil {
		return
	}
	if v.Controller == nil {
		err = fmt.Errorf("Volume %v is not running, cannot perform cron job", v.Name)
		return
	}
	engine, err := v.GetEngineClient()
	if err != nil {
		return
	}
	switch job.Type {
	case types.RecurringJobTypeSnapshot:
		err = job.snapshotAndCleanup(engine)
	case types.RecurringJobTypeBackup:
		err = job.backupAndCleanup(engine)
	default:
		err = fmt.Errorf("invalid job type %v", job.Type)
	}
	return
}

func (job *CronJob) snapshotAndCleanup(engine engineapi.EngineClient) error {
	snapName := "recurring-" + util.UUID()
	return job.snapshotAndRetain(engine, snapName, job.Retain)
}

func (job *CronJob) snapshotAndRetain(engine engineapi.EngineClient, snapName string, retain int) error {
	labels := map[string]string{LabelRecurringJob: job.Name}
	if _, err := engine.SnapshotCreate(snapName, labels); err != nil {
		return err
	}
	snapshots, err := engine.SnapshotList()
	if err != nil {
		return err
	}
	cleanupSnapshotNames := job.listSnapshotNamesForCleanup(snapshots, retain)
	for _, snapshot := range cleanupSnapshotNames {
		if err := engine.SnapshotDelete(snapshot); err != nil {
			return err
		}
		logrus.Debugf("Cleaned up snapshot %v for %v", snapshot, engine.Name())
	}
	if len(cleanupSnapshotNames) > 0 {
		if err := engine.SnapshotPurge(); err != nil {
			return err
		}
	}
	return nil
}

type StringTime struct {
	Name string
	Time time.Time
}

func (job *CronJob) listSnapshotNamesForCleanup(snapshots map[string]*engineapi.Snapshot, retain int) []string {
	sts := []*StringTime{}

	for _, snapshot := range snapshots {
		if snapshot.Labels[LabelRecurringJob] == job.Name {
			t, err := time.Parse(time.RFC3339, snapshot.Created)
			if err != nil {
				logrus.Errorf("Fail to parse datetime %v for snapshot %v",
					snapshot.Created, snapshot.Name)
				continue
			}
			sts = append(sts, &StringTime{
				Name: snapshot.Name,
				Time: t,
			})
		}
	}
	return job.getCleanupList(sts, retain)
}

func (job *CronJob) getCleanupList(sts []*StringTime, retain int) []string {
	sort.Slice(sts, func(i, j int) bool {
		if sts[i].Time.Before(sts[j].Time) {
			return true
		}
		return false
	})

	ret := []string{}
	for i := 0; i < len(sts)-retain; i++ {
		ret = append(ret, sts[i].Name)
	}
	return ret
}

func (job *CronJob) backupAndCleanup(engine engineapi.EngineClient) error {
	snapName := "recurring-" + util.UUID()
	// need to retain most recent recurring snapshot, since we may
	// need the last one to backup
	if err := job.snapshotAndRetain(engine, snapName, 1); err != nil {
		return err
	}
	settings, err := job.m.SettingsGet()
	if err != nil {
		return err
	}
	backupTarget := settings.BackupTarget

	labels := map[string]string{LabelRecurringJob: job.Name}
	if err := engine.SnapshotBackup(snapName, backupTarget, labels); err != nil {
		return err
	}
	bt := engineapi.NewBackupTarget(backupTarget)
	backups, err := bt.List(engine.Name())
	if err != nil {
		return err
	}
	cleanupBackupURLs := job.listBackupURLsForCleanup(backups, job.Retain)
	for _, url := range cleanupBackupURLs {
		if err := bt.Delete(url); err != nil {
			return fmt.Errorf("Cleaned up backup %v failed for %v: %v", url, engine.Name(), err)
		}
		logrus.Debugf("Cleaned up backup %v for %v", url, engine.Name())
	}
	return nil
}

func (job *CronJob) listBackupURLsForCleanup(backups []*engineapi.Backup, retain int) []string {
	sts := []*StringTime{}

	for _, backup := range backups {
		if backup.Labels[LabelRecurringJob] == job.Name {
			t, err := time.Parse(time.RFC3339, backup.Created)
			if err != nil {
				logrus.Errorf("Fail to parse datetime %v for backup %v",
					backup.Created, backup.URL)
				continue
			}
			sts = append(sts, &StringTime{
				Name: backup.URL,
				Time: t,
			})
		}
	}
	return job.getCleanupList(sts, retain)
}
