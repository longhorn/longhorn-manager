package manager

import (
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/orchestrator"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

func (v *ManagedVolume) registerJob(jobType JobType, assoicateID string, data map[string]string, errCh chan error) (string, error) {
	job := &Job{
		ID:          util.UUID(),
		AssoicateID: assoicateID,
		Type:        jobType,
		State:       JobStateOngoing,
		CreatedAt:   util.Now(),
		Data:        data,
	}

	v.setJob(job)
	go v.waitForJob(job.ID, errCh)
	return job.ID, nil
}

func (v *ManagedVolume) waitForJob(jobID string, errCh chan error) {
	err := <-errCh
	job := v.getJob(jobID)
	updateJob := *job
	updateJob.CompletedAt = util.Now()

	if err != nil {
		updateJob.State = JobStateFailed
		updateJob.Error = err
		logrus.Errorf("job: failed: %v %+v", err, updateJob)
	} else {
		updateJob.State = JobStateSucceed
		logrus.Debugf("Job: successed: %+v", updateJob)
	}
	v.setJob(&updateJob)
	return
}

func (v *ManagedVolume) setJob(job *Job) {
	v.jobsMutex.Lock()
	defer v.jobsMutex.Unlock()

	if v.Jobs[job.ID] == nil {
		logrus.Debugf("Job: created: %+v", job)
	} else {
		logrus.Debugf("Job: updated: %+v", job)
	}
	v.Jobs[job.ID] = job
}

func (v *ManagedVolume) getJob(id string) *Job {
	v.jobsMutex.Lock()
	defer v.jobsMutex.Unlock()

	return v.Jobs[id]
}

// ListJobsInfo provides a copy of job list
func (v *ManagedVolume) ListJobsInfo() map[string]Job {
	v.jobsMutex.Lock()
	defer v.jobsMutex.Unlock()

	result := map[string]Job{}
	for id, job := range v.Jobs {
		result[id] = *job
	}
	return result
}

func (v *ManagedVolume) listJobsByTypeAndAssociateID(jobType JobType, assoicateID string) map[string]*Job {
	v.jobsMutex.Lock()
	defer v.jobsMutex.Unlock()

	result := map[string]*Job{}
	for id, job := range v.Jobs {
		if job.Type == jobType && job.AssoicateID == assoicateID {
			result[id] = job
		}
	}
	return result
}

func (v *ManagedVolume) listOngoingJobsByType(jobType JobType) map[string]*Job {
	v.jobsMutex.Lock()
	defer v.jobsMutex.Unlock()

	result := map[string]*Job{}
	for id, job := range v.Jobs {
		if job.State == JobStateOngoing && job.Type == jobType {
			result[id] = job
		}
	}
	return result
}

func (v *ManagedVolume) listOngoingJobsByTypeAndAssociateID(jobType JobType, assoicateID string) map[string]*Job {
	v.jobsMutex.Lock()
	defer v.jobsMutex.Unlock()

	result := map[string]*Job{}
	for id, job := range v.Jobs {
		if job.State == JobStateOngoing && job.Type == jobType && job.AssoicateID == assoicateID {
			result[id] = job
		}
	}
	return result
}

// following method cannot be called with v.mutex hold

func (v *ManagedVolume) jobReplicaCreate(req *orchestrator.Request) (err error) {
	defer func() {
		errors.Wrap(err, "fail to finish job replica create")
	}()
	instance, err := v.m.orch.CreateReplica(req)
	if err != nil {
		return err
	}

	v.mutex.Lock()
	defer v.mutex.Unlock()

	replica := &types.ReplicaInfo{
		InstanceInfo: types.InstanceInfo{
			NodeID:     req.NodeID,
			IP:         instance.IP,
			Running:    instance.Running,
			VolumeName: v.Name,
			Metadata: types.Metadata{
				Name: instance.Name,
			},
		},
	}

	if err := v.m.ds.CreateVolumeReplica(replica); err != nil {
		return err
	}

	v.setReplica(replica)
	return nil
}

func (v *ManagedVolume) jobReplicaRebuild(req *orchestrator.Request) (err error) {
	defer func() {
		errors.Wrap(err, "fail to finish job replica rebuild")
	}()

	if err := v.jobReplicaCreate(req); err != nil {
		return err
	}

	replicaName := req.Instance

	rURL, err := v.StartReplicaAndGetURL(replicaName)
	if err != nil {
		return err
	}

	engine, err := v.GetEngineClient()
	if err != nil {
		return err
	}

	if err := engine.ReplicaAdd(rURL); err != nil {
		return err
	}

	return nil
}

func (v *ManagedVolume) jobSnapshotPurge() (err error) {
	defer func() {
		errors.Wrap(err, "fail to finish job snapshot purge")
	}()

	engine, err := v.GetEngineClient()
	if err != nil {
		return err
	}

	return engine.SnapshotPurge()
}

func (v *ManagedVolume) jobSnapshotBackup(snapName, backupTarget string) (err error) {
	defer func() {
		errors.Wrap(err, "fail to finish job snapshot backup")
	}()

	engine, err := v.GetEngineClient()
	if err != nil {
		return err
	}

	return engine.SnapshotBackup(snapName, backupTarget, nil)
}
