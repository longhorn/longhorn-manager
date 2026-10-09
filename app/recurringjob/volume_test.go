package recurringjob

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const testVolumeJob = "snapshot-cleanup"

// volumeOperationsWithGetError is a VolumeOperations whose ById always fails,
// so the real startVolumeJob worker path returns an error.
type volumeOperationsWithGetError struct {
	longhornclient.VolumeOperations
	err error
}

func (o *volumeOperationsWithGetError) ById(string) (*longhornclient.Volume, error) {
	return nil, o.err
}

// volumeOperationsRebuilding is a VolumeOperations whose ById succeeds and
// returns a volume with an in-progress replica rebuild. ActionSnapshotPurge
// fails the test if called, since a purge must not be attempted while
// rebuilding.
//type volumeOperationsRebuilding struct {
//	longhornclient.VolumeOperations
//	t                 *testing.T
//	volumes           map[string]*longhornclient.Volume
//	replicaRebuilding bool
//}
//
//func (o *volumeOperationsRebuilding) ById(id string) (*longhornclient.Volume, error) {
//	return o.volumes[id], nil
//}
//
//func (o *volumeOperationsRebuilding) ActionSnapshotCRList(*longhornclient.Volume) (*longhornclient.SnapshotCRListOutput, error) {
//	return &longhornclient.SnapshotCRListOutput{}, nil
//}
//
//func (o *volumeOperationsRebuilding) ActionSnapshotPurge(v *longhornclient.Volume) (*longhornclient.Volume, error) {
//	if o.replicaRebuilding {
//		o.t.Error("ActionSnapshotPurge must not be called while a replica is rebuilding")
//		return nil, nil
//	}
//	return v, nil
//}

// fakeVolumeOperations is a configurable VolumeOperations for recurring-job tests.
// Embedding the interface leaves every method we don't override unimplemented, so
// an unexpected call panics instead of silently passing.
type fakeVolumeOperations struct {
	longhornclient.VolumeOperations

	t *testing.T

	volumes map[string]*longhornclient.Volume

	// ActionSnapshotCRList / ActionSnapshotCRDelete.
	snapshots []longhornclient.SnapshotCR
	listErr   error
	deleteErr error

	// ActionSnapshotPurge: fails the test if called while a replica is rebuilding.
	replicaRebuilding bool

	// Recorded calls. Guarded by mu because the job runner can call concurrently.
	mu         sync.Mutex
	deleted    []string
	purgeCalls int
}

func (o *fakeVolumeOperations) ById(id string) (*longhornclient.Volume, error) {
	return o.volumes[id], nil
}

func (o *fakeVolumeOperations) ActionSnapshotCRList(*longhornclient.Volume) (*longhornclient.SnapshotCRListOutput, error) {
	if o.listErr != nil {
		return nil, o.listErr
	}
	return &longhornclient.SnapshotCRListOutput{Data: o.snapshots}, nil
}

func (o *fakeVolumeOperations) ActionSnapshotCRDelete(_ *longhornclient.Volume, input *longhornclient.SnapshotCRInput) (*longhornclient.Empty, error) {
	if o.deleteErr != nil {
		return nil, o.deleteErr
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.deleted = append(o.deleted, input.Name)
	return &longhornclient.Empty{}, nil
}

func (o *fakeVolumeOperations) ActionSnapshotPurge(v *longhornclient.Volume) (*longhornclient.Volume, error) {
	o.mu.Lock()
	o.purgeCalls++
	o.mu.Unlock()

	if o.replicaRebuilding {
		o.t.Error("ActionSnapshotPurge must not be called while a replica is rebuilding")
		return nil, nil
	}
	return v, nil
}

// newAttachedRecurringVolume builds a healthy, attached volume carrying the
// recurring-job label so getVolumesBySelector/filterVolumesForJob select it.
func newAttachedRecurringVolume(name, jobName string) *longhorn.Volume {
	return &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Labels: map[string]string{
				types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJob, jobName): types.LonghornLabelValueEnabled,
			},
		},
		Status: longhorn.VolumeStatus{
			State:      longhorn.VolumeStateAttached,
			Robustness: longhorn.VolumeRobustnessHealthy,
		},
	}
}

// TestStartVolumeJobsSkipsFailedVolumes verifies that a per-volume failure (for
// example, a rebuilding replica blocking snapshot purge) does not abort the
// whole sweep (longhorn/longhorn#13623): every volume is still attempted and
// each failure is surfaced in the logs. It also verifies that the sweep still
// returns a non-nil error so the process exits non-zero and Kubernetes applies
// the Job's OnFailure/backoff retry (longhorn/longhorn#13587).
func TestStartVolumeJobsSkipsFailedVolumes(t *testing.T) {
	assert := assert.New(t)

	allowDetached := &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(types.SettingNameAllowRecurringJobWhileVolumeDetached),
			Namespace: testNamespace,
		},
		Value: "true",
	}
	volA := newAttachedRecurringVolume("vol-a", testVolumeJob)
	volFail := newAttachedRecurringVolume("vol-fail", testVolumeJob)
	volC := newAttachedRecurringVolume("vol-c", testVolumeJob)

	lhClient := lhfake.NewSimpleClientset( // nolint: staticcheck
		runtime.Object(allowDetached), volA, volFail, volC)

	var logBuf bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&logBuf)

	job := &Job{
		lhClient:      lhClient,
		logger:        logger,
		eventRecorder: record.NewFakeRecorder(10),
		name:          testVolumeJob,
		namespace:     testNamespace,
		task:          longhorn.RecurringJobTypeSnapshotCleanup,
	}
	recurringJob := &longhorn.RecurringJob{
		ObjectMeta: metav1.ObjectMeta{Name: testVolumeJob, Namespace: testNamespace},
		Spec:       longhorn.RecurringJobSpec{Concurrency: 2},
	}

	// The engine's rebuild rejection, as it reaches startVolumeJob.
	rebuildErr := errors.New("cannot purge snapshots because tcp://10.0.0.1:10000 is rebuilding")

	var mu sync.Mutex
	attempted := []string{}
	startJob := func(_ *Job, _ *longhorn.RecurringJob, volumeName string, concurrentLimiter chan struct{}, _ []string) error {
		concurrentLimiter <- struct{}{}
		defer func() { <-concurrentLimiter }()

		mu.Lock()
		attempted = append(attempted, volumeName)
		mu.Unlock()

		if volumeName == "vol-fail" {
			return rebuildErr
		}
		return nil
	}

	err := startVolumeJobs(job, recurringJob, startJob)

	// A per-volume failure must not abort the sweep, but it must still be
	// propagated so the CronJob Job exits non-zero and Kubernetes can retry.
	assert.Error(err, "a failing volume must surface an error from the sweep")
	assert.ErrorIs(err, rebuildErr)

	sort.Strings(attempted)
	assert.Equal([]string{"vol-a", "vol-c", "vol-fail"}, attempted,
		"every selected volume must be attempted, including those after the failing one")

	// The failure is not silently swallowed: it is logged. The per-volume event is
	// intentionally left to the task-specific handlers to avoid duplicate events.
	logOutput := logBuf.String()
	assert.Contains(logOutput, "vol-fail")
	assert.Contains(logOutput, "Failed to run recurring job for volume")
}

const (
	namespace  = "longhorn-system"
	jobName    = "daily-backup"
	volumeName = "test-volume"
)

func newSetting(value string) *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(types.SettingNameAllowRecurringJobWhileVolumeDetached),
			Namespace: namespace,
		},
		Value: value,
	}
}

// TestStartVolumeJobs exercises StartVolumeJobs through the real startVolumeJob
// worker (not the seam), covering the new behavior end to end.
func TestStartVolumeJobs(t *testing.T) {
	newJob := func(api *longhornclient.RancherClient, objects ...runtime.Object) *Job {
		logger := logrus.New()
		logger.SetOutput(io.Discard)
		return &Job{
			api:           api,
			lhClient:      lhfake.NewSimpleClientset(objects...), // nolint: staticcheck
			logger:        logger,
			eventRecorder: record.NewFakeRecorder(10),
			name:          jobName,
			namespace:     namespace,
			task:          longhorn.RecurringJobTypeSnapshotCleanup,
		}
	}
	recurringJob := &longhorn.RecurringJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: namespace,
		},
		Spec: longhorn.RecurringJobSpec{Concurrency: 1},
	}

	t.Run("propagates a volume worker error", func(t *testing.T) {
		// A per-volume worker failure is logged and propagated so the sweep
		// returns non-nil and the process exits non-zero. Here the real worker
		// path fails at Volume.ById.
		volume := &longhorn.Volume{
			ObjectMeta: metav1.ObjectMeta{
				Name:      volumeName,
				Namespace: namespace,
				Labels:    types.GetRecurringJobLabelValueMap(types.LonghornLabelRecurringJob, jobName),
			},
			Status: longhorn.VolumeStatus{State: longhorn.VolumeStateAttached},
		}
		job := newJob(&longhornclient.RancherClient{
			Volume: &volumeOperationsWithGetError{err: errors.New("volume API unavailable")},
		}, newSetting("false"), volume)

		err := StartVolumeJobs(job, recurringJob)

		assert.Error(t, err, "a per-volume worker error must be propagated")
	})

	t.Run("returns nil with no selected volumes", func(t *testing.T) {
		job := newJob(&longhornclient.RancherClient{}, newSetting("false"))

		err := StartVolumeJobs(job, recurringJob)

		assert.NoError(t, err)
	})

	t.Run("skips a volume with a rebuilding replica without failing the sweep", func(t *testing.T) {
		// a rebuild-in-progress error must be treated as a skip, not a per-volume failure,
		// so it must not cause StartVolumeJobs to return a non-nil error.
		volume := &longhorn.Volume{
			ObjectMeta: metav1.ObjectMeta{
				Name:      volumeName,
				Namespace: namespace,
				Labels:    types.GetRecurringJobLabelValueMap(types.LonghornLabelRecurringJob, jobName),
			},
			Status: longhorn.VolumeStatus{
				State:      longhorn.VolumeStateAttached,
				Robustness: longhorn.VolumeRobustnessDegraded,
			},
		}

		clientVolume := &longhornclient.Volume{
			Name:          volumeName,
			State:         string(longhorn.VolumeStateAttached),
			RebuildStatus: []longhornclient.RebuildStatus{{IsRebuilding: true}},
		}

		job := newJob(&longhornclient.RancherClient{
			Volume: &fakeVolumeOperations{t: t, volumes: map[string]*longhornclient.Volume{volumeName: clientVolume}, replicaRebuilding: true},
		}, newSetting("false"), volume, recurringJob.DeepCopy())

		err := StartVolumeJobs(job, recurringJob)
		assert.NoError(t, err, "a rebuild-related skip must not fail the sweep")

		// skippedVolumes condition should be set to true in recurringJob.Status
		rj, err := job.lhClient.LonghornV1beta2().RecurringJobs(namespace).Get(context.Background(), recurringJob.Name, metav1.GetOptions{})
		if assert.NoError(t, err, "no error expected when getting a recurring job") {
			cond := types.GetCondition(rj.Status.Conditions, longhorn.RecurringJobConditionTypeVolumesSkipped)
			assert.Equal(t, longhorn.ConditionStatusTrue, cond.Status)
			assert.Equal(t, longhorn.RecurringJobConditionReasonReplicaRebuilding, cond.Reason)
			assert.Contains(t, cond.Message, ErrSnapshotPurgeSkipped.Error(), "expected snapshot purge skipped message")
			assert.Contains(t, cond.Message, volume.Name, "condition message should list the skipped volume")
		}
	})

	t.Run("lists every skipped volume in the condition", func(t *testing.T) {
		names := []string{"vol-c", "vol-a", "vol-b"}
		objs, clientVols := seedVolumesForJob(names, recurringJob, true)

		job := newJob(&longhornclient.RancherClient{
			Volume: &fakeVolumeOperations{t: t, volumes: clientVols, replicaRebuilding: true},
		}, objs...)

		err := StartVolumeJobs(job, recurringJob)
		assert.NoError(t, err, "a rebuild-related skip must not fail the sweep")

		rj, getErr := job.lhClient.LonghornV1beta2().RecurringJobs(namespace).
			Get(context.Background(), jobName, metav1.GetOptions{})
		if assert.NoError(t, getErr, "no error expected when getting a recurring job") {
			cond := types.GetCondition(rj.Status.Conditions, longhorn.RecurringJobConditionTypeVolumesSkipped)
			assert.Equal(t, longhorn.ConditionStatusTrue, cond.Status)
			assert.Contains(t, cond.Message, "3 volume(s)")
			assert.Contains(t, cond.Message, "vol-a, vol-b, vol-c")
		}
	})

	t.Run("update Condition[VolumesSkipped] to false if no volume was skipped in current run", func(t *testing.T) {
		names := []string{"vol-c", "vol-a", "vol-b"}

		rjVolumesSkippedTrue := &longhorn.RecurringJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      jobName,
				Namespace: namespace,
			},
			Spec: longhorn.RecurringJobSpec{Concurrency: 1},
			Status: longhorn.RecurringJobStatus{
				Conditions: []longhorn.Condition{
					{
						Type:   longhorn.RecurringJobConditionTypeVolumesSkipped,
						Status: longhorn.ConditionStatusTrue,
						Reason: longhorn.RecurringJobConditionReasonReplicaRebuilding,
					},
				},
			},
		}

		objs, clientVols := seedVolumesForJob(names, rjVolumesSkippedTrue, false)

		job := newJob(&longhornclient.RancherClient{
			Volume: &fakeVolumeOperations{t: t, volumes: clientVols, replicaRebuilding: false},
		}, objs...)

		err := StartVolumeJobs(job, rjVolumesSkippedTrue)
		assert.NoError(t, err, "a rebuild-related skip must not fail the sweep")

		rj, getErr := job.lhClient.LonghornV1beta2().RecurringJobs(namespace).
			Get(context.Background(), jobName, metav1.GetOptions{})
		if assert.NoError(t, getErr, "no error expected when getting a recurring job") {
			cond := types.GetCondition(rj.Status.Conditions, longhorn.RecurringJobConditionTypeVolumesSkipped)
			assert.Equal(t, longhorn.ConditionStatusFalse, cond.Status,
				"expected recurring job status to be 'False' as no volume was skipped during this run")
		}
	})
}

func seedVolumesForJob(names []string, recurringJob *longhorn.RecurringJob, rebuilding bool) ([]runtime.Object, map[string]*longhornclient.Volume) {
	objs := []runtime.Object{newSetting("false"), recurringJob.DeepCopy()}
	clientVols := map[string]*longhornclient.Volume{}
	for _, n := range names {
		objs = append(objs, &longhorn.Volume{
			ObjectMeta: metav1.ObjectMeta{
				Name:      n,
				Namespace: namespace,
				Labels:    types.GetRecurringJobLabelValueMap(types.LonghornLabelRecurringJob, jobName),
			},
			Status: longhorn.VolumeStatus{
				State:      longhorn.VolumeStateAttached,
				Robustness: longhorn.VolumeRobustnessDegraded,
			},
		})
		clientVols[n] = &longhornclient.Volume{
			Name:          n,
			State:         string(longhorn.VolumeStateAttached),
			RebuildStatus: []longhornclient.RebuildStatus{{IsRebuilding: rebuilding}},
		}
	}
	return objs, clientVols
}

func TestDoSnapshotCleanup(t *testing.T) {
	tests := []struct {
		name string

		task        longhorn.RecurringJobType
		policy      longhorn.RecurringJobRetentionPolicy
		retainCount int
		retainAge   time.Duration

		volumeMissing bool
		snapshots     []longhornclient.SnapshotCR
		listErr       error
		deleteErr     error

		wantDeleted []string
		wantPurge   bool
		wantErr     bool
	}{
		{
			name:        "snapshot-delete retain 0 skips group members and deletes the rest",
			task:        longhorn.RecurringJobTypeSnapshotDelete,
			policy:      longhorn.RecurringJobRetentionPolicyCountBased,
			retainCount: 0,
			snapshots: []longhornclient.SnapshotCR{
				newSnapshotCRForCleanup("plain-old", 3*time.Hour, ""),
				newSnapshotCRForCleanup("member", 2*time.Hour, "group-a"),
				newSnapshotCRForCleanup("plain-new", 1*time.Hour, ""),
			},
			wantDeleted: []string{"plain-old", "plain-new"},
		},
		{
			name:        "snapshot-delete count-based: group members do not consume a retain slot",
			task:        longhorn.RecurringJobTypeSnapshotDelete,
			policy:      longhorn.RecurringJobRetentionPolicyCountBased,
			retainCount: 1,
			snapshots: []longhornclient.SnapshotCR{
				newSnapshotCRForCleanup("plain-old", 3*time.Hour, ""),
				newSnapshotCRForCleanup("member", 2*time.Hour, "group-a"),
				newSnapshotCRForCleanup("plain-new", 1*time.Hour, ""),
			},
			// Without the exclusion, "member" would be a candidate and also be deleted.
			wantDeleted: []string{"plain-old"},
		},
		{
			name:      "snapshot-delete age-based: expired group member is not deleted",
			task:      longhorn.RecurringJobTypeSnapshotDelete,
			policy:    longhorn.RecurringJobRetentionPolicyAgeBased,
			retainAge: 24 * time.Hour,
			snapshots: []longhornclient.SnapshotCR{
				newSnapshotCRForCleanup("plain-expired", 48*time.Hour, ""),
				newSnapshotCRForCleanup("member-expired", 48*time.Hour, "group-a"),
				newSnapshotCRForCleanup("plain-fresh", 1*time.Hour, ""),
			},
			wantDeleted: []string{"plain-expired"},
		},
		{
			name:        "snapshot-delete retain 0 with only group members deletes nothing",
			task:        longhorn.RecurringJobTypeSnapshotDelete,
			policy:      longhorn.RecurringJobRetentionPolicyCountBased,
			retainCount: 0,
			snapshots: []longhornclient.SnapshotCR{
				newSnapshotCRForCleanup("member-1", 2*time.Hour, "group-a"),
				newSnapshotCRForCleanup("member-2", 1*time.Hour, "group-b"),
			},
			wantDeleted: nil,
		},
		{
			name:   "snapshot-cleanup deletes no snapshot CRs and purges",
			task:   longhorn.RecurringJobTypeSnapshotCleanup,
			policy: longhorn.RecurringJobRetentionPolicyCountBased,
			snapshots: []longhornclient.SnapshotCR{
				newSnapshotCRForCleanup("plain", 2*time.Hour, ""),
				newSnapshotCRForCleanup("member", 1*time.Hour, "group-a"),
			},
			wantDeleted: nil,
			wantPurge:   true,
		},
		{
			name:          "missing volume is skipped without error",
			task:          longhorn.RecurringJobTypeSnapshotDelete,
			policy:        longhorn.RecurringJobRetentionPolicyCountBased,
			volumeMissing: true,
			wantDeleted:   nil,
		},
		{
			name:    "snapshot CR list error is returned and nothing is deleted",
			task:    longhorn.RecurringJobTypeSnapshotDelete,
			policy:  longhorn.RecurringJobRetentionPolicyCountBased,
			listErr: errors.New("list failed"),
			wantErr: true,
		},
		{
			name:        "snapshot CR delete error is returned",
			task:        longhorn.RecurringJobTypeSnapshotDelete,
			policy:      longhorn.RecurringJobRetentionPolicyCountBased,
			retainCount: 0,
			snapshots: []longhornclient.SnapshotCR{
				newSnapshotCRForCleanup("plain", 1*time.Hour, ""),
			},
			deleteErr: errors.New("delete failed"),
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ops := &fakeVolumeOperations{
				t:         t,
				snapshots: tc.snapshots,
				listErr:   tc.listErr,
				deleteErr: tc.deleteErr,
				volumes:   map[string]*longhornclient.Volume{},
			}
			if !tc.volumeMissing {
				ops.volumes[volumeName] = &longhornclient.Volume{
					Name:  volumeName,
					State: string(longhorn.VolumeStateAttached),
				}
			}

			logger := logrus.New()
			logger.SetOutput(io.Discard)

			// ADAPT: construct VolumeJob with the real field names. Only the fields
			// doSnapshotCleanup and the retention filters read are needed.
			job := &VolumeJob{
				Job: &Job{
					api:             &longhornclient.RancherClient{Volume: ops},
					lhClient:        lhfake.NewClientset(),
					logger:          logger,
					eventRecorder:   record.NewFakeRecorder(10),
					name:            jobName,
					namespace:       namespace,
					task:            tc.task,
					retainCount:     tc.retainCount,
					retainAge:       tc.retainAge,
					retentionPolicy: tc.policy,
				},
				volumeName: volumeName,
				logger:     logger.WithField("volume", volumeName),
			}

			err := job.doSnapshotCleanup(false)

			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.ElementsMatch(t, tc.wantDeleted, ops.deleted)
			assert.Equal(t, tc.wantPurge, ops.purgeCalls > 0, "unexpected purge behavior")
		})
	}
}

// newSnapshotCRForCleanup builds a ready snapshot CR created `age` ago. A
// non-empty group stamps the SnapshotGroup ownership label.
func newSnapshotCRForCleanup(name string, age time.Duration, group string) longhornclient.SnapshotCR {
	created := time.Now().Add(-age).UTC().Format(time.RFC3339)
	snapshotCR := longhornclient.SnapshotCR{
		Name:           name,
		Volume:         volumeName,
		ReadyToUse:     true,
		CreationTime:   created,
		CrCreationTime: created,
		SnapshotGroup:  group,
	}
	return snapshotCR
}
