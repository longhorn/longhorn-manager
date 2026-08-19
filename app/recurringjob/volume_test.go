package recurringjob

import (
	"bytes"
	"errors"
	"io"
	"sort"
	"sync"
	"testing"

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

// TestStartVolumeJobsSkipsFailedVolumes verifies the core fix for
// longhorn/longhorn#13623: a per-volume failure (for example, a rebuilding
// replica blocking snapshot purge) must not abort the whole sweep. Every volume
// is still attempted, the run returns nil so the process exits zero and the next
// scheduled run retries, and each failure is surfaced in the logs.
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

	// One volume failing must not fail the run; otherwise the CronJob Job exits
	// non-zero and re-runs every volume from scratch.
	assert.NoError(err, "a single volume's failure must not abort the sweep")

	sort.Strings(attempted)
	assert.Equal([]string{"vol-a", "vol-c", "vol-fail"}, attempted,
		"every selected volume must be attempted, including those after the failing one")

	// The failure is not silently swallowed: it is logged. The per-volume event is
	// intentionally left to the task-specific handlers to avoid duplicate events.
	logOutput := logBuf.String()
	assert.Contains(logOutput, "vol-fail")
	assert.Contains(logOutput, "Failed to run recurring job for volume")
}

// TestStartVolumeJobs exercises StartVolumeJobs through the real startVolumeJob
// worker (not the seam), covering the new behavior end to end.
func TestStartVolumeJobs(t *testing.T) {
	const (
		namespace  = "longhorn-system"
		jobName    = "daily-backup"
		volumeName = "test-volume"
	)

	newSetting := func() *longhorn.Setting {
		return &longhorn.Setting{
			ObjectMeta: metav1.ObjectMeta{
				Name:      string(types.SettingNameAllowRecurringJobWhileVolumeDetached),
				Namespace: namespace,
			},
			Value: "false",
		}
	}
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
		}
	}
	recurringJob := &longhorn.RecurringJob{
		Spec: longhorn.RecurringJobSpec{Concurrency: 1},
	}

	t.Run("swallows a volume worker error", func(t *testing.T) {
		// A per-volume worker failure must not propagate: it is logged and the
		// sweep returns nil so the next run retries. Here the real worker path
		// fails at Volume.ById.
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
		}, newSetting(), volume)

		err := StartVolumeJobs(job, recurringJob)

		assert.NoError(t, err, "a per-volume worker error must not abort the sweep")
	})

	t.Run("returns nil with no selected volumes", func(t *testing.T) {
		job := newJob(&longhornclient.RancherClient{}, newSetting())

		err := StartVolumeJobs(job, recurringJob)

		assert.NoError(t, err)
	})
}
