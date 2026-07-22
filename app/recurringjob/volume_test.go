package recurringjob

import (
	"errors"
	"io"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/runtime"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

type volumeOperationsWithGetError struct {
	longhornclient.VolumeOperations
	err error
}

func (o *volumeOperationsWithGetError) ById(string) (*longhornclient.Volume, error) {
	return nil, o.err
}

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
			api:       api,
			lhClient:  lhfake.NewSimpleClientset(objects...), // nolint: staticcheck
			logger:    logger,
			name:      jobName,
			namespace: namespace,
		}
	}
	recurringJob := &longhorn.RecurringJob{
		Spec: longhorn.RecurringJobSpec{Concurrency: 1},
	}

	t.Run("returns a volume worker error", func(t *testing.T) {
		expectedErr := errors.New("volume API unavailable")
		volume := &longhorn.Volume{
			ObjectMeta: metav1.ObjectMeta{
				Name:      volumeName,
				Namespace: namespace,
				Labels:    types.GetRecurringJobLabelValueMap(types.LonghornLabelRecurringJob, jobName),
			},
			Status: longhorn.VolumeStatus{State: longhorn.VolumeStateAttached},
		}
		job := newJob(&longhornclient.RancherClient{
			Volume: &volumeOperationsWithGetError{err: expectedErr},
		}, newSetting(), volume)

		err := StartVolumeJobs(job, recurringJob)

		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("returns nil with no selected volumes", func(t *testing.T) {
		job := newJob(&longhornclient.RancherClient{}, newSetting())

		err := StartVolumeJobs(job, recurringJob)

		assert.NoError(t, err)
	})
}
