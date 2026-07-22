package app

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/runtime"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func TestGetRecurringJob(t *testing.T) {
	const (
		namespace = "longhorn-system"
		jobName   = "daily-backup"
	)

	t.Run("returns API errors", func(t *testing.T) {
		expectedErr := errors.New("Kubernetes API unavailable")
		client := lhfake.NewSimpleClientset() // nolint: staticcheck
		client.PrependReactor("get", "recurringjobs", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, expectedErr
		})

		job, err := getRecurringJob(client.LonghornV1beta2().RecurringJobs(namespace), jobName)

		assert.Nil(t, job)
		assert.ErrorIs(t, err, expectedErr)
		assert.ErrorContains(t, err, "failed to get recurring job daily-backup")
	})

	t.Run("returns a missing recurring job error", func(t *testing.T) {
		client := lhfake.NewSimpleClientset() // nolint: staticcheck

		job, err := getRecurringJob(client.LonghornV1beta2().RecurringJobs(namespace), jobName)

		assert.Nil(t, job)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "failed to get recurring job daily-backup")
	})

	t.Run("returns an existing recurring job", func(t *testing.T) {
		expected := &longhorn.RecurringJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      jobName,
				Namespace: namespace,
			},
		}
		client := lhfake.NewSimpleClientset(expected) // nolint: staticcheck

		job, err := getRecurringJob(client.LonghornV1beta2().RecurringJobs(namespace), jobName)

		assert.NoError(t, err)
		assert.Equal(t, expected, job)
	})
}
