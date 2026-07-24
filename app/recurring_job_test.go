package app

import (
	"context"
	"errors"
	"net"
	"net/url"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"

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

		job, err := getRecurringJob(context.Background(), client.LonghornV1beta2().RecurringJobs(namespace), jobName)

		assert.Nil(t, job)
		assert.ErrorIs(t, err, expectedErr)
		assert.ErrorContains(t, err, "failed to get recurring job daily-backup")
	})

	t.Run("retries a transient error and returns the job", func(t *testing.T) {
		expected := &longhorn.RecurringJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      jobName,
				Namespace: namespace,
			},
		}
		expectedErr := &url.Error{
			Op:  "Get",
			URL: "https://kubernetes.default.svc",
			Err: &net.DNSError{Err: "i/o timeout", IsTimeout: true},
		}
		attempts := 0
		client := lhfake.NewSimpleClientset(expected) // nolint: staticcheck
		client.PrependReactor("get", "recurringjobs", func(k8stesting.Action) (bool, runtime.Object, error) {
			attempts++
			if attempts < 3 {
				return true, nil, expectedErr
			}
			return false, nil, nil
		})

		job, err := getRecurringJobWithBackoff(context.Background(), client.LonghornV1beta2().RecurringJobs(namespace), jobName, wait.Backoff{Steps: 3})

		assert.NoError(t, err)
		assert.Equal(t, expected, job)
		assert.Equal(t, 3, attempts)
	})

	t.Run("returns the last transient error after retries are exhausted", func(t *testing.T) {
		expectedErr := &url.Error{
			Op:  "Get",
			URL: "https://kubernetes.default.svc",
			Err: &net.DNSError{Err: "i/o timeout", IsTimeout: true},
		}
		attempts := 0
		client := lhfake.NewSimpleClientset() // nolint: staticcheck
		client.PrependReactor("get", "recurringjobs", func(k8stesting.Action) (bool, runtime.Object, error) {
			attempts++
			return true, nil, expectedErr
		})

		job, err := getRecurringJobWithBackoff(context.Background(), client.LonghornV1beta2().RecurringJobs(namespace), jobName, wait.Backoff{Steps: 3})

		assert.Nil(t, job)
		assert.ErrorIs(t, err, expectedErr)
		assert.ErrorContains(t, err, "failed to get recurring job daily-backup")
		assert.Equal(t, 3, attempts)
	})

	t.Run("does not retry a permanent API error", func(t *testing.T) {
		expectedErr := apierrors.NewForbidden(
			schema.GroupResource{Group: "longhorn.io", Resource: "recurringjobs"},
			jobName,
			errors.New("access denied"),
		)
		attempts := 0
		client := lhfake.NewSimpleClientset() // nolint: staticcheck
		client.PrependReactor("get", "recurringjobs", func(k8stesting.Action) (bool, runtime.Object, error) {
			attempts++
			return true, nil, expectedErr
		})

		job, err := getRecurringJobWithBackoff(context.Background(), client.LonghornV1beta2().RecurringJobs(namespace), jobName, wait.Backoff{Steps: 3})

		assert.Nil(t, job)
		assert.ErrorIs(t, err, expectedErr)
		assert.Equal(t, 1, attempts)
	})

	t.Run("returns a missing recurring job error", func(t *testing.T) {
		client := lhfake.NewSimpleClientset() // nolint: staticcheck

		job, err := getRecurringJob(context.Background(), client.LonghornV1beta2().RecurringJobs(namespace), jobName)

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

		job, err := getRecurringJob(context.Background(), client.LonghornV1beta2().RecurringJobs(namespace), jobName)

		assert.NoError(t, err)
		assert.Equal(t, expected, job)
	})
}

func TestIsRetryableRecurringJobGetError(t *testing.T) {
	resource := schema.GroupResource{Group: "longhorn.io", Resource: "recurringjobs"}
	tests := map[string]struct {
		err      error
		expected bool
	}{
		"transport timeout": {
			err:      &url.Error{Op: "Get", URL: "https://kubernetes.default.svc", Err: &net.DNSError{Err: "i/o timeout", IsTimeout: true}},
			expected: true,
		},
		"connection reset": {
			err:      &url.Error{Op: "Get", URL: "https://kubernetes.default.svc", Err: syscall.ECONNRESET},
			expected: true,
		},
		"connection refused": {
			err:      &url.Error{Op: "Get", URL: "https://kubernetes.default.svc", Err: syscall.ECONNREFUSED},
			expected: true,
		},
		"API timeout": {
			err:      apierrors.NewTimeoutError("request timed out", 1),
			expected: true,
		},
		"server timeout": {
			err:      apierrors.NewServerTimeout(resource, "get", 1),
			expected: true,
		},
		"server throttling": {
			err:      apierrors.NewTooManyRequests("slow down", 1),
			expected: true,
		},
		"service unavailable": {
			err:      apierrors.NewServiceUnavailable("unavailable"),
			expected: true,
		},
		"forbidden": {
			err:      apierrors.NewForbidden(resource, "daily-backup", errors.New("access denied")),
			expected: false,
		},
		"unauthorized": {
			err:      apierrors.NewUnauthorized("authentication failed"),
			expected: false,
		},
		"not found": {
			err:      apierrors.NewNotFound(resource, "daily-backup"),
			expected: false,
		},
		"bad request": {
			err:      apierrors.NewBadRequest("invalid request"),
			expected: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.expected, isRetryableRecurringJobGetError(test.err))
		})
	}
}
