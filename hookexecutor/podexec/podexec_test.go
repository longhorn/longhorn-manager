package podexec

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestExecKeepsAPIStatusWhenServerRejectsExec rejects the exec request with
// a metav1.Status body, the way a real API server answers Forbidden,
// NotFound, an unreachable kubelet, or a container that is not running. The
// caller must receive that status from the first (WebSocket) attempt: a
// SPDY retry would flatten it to a string, and the hook runner would charge
// a release attempt for a command that never started.
func TestExecKeepsAPIStatusWhenServerRejectsExec(t *testing.T) {
	podsResource := schema.GroupResource{Resource: "pods"}
	tests := map[string]struct {
		status *metav1.Status
		check  func(error) bool
	}{
		"400 container not running": {
			status: &apierrors.NewBadRequest("container \"mongod\" in pod \"mongo-1\" is waiting to start").ErrStatus,
			check:  apierrors.IsBadRequest,
		},
		"403 rolebinding removed": {
			status: &apierrors.NewForbidden(podsResource, "mongo-1", errors.New("no exec permission")).ErrStatus,
			check:  apierrors.IsForbidden,
		},
		"404 pod gone": {
			status: &apierrors.NewNotFound(podsResource, "mongo-1").ErrStatus,
			check:  apierrors.IsNotFound,
		},
		"500 kubelet unreachable": {
			status: &apierrors.NewInternalError(errors.New("error dialing backend: connection refused")).ErrStatus,
			check:  apierrors.IsInternalError,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				// The API server sends the status with its type metadata set;
				// the websocket transport only decodes a typed body.
				status := tc.status.DeepCopy()
				status.TypeMeta = metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(int(status.Code))
				require.NoError(t, json.NewEncoder(w).Encode(status))
			}))
			defer server.Close()

			err := newTestExecutor(t, server.URL).Exec(context.Background(), "default", "mongo-1", "mongod", []string{"true"})

			require.Error(t, err)
			assert.True(t, tc.check(err), "error should keep the API status: %v", err)
			assert.Equal(t, ExecFailureCouldNotStart, ClassifyExecError(err))
			assert.Equal(t, int32(1), requests.Load(), "a decoded status must not be retried over SPDY")
		})
	}
}

// TestExecFallsBackToSPDYWithoutAPIStatus keeps the kubectl behavior for a
// WebSocket connection rejected by something that does not speak
// metav1.Status, such as a proxy in front of the API server: the SPDY
// attempt still happens.
func TestExecFallsBackToSPDYWithoutAPIStatus(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "websocket not supported here", http.StatusNotFound)
	}))
	defer server.Close()

	err := newTestExecutor(t, server.URL).Exec(context.Background(), "default", "mongo-1", "mongod", []string{"true"})

	require.Error(t, err)
	var statusErr *apierrors.StatusError
	assert.False(t, errors.As(err, &statusErr))
	assert.Equal(t, int32(2), requests.Load(), "a non-status refusal should fall back to SPDY")
}

// TestExecDialFailureIsCouldNotStart covers the API server being unreachable
// at dial time on the WebSocket path, where the connect error arrives
// without the url.Error wrapper the SPDY path would add.
func TestExecDialFailureIsCouldNotStart(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	host := server.URL
	server.Close()

	err := newTestExecutor(t, host).Exec(context.Background(), "default", "mongo-1", "mongod", []string{"true"})

	require.Error(t, err)
	assert.Equal(t, ExecFailureCouldNotStart, ClassifyExecError(err), "error: %v", err)
}

func newTestExecutor(t *testing.T, host string) Executor {
	t.Helper()
	config := &rest.Config{Host: host}
	kubeClient, err := kubernetes.NewForConfig(config)
	require.NoError(t, err)
	return NewRemoteCommandExecutor(config, kubeClient)
}
