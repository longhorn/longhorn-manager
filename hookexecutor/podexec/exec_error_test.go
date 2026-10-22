package podexec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/exec"
	"k8s.io/streaming/pkg/httpstream"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func TestClassifyExecError(t *testing.T) {
	podsResource := schema.GroupResource{Resource: "pods"}

	tests := map[string]struct {
		err  error
		want ExecFailure
	}{
		// Failures before the command starts: the API server refused or
		// could not reach the kubelet, so nothing ran in the pod.
		"forbidden from a removed rolebinding": {
			err:  apierrors.NewForbidden(podsResource, "mongo-1", errors.New("no exec permission")),
			want: ExecFailureCouldNotStart,
		},
		"pod not found": {
			err:  apierrors.NewNotFound(podsResource, "mongo-1"),
			want: ExecFailureCouldNotStart,
		},
		"api server could not dial the kubelet": {
			err:  apierrors.NewInternalError(errors.New("error dialing backend: dial tcp 10.0.0.5:10250: connect: connection refused")),
			want: ExecFailureCouldNotStart,
		},
		"container not running": {
			err:  apierrors.NewBadRequest("container \"mongod\" in pod \"mongo-1\" is waiting to start: ContainerCreating"),
			want: ExecFailureCouldNotStart,
		},
		"status error wrapped by the caller": {
			err:  fmt.Errorf("release exec: %w", apierrors.NewForbidden(podsResource, "mongo-1", nil)),
			want: ExecFailureCouldNotStart,
		},
		"connection rejected during the SPDY or websocket round trip": {
			err:  &httpstream.UpgradeFailureError{Cause: errors.New("unable to upgrade connection")},
			want: ExecFailureCouldNotStart,
		},
		"connection rejection wrapped by the caller": {
			err:  fmt.Errorf("release exec: %w", &httpstream.UpgradeFailureError{Cause: io.EOF}),
			want: ExecFailureCouldNotStart,
		},
		"url error from an unreachable api server": {
			err: &url.Error{
				Op:  "Post",
				URL: "https://10.43.0.1:443/api/v1/namespaces/default/pods/mongo-1/exec",
				Err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")},
			},
			want: ExecFailureCouldNotStart,
		},
		"url error carrying a context deadline from the connection round trip": {
			err:  &url.Error{Op: "Post", URL: "https://10.43.0.1:443/exec", Err: context.DeadlineExceeded},
			want: ExecFailureCouldNotStart,
		},
		"dial error from the websocket dialer": {
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")},
			want: ExecFailureCouldNotStart,
		},
		"dial error wrapped by the caller": {
			err:  fmt.Errorf("release exec: %w", &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "kubernetes.default"}}),
			want: ExecFailureCouldNotStart,
		},

		// Failures after the connection is established: a process may have
		// started, so a retry counts against the attempt budget.
		"command exited non-zero": {
			err:  exec.CodeExitError{Err: errors.New("command terminated with exit code 1"), Code: 1},
			want: ExecFailureRanAndFailed,
		},
		"exit error wrapped by the caller": {
			err:  fmt.Errorf("release exec: %w", exec.CodeExitError{Err: errors.New("exit 2"), Code: 2}),
			want: ExecFailureRanAndFailed,
		},
		"session cancelled at its timeout": {
			err:  context.DeadlineExceeded,
			want: ExecFailureRanAndFailed,
		},
		"session cancelled by a release trigger": {
			err:  context.Canceled,
			want: ExecFailureRanAndFailed,
		},
		"stream closed mid-session": {
			err:  io.ErrUnexpectedEOF,
			want: ExecFailureRanAndFailed,
		},
		"connection reset mid-session": {
			err:  fmt.Errorf("next reader: %w", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}),
			want: ExecFailureRanAndFailed,
		},
		"unrecognized error defaults to the bounded regime": {
			err:  errors.New("something the transports never return"),
			want: ExecFailureRanAndFailed,
		},
		"http error that is neither status nor url error": {
			err:  http.ErrServerClosed,
			want: ExecFailureRanAndFailed,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, ClassifyExecError(tc.err))
		})
	}
}

func TestExecFailureString(t *testing.T) {
	assert.Equal(t, "could-not-start", ExecFailureCouldNotStart.String())
	assert.Equal(t, "ran-and-failed", ExecFailureRanAndFailed.String())
	assert.Equal(t, "unknown", ExecFailure(42).String())
}
