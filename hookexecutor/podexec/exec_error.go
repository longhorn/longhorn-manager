package podexec

import (
	"errors"
	"net"
	"net/url"

	"k8s.io/client-go/util/exec"
	"k8s.io/streaming/pkg/httpstream"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// ExecFailure reports whether a failed pods/exec could have run a process
// inside the pod. A command that never started may be retried freely; a
// command that may have run must count against a bounded attempt budget,
// because each retry can leave another orphaned process behind.
type ExecFailure int

const (
	// ExecFailureCouldNotStart means the request failed before reaching
	// the pod, so no process ran.
	ExecFailureCouldNotStart ExecFailure = iota
	// ExecFailureRanAndFailed means the process may have started, so a
	// retry is not free.
	ExecFailureRanAndFailed
)

func (f ExecFailure) String() string {
	switch f {
	case ExecFailureCouldNotStart:
		return "could-not-start"
	case ExecFailureRanAndFailed:
		return "ran-and-failed"
	default:
		return "unknown"
	}
}

// ClassifyExecError reports whether the command could have run in the pod.
// Errors that happen before the command starts, such as an API rejection or
// a connection failure, mean it could not start. Any other error, including
// an unrecognised one, means it may have run: assuming the command never
// started when it did would allow unbounded retries and leave orphaned
// processes behind.
func ClassifyExecError(err error) ExecFailure {
	if _, ok := errors.AsType[exec.CodeExitError](err); ok {
		return ExecFailureRanAndFailed
	}
	if _, ok := errors.AsType[*apierrors.StatusError](err); ok {
		return ExecFailureCouldNotStart
	}
	if httpstream.IsUpgradeFailure(err) {
		return ExecFailureCouldNotStart
	}
	if _, ok := errors.AsType[*url.Error](err); ok {
		return ExecFailureCouldNotStart
	}
	if opErr, ok := errors.AsType[*net.OpError](err); ok && opErr.Op == "dial" {
		return ExecFailureCouldNotStart
	}

	return ExecFailureRanAndFailed
}
