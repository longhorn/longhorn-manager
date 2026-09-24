package runner

import (
	"context"
	"errors"

	"k8s.io/client-go/util/exec"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// commandOutcome is the result of one exec, recorded in a pod's status
// entry.
type commandOutcome struct {
	// ok is true when the process exited zero.
	ok bool
	// exitCode is set when the process exited non-zero.
	exitCode *int32
	// reason names the failure. It is empty when ok.
	reason longhorn.HookReason
}

// classifyRunOutcome translates an exec error into a HookReason. It checks
// ctx as well as err, because the transport may swallow the context
// deadline. On timeout the stream is canceled, but the process may still
// be running in the pod.
func classifyRunOutcome(ctx context.Context, err error) commandOutcome {
	if err == nil {
		return commandOutcome{ok: true}
	}
	if exitErr, ok := errors.AsType[exec.CodeExitError](err); ok {
		code := int32(exitErr.Code)
		return commandOutcome{exitCode: &code, reason: longhorn.HookReasonExitNonZero}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return commandOutcome{reason: longhorn.HookReasonTimeout}
	}
	return commandOutcome{reason: longhorn.HookReasonExecFailed}
}
