package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"k8s.io/client-go/util/exec"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestClassifyRunOutcome(t *testing.T) {
	background := context.Background()

	assert.True(t, classifyRunOutcome(background, nil).ok)

	nonzero := classifyRunOutcome(background, exec.CodeExitError{Err: errors.New("exit 3"), Code: 3})
	assert.Equal(t, longhorn.HookReasonExitNonZero, nonzero.reason)
	assert.Equal(t, int32(3), *nonzero.exitCode)

	expired, cancel := context.WithDeadline(background, time.Now().Add(-time.Second))
	defer cancel()
	timeout := classifyRunOutcome(expired, errors.New("stream closed"))
	assert.Equal(t, longhorn.HookReasonTimeout, timeout.reason)
	assert.Equal(t, longhorn.HookReasonTimeout, classifyRunOutcome(background, context.DeadlineExceeded).reason)

	assert.Equal(t, longhorn.HookReasonExecFailed, classifyRunOutcome(background, errors.New("stream reset")).reason)
}
