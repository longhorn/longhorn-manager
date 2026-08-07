package recurringjob

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIsTransientPurgeRejection pins the matcher that decides whether a failed
// ActionSnapshotPurge aborts the recurring job or is skipped until the next
// run.
//
// This matters more than its size suggests. recurringJob() collects per-volume
// errors through an errgroup and RecurringJobCmd() turns any non-nil result
// into logrus.Fatal, so a single volume's error ends the whole sweep and every
// volume not yet processed is skipped for that tick. Matching too narrowly
// re-introduces that abort; matching too broadly silently swallows real purge
// failures. Both regressions stay invisible until snapshots have accumulated
// enough to fill a disk.
func TestIsTransientPurgeRejection(t *testing.T) {
	assert := assert.New(t)

	// The real message, as produced by the engine and surfaced through the
	// proxy. Captured verbatim from a v1.7.2 cluster.
	realWorld := errors.New(
		"Bad response statusCode [500]. Status [500 Internal Server Error]. " +
			"Body: [message=failed to purge snapshot: proxyServer=10.244.77.1:8501 " +
			"destination=10.244.77.1:18795: failed to purge snapshots: rpc error: " +
			"code = Unknown desc = tcp://10.244.76.157:10169: cannot purge snapshots " +
			"because tcp://10.244.76.157:10169 is rebuilding, code=Internal Server Error, " +
			"detail=] from [http://longhorn-backend:9500/v1/volumes/pvc-x?action=snapshotPurge]")
	assert.True(isTransientPurgeRejection(realWorld),
		"the engine's rebuild rejection must be treated as transient, or one "+
			"rebuilding volume aborts the entire recurring job")

	// Minimal form, in case the surrounding wrapping changes.
	assert.True(isTransientPurgeRejection(
		errors.New("cannot purge snapshots because tcp://10.0.0.1:10000 is rebuilding")))

	// Wrapped errors must still match -- callers may add context.
	assert.True(isTransientPurgeRejection(
		fmt.Errorf("purging volume pvc-abc: %w",
			errors.New("cannot purge snapshots because tcp://10.0.0.1:10000 is rebuilding"))))

	// Genuine failures must NOT be swallowed: these should still fail the job
	// loudly rather than be silently skipped on every run.
	assert.False(isTransientPurgeRejection(nil))
	assert.False(isTransientPurgeRejection(errors.New("timed out waiting for snapshot purge to complete")))
	assert.False(isTransientPurgeRejection(errors.New("volume pvc-x not found")))
	assert.False(isTransientPurgeRejection(errors.New("Bad response statusCode [500]")))
	assert.False(isTransientPurgeRejection(errors.New("failed to purge snapshots: connection refused")))
	// "rebuild" alone is not the guard condition -- only an in-progress
	// rebuild is, and the engine says "is rebuilding".
	assert.False(isTransientPurgeRejection(errors.New("replica rebuild scheduled")))
}
