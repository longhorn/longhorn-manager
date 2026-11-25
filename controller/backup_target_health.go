package controller

import (
	"sync"
	"time"
)

// Backup target health gate.
//
// A backup target that is unreachable or misconfigured used to be retried on
// every reconcile: each failed attempt logged the error and the work item was
// re-enqueued immediately, so the controllers kept hammering the target while it
// was down. See https://github.com/longhorn/longhorn/issues/13831 (volatile
// error content made every attempt look like a status change, so the controller
// never stopped re-enqueuing) and https://github.com/longhorn/longhorn/issues/14121
// (unbounded memory growth on every node during such a storm, up to node-wide
// OOM kills).
//
// The gate bounds the attempts instead: after a failure, the next attempt for
// that target is allowed only after an exponentially growing window capped at
// backupStoreRetryMaxInterval, and it is cleared as soon as the target answers
// successfully. Callers re-enqueue with that delay, which also means a target
// recovers when "Backupstore Poll Interval" is 0 - no poll timer runs in that
// case, so a failed target previously stayed unavailable until some unrelated
// event triggered a reconcile.
//
// The number of tracked targets is bounded so the gate itself cannot grow
// without limit.
const (
	backupStoreRetryMinInterval = 1 * time.Second
	backupStoreRetryMaxInterval = 5 * time.Minute

	maxTrackedBackupTargets = 1024
)

// targetHealthGate is safe for concurrent use.
type targetHealthGate struct {
	lock    sync.Mutex
	entries map[string]*targetFailure
}

type targetFailure struct {
	consecutiveFailures int
	nextAllowed         time.Time
}

func newTargetHealthGate() *targetHealthGate {
	return &targetHealthGate{entries: map[string]*targetFailure{}}
}

// Allow reports whether a backup store attempt for this target is due now.
func (g *targetHealthGate) Allow(target string, now time.Time) bool {
	g.lock.Lock()
	defer g.lock.Unlock()

	failure, ok := g.entries[target]
	if !ok {
		return true
	}
	return !now.Before(failure.nextAllowed)
}

// WaitTime returns how long an attempt still has to wait, and ok=false when an
// attempt is due now.
func (g *targetHealthGate) WaitTime(target string, now time.Time) (time.Duration, bool) {
	g.lock.Lock()
	defer g.lock.Unlock()

	failure, ok := g.entries[target]
	if !ok || !now.Before(failure.nextAllowed) {
		return 0, false
	}
	return failure.nextAllowed.Sub(now), true
}

// RecordFailure registers a failed attempt and returns how long the next attempt
// for this target should wait.
func (g *targetHealthGate) RecordFailure(target string, now time.Time) time.Duration {
	g.lock.Lock()
	defer g.lock.Unlock()

	failure, ok := g.entries[target]
	if !ok {
		// Bounded so this map cannot grow without limit, which was part of the
		// memory growth reported in https://github.com/longhorn/longhorn/issues/14121.
		// Backup targets are few in practice; this only guards against a pathological
		// number of them.
		if len(g.entries) >= maxTrackedBackupTargets {
			return backupStoreRetryMaxInterval
		}
		failure = &targetFailure{}
		g.entries[target] = failure
	}

	failure.consecutiveFailures++
	window := backupStoreRetryMinInterval
	for i := 1; i < failure.consecutiveFailures && window < backupStoreRetryMaxInterval; i++ {
		window *= 2
	}
	if window > backupStoreRetryMaxInterval {
		window = backupStoreRetryMaxInterval
	}
	failure.nextAllowed = now.Add(window)

	return window
}

// RecordSuccess clears the gate for a target that answered successfully, so the
// next attempt is immediate.
func (g *targetHealthGate) RecordSuccess(target string) {
	g.lock.Lock()
	defer g.lock.Unlock()
	delete(g.entries, target)
}

// Forget drops the bookkeeping for a target that no longer exists.
func (g *targetHealthGate) Forget(target string) {
	g.RecordSuccess(target)
}

// TrackedTargets is the number of targets the gate holds bookkeeping for.
func (g *targetHealthGate) TrackedTargets() int {
	g.lock.Lock()
	defer g.lock.Unlock()
	return len(g.entries)
}
