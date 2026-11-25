package controller

import (
	"fmt"
	"testing"
	"time"
)

// Regression tests for https://github.com/longhorn/longhorn/issues/13831 and
// https://github.com/longhorn/longhorn/issues/14121.

func TestTargetHealthGateBacksOffExponentially(t *testing.T) {
	gate := newTargetHealthGate()
	now := time.Now()

	if !gate.Allow("default", now) {
		t.Fatal("the first attempt for an unknown target must be allowed")
	}

	window := gate.RecordFailure("default", now)
	if window != backupStoreRetryMinInterval {
		t.Fatalf("first failure window = %v, want %v", window, backupStoreRetryMinInterval)
	}

	if gate.Allow("default", now.Add(window/2)) {
		t.Fatalf("a target that just failed must not be retried after %v of a %v window", window/2, window)
	}
	if !gate.Allow("default", now.Add(window)) {
		t.Fatalf("target should be allowed again after %v", window)
	}

	windows := []time.Duration{}
	for i := 0; i < 10; i++ {
		windows = append(windows, gate.RecordFailure("default", now))
	}
	for i, w := range windows {
		if w > backupStoreRetryMaxInterval {
			t.Fatalf("window %v exceeded the cap %v", w, backupStoreRetryMaxInterval)
		}
		if i > 0 && windows[i-1] < backupStoreRetryMaxInterval && w <= windows[i-1] {
			t.Fatalf("window %d (%v) did not grow from %v", i, w, windows[i-1])
		}
	}
	t.Logf("windows for 10 consecutive failures: %v", windows)
	if windows[len(windows)-1] != backupStoreRetryMaxInterval {
		t.Fatalf("expected the window to reach the cap %v, got %v", backupStoreRetryMaxInterval, windows[len(windows)-1])
	}

	// A successful attempt clears the gate, so recovery is immediate.
	gate.RecordSuccess("default")
	if !gate.Allow("default", now) {
		t.Fatal("a target that answered successfully must be allowed again immediately")
	}
}

func TestTargetHealthGateTracksTargetsIndependently(t *testing.T) {
	gate := newTargetHealthGate()
	now := time.Now()

	gate.RecordFailure("target-a", now)
	if gate.Allow("target-a", now) {
		t.Fatal("target-a must be in backoff")
	}
	if !gate.Allow("target-b", now) {
		t.Fatal("a healthy target must not be affected by another target failing")
	}
}

// TestAttemptCountOverOneMinute is the before/after comparison for the reconcile
// storm: the same loop, once without a gate (re-enqueue immediately, which is
// what the controllers did) and once with it.
func TestAttemptCountOverOneMinute(t *testing.T) {
	const period = time.Minute

	// Before: every failed attempt re-enqueues immediately, so one attempt per
	// loop iteration regardless of how long the target has been failing.
	before := 0
	for elapsed := time.Duration(0); elapsed < period; elapsed += time.Millisecond {
		before++
	}

	// After: an attempt is only made when the gate allows it.
	gate := newTargetHealthGate()
	after := 0
	start := time.Now()
	elapsed := time.Duration(0)
	for elapsed < period {
		now := start.Add(elapsed)
		if gate.Allow("default", now) {
			after++
			// The next attempt is re-enqueued after the returned window.
			elapsed += gate.RecordFailure("default", now)
			continue
		}
		elapsed += time.Millisecond
	}

	t.Logf("attempts against one continuously failing target in 60s: before=%d, after=%d", before, after)
	if after >= before {
		t.Fatalf("gate did not bound the attempts: before=%d after=%d", before, after)
	}
	if after > 12 {
		t.Fatalf("expected at most ~12 gated attempts in 60s (1,2,4,8,16,32,64,... capped), got %d", after)
	}
	if after < 1 {
		t.Fatal("the gate must still allow an attempt, otherwise a target can never recover")
	}
}

// TestTargetHealthGateIsBounded guards against the gate itself growing without
// limit, which was part of https://github.com/longhorn/longhorn/issues/14121.
func TestTargetHealthGateIsBounded(t *testing.T) {
	gate := newTargetHealthGate()
	now := time.Now()

	for i := 0; i < maxTrackedBackupTargets+50; i++ {
		gate.RecordFailure(fmt.Sprintf("target-%d", i), now)
	}
	if gate.TrackedTargets() > maxTrackedBackupTargets {
		t.Fatalf("gate tracks %d targets, expected at most %d", gate.TrackedTargets(), maxTrackedBackupTargets)
	}

	gate.Forget("gone-target")
	if gate.TrackedTargets() == 0 {
		t.Fatal("forgetting an untracked target must not clear the whole gate")
	}
}
