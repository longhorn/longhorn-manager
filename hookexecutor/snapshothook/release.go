package snapshothook

import "time"

// MaxReleaseBackoff is the longest wait between two release attempts on
// the same pod. It is a power of two, so the queue's doubling backoff
// keeps a predictable retry timeline. It is also short enough that a
// failing pod is retried about twice a minute.
const MaxReleaseBackoff = 32 * time.Second

// ReleaseAbandonTime returns when the group controller gives up waiting
// for paused pods to be released. It is computed from the group and the
// settings alone, so it holds even when the executor is gone.
func ReleaseAbandonTime(deadline, deletionTime time.Time, attempts, members int, timeout time.Duration) time.Time {
	start := deadline
	if deletionTime.After(start) {
		start = deletionTime
	}
	if attempts < 1 {
		attempts = 1
	}
	if members < 1 {
		members = 1
	}
	return start.Add(time.Duration(attempts)*time.Duration(members)*timeout + time.Duration(attempts-1)*MaxReleaseBackoff)
}
