package controller

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestExponentialBackoff(t *testing.T) {
	assert := assert.New(t)

	eb := NewExponentialBackoff(10)
	start := time.Now()
	runCount := 0
	backoffKey := "key"
	for time.Since(start) < time.Minute {
		if ok, _ := eb.CanRun(backoffKey); ok {
			runCount++
		}
	}
	// during the first 60 seconds CanRun should return true exactly 9 times:
	// 1. first attempt always true
	// 2. after 1 second backoff
	// 3. after 2 seconds backoff
	// 4. after 4 seconds backoff
	// 5. after 8 seconds backoff
	// 6. after 10 seconds backoff (max backoff is set to 10 seconds)
	// 7. after 10 seconds backoff
	// 8. after 10 seconds backoff
	// 9. after 10 seconds backoff
	// total time elapsed = 1 + 2 + 4 + 8 + 10 + 10 + 10 + 10 = 55 seconds
	assert.Equal(runCount, 9)

	// make sure entry is removed after staying inactive for long enough
	time.Sleep(3 * eb.maxBackoffInterval)
	eb.mu.Lock()
	_, exists := eb.interval[backoffKey]
	assert.False(exists)
	_, exists = eb.lastAttempt[backoffKey]
	assert.False(exists)
	eb.mu.Unlock()
}
