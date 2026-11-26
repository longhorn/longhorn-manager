package controller

import (
	"time"

	. "gopkg.in/check.v1"

	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (s *TestSuite) TestCalculateFailedReplicaReuseDelay(c *C) {
	now, err := util.ParseTime(getTestNow())
	c.Assert(err, IsNil)

	testCases := map[string]struct {
		failedAt     string
		healthyAt    string
		waitInterval time.Duration
		expected     time.Duration
		expectError  bool
	}{
		"recent previously healthy replica is protected": {
			failedAt:     now.Add(-2 * time.Minute).Format(time.RFC3339),
			healthyAt:    now.Add(-time.Hour).Format(time.RFC3339),
			waitInterval: 10 * time.Minute,
			expected:     8 * time.Minute,
		},
		"protection expires at the deadline": {
			failedAt:     now.Add(-10 * time.Minute).Format(time.RFC3339),
			healthyAt:    now.Add(-time.Hour).Format(time.RFC3339),
			waitInterval: 10 * time.Minute,
		},
		"replica without current healthy data is not protected": {
			failedAt:     now.Format(time.RFC3339),
			waitInterval: 10 * time.Minute,
		},
		"non-failed replica is not protected": {
			healthyAt:    now.Add(-time.Hour).Format(time.RFC3339),
			waitInterval: 10 * time.Minute,
		},
		"disabled wait interval does not protect replica": {
			failedAt:  now.Format(time.RFC3339),
			healthyAt: now.Add(-time.Hour).Format(time.RFC3339),
		},
		"invalid failed timestamp is rejected": {
			failedAt:     "invalid",
			healthyAt:    now.Add(-time.Hour).Format(time.RFC3339),
			waitInterval: 10 * time.Minute,
			expectError:  true,
		},
	}

	for name, tc := range testCases {
		c.Logf("testing failed replica reuse delay: %v", name)

		r := &longhorn.Replica{}
		r.Name = "test-replica"
		r.Spec.FailedAt = tc.failedAt
		r.Spec.HealthyAt = tc.healthyAt

		delay, err := calculateFailedReplicaReuseDelay(r, now, tc.waitInterval)
		if tc.expectError {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(delay, Equals, tc.expected)
	}
}
