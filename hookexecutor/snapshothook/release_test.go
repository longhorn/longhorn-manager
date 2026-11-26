package snapshothook

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReleaseAbandonTime(t *testing.T) {
	base := time.Date(2026, time.July, 30, 0, 0, 0, 0, time.UTC)
	deadline := base.Add(60 * time.Second)
	deletion := base.Add(time.Hour)

	cases := []struct {
		name     string
		deletion time.Time
		attempts int
		members  int
		timeout  time.Duration
		want     time.Time
	}{
		{
			name:     "deleted before the deadline: the deadline anchors the bound",
			deletion: base.Add(50 * time.Second),
			attempts: 3, members: 1, timeout: 60 * time.Second,
			want: deadline.Add(3*60*time.Second + 2*MaxReleaseBackoff),
		},
		{
			// Members are released one at a time, so each round is
			// members x timeout: two members whose post-commands all time
			// out need 3 x 2 x 60s plus the two backoffs before the third
			// attempt on the last member has finished.
			name:     "each round is members x timeout",
			deletion: base.Add(50 * time.Second),
			attempts: 3, members: 2, timeout: 60 * time.Second,
			want: deadline.Add(3*2*60*time.Second + 2*MaxReleaseBackoff),
		},
		{
			name:     "deleted long after the deadline: the deletion anchors it",
			deletion: deletion,
			attempts: 1, members: 1, timeout: 10 * time.Second,
			want: deletion.Add(10 * time.Second),
		},
		{
			name:     "zero attempt or member count still leaves one timeout of room",
			deletion: deletion,
			attempts: 0, members: 0, timeout: 10 * time.Second,
			want: deletion.Add(10 * time.Second),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, ReleaseAbandonTime(deadline, c.deletion, c.attempts, c.members, c.timeout))
		})
	}
}
