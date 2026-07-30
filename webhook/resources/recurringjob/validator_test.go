package recurringjob

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// retainAge is a rolling window measured backwards from the moment the job
// runs, so a negative value would put the window boundary in the future and
// make every existing snapshot/backup count as expired. Admission has to be the
// place that stops it: by the time the recurring job pod reads the spec, the
// only signal left would be a mass deletion.
func TestValidateRetainAge(t *testing.T) {
	cases := []struct {
		name      string
		retainAge time.Duration
		expectErr bool
	}{
		{"zero disables age-based retention", 0, false},
		{"ten minutes", 10 * time.Minute, false},
		{"one day as hours", 24 * time.Hour, false},
		{"negative would expire everything", -1 * time.Second, true},
		{"large negative would expire everything", -8760 * time.Hour, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRetainAge(metav1.Duration{Duration: tc.retainAge})
			if tc.expectErr {
				assert.Error(t, err, "expected retainAge %v to be rejected", tc.retainAge)
				assert.Contains(t, err.Error(), RecurringJobErrRetainAgeNegative)
			} else {
				assert.NoError(t, err, "expected retainAge %v to be accepted", tc.retainAge)
			}
		})
	}
}

// The retention policy picks which field the job cleans up by, so a typo silently
// changes which snapshots survive. filterExpiredItems only special-cases "age-base"
// and treats everything else as "count-base", so an unrecognized value would quietly
// ignore the age window the user configured. The CRD enum catches this for
// RecurringJob CRs; this check is what produces a readable error and covers the same
// spec arriving through the validator.
//
// "age-base" also has to be paired with a positive retainAge. That job goes by the
// age alone, so a zero window means nothing ever expires — a recurring job that
// looks configured and never cleans up is worse than one that is rejected.
func TestValidateRetentionPolicy(t *testing.T) {
	cases := []struct {
		name      string
		policy    longhorn.RecurringJobRetentionPolicy
		retainAge time.Duration
		expectErr bool
	}{
		{"empty means the count-base default", "", 0, false},
		{"count-base", longhorn.RecurringJobRetentionPolicyCountBase, 0, false},
		{"count-base ignores an unset retainAge", longhorn.RecurringJobRetentionPolicyCountBase, 0, false},
		{"count-base tolerates a retainAge it will not read", longhorn.RecurringJobRetentionPolicyCountBase, time.Hour, false},
		{"age-base with a window", longhorn.RecurringJobRetentionPolicyAgeBase, 10 * time.Minute, false},
		{"age-base without a window never cleans up", longhorn.RecurringJobRetentionPolicyAgeBase, 0, true},
		{"age-base with a negative window", longhorn.RecurringJobRetentionPolicyAgeBase, -time.Hour, true},
		{"wrong case is not accepted", longhorn.RecurringJobRetentionPolicy("Age-Base"), time.Hour, true},
		{"underscore instead of dash", longhorn.RecurringJobRetentionPolicy("age_base"), time.Hour, true},
		{"whitespace is not trimmed away", longhorn.RecurringJobRetentionPolicy(" age-base"), time.Hour, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRetentionPolicy(tc.policy, metav1.Duration{Duration: tc.retainAge})
			if tc.expectErr {
				assert.Error(t, err, "expected retentionPolicy %q to be rejected", tc.policy)
				assert.Contains(t, err.Error(), "retentionPolicy")
			} else {
				assert.NoError(t, err, "expected retentionPolicy %q to be accepted", tc.policy)
			}
		})
	}
}
