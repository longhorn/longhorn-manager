package recurringjob

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// The retention policy picks which field the job cleans up by, so a typo silently
// changes which snapshots survive. filterExpiredItems only special-cases "age-based"
// and treats everything else as "count-based", so an unrecognized value would quietly
// ignore the age window the user configured. The CRD enum catches this for
// RecurringJob CRs; this check is what produces a readable error and covers the same
// spec arriving through the validator.
//
// "age-based" also has to be paired with a positive retainAge. That job goes by the
// age alone, so a zero window means nothing ever expires — a recurring job that
// looks configured and never cleans up is worse than one that is rejected.
func TestValidateRetentionPolicy(t *testing.T) {
	cases := []struct {
		name      string
		policy    longhorn.RecurringJobRetentionPolicy
		task      longhorn.RecurringJobType
		retain    int
		retainAge time.Duration
		expectErr bool
	}{
		{"count-based", longhorn.RecurringJobRetentionPolicyCountBased, longhorn.RecurringJobTypeSnapshot, 1, 0, false},
		{"count-based ignores an unset retainAge", longhorn.RecurringJobRetentionPolicyCountBased, longhorn.RecurringJobTypeSnapshot, 1, 0, false},
		{"count-based tolerates a retainAge it will not read", longhorn.RecurringJobRetentionPolicyCountBased, longhorn.RecurringJobTypeSnapshot, 1, time.Hour, false},
		{"age-based with a window", longhorn.RecurringJobRetentionPolicyAgeBased, longhorn.RecurringJobTypeSnapshot, 1, 10 * time.Minute, false},
		{"age-based with a zero window", longhorn.RecurringJobRetentionPolicyAgeBased, longhorn.RecurringJobTypeSnapshot, 1, 0, false},
		{"age-based with a negative window is rejected", longhorn.RecurringJobRetentionPolicyAgeBased, longhorn.RecurringJobTypeSnapshot, 1, -time.Hour, true},
		{"wrong case is not accepted", longhorn.RecurringJobRetentionPolicy("Age-Based"), longhorn.RecurringJobTypeSnapshot, 1, time.Hour, true},
		{"underscore instead of dash", longhorn.RecurringJobRetentionPolicy("age_based"), longhorn.RecurringJobTypeSnapshot, 1, time.Hour, true},
		{"whitespace is not trimmed away", longhorn.RecurringJobRetentionPolicy(" age-based"), longhorn.RecurringJobTypeSnapshot, 1, time.Hour, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRetentionPolicy(tc.policy, tc.task, tc.retain, metav1.Duration{Duration: tc.retainAge})
			if tc.expectErr {
				assert.Error(t, err, "expected retentionPolicy %q to be rejected", tc.policy)
			} else {
				assert.NoError(t, err, "expected retentionPolicy %q to be accepted", tc.policy)
			}
		})
	}
}

// A negative retainAge is nonsensical under any policy, so it is rejected before the
// policy is even considered.
func TestValidateRetentionPolicyRejectsNegativeRetainAge(t *testing.T) {
	err := validateRetentionPolicy(longhorn.RecurringJobRetentionPolicyAgeBased, longhorn.RecurringJobTypeSnapshot, 1, metav1.Duration{Duration: -time.Hour})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), RecurringJobErrRetainAgeNegative)
}
