package controller

import (
	. "gopkg.in/check.v1"

	"k8s.io/apimachinery/pkg/api/resource"

	corev1 "k8s.io/api/core/v1"
)

func (s *TestSuite) TestIsInstanceManagerResourcesSynced(c *C) {
	mustQ := func(s string) resource.Quantity { return resource.MustParse(s) }

	type testCase struct {
		override   *corev1.ResourceRequirements
		actual     corev1.ResourceRequirements
		wantSynced bool
	}
	testCases := map[string]testCase{
		"override nil, actual empty": {
			override:   nil,
			actual:     corev1.ResourceRequirements{},
			wantSynced: true,
		},
		"override nil, actual has CPU request only (today's default)": {
			override: nil,
			actual: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: mustQ("1")},
			},
			wantSynced: true,
		},
		"override nil, actual has hugepages-2Mi limit (ignored)": {
			override: nil,
			actual: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{corev1.ResourceName("hugepages-2Mi"): mustQ("2Gi")},
			},
			wantSynced: true,
		},
		"override nil, actual has stale CPU limit -> not synced": {
			override: nil,
			actual: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{corev1.ResourceCPU: mustQ("2")},
			},
			wantSynced: false,
		},
		"override nil, actual has stale memory limit -> not synced": {
			override: nil,
			actual: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{corev1.ResourceMemory: mustQ("2Gi")},
			},
			wantSynced: false,
		},
		"override matches actual exactly": {
			override: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    mustQ("1250m"),
					corev1.ResourceMemory: mustQ("512Mi"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    mustQ("2"),
					corev1.ResourceMemory: mustQ("2Gi"),
				},
			},
			actual: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    mustQ("1250m"),
					corev1.ResourceMemory: mustQ("512Mi"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    mustQ("2"),
					corev1.ResourceMemory: mustQ("2Gi"),
				},
			},
			wantSynced: true,
		},
		"override matches actual, actual also has hugepages-2Mi (ignored)": {
			override: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: mustQ("1")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: mustQ("2")},
			},
			actual: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: mustQ("1")},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:                   mustQ("2"),
					corev1.ResourceName("hugepages-2Mi"): mustQ("2Gi"),
				},
			},
			wantSynced: true,
		},
		"override CPU request mismatches actual": {
			override: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: mustQ("1")},
			},
			actual: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: mustQ("500m")},
			},
			wantSynced: false,
		},
		"override memory limit mismatches actual": {
			override: &corev1.ResourceRequirements{
				Limits: corev1.ResourceList{corev1.ResourceMemory: mustQ("2Gi")},
			},
			actual: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{corev1.ResourceMemory: mustQ("1Gi")},
			},
			wantSynced: false,
		},
		"override has CPU limit but actual has none -> not synced": {
			override: &corev1.ResourceRequirements{
				Limits: corev1.ResourceList{corev1.ResourceCPU: mustQ("2")},
			},
			actual:     corev1.ResourceRequirements{},
			wantSynced: false,
		},
	}
	for name, tc := range testCases {
		got := IsInstanceManagerResourcesSynced(tc.override, tc.actual)
		c.Assert(got, Equals, tc.wantSynced, Commentf("test case: %s", name))
	}
}
